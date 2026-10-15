package metrics

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"stellar-explorer/worker/sep54"
)

// Health are the checks of /readyz: the lake is reachable, Postgres is
// reachable, and the explorer pipeline's lag is under the threshold.
type Health struct {
	LakeTip interface {
		Get(ctx context.Context) (sep54.Tip, error)
	}
	// Database pings the database (nil: not checked).
	Database func(ctx context.Context) error
	// Cursor is the pipeline's cursor, if known.
	Cursor func() (uint32, bool)
	// MaxLag is the largest tip − cursor that is still ready.
	MaxLag uint32
	// The names in the failures: what the tip is read from, the database
	// and the pipeline (defaults lake, postgres, explorer; the clickhouse
	// pipeline follows the archive and writes to ClickHouse).
	TipName, DatabaseName, Pipeline string
}

// Check runs the readiness checks and returns the failures.
func (h Health) Check(ctx context.Context) []string {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var failed []string
	name := func(v, def string) string {
		if v == "" {
			return def
		}
		return v
	}
	tipName, db, pipeline := name(h.TipName, "lake"), name(h.DatabaseName, "postgres"), name(h.Pipeline, "explorer")
	tip, err := h.LakeTip.Get(ctx)
	if err != nil {
		failed = append(failed, tipName+": "+err.Error())
	}
	if h.Database != nil {
		if err := h.Database(ctx); err != nil {
			failed = append(failed, db+": "+err.Error())
		}
	}
	cur, known := h.Cursor()
	switch {
	case !known:
		failed = append(failed, fmt.Sprintf("%s: cursor unknown (ingest is starting, or %s is unreachable)", pipeline, db))
	case err == nil && tip.Tip != nil && *tip.Tip > cur && *tip.Tip-cur > h.MaxLag:
		failed = append(failed, fmt.Sprintf("%s: %d ledgers behind the %s tip (limit %d)", pipeline, *tip.Tip-cur, tipName, h.MaxLag))
	}
	return failed
}

// Handler serves /metrics, /healthz (the process is alive) and /readyz.
func Handler(m *Ingest, h Health) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(m.Reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		if failed := h.Check(r.Context()); len(failed) > 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(strings.Join(failed, "\n") + "\n"))
			return
		}
		_, _ = w.Write([]byte("ready\n"))
	})
	return mux
}

// privateRetry is how often the private listener retries its bind.
const privateRetry = 2 * time.Second

// Server is a running metrics endpoint.
type Server struct {
	done chan error
}

// Wait blocks until the server has stopped and returns its error.
func (s *Server) Wait() error { return <-s.done }

// Start binds listen (loopback; an error is returned at once) and, if set,
// private in the background (retried: the private interface can come up
// late), then serves h until ctx ends.
func Start(ctx context.Context, listen, private string, h http.Handler, log *slog.Logger) (*Server, error) {
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, fmt.Errorf("metrics listener: %w", err)
	}
	log.Info("metrics listener bound", "addr", listen)
	newServer := func() *http.Server {
		return &http.Server{Handler: h, ReadHeaderTimeout: 5 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 2 * time.Minute}
	}
	srv, psrv := newServer(), newServer()
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
		_ = psrv.Shutdown(sctx)
	}()
	if private != "" {
		go func() {
			for attempt := 0; ; attempt++ {
				pln, err := net.Listen("tcp", private)
				if err == nil {
					log.Info("private metrics listener bound", "addr", private)
					if err := psrv.Serve(pln); err != nil && !errors.Is(err, http.ErrServerClosed) {
						log.Error("private metrics listener failed", "addr", private, "err", err)
					}
					return
				}
				if attempt == 0 {
					log.Warn("cannot bind the private metrics listener yet; retrying in the background", "addr", private, "err", err)
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(privateRetry):
				}
			}
		}()
	}
	s := &Server{done: make(chan error, 1)}
	go func() {
		err := srv.Serve(ln)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		s.done <- err
	}()
	return s, nil
}
