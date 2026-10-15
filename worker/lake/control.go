package lake

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// privateRetry is how often the private metrics listener retries its bind:
// the private interface can come up after the lake starts.
const privateRetry = 2 * time.Second

type controlServer struct {
	srv  *http.Server
	done chan error
}

// listenControl binds the loopback control listener (/v1/tip, /metrics,
// /healthz) and serves it in the background.
func (l *Lake) listenControl() (*controlServer, error) {
	ln, err := net.Listen("tcp", l.cfg.ControlListen)
	if err != nil {
		return nil, fmt.Errorf("control listener: %w", err)
	}
	c := &controlServer{srv: l.newHTTPServer(l.handler(true)), done: make(chan error, 1)}
	go func() {
		err := c.srv.Serve(ln)
		if errors.Is(err, http.ErrServerClosed) {
			return
		}
		c.done <- err
	}()
	return c, nil
}

func (l *Lake) newHTTPServer(h http.Handler) *http.Server {
	return &http.Server{
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      l.opts.LongPoll + 10*time.Second,
		IdleTimeout:       2 * time.Minute,
	}
}

// handler serves /metrics and /healthz, plus /v1/tip on the loopback
// listener only.
func (l *Lake) handler(withTip bool) http.Handler {
	mux := http.NewServeMux()
	if withTip {
		mux.HandleFunc("GET /v1/tip", l.handleTip)
	}
	mux.Handle("GET /metrics", promhttp.HandlerFor(l.m.reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /healthz", l.handleHealthz)
	return mux
}

// handleTip answers GET /v1/tip at once, and GET /v1/tip?after=N as soon as
// the tip exceeds N, or with the current values after the long-poll timeout
// or at shutdown.
func (l *Lake) handleTip(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if !q.Has("after") {
		writeJSON(w, l.idx.snapshot())
		return
	}
	after, err := strconv.ParseUint(q.Get("after"), 10, 32)
	if err != nil {
		http.Error(w, "after must be a ledger sequence (uint32)", http.StatusBadRequest)
		return
	}
	writeJSON(w, l.idx.wait(r.Context(), uint32(after), l.opts.LongPoll))
}

// writeJSON writes v as an uncacheable JSON response.
func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(v)
}

func (l *Lake) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	switch {
	case l.failed():
		http.Error(w, "fail-closed: an fsync failed after a link; every PUT is refused until the lake is restarted", http.StatusServiceUnavailable)
	case l.stopping.Load():
		http.Error(w, "stopping", http.StatusServiceUnavailable)
	case !l.ready.Load():
		http.Error(w, "starting", http.StatusServiceUnavailable)
	default:
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	}
}

// servePrivate binds the private metrics listener, retrying until it
// succeeds or ctx ends, then serves until srv is shut down.
func (l *Lake) servePrivate(ctx context.Context, srv *http.Server) {
	addr := l.cfg.MetricsListen
	for attempt := 0; ; attempt++ {
		ln, err := net.Listen("tcp", addr)
		if err == nil {
			l.log.Info("private metrics listener bound", "addr", addr)
			if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
				l.log.Error("private metrics listener failed", "addr", addr, "err", err)
			}
			return
		}
		if attempt == 0 {
			l.log.Warn("cannot bind the private metrics listener yet; retrying in the background", "addr", addr, "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(privateRetry):
		}
	}
}
