// Package laketest runs a real, in-process lake for the tests of other
// packages (dev-feed, the archive smoke test, the CLI).
//
// Only one lake can run per process at a time (embedgw keeps package state,
// and posix.New changes the working directory of the whole process), so
// tests that use it must not run in parallel. Start restores the working
// directory when the lake stops.
package laketest

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"stellar-explorer/worker/config"
	"stellar-explorer/worker/lake"
)

// Credentials are the gateway root keys of every test lake.
var Credentials = config.GatewayCredentials{AccessKey: "laketest-access", SecretKey: "laketest-secret-0123456789abcdef"}

// Options configure a test lake. The zero value is a fresh, empty lake.
type Options struct {
	// Dir reuses an existing lake directory (a restart); empty creates one.
	Dir string
	// BucketPath defaults to the production "stellar/ledgers/pubnet".
	BucketPath string
	// LongPoll bounds GET /v1/tip?after=N (0: the lake's 30 s).
	LongPoll time.Duration
	// S3Listen and ControlListen pin the addresses (a restart on the same
	// ports); empty picks free ones.
	S3Listen, ControlListen string
}

// Lake is a running test lake.
type Lake struct {
	Dir   string
	Cfg   config.Lake
	Creds config.GatewayCredentials

	t       testing.TB
	wd      string
	cancel  context.CancelFunc
	done    chan error
	stopped atomic.Bool
	logs    *logWriter
}

// NewDir creates a lake directory like lab galexie does: s3/ and state/
// with the lake-id marker.
func NewDir(t testing.TB) string {
	t.Helper()
	dir := t.TempDir()
	for _, d := range []string{"s3", "state"} {
		if err := os.Mkdir(filepath.Join(dir, d), 0o750); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "state", "lake-id"), []byte("laketest\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	return dir
}

// FreeAddr returns a free loopback address.
func FreeAddr(t testing.TB) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

// Start opens and serves a lake and waits until it is READY. The lake is
// stopped when the test ends.
func Start(t testing.TB, o Options) *Lake {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	dir := o.Dir
	if dir == "" {
		dir = NewDir(t)
	}
	cfg := config.Default().Lake
	cfg.Dir = dir
	if o.BucketPath != "" {
		cfg.BucketPath = o.BucketPath
	}
	cfg.S3Listen, cfg.ControlListen = o.S3Listen, o.ControlListen
	if cfg.S3Listen == "" {
		cfg.S3Listen = FreeAddr(t)
	}
	if cfg.ControlListen == "" {
		cfg.ControlListen = FreeAddr(t)
	}
	cfg.AllowUnmounted = true // t.TempDir() is not a mountpoint; the marker is still required

	l := &Lake{Dir: dir, Cfg: cfg, Creds: Credentials, t: t, wd: wd, done: make(chan error, 1), logs: &logWriter{t: t}}
	ready := make(chan struct{})
	var readyOnce sync.Once
	lk, err := lake.Open(cfg, Credentials, lake.Options{
		Logger: slog.New(slog.NewTextHandler(l.logs, &slog.HandlerOptions{Level: slog.LevelInfo})),
		BootID: func() (string, error) { return "laketest-boot", nil },
		Notify: func(state string) error {
			if state == "READY=1" {
				readyOnce.Do(func() { close(ready) })
			}
			return nil
		},
		LongPoll: o.LongPoll,
	})
	if err != nil {
		_ = os.Chdir(wd)
		t.Fatalf("lake.Open: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	l.cancel = cancel
	go func() { l.done <- lk.Serve(ctx) }()
	t.Cleanup(func() { _ = l.Stop() })
	select {
	case <-ready:
	case err := <-l.done:
		l.stopped.Store(true)
		_ = os.Chdir(wd)
		t.Fatalf("lake stopped before READY: %v", err)
	case <-time.After(20 * time.Second):
		t.Fatal("lake not READY within 20s")
	}
	return l
}

// Restart stops the lake and starts it again on the same directory and
// ports, like `systemctl restart stellar-lake`.
func (l *Lake) Restart(t testing.TB, o Options) *Lake {
	t.Helper()
	if err := l.Stop(); err != nil {
		t.Fatalf("stop lake: %v", err)
	}
	o.Dir, o.BucketPath = l.Dir, l.Cfg.BucketPath
	o.S3Listen, o.ControlListen = l.Cfg.S3Listen, l.Cfg.ControlListen
	return Start(t, o)
}

// S3Endpoint is the lake's S3 URL.
func (l *Lake) S3Endpoint() string { return "http://" + l.Cfg.S3Listen }

// ControlAddr is the lake's control listener address (for sep54.NewTipClient).
func (l *Lake) ControlAddr() string { return l.Cfg.ControlListen }

// ObjectPath is the on-disk path of ledger object key rel (relative to the
// lake prefix).
func (l *Lake) ObjectPath(rel string) string {
	return filepath.Join(l.Dir, "s3", l.Cfg.Bucket(), filepath.FromSlash(l.Cfg.Prefix()), filepath.FromSlash(rel))
}

// Stop shuts the lake down like SIGTERM and restores the working directory.
// It is idempotent.
func (l *Lake) Stop() error {
	if !l.stopped.CompareAndSwap(false, true) {
		return nil
	}
	defer os.Chdir(l.wd)
	defer l.logs.close()
	l.cancel()
	select {
	case err := <-l.done:
		return err
	case <-time.After(30 * time.Second):
		l.t.Error("lake did not stop within 30s")
		return fmt.Errorf("lake did not stop")
	}
}

// logWriter forwards lake logs to t.Log until the lake stops.
type logWriter struct {
	t      testing.TB
	mu     sync.Mutex
	closed bool
}

func (w *logWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.closed {
		w.t.Log(strings.TrimRight(string(p), "\n"))
	}
	return len(p), nil
}

func (w *logWriter) close() {
	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()
}

var _ io.Writer = (*logWriter)(nil)
