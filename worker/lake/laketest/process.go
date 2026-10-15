package laketest

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"stellar-explorer/worker/config"
	"stellar-explorer/worker/lake"
)

// A second lake in a subprocess: only one lake can run per process, so a
// test that needs two S3 endpoints (the archiver copies one lake into
// another, the stand-in archive) runs the second one by re-executing the
// test binary. The package's TestMain must call Main.

// processEnv carries the subprocess lake's spec.
const processEnv = "LAKETEST_PROCESS_SPEC"

type processSpec struct {
	Cfg   config.Lake               `json:"cfg"`
	Creds config.GatewayCredentials `json:"creds"`
}

// Main is a TestMain for packages that use StartProcess: it runs a lake if
// this process was started by StartProcess, and the tests otherwise.
func Main(m *testing.M) {
	if spec := os.Getenv(processEnv); spec != "" {
		os.Exit(runProcess(spec))
	}
	os.Exit(m.Run())
}

func runProcess(spec string) int {
	var s processSpec
	if err := json.Unmarshal([]byte(spec), &s); err != nil {
		fmt.Fprintln(os.Stderr, "laketest process:", err)
		return 2
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	l, err := lake.Open(s.Cfg, s.Creds, lake.Options{
		Logger: log,
		BootID: func() (string, error) { return "laketest-boot", nil },
		Notify: func(string) error { return nil },
	})
	if err != nil {
		log.Error("open", "err", err)
		return 1
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	if err := l.Serve(ctx); err != nil {
		log.Error("serve", "err", err)
		return 1
	}
	return 0
}

// Process is a lake running in a subprocess.
type Process struct {
	Dir   string
	Cfg   config.Lake
	Creds config.GatewayCredentials

	cmd  *exec.Cmd
	done chan struct{}
	err  error
	once sync.Once
}

// StartProcess runs a lake in a subprocess and waits until /healthz
// answers. It is stopped (SIGTERM) when the test ends.
func StartProcess(t testing.TB, o Options) *Process {
	t.Helper()
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
	cfg.AllowUnmounted = true
	spec, err := json.Marshal(processSpec{Cfg: cfg, Creds: Credentials})
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(), processEnv+"="+string(spec))
	cmd.Stderr = &prefixWriter{t: t, prefix: "lake process: "}
	p := &Process{Dir: dir, Cfg: cfg, Creds: Credentials, cmd: cmd, done: make(chan struct{})}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { p.err = cmd.Wait(); close(p.done) }()
	t.Cleanup(p.Stop)

	deadline := time.Now().Add(30 * time.Second)
	hc := &http.Client{Timeout: time.Second}
	for {
		resp, err := hc.Get("http://" + cfg.ControlListen + "/healthz")
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return p
			}
		}
		select {
		case <-p.done:
			t.Fatalf("lake process exited before it was ready: %v", p.err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("lake process not ready within 30s")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// S3Endpoint is the lake's S3 URL.
func (p *Process) S3Endpoint() string { return "http://" + p.Cfg.S3Listen }

// ObjectPath is the on-disk path of key rel (relative to the lake prefix).
func (p *Process) ObjectPath(rel string) string {
	return filepath.Join(p.Dir, "s3", p.Cfg.Bucket(), filepath.FromSlash(p.Cfg.Prefix()), filepath.FromSlash(rel))
}

// Stop shuts the lake down with SIGTERM and waits. It is idempotent.
func (p *Process) Stop() {
	p.once.Do(func() {
		_ = p.cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-p.done:
		case <-time.After(30 * time.Second):
			_ = p.cmd.Process.Kill()
			<-p.done
		}
	})
}

// prefixWriter forwards a subprocess's stderr to t.Log until the test ends.
type prefixWriter struct {
	t      testing.TB
	prefix string
	mu     sync.Mutex
}

func (w *prefixWriter) Write(b []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	defer func() { _ = recover() }() // t.Log after the test ended
	for _, line := range strings.Split(strings.TrimRight(string(b), "\n"), "\n") {
		w.t.Log(w.prefix + line)
	}
	return len(b), nil
}
