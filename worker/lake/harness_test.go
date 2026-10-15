package lake

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"stellar-explorer/worker/config"
	"stellar-explorer/worker/sep54"
)

// Test credentials, shared by the lake (via gateway.env) and the Galexie
// client (via AWS_* variables, like `docker run --env-file`).
const (
	testAccessKey  = "lake-test-access"
	testSecretKey  = "lake-test-secret-0123456789abcdef"
	testBucketPath = "stellar/ledgers/pubnet"
	testBucket     = "stellar"
	testPrefix     = "ledgers/pubnet"

	// Subprocess lake (helperMain) settings, passed through the environment.
	envHelperConfig = "LAKE_TEST_HELPER_CONFIG"
	envHelperBootID = "LAKE_TEST_HELPER_BOOT_ID"
	envHelperKillAt = "LAKE_TEST_HELPER_KILL_AFTER_LINK"
)

var (
	// testWD is the package directory. posix.New changes the working
	// directory of the whole process, so everything else uses absolute paths.
	testWD  string
	binDir  string
	gcOnce  sync.Once
	gcPath  string
	gcErr   error
	gcBuild time.Duration
)

func TestMain(m *testing.M) {
	if os.Getenv(envHelperConfig) != "" {
		os.Exit(helperMain())
	}
	var err error
	if testWD, err = os.Getwd(); err != nil {
		panic(err)
	}
	if binDir, err = os.MkdirTemp("", "lake-test-bin-"); err != nil {
		panic(err)
	}
	code := m.Run()
	_ = os.Chdir(testWD)
	_ = os.RemoveAll(binDir)
	os.Exit(code)
}

// galexieClient builds testdata/galexieclient (a nested module with
// Galexie's exact go.mod/go.sum) once per test binary.
func galexieClient(t *testing.T) string {
	t.Helper()
	gcOnce.Do(func() {
		goBin, err := exec.LookPath("go")
		if err != nil {
			goBin = filepath.Join(runtime.GOROOT(), "bin", "go")
		}
		start := time.Now()
		gcPath = filepath.Join(binDir, "galexieclient")
		cmd := exec.Command(goBin, "build", "-o", gcPath, ".")
		cmd.Dir = filepath.Join(testWD, "testdata", "galexieclient")
		cmd.Env = append(os.Environ(), "GOWORK=off", "GOFLAGS=-mod=readonly", "CGO_ENABLED=0")
		if out, err := cmd.CombinedOutput(); err != nil {
			gcErr = fmt.Errorf("build galexieclient: %v\n%s", err, out)
		}
		gcBuild = time.Since(start)
	})
	if gcErr != nil {
		t.Fatal(gcErr)
	}
	return gcPath
}

// ---------------------------------------------------------------- lake dirs

// newLakeDir creates a lake directory like lab galexie does:
// s3/ and state/ with the lake-id marker.
func newLakeDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for _, d := range []string{"s3", "state"} {
		if err := os.Mkdir(filepath.Join(dir, d), 0o750); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "state", markerFile), []byte("test-volume\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	return dir
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func testLakeConfig(t *testing.T, dir string) config.Lake {
	cfg := config.Default().Lake
	cfg.Dir = dir
	cfg.BucketPath = testBucketPath
	cfg.S3Listen = freeAddr(t)
	cfg.ControlListen = freeAddr(t)
	cfg.AllowUnmounted = true // t.TempDir() is not a mountpoint; the marker is still required
	return cfg
}

var testCreds = config.GatewayCredentials{AccessKey: testAccessKey, SecretKey: testSecretKey}

// ---------------------------------------------------------------- fsync hook

type fsyncEvent struct {
	stage Stage
	path  string
}

// fsyncRecorder records every commit-path fsync and can inject failures.
type fsyncRecorder struct {
	mu     sync.Mutex
	events []fsyncEvent
	inject func(stage Stage, path string) error
}

func (r *fsyncRecorder) hook(stage Stage, f *os.File) error {
	r.mu.Lock()
	r.events = append(r.events, fsyncEvent{stage, f.Name()})
	inject := r.inject
	r.mu.Unlock()
	if inject != nil {
		if err := inject(stage, f.Name()); err != nil {
			return err
		}
	}
	return f.Sync()
}

func (r *fsyncRecorder) setInject(fn func(Stage, string) error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.inject = fn
}

func (r *fsyncRecorder) snapshot() []fsyncEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]fsyncEvent(nil), r.events...)
}

func (r *fsyncRecorder) count(stage Stage, path string) int {
	n := 0
	for _, e := range r.snapshot() {
		if e.stage == stage && e.path == path {
			n++
		}
	}
	return n
}

// ---------------------------------------------------------------- in-process lake

type lakeOpts struct {
	dir      string // existing lake dir (a restart); empty creates one
	cfg      *config.Lake
	syncfs   func(string) error
	bootID   string
	longPoll time.Duration
	onReady  func(tl *testLake)
}

type testLake struct {
	t       *testing.T
	dir     string
	cfg     config.Lake
	l       *Lake
	fsync   *fsyncRecorder
	s3URL   string
	ctrlURL string
	cancel  context.CancelFunc
	done    chan error
	mu      sync.Mutex
	notes   []string
	stopped bool
	logs    *syncBuffer
	readyCh chan struct{}
}

func startLake(t *testing.T, o lakeOpts) *testLake {
	t.Helper()
	tl, err := openLake(t, o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	tl.serve(o.onReady)
	return tl
}

func openLake(t *testing.T, o lakeOpts) (*testLake, error) {
	t.Helper()
	dir := o.dir
	if dir == "" {
		dir = newLakeDir(t)
	}
	cfg := testLakeConfig(t, dir)
	if o.cfg != nil {
		cfg = *o.cfg
	}
	bootID := o.bootID
	if bootID == "" {
		bootID = "boot-1"
	}
	tl := &testLake{t: t, dir: dir, cfg: cfg, fsync: &fsyncRecorder{}, logs: &syncBuffer{},
		s3URL: "http://" + cfg.S3Listen, ctrlURL: "http://" + cfg.ControlListen}
	ready := make(chan struct{})
	opts := Options{
		Logger: slog.New(slog.NewTextHandler(io.MultiWriter(tl.logs, testWriter{t}), &slog.HandlerOptions{Level: slog.LevelDebug})),
		Fsync:  tl.fsync.hook,
		Syncfs: o.syncfs,
		BootID: func() (string, error) { return bootID, nil },
		Notify: func(state string) error {
			tl.mu.Lock()
			tl.notes = append(tl.notes, state)
			tl.mu.Unlock()
			if state == "READY=1" {
				// READY must mean both listeners accept connections.
				for _, addr := range []string{cfg.S3Listen, cfg.ControlListen} {
					c, err := net.DialTimeout("tcp", addr, time.Second)
					if err != nil {
						t.Errorf("READY sent but %s does not accept connections: %v", addr, err)
						continue
					}
					c.Close()
				}
				close(ready)
			}
			return nil
		},
		LongPoll: o.longPoll,
	}
	l, err := Open(cfg, testCreds, opts)
	if err != nil {
		_ = os.Chdir(testWD)
		return nil, err
	}
	tl.l = l
	tl.done = make(chan error, 1)
	t.Cleanup(func() { tl.stop() })
	tl.readyCh = ready
	return tl, nil
}

func (tl *testLake) serve(onReady func(*testLake)) {
	tl.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	tl.cancel = cancel
	go func() { tl.done <- tl.l.Serve(ctx) }()
	select {
	case <-tl.readyCh:
	case err := <-tl.done:
		tl.stopped = true
		tl.t.Fatalf("Serve returned before READY: %v", err)
	case <-time.After(15 * time.Second):
		tl.t.Fatal("lake not READY within 15s")
	}
	if onReady != nil {
		onReady(tl)
	}
}

// stop shuts the lake down (like SIGTERM) and returns Serve's error.
func (tl *testLake) stop() error {
	if tl.stopped || tl.cancel == nil {
		return nil
	}
	tl.stopped = true
	tl.cancel()
	defer os.Chdir(testWD)
	select {
	case err := <-tl.done:
		return err
	case <-time.After(30 * time.Second):
		tl.t.Error("lake did not stop within 30s")
		return errors.New("timeout")
	}
}

func (tl *testLake) notifications() []string {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	return append([]string(nil), tl.notes...)
}

func (tl *testLake) objectPath(seq uint32) string {
	return filepath.Join(tl.dir, "s3", testBucket, testPrefix, sep54.PartitionName(seq), sep54.FileName(seq))
}

// testWriter forwards log lines to t.Log.
type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Helper()
	w.t.Log(strings.TrimRight(string(p), "\n"))
	return len(p), nil
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// ---------------------------------------------------------------- control endpoint

func getTip(t *testing.T, ctrlURL string) sep54.Tip {
	t.Helper()
	resp, err := http.Get(ctrlURL + "/v1/tip")
	if err != nil {
		t.Fatalf("GET /v1/tip: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/tip: %s", resp.Status)
	}
	var tip sep54.Tip
	if err := json.NewDecoder(resp.Body).Decode(&tip); err != nil {
		t.Fatal(err)
	}
	return tip
}

func requireTip(t *testing.T, ctrlURL string, tip, oldest uint32) {
	t.Helper()
	got := getTip(t, ctrlURL)
	if got.Tip == nil || got.Oldest == nil || got.TipCommittedAt == nil {
		t.Fatalf("tip = %s, want tip %d oldest %d", fmtTip(got), tip, oldest)
	}
	if *got.Tip != tip || *got.Oldest != oldest {
		t.Fatalf("tip = %s, want tip %d oldest %d", fmtTip(got), tip, oldest)
	}
}

func requireEmptyTip(t *testing.T, ctrlURL string) {
	t.Helper()
	if got := getTip(t, ctrlURL); got.Tip != nil || got.Oldest != nil || got.TipCommittedAt != nil {
		t.Fatalf("tip = %s, want all null", fmtTip(got))
	}
}

func fmtTip(t sep54.Tip) string {
	f := func(p *uint32) string {
		if p == nil {
			return "null"
		}
		return strconv.FormatUint(uint64(*p), 10)
	}
	return fmt.Sprintf("{tip:%s oldest:%s committed_at_set:%v}", f(t.Tip), f(t.Oldest), t.TipCommittedAt != nil)
}

func httpGet(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// metricValue returns the value of one series from a /metrics page, e.g.
// `lake_puts_total{result="exists"}`; ok is false if it is absent.
func metricValue(t *testing.T, baseURL, series string) (float64, bool) {
	t.Helper()
	code, body := httpGet(t, baseURL+"/metrics")
	if code != http.StatusOK {
		t.Fatalf("/metrics: %d", code)
	}
	sc := bufio.NewScanner(strings.NewReader(body))
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "#") {
			continue
		}
		name, val, ok := strings.Cut(line, " ")
		if ok && name == series {
			v, err := strconv.ParseFloat(val, 64)
			if err != nil {
				t.Fatalf("metric %s: %v", line, err)
			}
			return v, true
		}
	}
	return 0, false
}

func requireMetric(t *testing.T, baseURL, series string, want float64) {
	t.Helper()
	got, ok := metricValue(t, baseURL, series)
	if !ok || got != want {
		t.Fatalf("metric %s = %v (present %v), want %v", series, got, ok, want)
	}
}

// ---------------------------------------------------------------- Galexie client

type putResult struct {
	Seq      uint32            `json:"seq"`
	Key      string            `json:"key"`
	Uploaded bool              `json:"uploaded"`
	Err      string            `json:"err"`
	SHA256   string            `json:"sha256"`
	Size     int               `json:"size"`
	Hash     string            `json:"hash"`
	Meta     map[string]string `json:"meta"`
	NotFound bool              `json:"not_found"`
	Fatal    string            `json:"fatal"`
}

// runClient runs the Galexie client against endpoint and returns its JSON
// output lines and exit code.
func runClient(t *testing.T, endpoint string, args ...string) ([]json.RawMessage, int) {
	t.Helper()
	bin := galexieClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	full := append([]string{args[0], "--endpoint", endpoint, "--bucket-path", testBucketPath}, args[1:]...)
	cmd := exec.CommandContext(ctx, bin, full...)
	home := t.TempDir()
	cmd.Dir = home
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + home,
		"AWS_ACCESS_KEY_ID=" + testAccessKey,
		"AWS_SECRET_ACCESS_KEY=" + testSecretKey,
		"AWS_EC2_METADATA_DISABLED=true",
		"AWS_CONFIG_FILE=/dev/null",
		"AWS_SHARED_CREDENTIALS_FILE=/dev/null",
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	code := 0
	var ee *exec.ExitError
	switch {
	case errors.As(err, &ee):
		code = ee.ExitCode()
	case err != nil:
		t.Fatalf("galexieclient %v: %v\n%s", args, err, stderr.String())
	}
	var lines []json.RawMessage
	for _, l := range bytes.Split(bytes.TrimSpace(out), []byte("\n")) {
		if len(l) > 0 {
			lines = append(lines, json.RawMessage(l))
		}
	}
	if stderr.Len() > 0 {
		t.Logf("galexieclient %v stderr: %s", args, stderr.String())
	}
	return lines, code
}

func decode[T any](t *testing.T, raw json.RawMessage) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	return v
}

// put uploads [from, to] like Galexie and returns one result per attempted
// ledger (the client stops at the first error, like Galexie).
func put(t *testing.T, endpoint string, from, to uint32) []putResult {
	t.Helper()
	lines, _ := runClient(t, endpoint, "put", "--from", fmt.Sprint(from), "--to", fmt.Sprint(to))
	var res []putResult
	for _, l := range lines {
		r := decode[putResult](t, l)
		if r.Fatal != "" {
			t.Fatalf("galexieclient put: %s", r.Fatal)
		}
		res = append(res, r)
	}
	return res
}

// mustPut uploads [from, to] and requires every ledger to be newly uploaded.
func mustPut(t *testing.T, endpoint string, from, to uint32) []putResult {
	t.Helper()
	res := put(t, endpoint, from, to)
	if len(res) != int(to-from+1) {
		t.Fatalf("put %d..%d: %d results: %+v", from, to, len(res), res)
	}
	for _, r := range res {
		if !r.Uploaded || r.Err != "" {
			t.Fatalf("put %d: uploaded=%v err=%q", r.Seq, r.Uploaded, r.Err)
		}
	}
	return res
}

func putOne(t *testing.T, endpoint string, seq uint32) putResult {
	t.Helper()
	res := put(t, endpoint, seq, seq)
	if len(res) != 1 {
		t.Fatalf("put %d: %d results", seq, len(res))
	}
	return res[0]
}

func headObject(t *testing.T, endpoint, key string) putResult {
	t.Helper()
	lines, _ := runClient(t, endpoint, "head", "--key", key)
	if len(lines) != 1 {
		t.Fatalf("head: %d lines", len(lines))
	}
	return decode[putResult](t, lines[0])
}

func getObject(t *testing.T, endpoint, key string) putResult {
	t.Helper()
	lines, _ := runClient(t, endpoint, "get", "--key", key)
	if len(lines) != 1 {
		t.Fatalf("get: %d lines", len(lines))
	}
	return decode[putResult](t, lines[0])
}

// ---------------------------------------------------------------- raw S3 client

// s3Client is the worker's own S3 client stack (used by the pruner later).
func s3Client(endpoint string) *s3.Client {
	return s3.New(s3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(endpoint),
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider(testAccessKey, testSecretKey, ""),
	})
}

// ---------------------------------------------------------------- subprocess lake

// helperMain runs a lake in a subprocess of the test binary, configured
// from a TOML file like `worker lake`, optionally killing itself with
// SIGKILL right after linking a given ledger (before any fsync of it).
func helperMain() int {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	cfg, err := config.Load(os.Getenv(envHelperConfig))
	if err != nil {
		log.Error("config", "err", err)
		return 1
	}
	creds, err := config.LoadGatewayEnv(cfg.Credentials.GatewayEnv)
	if err != nil {
		log.Error("gateway env", "err", err)
		return 1
	}
	opts := Options{Logger: log, BootID: func() (string, error) { return os.Getenv(envHelperBootID), nil }}
	if s := os.Getenv(envHelperKillAt); s != "" {
		seq, err := strconv.ParseUint(s, 10, 32)
		if err != nil {
			log.Error("kill-at", "err", err)
			return 1
		}
		name := sep54.FileName(uint32(seq))
		opts.Fsync = func(stage Stage, f *os.File) error {
			if stage == StagePublished && filepath.Base(f.Name()) == name {
				log.Info("killing the lake between link and fsync", "ledger", seq)
				_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
				select {}
			}
			return f.Sync()
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	l, err := Open(cfg.Lake, creds, opts)
	if err != nil {
		log.Error("open", "err", err)
		return 1
	}
	if err := l.Serve(ctx); err != nil {
		log.Error("serve", "err", err)
		return 1
	}
	return 0
}

// writeLakeConfig writes worker.toml and gateway.env for a subprocess lake.
func writeLakeConfig(t *testing.T, dir string) (path string, cfg config.Lake) {
	t.Helper()
	cfg = testLakeConfig(t, dir)
	etc := t.TempDir()
	gw := filepath.Join(etc, "gateway.env")
	if err := os.WriteFile(gw, []byte("AWS_ACCESS_KEY_ID="+testAccessKey+"\nAWS_SECRET_ACCESS_KEY="+testSecretKey+
		"\nAWS_EC2_METADATA_DISABLED=true\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	toml := fmt.Sprintf(`log_level = "debug"
[credentials]
gateway_env = %q
archive_env = %q
[lake]
dir = %q
bucket_path = %q
s3_listen = %q
control_listen = %q
allow_unmounted = true
`, gw, filepath.Join(etc, "archive.env"), dir, testBucketPath, cfg.S3Listen, cfg.ControlListen)
	path = filepath.Join(etc, "worker.toml")
	if err := os.WriteFile(path, []byte(toml), 0o640); err != nil {
		t.Fatal(err)
	}
	return path, cfg
}

// notifySocket listens on a unixgram socket like systemd's NOTIFY_SOCKET.
type notifySocket struct {
	path  string
	conn  *net.UnixConn
	msgs  chan string
	close func()
}

func newNotifySocket(t *testing.T) *notifySocket {
	t.Helper()
	// unix socket paths are limited to ~104 bytes on macOS: keep it short.
	dir, err := os.MkdirTemp("/tmp", "sdn")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "notify.sock")
	conn, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: path, Net: "unixgram"})
	if err != nil {
		t.Fatal(err)
	}
	ns := &notifySocket{path: path, conn: conn, msgs: make(chan string, 64)}
	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := conn.Read(buf)
			if err != nil {
				close(ns.msgs)
				return
			}
			for _, line := range strings.Split(strings.TrimSpace(string(buf[:n])), "\n") {
				ns.msgs <- line
			}
		}
	}()
	t.Cleanup(func() { conn.Close(); os.RemoveAll(dir) })
	return ns
}

// await waits for state, failing the test on timeout or if exited closes first.
func (ns *notifySocket) await(t *testing.T, state string, timeout time.Duration, exited <-chan struct{}) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case m, ok := <-ns.msgs:
			if !ok {
				t.Fatalf("notify socket closed before %s", state)
			}
			if m == state {
				return
			}
		case <-exited:
			t.Fatalf("process exited before sending %s", state)
		case <-deadline:
			t.Fatalf("no %s within %v", state, timeout)
		}
	}
}

type lakeProcess struct {
	cmd    *exec.Cmd
	stderr *syncBuffer
	notify *notifySocket
	done   chan struct{} // closed when the process has exited
	err    error         // Wait's result, valid after done
}

// startLakeProcess starts helperMain in a subprocess and waits for READY=1
// on its NOTIFY_SOCKET.
func startLakeProcess(t *testing.T, configPath, bootID string, env ...string) *lakeProcess {
	t.Helper()
	ns := newNotifySocket(t)
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(), envHelperConfig+"="+configPath, envHelperBootID+"="+bootID, "NOTIFY_SOCKET="+ns.path)
	cmd.Env = append(cmd.Env, env...)
	p := &lakeProcess{cmd: cmd, stderr: &syncBuffer{}, notify: ns, done: make(chan struct{})}
	cmd.Stderr = io.MultiWriter(p.stderr, testWriter{t})
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { p.err = cmd.Wait(); close(p.done) }()
	t.Cleanup(func() {
		select {
		case <-p.done:
		default:
			_ = cmd.Process.Kill()
			<-p.done
		}
	})
	ns.await(t, "READY=1", 30*time.Second, p.done)
	return p
}

// wait waits for the process to exit and returns Wait's error.
func (p *lakeProcess) wait(t *testing.T, timeout time.Duration) error {
	t.Helper()
	select {
	case <-p.done:
		return p.err
	case <-time.After(timeout):
		t.Fatalf("lake process did not exit within %v", timeout)
		return nil
	}
}
