package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"stellar-explorer/worker/config"
	"stellar-explorer/worker/lake/laketest"
	"stellar-explorer/worker/sep54"
)

const envMainHelper = "WORKER_TEST_RUN_MAIN"

var testWD string

// TestMain doubles as the worker binary: with WORKER_TEST_RUN_MAIN set, the
// test binary runs main's command dispatch on its arguments, so the tests
// exercise the real CLI in a subprocess (signals, sd_notify, exit codes).
func TestMain(m *testing.M) {
	if os.Getenv(envMainHelper) != "" {
		args := os.Args[1:]
		for i, a := range args {
			if a == "--" {
				args = args[i+1:]
				break
			}
		}
		os.Exit(run(context.Background(), args, os.Stdout, os.Stderr))
	}
	testWD, _ = os.Getwd()
	os.Exit(m.Run())
}

func runCLI(args ...string) (code int, stdout, stderr string) {
	var out, errb bytes.Buffer
	code = run(context.Background(), args, &out, &errb)
	return code, out.String(), errb.String()
}

func TestCLIUsageAndVersion(t *testing.T) {
	if code, _, stderr := runCLI(); code != 2 || !strings.Contains(stderr, "usage: worker") {
		t.Fatalf("no args: %d %q", code, stderr)
	}
	if code, stdout, _ := runCLI("help"); code != 0 || !strings.Contains(stdout, "smoke-archive") {
		t.Fatalf("help: %d %q", code, stdout)
	}
	if code, stdout, _ := runCLI("version"); code != 0 || stdout != "worker dev\n" {
		t.Fatalf("version: %d %q", code, stdout)
	}
	if code, _, stderr := runCLI("nope"); code != 2 || !strings.Contains(stderr, `unknown command "nope"`) {
		t.Fatalf("unknown command: %d %q", code, stderr)
	}
	if code, _, _ := runCLI("lake", "--no-such-flag"); code != 2 {
		t.Fatalf("bad flag: %d", code)
	}
	if code, _, _ := runCLI("lake", "extra"); code != 2 {
		t.Fatalf("extra argument: %d", code)
	}
	if code, _, _ := runCLI("lake", "-h"); code != 0 {
		t.Fatalf("lake -h: %d", code)
	}
}

// Every subcommand exists and takes --config.
func TestCLISubcommands(t *testing.T) {
	want := []string{"lake", "ingest", "galexie-start", "watchdog", "gapcheck", "bootstrap", "migrate",
		"reindex", "dev-feed", "smoke-archive", "clickhouse-migrate", "clickhouse-init", "clickhouse-ingest", "clickhouse-check", "state-copy"}
	var got []string
	for _, c := range commands() {
		got = append(got, c.name)
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("subcommands %v, want %v", got, want)
	}
	args := map[string][]string{
		"galexie-start":      {"--out", "/run/x.env"},
		"bootstrap":          {"--history-only", "--at", "100"},
		"reindex":            {"--from", "2", "--to", "3", "--source", "archive", "--dry-run"},
		"dev-feed":           {"--start", "5", "--end", "9", "--speed", "2"},
		"smoke-archive":      {"--bucket", "scratch", "--delete-probe-only"},
		"clickhouse-migrate": {"--check"},
		"clickhouse-init":    {"--at", "100"},
		"clickhouse-check":   {"--from", "2", "--to", "3"},
		"state-copy":         {"--snapshot"},
	}
	for _, name := range want[1:] {
		// Every command is implemented: the flags parse and the missing
		// config is reported.
		code, _, stderr := runCLI(append([]string{name, "--config", "/nonexistent/worker.toml"}, args[name]...)...)
		if code != 1 || !strings.Contains(stderr, "/nonexistent/worker.toml") {
			t.Errorf("%s: exit %d %q", name, code, stderr)
		}
	}
}

func TestCLILakeConfigErrors(t *testing.T) {
	code, _, stderr := runCLI("lake", "--config", filepath.Join(t.TempDir(), "missing.toml"))
	if code != 1 || !strings.Contains(stderr, "missing.toml") {
		t.Fatalf("missing config: %d %q", code, stderr)
	}
	dir := t.TempDir()
	cfg := filepath.Join(dir, "worker.toml")
	writeFile(t, cfg, "[lake]\ntypo = 1\n", 0o640)
	if code, _, stderr := runCLI("lake", "--config", cfg); code != 1 || !strings.Contains(stderr, "unknown keys") {
		t.Fatalf("unknown key: %d %q", code, stderr)
	}
	// A readable config whose gateway.env is missing.
	writeFile(t, cfg, fmt.Sprintf("[credentials]\ngateway_env = %q\n", filepath.Join(dir, "gateway.env")), 0o640)
	if code, _, stderr := runCLI("lake", "--config", cfg); code != 1 || !strings.Contains(stderr, "gateway.env") {
		t.Fatalf("missing gateway.env: %d %q", code, stderr)
	}
}

func writeFile(t *testing.T, path, content string, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
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

// `worker lake` end to end in a subprocess with production options: config
// and gateway.env by path, READY=1 over NOTIFY_SOCKET once both ports
// accept, the control endpoint, and a clean exit on SIGTERM that releases a
// pending long-poll and sends STOPPING=1.
func TestCLILakeProcess(t *testing.T) {
	lakeDir := t.TempDir()
	for _, d := range []string{"s3", "state"} {
		if err := os.Mkdir(filepath.Join(lakeDir, d), 0o750); err != nil {
			t.Fatal(err)
		}
	}
	writeFile(t, filepath.Join(lakeDir, "state", "lake-id"), "vol-test\n", 0o640)
	etc := t.TempDir()
	gw := filepath.Join(etc, "gateway.env")
	writeFile(t, gw, "AWS_ACCESS_KEY_ID=cli-test\nAWS_SECRET_ACCESS_KEY=cli-test-secret-0123456789\nAWS_EC2_METADATA_DISABLED=true\n", 0o640)
	s3Addr, ctrlAddr := freeAddr(t), freeAddr(t)
	cfg := filepath.Join(etc, "worker.toml")
	writeFile(t, cfg, fmt.Sprintf(`[credentials]
gateway_env = %q
[lake]
dir = %q
s3_listen = %q
control_listen = %q
allow_unmounted = true
`, gw, lakeDir, s3Addr, ctrlAddr), 0o640)

	sockDir, err := os.MkdirTemp("/tmp", "sdn")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(sockDir)
	sock := filepath.Join(sockDir, "n.sock")
	conn, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: sock, Net: "unixgram"})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	notes := make(chan string, 16)
	go func() {
		buf := make([]byte, 1024)
		for {
			n, err := conn.Read(buf)
			if err != nil {
				close(notes)
				return
			}
			notes <- string(buf[:n])
		}
	}()

	cmd := exec.Command(os.Args[0], "-test.run=^$", "--", "lake", "--config", cfg)
	cmd.Env = append(os.Environ(), envMainHelper+"=1", "NOTIFY_SOCKET="+sock)
	stderr := &syncBuffer{}
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	finished := false
	defer func() {
		if !finished {
			_ = cmd.Process.Kill()
			<-exited
		}
	}()

	await := func(state string) {
		t.Helper()
		for {
			select {
			case n := <-notes:
				if n == state {
					return
				}
			case err := <-exited:
				finished = true
				t.Fatalf("worker lake exited before %s: %v\n%s", state, err, stderr.String())
			case <-time.After(30 * time.Second):
				t.Fatalf("no %s\n%s", state, stderr.String())
			}
		}
	}
	await("READY=1")
	for _, addr := range []string{s3Addr, ctrlAddr} {
		c, err := net.DialTimeout("tcp", addr, time.Second)
		if err != nil {
			t.Fatalf("READY but %s refuses connections: %v", addr, err)
		}
		c.Close()
	}
	resp, err := http.Get("http://" + ctrlAddr + "/v1/tip")
	if err != nil {
		t.Fatal(err)
	}
	var tip sep54.Tip
	err = json.NewDecoder(resp.Body).Decode(&tip)
	resp.Body.Close()
	if err != nil || tip.Tip != nil {
		t.Fatalf("empty lake tip: %+v %v", tip, err)
	}
	if b, err := os.ReadFile(filepath.Join(lakeDir, "state", "boot-id")); err != nil || len(bytes.TrimSpace(b)) == 0 {
		t.Fatalf("boot-id not recorded: %q %v", b, err)
	}

	// A long-poll pending at shutdown is released at once.
	polled := make(chan int, 1)
	go func() {
		resp, err := http.Get("http://" + ctrlAddr + "/v1/tip?after=0")
		if err != nil {
			polled <- -1
			return
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		polled <- resp.StatusCode
	}()
	time.Sleep(100 * time.Millisecond)
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case code := <-polled:
		if code != http.StatusOK {
			t.Fatalf("released long-poll status %d", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SIGTERM did not release the long-poll")
	}
	await("STOPPING=1")
	select {
	case err := <-exited:
		finished = true
		if err != nil {
			t.Fatalf("worker lake exit: %v\n%s", err, stderr.String())
		}
	case <-time.After(30 * time.Second):
		t.Fatal("worker lake did not exit after SIGTERM")
	}
	if !strings.Contains(stderr.String(), "lake stopped") {
		t.Fatalf("no clean-stop log:\n%s", stderr.String())
	}
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

func TestCLIWarnsAboutAWSEnv(t *testing.T) {
	t.Setenv("AWS_SECRET_ACCESS_KEY", "should-not-be-here")
	dir := t.TempDir()
	cfg := filepath.Join(dir, "worker.toml")
	writeFile(t, cfg, fmt.Sprintf("[credentials]\ngateway_env = %q\n", filepath.Join(dir, "gateway.env")), 0o640)
	_, _, stderr := runCLI("lake", "--config", cfg)
	if !strings.Contains(stderr, "AWS_SECRET_ACCESS_KEY") || strings.Contains(stderr, "should-not-be-here") {
		t.Fatalf("expected a warning naming (not revealing) the AWS variable:\n%s", stderr)
	}
}

// writeWorkerConfig writes a worker.toml (plus the env files it names) for
// a lake at lakeCfg; extra is appended verbatim.
func writeWorkerConfig(t *testing.T, lakeCfg config.Lake, creds config.GatewayCredentials, extra string) string {
	t.Helper()
	etc := t.TempDir()
	gw := filepath.Join(etc, "gateway.env")
	writeFile(t, gw, fmt.Sprintf("AWS_ACCESS_KEY_ID=%s\nAWS_SECRET_ACCESS_KEY=%s\nAWS_EC2_METADATA_DISABLED=true\n", creds.AccessKey, creds.SecretKey), 0o640)
	archiveEnv := filepath.Join(etc, "archive.env")
	writeFile(t, archiveEnv, fmt.Sprintf("ARCHIVE_S3_ENDPOINT=http://%s\nARCHIVE_S3_REGION=%s\nARCHIVE_S3_ACCESS_KEY=%s\nARCHIVE_S3_SECRET_KEY=%s\n",
		lakeCfg.S3Listen, lakeCfg.Region, creds.AccessKey, creds.SecretKey), 0o640)
	cfg := filepath.Join(etc, "worker.toml")
	writeFile(t, cfg, fmt.Sprintf(`[credentials]
gateway_env = %q
archive_env = %q
[metrics]
listen = %q
network_tip = false
[lake]
dir = %q
bucket_path = %q
s3_listen = %q
control_listen = %q
allow_unmounted = true
%s`, gw, archiveEnv, freeAddr(t), lakeCfg.Dir, lakeCfg.BucketPath, lakeCfg.S3Listen, lakeCfg.ControlListen, extra), 0o640)
	return cfg
}

func TestCLIDevFeedRefusals(t *testing.T) {
	lakeCfg := config.Default().Lake
	lakeCfg.Dir = t.TempDir()
	lakeCfg.S3Listen, lakeCfg.ControlListen = freeAddr(t), freeAddr(t)
	creds := config.GatewayCredentials{AccessKey: "k", SecretKey: "secret-0123456789"}

	if code, _, stderr := runCLI("dev-feed", "--config", "/nonexistent.toml"); code != 2 || !strings.Contains(stderr, "--start is required") {
		t.Fatalf("no --start: %d %q", code, stderr)
	}
	if code, _, _ := runCLI("dev-feed", "--start", "4294967296"); code != 2 {
		t.Fatalf("--start overflow: %d", code)
	}
	// The default config archives to the real bucket: refused.
	cfg := writeWorkerConfig(t, lakeCfg, creds, "")
	if code, _, stderr := runCLI("dev-feed", "--config", cfg, "--start", "100"); code != 1 || !strings.Contains(stderr, "refusing to run: archiving is enabled") {
		t.Fatalf("archive enabled: %d %q", code, stderr)
	}
	cfg = writeWorkerConfig(t, lakeCfg, creds, "[archive]\nbucket = \"stellar-ledgers-pubnet-staging\"\n")
	if code, _, stderr := runCLI("dev-feed", "--config", cfg, "--start", "100"); code != 1 || !strings.Contains(stderr, "refusing") {
		t.Fatalf("staging bucket: %d %q", code, stderr)
	}
	// A -dev bucket passes the check; nothing listens on the lake port.
	cfg = writeWorkerConfig(t, lakeCfg, creds, "[archive]\nbucket = \"stellar-ledgers-pubnet-dev\"\n")
	if code, _, stderr := runCLI("dev-feed", "--config", cfg, "--start", "100"); code != 1 || !strings.Contains(stderr, "lake") || strings.Contains(stderr, "refusing") {
		t.Fatalf("lake down: %d %q", code, stderr)
	}
}

func TestCLISmokeArchive(t *testing.T) {
	if code, _, stderr := runCLI("smoke-archive", "--config", "/nonexistent.toml"); code != 2 || !strings.Contains(stderr, "--bucket is required") {
		t.Fatalf("no --bucket: %d %q", code, stderr)
	}
	l := laketest.Start(t, laketest.Options{})
	cfg := writeWorkerConfig(t, l.Cfg, l.Creds, "[archive]\nbucket = \"stellar\"\npath_style = true\n")
	// Never the archive bucket itself...
	if code, _, stderr := runCLI("smoke-archive", "--config", cfg, "--bucket", "stellar"); code != 1 || !strings.Contains(stderr, "refusing to run against the archive bucket") {
		t.Fatalf("archive bucket: %d %q", code, stderr)
	}
	// ...except for the delete probe. The lake's root key may delete, so it fails.
	code, stdout, _ := runCLI("smoke-archive", "--config", cfg, "--bucket", "stellar", "--delete-probe-only")
	if code != 1 || !strings.Contains(stdout, "FAIL  delete-missing-denied") || strings.Count(stdout, "\n") != 1 {
		t.Fatalf("probe: %d %q", code, stdout)
	}
	// The full run against the lake as a stand-in provider: only the
	// delete-policy checks fail.
	cfg = writeWorkerConfig(t, l.Cfg, l.Creds, "[archive]\nenabled = false\nbucket = \"stellar-ledgers-pubnet\"\n")
	code, stdout, stderr := runCLI("smoke-archive", "--config", cfg, "--bucket", "stellar")
	t.Logf("smoke-archive against the lake stand-in:\n%s", stdout)
	if code != 1 || strings.Count(stdout, "PASS  ") != 11 || strings.Count(stdout, "FAIL  ") != 2 ||
		!strings.Contains(stderr, "2 of 13 checks failed: delete-denied, delete-missing-denied") {
		t.Fatalf("smoke-archive: %d\n%s\n%s", code, stdout, stderr)
	}
}

// Opt-in (network): `worker dev-feed` copies the newest AWS-lake ledgers
// into a lake at real ledger cadence, following the AWS tip.
//
//	WORKER_TEST_LIVE_AWS=1
func TestCLIDevFeedLiveAWS(t *testing.T) {
	if os.Getenv("WORKER_TEST_LIVE_AWS") == "" {
		t.Skip("set WORKER_TEST_LIVE_AWS=1 to run dev-feed against the AWS public lake (network)")
	}
	ctx := context.Background()
	aws, err := sep54.OpenAWSLake(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tip, err := aws.Newest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	l := laketest.Start(t, laketest.Options{})
	cfg := writeWorkerConfig(t, l.Cfg, l.Creds, "[archive]\nenabled = false\n")
	start, end := tip-2, tip+3 // three ledgers behind the tip, three not closed yet
	tc := sep54.NewTipClient(l.ControlAddr())
	type commit struct {
		seq uint32
		at  time.Time
	}
	commits := make(chan commit, 16)
	go func() {
		for after := start - 1; after < end; {
			t2, err := tc.WaitAbove(ctx, after, time.Minute)
			if err != nil {
				return
			}
			for s := after + 1; s <= *t2.Tip; s++ {
				commits <- commit{s, time.Now()}
			}
			after = *t2.Tip
		}
		close(commits)
	}()
	began := time.Now()
	code, _, stderr := runCLI("dev-feed", "--config", cfg, "--start", fmt.Sprint(start), "--end", fmt.Sprint(end), "--speed", "1")
	if code != 0 {
		t.Fatalf("dev-feed exit %d\n%s", code, stderr)
	}
	var lines []string
	for c := range commits {
		lines = append(lines, fmt.Sprintf("%d at +%v", c.seq, c.at.Sub(began).Round(time.Millisecond)))
	}
	t.Logf("dev-feed %d-%d (AWS tip was %d) finished in %v; lake commits: %s", start, end, tip, time.Since(began).Round(time.Millisecond), strings.Join(lines, ", "))
	if len(lines) != int(end-start+1) {
		t.Fatalf("saw %d commits", len(lines))
	}
	src, err := sep54.OpenLake(ctx, l.Cfg, l.Creds)
	if err != nil {
		t.Fatal(err)
	}
	var prev sep54.Header
	for seq := start; seq <= end; seq++ {
		got, err := src.GetLedger(ctx, seq)
		if err != nil {
			t.Fatal(err)
		}
		want, err := aws.GetLedger(ctx, seq)
		if err != nil {
			t.Fatal(err)
		}
		wantMD, _ := want.LedgerMetadata()
		if !bytes.Equal(got.Body, want.Body) || !maps.Equal(got.Metadata, wantMD) {
			t.Fatalf("ledger %d differs from the AWS lake", seq)
		}
		_, h, err := sep54.DecodeLedger(seq, got.Body)
		if err != nil {
			t.Fatal(err)
		}
		if seq > start {
			if err := sep54.CheckChain(prev, h); err != nil {
				t.Fatal(err)
			}
		}
		prev = h
	}
}

// No worker code may build an S3 client from the ambient AWS configuration
// (environment, profiles, instance metadata): every source has an explicit
// client with its own credentials file. The nested galexieclient mirrors
// Galexie and is exempt.
func TestNoAmbientAWSConfig(t *testing.T) {
	banned := []string{"LoadDefault" + "Config(", "NewS3" + "DataStore(", "datastore.New" + "DataStore("}
	err := filepath.WalkDir(testWD, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && (d.Name() == "testdata" || d.Name() == "bin") {
			return filepath.SkipDir
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, pat := range banned {
			if bytes.Contains(b, []byte(pat)) {
				t.Errorf("%s calls %s", path, pat)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
