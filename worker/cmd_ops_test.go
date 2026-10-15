package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"

	"stellar-explorer/worker/archive"
	"stellar-explorer/worker/bootstrap"
	"stellar-explorer/worker/explorer"
	"stellar-explorer/worker/explorer/explorertest"
	"stellar-explorer/worker/lake/laketest"
	"stellar-explorer/worker/sep54"
	"stellar-explorer/worker/sep54/sep54test"
	"stellar-explorer/worker/watchdog"
)

// sdkDir runs in the package directory: a test lake changes the working
// directory of the whole process.
var sdkDir = sync.OnceValues(func() (string, error) {
	cmd := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", "github.com/stellar/go-stellar-sdk")
	cmd.Dir = testWD
	out, err := cmd.Output()
	return strings.TrimSpace(string(out)), err
})

// futurenetArchive is the real futurenet history archive shipped with the
// SDK (see bootstrap/checkpoint_test.go).
func futurenetArchive(t *testing.T) string {
	t.Helper()
	dir, err := sdkDir()
	if err != nil || dir == "" {
		t.Fatalf("locate the SDK module: %v", err)
	}
	return filepath.Join(dir, "historyarchive/testdata/futurenet-2025-12-10-last-100")
}

// lakeAsArchive configures the lake itself as the archive (same bucket and
// prefix): enough for the commands that only read the archive.
func lakeAsArchive(l *laketest.Lake) string {
	return fmt.Sprintf("[archive]\nenabled = true\nbucket = %q\npath_style = true\n", l.Cfg.Bucket())
}

func TestCLIGalexieStart(t *testing.T) {
	l := laketest.Start(t, laketest.Options{})
	out := filepath.Join(t.TempDir(), "galexie.env")
	disabled := writeWorkerConfig(t, l.Cfg, l.Creds, "[archive]\nenabled = false\n")

	if code, _, _ := runCLI("galexie-start", "--config", disabled); code != 2 {
		t.Fatalf("without --out: %d", code)
	}
	code, _, stderr := runCLI("galexie-start", "--config", disabled, "--out", out)
	if code != 1 || !strings.Contains(stderr, "run `worker bootstrap` first") {
		t.Fatalf("empty lake, no origin: %d %s", code, stderr)
	}
	if _, err := os.Stat(out); !os.IsNotExist(err) {
		t.Fatalf("START written on a refusal: %v", err)
	}
	if _, _, err := bootstrap.WriteOriginIfAbsent(l.Dir, bootstrap.NewOrigin(network.PublicNetworkPassphrase, 30_000_063, xdr.Hash{1})); err != nil {
		t.Fatal(err)
	}
	code, stdout, stderr := runCLI("galexie-start", "--config", disabled, "--out", out)
	if b, _ := os.ReadFile(out); code != 0 || string(b) != "START=30000064\n" || !strings.Contains(stdout, "archive_origin") {
		t.Fatalf("first start: %d %q %s %s", code, b, stdout, stderr)
	}
	// With ledgers in the lake (and the lake standing in for the archive).
	feedLake(t, l, 30_000_064, 30_000_070)
	code, stdout, stderr = runCLI("galexie-start", "--config", writeWorkerConfig(t, l.Cfg, l.Creds, lakeAsArchive(l)), "--out", out)
	if b, _ := os.ReadFile(out); code != 0 || string(b) != "START=30000071\n" {
		t.Fatalf("resume: %d %q %s %s", code, b, stdout, stderr)
	}
}

func feedLake(t *testing.T, l *laketest.Lake, from, to uint32) {
	t.Helper()
	ctx := context.Background()
	src, err := sep54.OpenLake(ctx, l.Cfg, l.Creds)
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range sep54test.Chain(from, int(to-from+1)) {
		if _, err := src.DataStore().PutFileIfNotExists(ctx, o.Key, strings.NewReader(string(o.Body)), o.Metadata); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := src.DataStore().PutFileIfNotExists(ctx, sep54.ManifestFile, strings.NewReader(string(sep54.ManifestBytes(network.PublicNetworkPassphrase))), nil); err != nil {
		t.Fatal(err)
	}
}

// The watchdog command, wired: on a host without the Galexie unit (or
// without systemd) it does nothing and records its state.
func TestCLIWatchdog(t *testing.T) {
	l := laketest.Start(t, laketest.Options{})
	gx := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, "galexie_export_manager_latest_ledger 10\ngalexie_uploader_latest_ledger 10\n")
	}))
	defer gx.Close()
	state := filepath.Join(t.TempDir(), "state.json")
	cfg := writeWorkerConfig(t, l.Cfg, l.Creds, fmt.Sprintf("[galexie]\nmetrics_url = %q\n[watchdog]\nstate_file = %q\nnetwork_tip = false\n",
		gx.URL+"/metrics", state))
	code, stdout, stderr := runCLI("watchdog", "--config", cfg)
	if code != 0 || strings.TrimSpace(stdout) != watchdog.DecisionInactive {
		t.Fatalf("watchdog: %d %q %s", code, stdout, stderr)
	}
	st, err := watchdog.ReadState(state)
	if err != nil || st.LastRun.IsZero() || st.GalexieActive {
		t.Fatalf("state %+v %v", st, err)
	}
}

// The gap check command (with the dry-run method, against the lake
// standing in for the archive): clean, then a hole.
func TestCLIGapcheck(t *testing.T) {
	l := laketest.Start(t, laketest.Options{})
	result := filepath.Join(t.TempDir(), "result.json")
	if code, _, stderr := runCLI("gapcheck", "--config", writeWorkerConfig(t, l.Cfg, l.Creds, "[archive]\nenabled = false\n")); code != 1 || !strings.Contains(stderr, "disabled") {
		t.Fatalf("archive disabled: %d %s", code, stderr)
	}
	feedLake(t, l, 31_000_000, 31_000_009)
	first := sep54test.Chain(31_000_000, 1)[0]
	h, err := sep54.DecodeHeader(31_000_000, first.Body)
	if err != nil {
		t.Fatal(err)
	}
	origin := bootstrap.NewOrigin(network.PublicNetworkPassphrase, 31_000_000, h.Hash)
	if _, _, err := bootstrap.WriteOriginIfAbsent(l.Dir, origin); err != nil {
		t.Fatal(err)
	}
	cfg := writeWorkerConfig(t, l.Cfg, l.Creds, lakeAsArchive(l)+fmt.Sprintf("[gapcheck]\nmethod = \"reindex-dry-run\"\nresult_file = %q\n", result))
	code, stdout, stderr := runCLI("gapcheck", "--config", cfg)
	if code != 0 || !strings.Contains(stdout, "archive 31000001-31000009 has no gaps") {
		t.Fatalf("clean: %d %s %s", code, stdout, stderr)
	}
	src, err := sep54.OpenLake(context.Background(), l.Cfg, l.Creds)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = src.Client().DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(src.Bucket()),
		Key: aws.String(src.Key(sep54.ObjectPath(31_000_004)))}); err != nil {
		t.Fatal(err)
	}
	code, stdout, stderr = runCLI("gapcheck", "--config", cfg)
	if code != 1 || !strings.Contains(stdout, "gap 31000004-31000004") {
		t.Fatalf("hole: %d %s %s", code, stdout, stderr)
	}
	rep, err := archive.ReadGapReport(result)
	if err != nil || rep.Missing != 1 || rep.Method != "reindex-dry-run" {
		t.Fatalf("result file %+v %v", rep, err)
	}
}

// Usage errors of the bootstrap flags.
func TestCLIBootstrapFlags(t *testing.T) {
	for _, args := range [][]string{
		{"--force", "--history-only", "--at", "5"},
		{"--force", "--recreate-origin"},
		{"--at", "5"},
		{"--history-only"},
	} {
		if code, _, stderr := runCLI(append([]string{"bootstrap", "--config", "/nonexistent.toml"}, args...)...); code != 2 {
			t.Errorf("%v: %d %s", args, code, stderr)
		}
	}
}

// `bootstrap --recreate-origin` rebuilds a lost bootstrap.json from
// Postgres: after `--history-only --at A`, archive_origin is A.
func TestCLIRecreateOrigin(t *testing.T) {
	e := newPipelineEnv(t, 64_800_000, 3)
	if code, stdout, stderr := runCLI("migrate", "--config", e.cfg); code != 0 {
		t.Fatalf("migrate: %d %s %s", code, stdout, stderr)
	}
	e.db.PostMigrateGrants()
	if code, stdout, stderr := runCLI("bootstrap", "--config", e.cfg, "--history-only", "--at", fmt.Sprint(e.start)); code != 0 {
		t.Fatalf("bootstrap: %d %s %s", code, stdout, stderr)
	}
	before, err := bootstrap.ReadOrigin(e.lake.Dir)
	if err != nil || before.ArchiveOrigin != e.start {
		t.Fatalf("origin after --history-only: %+v %v", before, err)
	}
	if err := os.Remove(bootstrap.OriginFile(e.lake.Dir)); err != nil {
		t.Fatal(err)
	}
	code, stdout, stderr := runCLI("bootstrap", "--config", e.cfg, "--recreate-origin")
	after, err := bootstrap.ReadOrigin(e.lake.Dir)
	if code != 0 || !strings.Contains(stdout, "recreated") || err != nil || after != before {
		t.Fatalf("recreate: %d %s %s; %+v %v", code, stdout, stderr, after, err)
	}
	if code, stdout, _ := runCLI("bootstrap", "--config", e.cfg, "--recreate-origin"); code != 0 || !strings.Contains(stdout, "kept the existing") {
		t.Fatalf("second recreate: %d %s", code, stdout)
	}
}

// `migrate --check` changes nothing: it lists the pending migrations and
// exits 1 while there are any, 0 once the schema is current. An edited
// migration is an error, not "pending".
func TestCLIMigrateCheck(t *testing.T) {
	db := explorertest.New(t, explorertest.Options{WithRoles: true})
	cfg := filepath.Join(t.TempDir(), "worker.toml")
	writeFile(t, cfg, fmt.Sprintf("[postgres]\ndsn = %q\nmigrate_dsn = %q\n", db.WriterDSN, db.OwnerDSN), 0o640)
	all, err := explorer.Migrations()
	if err != nil {
		t.Fatal(err)
	}
	check := func() (int, string, string) { return runCLI("migrate", "--config", cfg, "--check") }

	code, stdout, stderr := check()
	if code != 1 || strings.Count(stdout, "pending ") != len(all) || !strings.HasPrefix(stdout, "pending 1 ingest_state (ingest_state.sql)\n") ||
		!strings.Contains(stderr, fmt.Sprintf("%d pending migration(s)", len(all))) {
		t.Fatalf("empty database: %d %q %q", code, stdout, stderr)
	}
	var schemas int
	if err := db.Conn().QueryRow(context.Background(), "SELECT count(*) FROM pg_namespace WHERE nspname = 'explorer'").Scan(&schemas); err != nil || schemas != 0 {
		t.Fatalf("--check created the schema: %d %v", schemas, err)
	}

	if code, stdout, stderr := runCLI("migrate", "--config", cfg); code != 0 {
		t.Fatalf("migrate: %d %s %s", code, stdout, stderr)
	}
	if code, stdout, stderr := check(); code != 0 || stdout != "schema is up to date\n" {
		t.Fatalf("after migrate: %d %q %q", code, stdout, stderr)
	}

	last := all[len(all)-1]
	db.Exec(fmt.Sprintf("DELETE FROM explorer.schema_migrations WHERE version = %d", last.Version))
	if code, stdout, _ := check(); code != 1 || stdout != fmt.Sprintf("pending %d %s (%s)\n", last.Version, last.Name, last.File) {
		t.Fatalf("one missing: %d %q", code, stdout)
	}
	db.Exec("UPDATE explorer.schema_migrations SET checksum = 'edited' WHERE version = 1")
	if code, stdout, stderr := check(); code != 1 || stdout != "" || !strings.Contains(stderr, "never edit") {
		t.Fatalf("edited migration: %d %q %q", code, stdout, stderr)
	}
}
