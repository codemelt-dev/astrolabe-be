package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/network"

	"stellar-explorer/worker/devfeed"
	"stellar-explorer/worker/explorer"
	"stellar-explorer/worker/explorer/explorertest"
	"stellar-explorer/worker/lake/laketest"
	"stellar-explorer/worker/sep54"
	"stellar-explorer/worker/sep54/sep54test"
	"stellar-explorer/worker/sequencer"
)

// pipelineEnv is a running lake fed with re-chained real-content ledgers,
// a scratch database and a worker.toml pointing at both.
type pipelineEnv struct {
	db    *explorertest.DB
	lake  *laketest.Lake
	cfg   string
	start uint32
	n     int
}

func newPipelineEnv(t *testing.T, start uint32, n int) *pipelineEnv {
	t.Helper()
	db := explorertest.New(t, explorertest.Options{WithRoles: true})
	l := laketest.Start(t, laketest.Options{})
	ctx := context.Background()
	lakeSrc, err := sep54.OpenLake(ctx, l.Cfg, l.Creds)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := devfeed.Run(ctx, devfeed.Options{
		Upstream: sep54test.NewMemSource(sep54test.Rechain(start, n)...), Lake: lakeSrc.DataStore(),
		Passphrase: network.PublicNetworkPassphrase, Start: start, End: start + uint32(n) - 1,
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}); err != nil {
		t.Fatal(err)
	}
	cfg := writeWorkerConfig(t, l.Cfg, l.Creds, fmt.Sprintf(`[archive]
enabled = false
[postgres]
dsn = %q
migrate_dsn = %q
`, db.WriterDSN, db.OwnerDSN))
	return &pipelineEnv{db: db, lake: l, cfg: cfg, start: start, n: n}
}

func (e *pipelineEnv) last() uint32 { return e.start + uint32(e.n) - 1 }

func (e *pipelineEnv) cursor(t *testing.T) uint32 {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), e.db.DSN)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	seq, _, err := explorer.ReadCursor(context.Background(), conn, explorer.Pipeline)
	if err != nil {
		t.Fatal(err)
	}
	return seq
}

// rowsChecksum hashes every history table's ordered content.
func (e *pipelineEnv) rowsChecksum(t *testing.T) string {
	t.Helper()
	var parts []string
	conn := e.db.Conn()
	for _, tbl := range []string{"ledgers"} {
		var sum string
		var n int64
		q := fmt.Sprintf("SELECT md5(coalesce(string_agg(r::text, '|' ORDER BY r::text), '')), count(*) FROM explorer.%s r", tbl)
		if err := conn.QueryRow(context.Background(), q).Scan(&sum, &n); err != nil {
			t.Fatal(err)
		}
		parts = append(parts, fmt.Sprintf("%s=%d/%s", tbl, n, sum))
	}
	return strings.Join(parts, " ")
}

// checkRows verifies the database holds every ledger of the chain exactly
// once, with all its rows.
func (e *pipelineEnv) checkRows(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	conn := e.db.Conn()
	var n, lo, hi int64
	if err := conn.QueryRow(ctx, "SELECT count(*), min(sequence), max(sequence) FROM explorer.ledgers").Scan(&n, &lo, &hi); err != nil {
		t.Fatal(err)
	}
	if n != int64(e.n) || lo != int64(e.start) || hi != int64(e.last()) {
		t.Fatalf("ledgers: %d rows %d-%d, want %d-%d", n, lo, hi, e.start, e.last())
	}
	var perTemplate []map[string]int
	for _, rl := range sep54test.RealLedgers() {
		obj := sep54test.RealObject(rl.Seq)
		lcm, h, err := sep54.DecodeLedger(rl.Seq, obj.Body)
		if err != nil {
			t.Fatal(err)
		}
		p, err := explorer.Process(network.PublicNetworkPassphrase, sequencer.Ledger{Header: h, LCM: lcm, Source: sep54.SourceLake})
		if err != nil {
			t.Fatal(err)
		}
		perTemplate = append(perTemplate, p.RowCounts())
	}
	want := map[string]int64{}
	for i := range e.n {
		for tbl, k := range perTemplate[i%len(perTemplate)] {
			want[tbl] += int64(k)
		}
	}
	for tbl, w := range want {
		var got int64
		if err := conn.QueryRow(ctx, "SELECT count(*) FROM explorer."+tbl).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != w {
			t.Fatalf("%s: %d rows, want %d", tbl, got, w)
		}
	}
}

// `worker migrate` as explorer_owner, `worker bootstrap --history-only`,
// then the refusals.
func TestCLIMigrateAndBootstrap(t *testing.T) {
	e := newPipelineEnv(t, 64_800_000, 3)
	code, stdout, stderr := runCLI("migrate", "--config", e.cfg)
	if code != 0 || !strings.Contains(stdout, "applied 1 ingest_state (ingest_state.sql)") ||
		!strings.Contains(stdout, "applied 12 state_liquidity_pools") {
		t.Fatalf("migrate: %d\n%s\n%s", code, stdout, stderr)
	}
	e.db.PostMigrateGrants()
	if code, stdout, _ := runCLI("migrate", "--config", e.cfg); code != 0 || !strings.Contains(stdout, "schema is up to date") {
		t.Fatalf("second migrate: %d %s", code, stdout)
	}
	// The checkpoint bootstrap checks the archive's network: a futurenet
	// archive is refused for pubnet and nothing is written.
	futurenet := writeWorkerConfig(t, e.lake.Cfg, e.lake.Creds, fmt.Sprintf("[postgres]\ndsn = %q\n[bootstrap]\nhistory_archives = [%q]\n",
		e.db.WriterDSN, "file://"+futurenetArchive(t)))
	if code, _, stderr := runCLI("bootstrap", "--config", futurenet); code != 1 || !strings.Contains(stderr, "Network passphrase does not match") {
		t.Fatalf("bootstrap from a futurenet archive: %d %s", code, stderr)
	}
	if code, _, _ := runCLI("bootstrap", "--config", e.cfg, "--history-only"); code != 2 {
		t.Fatalf("--history-only without --at: %d", code)
	}
	code, stdout, stderr = runCLI("bootstrap", "--config", e.cfg, "--history-only", "--at", fmt.Sprint(e.start))
	if code != 0 || !strings.Contains(stdout, fmt.Sprintf("cursor seeded at %d", e.start-1)) || !strings.Contains(stdout, "read from lake") {
		t.Fatalf("bootstrap: %d\n%s\n%s", code, stdout, stderr)
	}
	if code, _, stderr := runCLI("bootstrap", "--config", e.cfg, "--history-only", "--at", fmt.Sprint(e.start)); code != 1 || !strings.Contains(stderr, "bootstrap runs once") {
		t.Fatalf("second bootstrap: %d %s", code, stderr)
	}
	// The ingest role cannot migrate: the schema belongs to explorer_owner.
	writerOnly := writeWorkerConfig(t, e.lake.Cfg, e.lake.Creds, fmt.Sprintf("[postgres]\ndsn = %q\n", e.db.WriterDSN))
	e.db.Exec("DELETE FROM explorer.schema_migrations WHERE version = 7", "DROP TABLE explorer.ttl")
	if code, _, stderr := runCLI("migrate", "--config", writerOnly); code != 1 || !strings.Contains(stderr, "permission denied") {
		t.Fatalf("migrate as the writer: %d %s", code, stderr)
	}
	if code, _, stderr := runCLI("ingest", "--config", writeWorkerConfig(t, e.lake.Cfg, e.lake.Creds, "")); code != 1 || !strings.Contains(stderr, "postgres.dsn") {
		t.Fatalf("ingest without a DSN: %d %s", code, stderr)
	}
}

// kill -9 of `worker ingest` at random points, over and over, while it
// catches up: the cursor and the rows always agree, no ledger is missing or
// duplicated, nothing conflicts (M3 acceptance). Then a clean SIGTERM, a
// reindex that changes nothing, and a dry run.
func TestCLIIngestKill9(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	e := newPipelineEnv(t, 64_850_000, 400)
	if code, out, errOut := runCLI("migrate", "--config", e.cfg); code != 0 {
		t.Fatalf("migrate: %s %s", out, errOut)
	}
	e.db.PostMigrateGrants()
	if code, out, errOut := runCLI("bootstrap", "--config", e.cfg, "--history-only", "--at", fmt.Sprint(e.start)); code != 0 {
		t.Fatalf("bootstrap: %s %s", out, errOut)
	}

	var logs syncBuffer
	start := func() *exec.Cmd {
		cmd := exec.Command(os.Args[0], "-test.run=^$", "--", "ingest", "--config", e.cfg)
		cmd.Env = append(os.Environ(), envMainHelper+"=1")
		cmd.Stdout, cmd.Stderr = &logs, &logs
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		return cmd
	}
	rng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 3))
	kills, midway := 0, 0
	prev := e.start - 1
	for i := 0; i < 80 && e.cursor(t) < e.last(); i++ {
		cmd := start()
		time.Sleep(time.Duration(150+rng.IntN(500)) * time.Millisecond)
		_ = cmd.Process.Signal(syscall.SIGKILL)
		_ = cmd.Wait()
		kills++
		cur := e.cursor(t)
		if cur < prev {
			t.Fatalf("cursor moved backwards: %d → %d", prev, cur)
		}
		if cur < e.last() {
			midway++
		}
		prev = cur
	}
	t.Logf("%d kill -9 (%d before the end); cursor %d of %d", kills, midway, e.cursor(t), e.last())
	if midway < 3 {
		t.Fatalf("only %d kills interrupted ingest", midway)
	}
	// Finish, then stop cleanly (once this run's sequencer is up).
	started := strings.Count(logs.String(), "sequencer starting")
	cmd := start()
	deadline := time.Now().Add(5 * time.Minute)
	for e.cursor(t) < e.last() || strings.Count(logs.String(), "sequencer starting") == started {
		if time.Now().After(deadline) {
			t.Fatalf("cursor stuck at %d\n%s", e.cursor(t), logs.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
	_ = cmd.Process.Signal(syscall.SIGTERM)
	if err := cmd.Wait(); err != nil {
		t.Fatalf("ingest after SIGTERM: %v", err)
	}
	e.checkRows(t)
	if out := logs.String(); strings.Contains(out, "alert=") || strings.Contains(out, "level=ERROR") {
		t.Fatalf("alerts or errors in the ingest logs:\n%s", out)
	}

	// Reindex part of the range: rows and the explorer cursor unchanged.
	before := e.rowsChecksum(t)
	from, to := e.start+10, e.start+60
	code, stdout, stderr := runCLI("reindex", "--config", e.cfg, "--from", fmt.Sprint(from), "--to", fmt.Sprint(to))
	if code != 0 || !strings.Contains(stdout, "(51 ledgers this run, 0 conflicts)") {
		t.Fatalf("reindex: %d\n%s\n%s", code, stdout, stderr)
	}
	if after := e.rowsChecksum(t); after != before {
		t.Fatalf("reindex changed rows:\n%s\n%s", before, after)
	}
	if e.cursor(t) != e.last() {
		t.Fatal("reindex moved the explorer cursor")
	}
	code, stdout, _ = runCLI("reindex", "--config", e.cfg, "--from", fmt.Sprint(e.start), "--to", fmt.Sprint(e.last()), "--dry-run")
	if code != 0 || !strings.Contains(stdout, fmt.Sprintf("validated %d ledgers", e.n)) {
		t.Fatalf("dry run: %d %s", code, stdout)
	}
	if code, _, stderr := runCLI("reindex", "--config", e.cfg, "--from", fmt.Sprint(e.start), "--to", fmt.Sprint(e.last()+5)); code != 1 || !strings.Contains(stderr, "committed ledgers") {
		t.Fatalf("reindex past the cursor: %d %s", code, stderr)
	}
	if code, _, _ := runCLI("reindex", "--config", e.cfg, "--from", "10", "--to", "20", "--source", "nowhere"); code != 2 {
		t.Fatalf("bad --source: %d", code)
	}
}
