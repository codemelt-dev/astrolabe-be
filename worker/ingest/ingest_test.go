package ingest

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/network"

	"stellar-explorer/worker/bootstrap"
	"stellar-explorer/worker/config"
	"stellar-explorer/worker/devfeed"
	"stellar-explorer/worker/explorer"
	"stellar-explorer/worker/explorer/explorertest"
	"stellar-explorer/worker/lake/laketest"
	"stellar-explorer/worker/sep54"
	"stellar-explorer/worker/sep54/sep54test"
	"stellar-explorer/worker/sequencer"
)

// testLog forwards logs to t.Log (only shown for failing or -v runs).
func testLog(t *testing.T) *slog.Logger {
	return slog.New(slog.NewTextHandler(tWriter{t: t}, &slog.HandlerOptions{Level: slog.LevelInfo}))
}

type tWriter struct {
	t    *testing.T
	keep *logLines
}

func (w tWriter) Write(p []byte) (int, error) {
	line := strings.TrimRight(string(p), "\n")
	w.t.Log(line)
	if w.keep != nil {
		w.keep.add(line)
	}
	return len(p), nil
}

// logLines keeps log lines for assertions.
type logLines struct {
	mu    sync.Mutex
	lines []string
}

func (l *logLines) add(s string) {
	l.mu.Lock()
	l.lines = append(l.lines, s)
	l.mu.Unlock()
}

func (l *logLines) count(substr string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, s := range l.lines {
		if strings.Contains(s, substr) {
			n++
		}
	}
	return n
}

type alertLog struct {
	mu    sync.Mutex
	kinds []string
}

func (a *alertLog) alert(kind, msg string, args ...any) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.kinds = append(a.kinds, kind)
}

func (a *alertLog) list() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.kinds...)
}

// namedMem is an in-memory store with a source name.
type namedMem struct {
	*sep54test.MemSource
	name string
}

func (m namedMem) Name() string { return m.name }

// harness is a lake, a migrated database and a chain of real-content
// ledgers to feed.
type harness struct {
	t      *testing.T
	db     *explorertest.DB
	lake   *laketest.Lake
	cfg    config.Config
	start  uint32
	chain  []sep54.Object
	up     *sep54test.MemSource // dev-feed's upstream
	aws    namedMem             // the AWS lake stand-in
	alerts *alertLog
	logs   *logLines

	mu      sync.Mutex
	commits []sequencer.CommitInfo
}

func newHarness(t *testing.T, start uint32, n int) *harness {
	t.Helper()
	db := explorertest.New(t, explorertest.Options{WithRoles: true})
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, db.OwnerDSN)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := explorer.Migrate(ctx, conn, slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
		t.Fatal(err)
	}
	conn.Close(ctx)
	db.PostMigrateGrants()

	h := &harness{t: t, db: db, start: start, alerts: &alertLog{}, logs: &logLines{}}
	h.chain = sep54test.Rechain(start, n)
	h.up = sep54test.NewMemSource(h.chain...)
	h.aws = namedMem{sep54test.NewMemSource(h.chain...), sep54.SourceAWS}
	h.lake = laketest.Start(t, laketest.Options{})
	h.cfg = configFor(t, h.lake, db)
	return h
}

// configFor is a worker config for a test lake and database: the
// gateway.env of the lake, archiving off, the writer's DSN.
func configFor(t *testing.T, l *laketest.Lake, db *explorertest.DB) config.Config {
	t.Helper()
	gw := filepath.Join(t.TempDir(), "gateway.env")
	env := fmt.Sprintf("AWS_ACCESS_KEY_ID=%s\nAWS_SECRET_ACCESS_KEY=%s\n", l.Creds.AccessKey, l.Creds.SecretKey)
	if err := os.WriteFile(gw, []byte(env), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	cfg.Lake = l.Cfg
	cfg.Credentials.GatewayEnv = gw
	cfg.Archive.Enabled = false
	cfg.Postgres.DSN = db.WriterDSN
	cfg.Metrics.Listen = laketest.FreeAddr(t)
	cfg.Metrics.NetworkTip = false
	return cfg
}

// bootstrap seeds the explorer cursor before the chain's first ledger.
func (h *harness) bootstrap() {
	h.t.Helper()
	res, err := bootstrap.HistoryOnly(context.Background(), bootstrap.HistoryOnlyOptions{
		DSN: h.db.WriterDSN, At: h.start, Sources: []sequencer.Source{h.aws}, Log: testLog(h.t),
	})
	if err != nil {
		h.t.Fatal(err)
	}
	if res.Cursor != h.start-1 || res.Source != sep54.SourceAWS {
		h.t.Fatalf("bootstrap %+v", res)
	}
	h.aws.Gets.Store(0)
}

// feed runs dev-feed for [from, to] at speed (0: unpaced).
func (h *harness) feed(ctx context.Context, from, to uint32, speed float64) error {
	lakeSrc, err := sep54.OpenLake(ctx, h.lake.Cfg, h.lake.Creds)
	if err != nil {
		return err
	}
	_, err = devfeed.Run(ctx, devfeed.Options{
		Upstream: h.up, Lake: lakeSrc.DataStore(), Passphrase: network.PublicNetworkPassphrase,
		Start: from, End: to, Speed: speed, Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	return err
}

// sources are the lake plus the AWS stand-in (never read in these tests).
func (h *harness) sources() *Sources {
	return &Sources{
		Lake: NewLazySource(sep54.SourceLake, func(ctx context.Context) (*sep54.Source, error) {
			return sep54.OpenLake(ctx, h.lake.Cfg, h.lake.Creds)
		}),
		AWS:     h.aws,
		LakeTip: sep54.NewTipClient(h.lake.ControlAddr()),
	}
}

// startIngest runs ingest in-process; stop cancels it and waits.
func (h *harness) startIngest() (stop func()) {
	h.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, Options{
			Config: h.cfg, Log: slog.New(slog.NewTextHandler(tWriter{t: h.t, keep: h.logs}, nil)), Alert: h.alerts.alert, Sources: h.sources(),
			OnCommit: func(c sequencer.CommitInfo) {
				h.mu.Lock()
				h.commits = append(h.commits, c)
				h.mu.Unlock()
			},
		})
	}()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					h.t.Errorf("ingest.Run: %v", err)
				}
			case <-time.After(60 * time.Second):
				h.t.Error("ingest did not stop")
			}
		})
	}
	h.t.Cleanup(stop)
	return stop
}

func (h *harness) cursor() uint32 {
	h.t.Helper()
	conn := h.db.Conn()
	seq, _, err := explorer.ReadCursor(context.Background(), conn, explorer.Pipeline)
	if err != nil {
		h.t.Fatal(err)
	}
	return seq
}

func (h *harness) waitCursor(seq uint32, within time.Duration) {
	h.t.Helper()
	conn := h.db.Conn()
	deadline := time.Now().Add(within)
	for {
		cur, _, err := explorer.ReadCursor(context.Background(), conn, explorer.Pipeline)
		if err != nil {
			h.t.Fatal(err)
		}
		if cur >= seq {
			return
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("cursor at %d, want %d", cur, seq)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// expectedRows returns the rows the chain [start, to] must produce, per
// table (re-chained ledgers repeat the fixtures' content in turn).
func expectedRows(t *testing.T, start, to uint32) map[string]int64 {
	t.Helper()
	var perTemplate []map[string]int
	for _, rl := range sep54test.RealLedgers() {
		obj := sep54test.RealObject(rl.Seq)
		lcm, hdr, err := sep54.DecodeLedger(rl.Seq, obj.Body)
		if err != nil {
			t.Fatal(err)
		}
		p, err := explorer.Process(network.PublicNetworkPassphrase, sequencer.Ledger{Header: hdr, LCM: lcm, Source: sep54.SourceLake})
		if err != nil {
			t.Fatal(err)
		}
		perTemplate = append(perTemplate, p.RowCounts())
	}
	out := map[string]int64{}
	for i := 0; i <= int(to-start); i++ {
		for tbl, n := range perTemplate[i%len(perTemplate)] {
			out[tbl] += int64(n)
		}
	}
	return out
}

// checkDB verifies the database holds exactly the ledgers [start, to] with
// every row, once: contiguous ledgers, per-table counts, per-ledger
// transaction counts, no duplicate transaction within a ledger.
func (h *harness) checkDB(to uint32) {
	h.t.Helper()
	ctx := context.Background()
	conn := h.db.Conn()
	var n, lo, hi int64
	if err := conn.QueryRow(ctx, "SELECT count(*), min(sequence), max(sequence) FROM explorer.ledgers").Scan(&n, &lo, &hi); err != nil {
		h.t.Fatal(err)
	}
	if n != int64(to-h.start+1) || lo != int64(h.start) || hi != int64(to) {
		h.t.Fatalf("ledgers: %d rows, %d-%d; want %d-%d", n, lo, hi, h.start, to)
	}
	want := expectedRows(h.t, h.start, to)
	for tbl, w := range want {
		var got int64
		if err := conn.QueryRow(ctx, "SELECT count(*) FROM explorer."+tbl).Scan(&got); err != nil {
			h.t.Fatal(err)
		}
		if got != w {
			h.t.Fatalf("%s: %d rows, want %d", tbl, got, w)
		}
	}
	if c := h.cursor(); c != to {
		h.t.Fatalf("cursor %d, want %d", c, to)
	}
}

func percentile(ds []time.Duration, p float64) time.Duration {
	s := append([]time.Duration(nil), ds...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[min(len(s)-1, int(float64(len(s))*p))]
}

// End to end: dev-feed fills the lake, ingest catches up through the
// BufferedStorageBackend, then follows dev-feed at an accelerated ledger
// cadence, one ledger per commit, with the ledger latency (lake commit →
// Postgres commit) measured (M3 acceptance, hermetic version).
func TestIngestCatchUpThenTip(t *testing.T) {
	const start, catchUp, live = 64_800_000, 90, 20
	h := newHarness(t, start, catchUp+live)
	h.bootstrap()
	ctx := context.Background()
	if err := h.feed(ctx, start, start+catchUp-1, 0); err != nil {
		t.Fatal(err)
	}
	stop := h.startIngest()
	h.waitCursor(start+catchUp-1, 3*time.Minute)
	h.mu.Lock()
	sizes := map[int]int{}
	for _, c := range h.commits {
		sizes[c.Ledgers]++
	}
	h.commits = nil
	h.mu.Unlock()
	// Batches form only when commits are the bottleneck (the sequencer
	// tests check the bounds); here the catch-up must at least have gone
	// through the BufferedStorageBackend.
	t.Logf("catch-up commits by size: %v", sizes)
	if h.logs.count(`msg="catching up"`) == 0 {
		t.Error("catch-up did not use the BufferedStorageBackend")
	}

	// At the tip: 20 ledgers at 5x ledger cadence (1 s apart).
	if err := h.feed(ctx, start+catchUp, start+catchUp+live-1, 5); err != nil {
		t.Fatal(err)
	}
	h.waitCursor(start+catchUp+live-1, time.Minute)
	stop()
	h.mu.Lock()
	var lat []time.Duration
	single := 0
	for _, c := range h.commits {
		if c.Ledgers == 1 {
			single++
		}
		if c.Latency > 0 {
			lat = append(lat, c.Latency)
		}
	}
	commits := len(h.commits)
	h.mu.Unlock()
	// One ledger per commit at the tip; a pipeline that falls behind (a
	// loaded machine, -race) batches, which is correct too.
	if single*4 < commits*3 || len(lat) < live/2 {
		t.Fatalf("%d of %d live commits were single-ledger; latency measured for %d of %d ledgers", single, commits, len(lat), live)
	}
	t.Logf("ledger latency (lake commit → Postgres commit) over %d ledgers: p50 %s, p90 %s, max %s",
		len(lat), percentile(lat, 0.5), percentile(lat, 0.9), percentile(lat, 1))
	h.checkDB(start + catchUp + live - 1)
	if len(h.alerts.list()) != 0 || h.aws.Gets.Load() != 0 {
		t.Fatalf("alerts %v, AWS reads %d", h.alerts.list(), h.aws.Gets.Load())
	}
	t.Logf("database size after %d ledgers: %d MiB", catchUp+live, h.db.Size()>>20)
}

// A lake restart during ingest (at the tip and mid catch-up): retries, no
// failover, no alert (M3 acceptance).
func TestIngestLakeRestart(t *testing.T) {
	const start, n = 64_900_000, 110
	h := newHarness(t, start, n)
	h.bootstrap()
	ctx := context.Background()
	if err := h.feed(ctx, start, start+19, 0); err != nil {
		t.Fatal(err)
	}
	stop := h.startIngest()
	h.waitCursor(start+19, 2*time.Minute)

	// 1. Restart while ingest waits at the tip.
	h.lake = h.lake.Restart(t, laketest.Options{})
	if err := h.feed(ctx, start+20, start+29, 20); err != nil {
		t.Fatal(err)
	}
	h.waitCursor(start+29, time.Minute)

	// 2. Restart while ingest catches up (80 ledgers, over the BSB
	// threshold), and keep feeding afterwards, like Galexie after a
	// lake restart.
	if err := h.feed(ctx, start+30, start+n-11, 0); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	h.lake = h.lake.Restart(t, laketest.Options{})
	if err := h.feed(ctx, start+n-10, start+n-1, 20); err != nil {
		t.Fatal(err)
	}
	h.waitCursor(start+n-1, 3*time.Minute)
	stop()
	h.checkDB(start + n - 1)
	h.mu.Lock()
	sources := map[string]int{}
	for _, c := range h.commits {
		for s, k := range c.Sources {
			sources[s] += k
		}
	}
	h.mu.Unlock()
	if sources[sep54.SourceLake] != n || len(sources) != 1 {
		t.Fatalf("read sources %v", sources)
	}
	if len(h.alerts.list()) != 0 || h.aws.Gets.Load() != 0 {
		t.Fatalf("alerts %v, AWS reads %d", h.alerts.list(), h.aws.Gets.Load())
	}
}

// The startup checks: ingest waits (with an alert) for `worker migrate` and
// `worker bootstrap`, then runs.
func TestIngestWaitsForBootstrap(t *testing.T) {
	const start = 65_000_000
	h := newHarness(t, start, 3)
	if err := h.feed(context.Background(), start, start+2, 0); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, Options{Config: h.cfg, Log: testLog(t), Alert: h.alerts.alert, Sources: h.sources(), StartRetry: 100 * time.Millisecond})
	}()
	deadline := time.Now().Add(20 * time.Second)
	for len(h.alerts.list()) == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if got := h.alerts.list(); len(got) != 1 || got[0] != "no_cursor" {
		t.Fatalf("alerts %v", got)
	}
	h.bootstrap()
	h.waitCursor(start+2, time.Minute)
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	// A second ingest while one runs cannot start: it exits, on the
	// metrics port, or (on another port) on the pipeline's advisory lock.
	stop := h.startIngest()
	time.Sleep(300 * time.Millisecond)
	err := Run(context.Background(), Options{Config: h.cfg, Log: testLog(t), Sources: h.sources()})
	if err == nil || !strings.Contains(err.Error(), "metrics listener") {
		t.Fatalf("second ingest on the same port: %v", err)
	}
	other := h.cfg
	other.Metrics.Listen = laketest.FreeAddr(t)
	err = Run(context.Background(), Options{Config: other, Log: testLog(t), Sources: h.sources()})
	if err == nil || !strings.Contains(err.Error(), "advisory lock") {
		t.Fatalf("second ingest: %v", err)
	}
	stop()
}

// Postgres unreachable at startup: ingest keeps retrying (the lake and
// Galexie are unaffected) and stops cleanly.
func TestIngestWaitsForPostgres(t *testing.T) {
	h := newHarness(t, 65_100_000, 1)
	h.cfg.Postgres.DSN = "postgres://nobody@127.0.0.1:1/none?sslmode=disable&connect_timeout=1"
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	start := time.Now()
	err := Run(ctx, Options{Config: h.cfg, Log: testLog(t), Alert: h.alerts.alert, Sources: h.sources(), StartRetry: 200 * time.Millisecond})
	if err != nil || time.Since(start) < 2*time.Second {
		t.Fatalf("Run returned %v after %s", err, time.Since(start))
	}
	if len(h.alerts.list()) != 0 {
		t.Fatalf("alerts %v", h.alerts.list())
	}
}
