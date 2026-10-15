package ingest

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/network"

	"stellar-explorer/worker/bootstrap"
	"stellar-explorer/worker/devfeed"
	"stellar-explorer/worker/explorer"
	"stellar-explorer/worker/explorer/explorertest"
	"stellar-explorer/worker/lake/laketest"
	"stellar-explorer/worker/reindex"
	"stellar-explorer/worker/sep54"
	"stellar-explorer/worker/sequencer"
)

// Opt-in network tests against the AWS public lake (M3 acceptance):
//
//	WORKER_TEST_AWS_REPLAY=2000  replay that many recent ledgers into a
//	                             scratch database, rerun, reindex 500
//	WORKER_TEST_LIVE_AWS=1       dev-feed at the live tip, latency measured

// rangeDigest returns per-table row counts and an order-independent digest
// of the rows of ledgers [from, to] (sums of 60-bit md5 prefixes, so no
// large strings are built).
func rangeDigest(t *testing.T, conn *pgx.Conn, from, to uint32) string {
	t.Helper()
	out := ""
	for _, tbl := range []struct{ name, col string }{{"ledgers", "sequence"}} {
		var n int64
		var sum string
		q := fmt.Sprintf(`SELECT count(*), coalesce(sum(('x' || substr(md5(r::text), 1, 15))::bit(60)::bigint::numeric), 0)::text
			FROM explorer.%s r WHERE %s BETWEEN $1 AND $2`, tbl.name, tbl.col)
		if err := conn.QueryRow(context.Background(), q, int64(from), int64(to)).Scan(&n, &sum); err != nil {
			t.Fatal(err)
		}
		out += fmt.Sprintf("%s=%d/%s ", tbl.name, n, sum)
	}
	return out
}

func liveDB(t *testing.T) *explorertest.DB {
	t.Helper()
	db := explorertest.New(t, explorertest.Options{WithRoles: true})
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, db.OwnerDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err := explorer.Migrate(ctx, conn, testLog(t)); err != nil {
		t.Fatal(err)
	}
	db.PostMigrateGrants()
	return db
}

// TestLiveReplayAWS replays WORKER_TEST_AWS_REPLAY consecutive recent
// AWS-lake ledgers through the sequencer's catch-up path into one scratch
// database with one cursor, then checks that a rerun is a no-op and that a
// reindex of 500 ledgers changes nothing. The cursor is seeded with
// --history-only, so the state is not maintained: PostgreSQL gets the
// ledger rows, the only history it keeps. The replay is checked in chunks
// of WORKER_TEST_AWS_REPLAY_CHUNK (default 500) ledgers; the hash chain over
// the whole replay.
func TestLiveReplayAWS(t *testing.T) {
	n, _ := strconv.Atoi(os.Getenv("WORKER_TEST_AWS_REPLAY"))
	if n <= 0 {
		t.Skip("set WORKER_TEST_AWS_REPLAY=<ledgers> to replay recent AWS-lake ledgers")
	}
	chunk, _ := strconv.Atoi(os.Getenv("WORKER_TEST_AWS_REPLAY_CHUNK"))
	if chunk <= 0 {
		chunk = 500
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
	from := tip - uint32(n) - 20
	to := from + uint32(n) - 1
	db := liveDB(t)
	if _, err := bootstrap.HistoryOnly(ctx, bootstrap.HistoryOnlyOptions{DSN: db.WriterDSN, At: from,
		Sources: []sequencer.Source{aws}, Log: testLog(t)}); err != nil {
		t.Fatal(err)
	}
	admin := db.Conn()

	sink, err := explorer.NewSink(db.WriterDSN, explorer.SinkOptions{Lock: true, Log: testLog(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	if err := sink.Start(ctx); err != nil {
		t.Fatal(err)
	}
	var commits, maxBatch int
	var commitTime, took time.Duration
	var peak int64
	var lastFrom uint32 // the last chunk
	for c0 := from; c0 <= to; c0 += uint32(chunk) {
		lastFrom = c0
		c1 := min(to, c0+uint32(chunk)-1)
		s, err := sequencer.New(sequencer.Options[*explorer.Prepared]{
			Pipeline: explorer.Pipeline, Sink: sink, Process: explorer.Processor(network.PublicNetworkPassphrase),
			Fixed: &sequencer.Fixed{Source: aws, To: c1}, Log: testLog(t),
			OnCommit: func(c sequencer.CommitInfo) {
				commits++
				maxBatch = max(maxBatch, c.Ledgers)
				commitTime += c.Duration
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		if err := s.Run(ctx); err != nil {
			t.Fatal(err)
		}
		took += time.Since(start)
		size := db.Size()
		peak = max(peak, size)
		checkConsistent(t, admin, from, c1)
		t.Logf("replayed %d-%d in %s; database %d MiB", c0, c1, time.Since(start).Round(time.Second), size>>20)
	}
	t.Logf("replayed %d AWS-lake ledgers %d-%d in %s (%.1f ledgers/s): %d commits (max %d ledgers), commit time %s, conflicts %d; peak database %d MiB (%.2f MiB/ledger)",
		n, from, to, took.Round(time.Second), float64(n)/took.Seconds(), commits, maxBatch, commitTime.Round(time.Second),
		sink.Conflicts(), peak>>20, float64(peak)/float64(n)/(1<<20))
	if sink.Conflicts() != 0 {
		t.Fatalf("%d conflicts", sink.Conflicts())
	}
	// The hash chain over every replayed ledger.
	var breaks int64
	if err := admin.QueryRow(ctx, `SELECT count(*) FROM (SELECT prev_hash, lag(hash) OVER (ORDER BY sequence) AS prev
		FROM explorer.ledgers) x WHERE prev IS NOT NULL AND prev <> prev_hash`).Scan(&breaks); err != nil || breaks != 0 {
		t.Fatalf("hash chain: %d breaks, %v", breaks, err)
	}

	// Rerun = no-op: the cursor is at the end, so replaying the range again
	// commits nothing, and re-committing the last ledger (a retry after an
	// ambiguous COMMIT) inserts and conflicts with nothing.
	before := rangeDigest(t, admin, lastFrom, to)
	s2, _ := sequencer.New(sequencer.Options[*explorer.Prepared]{
		Pipeline: explorer.Pipeline, Sink: sink, Process: explorer.Processor(network.PublicNetworkPassphrase),
		Fixed: &sequencer.Fixed{Source: aws, To: to}, Log: testLog(t),
	})
	if err := s2.Run(ctx); err != nil || s2.Stats().Committed != 0 {
		t.Fatalf("rerun: committed %d, %v", s2.Stats().Committed, err)
	}
	obj, err := aws.GetLedger(ctx, to)
	if err != nil {
		t.Fatal(err)
	}
	lcm, h, err := sep54.DecodeLedger(to, obj.Body)
	if err != nil {
		t.Fatal(err)
	}
	p, err := explorer.Process(network.PublicNetworkPassphrase, sequencer.Ledger{Header: h, LCM: lcm, Source: sep54.SourceAWS})
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.Commit(ctx, []*explorer.Prepared{p}); err != nil {
		t.Fatalf("re-commit of the last ledger: %v", err)
	}
	if after := rangeDigest(t, admin, lastFrom, to); after != before || sink.Conflicts() != 0 {
		t.Fatalf("rerun changed rows (conflicts %d):\n%s\n%s", sink.Conflicts(), before, after)
	}

	// Reindex 500 ledgers: row counts, content and the cursor unchanged.
	rFrom := max(lastFrom, to-499)
	digest := rangeDigest(t, admin, rFrom, to)
	rstart := time.Now()
	res, err := reindex.Run(ctx, reindex.Options{DSN: db.WriterDSN, From: rFrom, To: to, Source: aws, Log: testLog(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("reindexed %d ledgers %d-%d in %s, %d conflicts", res.Ledgers, rFrom, to, time.Since(rstart).Round(time.Second), res.Conflicts)
	if after := rangeDigest(t, admin, rFrom, to); after != digest || res.Conflicts != 0 {
		t.Fatalf("reindex changed rows:\n%s\n%s", digest, after)
	}
	if seq, _, _ := explorer.ReadCursor(ctx, admin, explorer.Pipeline); seq != to {
		t.Fatalf("explorer cursor moved to %d", seq)
	}
	t.Logf("rows %d-%d before and after the reindex: %s", rFrom, to, digest)
}

// checkConsistent verifies that explorer.ledgers holds exactly [from, to].
func checkConsistent(t *testing.T, conn *pgx.Conn, from, to uint32) {
	t.Helper()
	var n, lo, hi int64
	if err := conn.QueryRow(context.Background(), "SELECT count(*), min(sequence), max(sequence) FROM explorer.ledgers").Scan(&n, &lo, &hi); err != nil {
		t.Fatal(err)
	}
	if n != int64(to-from+1) || lo != int64(from) || hi != int64(to) {
		t.Fatalf("ledgers %d rows %d-%d, want %d-%d", n, lo, hi, from, to)
	}
}

// dev-feed at the live AWS tip (ledger cadence) into a real lake, ingest
// following it: the ledger latency, lake commit → Postgres commit, is
// measured (M3 acceptance).
func TestLiveDevFeedLatency(t *testing.T) {
	if os.Getenv("WORKER_TEST_LIVE_AWS") == "" {
		t.Skip("set WORKER_TEST_LIVE_AWS=1 to follow the live AWS-lake tip")
	}
	const ledgers = 15
	ctx := context.Background()
	aws, err := sep54.OpenAWSLake(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tip, err := aws.Newest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	db := liveDB(t)
	start := tip - 1
	if _, err := bootstrap.HistoryOnly(ctx, bootstrap.HistoryOnlyOptions{DSN: db.WriterDSN, At: start,
		Sources: []sequencer.Source{aws}, Log: testLog(t)}); err != nil {
		t.Fatal(err)
	}
	l := laketest.Start(t, laketest.Options{})
	h := &harness{t: t, db: db, lake: l, alerts: &alertLog{}, logs: &logLines{}}
	h.cfg = configFor(t, l, db)
	h.aws = namedMem{name: sep54.SourceAWS}
	srcs := h.sources()
	srcs.AWS = aws // the real fallback; it must not be needed
	ictx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- Run(ictx, Options{Config: h.cfg, Log: testLog(t), Alert: h.alerts.alert, Sources: srcs,
			OnCommit: func(c sequencer.CommitInfo) {
				h.mu.Lock()
				h.commits = append(h.commits, c)
				h.mu.Unlock()
			}})
	}()
	lakeSrc, err := sep54.OpenLake(ctx, l.Cfg, l.Creds)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := devfeed.Run(ctx, devfeed.Options{Upstream: aws, Lake: lakeSrc.DataStore(), Passphrase: network.PublicNetworkPassphrase,
		Start: start, End: start + ledgers - 1, Speed: 1, Log: testLog(t)}); err != nil {
		t.Fatal(err)
	}
	h.waitCursor(start+ledgers-1, time.Minute)
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	h.mu.Lock()
	var lat []time.Duration
	for _, c := range h.commits {
		if c.Latency > 0 {
			lat = append(lat, c.Latency)
			t.Logf("ledger %d: latency %s (read+decode %s, process %s, commit %s)", c.Last, c.Latency.Round(time.Millisecond),
				c.Read.Round(time.Millisecond), c.Process.Round(time.Millisecond), c.Duration.Round(time.Millisecond))
		}
	}
	h.mu.Unlock()
	if len(lat) < ledgers/2 {
		t.Fatalf("latency measured for %d of %d ledgers", len(lat), ledgers)
	}
	t.Logf("ledger latency at the live tip (lake commit → Postgres commit), %d ledgers: p50 %s, p90 %s, max %s",
		len(lat), percentile(lat, 0.5), percentile(lat, 0.9), percentile(lat, 1))
	if a := h.alerts.list(); len(a) != 0 {
		t.Fatalf("alerts %v", a)
	}
}
