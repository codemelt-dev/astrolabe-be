package clickhouse

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stellar/go-stellar-sdk/network"

	"stellar-explorer/worker/explorer"
	"stellar-explorer/worker/sep54"
	"stellar-explorer/worker/sep54/sep54test"
	"stellar-explorer/worker/sequencer"
)

// lakeSource is the AWS lake's stand-in: an in-memory store with its name.
type lakeSource struct{ *sep54test.MemSource }

func (lakeSource) Name() string { return sep54.SourceAWS }

// processed decodes and processes chained objects as the pipeline does.
func processed(t testing.TB, objs []sep54.Object) []*explorer.Prepared {
	t.Helper()
	out := make([]*explorer.Prepared, 0, len(objs))
	for _, o := range objs {
		seq, _ := sep54.ParseObjectPath(o.Key)
		lcm, h, err := sep54.DecodeHistoryLedger(seq, o.Body)
		if err != nil {
			t.Fatal(err)
		}
		p, err := explorer.ProcessHistory(network.PublicNetworkPassphrase, sequencer.Ledger{Header: h, LCM: lcm, Source: sep54.SourceAWS, Size: len(o.Body)})
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, p)
	}
	return out
}

func count(t testing.TB, conn driver.Conn, query string, args ...any) uint64 {
	t.Helper()
	var n uint64
	if err := conn.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

// A backfill below a live pipeline: two segments across a multiple of the
// segment size, with what a crashed run left above a segment's cursor. It
// fills every table once, leaves the live pipeline's rows alone, chains to
// them, marks its segments done, and a rerun does nothing.
func TestIntegrationBackfill(t *testing.T) {
	conn := testConn(t)
	ctx := context.Background()
	if _, err := Migrate(ctx, conn, quiet); err != nil {
		t.Fatal(err)
	}
	const from, first, last = 2_999_990, 3_000_020, 3_000_029
	chain := sep54test.Rechain(from, last-from+1)
	all := processed(t, chain)
	old, live := all[:first-from], all[first-from:]

	// The live pipeline holds [first, last].
	if _, err := Init(ctx, conn, Pipeline, first-1, live[0].Header().PreviousHash); err != nil {
		t.Fatal(err)
	}
	if err := NewSink(conn, SinkOptions{Log: quiet}).Commit(ctx, live); err != nil {
		t.Fatal(err)
	}
	want := func(batch []*explorer.Prepared) (txs, ops, trades, transfers uint64) {
		for _, p := range batch {
			txs, ops, trades, transfers = txs+uint64(len(p.Transactions)), ops+uint64(len(p.Operations)), trades+uint64(len(p.Trades)), transfers+uint64(len(p.Transfers))
		}
		return
	}
	liveTxs, _, liveTrades, _ := want(live)
	if liveTrades == 0 {
		t.Fatal("the fixtures have no trades")
	}

	o := BackfillOptions{Open: func(ctx context.Context) (driver.Conn, error) { return noClose{conn}, nil }, Source: lakeSource{sep54test.NewMemSource(chain...)},
		From: from, To: first - 1, Segment: 1_000_000, Parallel: 2, FirstLedger: first, MaxBatchLedgers: 7, MaxBatchBytes: 64 << 20,
		VerifyEvery: 10, Passphrase: network.PublicNetworkPassphrase, Log: quiet}

	// Refused: a range that reaches the live pipeline's ledgers, and a
	// segment size that splits tx_by_hash's partitions.
	bad := o
	bad.To = first
	if _, err := Backfill(ctx, bad); err == nil || !strings.Contains(err.Error(), "the live pipeline owns") {
		t.Fatalf("a range into the live ledgers: %v", err)
	}
	bad = o
	bad.Segment = 500_000
	if _, err := Backfill(ctx, bad); err == nil || !strings.Contains(err.Error(), "multiple of") {
		t.Fatalf("a segment of 500,000: %v", err)
	}

	// A crashed run of the newer segment: its cursor seeded, half its rows
	// written, no cursor for them.
	upper := "backfill:3000000-3000019"
	if _, err := Init(ctx, conn, upper, 2_999_999, old[10].Header().PreviousHash); err != nil {
		t.Fatal(err)
	}
	if err := NewSink(conn, SinkOptions{Pipeline: upper, Log: quiet, Ceiling: first - 1}).write(ctx, old[10:16], 3_000_000); err != nil {
		t.Fatal(err)
	}

	res, err := Backfill(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	if res.Segments != 2 || res.DoneBefore != 0 || res.Done != 2 || res.Ledgers != uint64(len(old)) {
		t.Fatalf("result %+v", res)
	}
	// Everything once: the whole range checks out against the ledgers' own
	// counts, and the trades and transfers are the processed ones.
	r, err := CheckSegment(ctx, conn, from, last)
	if err != nil || len(r.Problems()) > 0 || r.Distinct != uint64(len(all)) {
		t.Fatalf("check: %s (%v)", r, err)
	}
	_, _, allTrades, allTransfers := want(all)
	if n := count(t, conn, "SELECT count() FROM trades"); n != allTrades {
		t.Errorf("%d trades, want %d", n, allTrades)
	}
	if n := count(t, conn, "SELECT count() FROM token_transfers"); n != allTransfers {
		t.Errorf("%d transfers, want %d", n, allTransfers)
	}
	if n := count(t, conn, "SELECT count() FROM transactions WHERE ledger_sequence >= ?", uint32(first)); n != liveTxs {
		t.Errorf("the live pipeline's ledgers hold %d transactions, want %d", n, liveTxs)
	}
	if n := count(t, conn, "SELECT count() FROM trades WHERE ledger_sequence >= ?", uint32(first)); n != liveTrades {
		t.Errorf("the live pipeline's ledgers hold %d trades, want %d", n, liveTrades)
	}
	if lr, err := Check(ctx, conn, Pipeline, first, last); err != nil || len(lr.Problems()) > 0 {
		t.Errorf("the live pipeline's check: %s (%v)", lr, err)
	}
	if n := count(t, conn, "SELECT uniqExact(pipeline) FROM ingest_state WHERE startsWith(pipeline, 'done:backfill:')"); n != 2 {
		t.Errorf("%d done markers", n)
	}

	// A rerun does nothing; another range over the same ledgers is refused.
	if res, err := Backfill(ctx, o); err != nil || res.DoneBefore != 2 || res.Done != 0 || res.Ledgers != 0 {
		t.Fatalf("rerun: %+v %v", res, err)
	}
	bad = o
	bad.From = from + 3
	if _, err := Backfill(ctx, bad); err == nil || !strings.Contains(err.Error(), "overlaps the earlier pipeline") {
		t.Fatalf("another range: %v", err)
	}

	// daily_stats for the backfilled day, computed once.
	day := old[0].Ledger.ClosedAt
	if err := RecomputeDailyStats(ctx, conn, day, day, quiet); err != nil {
		t.Fatal(err)
	}
	allTxs, _, _, _ := want(all)
	var ledgers, txs uint64
	if err := conn.QueryRow(ctx, "SELECT sum(ledgers), sum(transactions) FROM daily_stats FINAL WHERE day = toDate(?)", day.Format(time.DateOnly)).
		Scan(&ledgers, &txs); err != nil || ledgers != uint64(len(all)) || txs != allTxs {
		t.Errorf("daily_stats: %d ledgers, %d transactions, want %d and %d (%v)", ledgers, txs, len(all), allTxs, err)
	}
}

// A chain that doesn't lead to the live pipeline's first ledger stops the
// backfill at the segment's end.
func TestIntegrationBackfillChainsToLive(t *testing.T) {
	conn := testConn(t)
	ctx := context.Background()
	if _, err := Migrate(ctx, conn, quiet); err != nil {
		t.Fatal(err)
	}
	const from, first = 5_000_000, 5_000_006
	chain := sep54test.Rechain(from, 6)
	stranger := processed(t, sep54test.Rechain(first, 2)) // another chain's ledgers as the live ones
	if _, err := Init(ctx, conn, Pipeline, first-1, stranger[0].Header().PreviousHash); err != nil {
		t.Fatal(err)
	}
	if err := NewSink(conn, SinkOptions{Log: quiet}).Commit(ctx, stranger); err != nil {
		t.Fatal(err)
	}
	_, err := Backfill(ctx, BackfillOptions{Open: func(context.Context) (driver.Conn, error) { return noClose{conn}, nil },
		Source: lakeSource{sep54test.NewMemSource(chain...)}, From: from, To: first - 1, Segment: 1_000_000, Parallel: 1, FirstLedger: first,
		MaxBatchLedgers: 50, MaxBatchBytes: 64 << 20, Passphrase: network.PublicNetworkPassphrase, Log: quiet})
	if err == nil || !strings.Contains(err.Error(), "does not chain to ledger 5000006") {
		t.Fatalf("a foreign chain: %v", err)
	}
	if n := count(t, conn, "SELECT count() FROM ingest_state WHERE startsWith(pipeline, 'done:')"); n != 0 {
		t.Errorf("%d done markers after a broken chain", n)
	}
}

// The trades of ledgers the live pipeline wrote before it knew trades: the
// fill ends just below the pipeline's first trade, writes the trades table
// alone, and a rerun keeps its range.
func TestIntegrationBackfillTradesOnly(t *testing.T) {
	conn := testConn(t)
	ctx := context.Background()
	if _, err := Migrate(ctx, conn, quiet); err != nil {
		t.Fatal(err)
	}
	const first, n, without = 7_000_000, 12, 8
	chain := sep54test.Rechain(first, n)
	live := processed(t, chain)
	if _, err := Init(ctx, conn, Pipeline, first-1, live[0].Header().PreviousHash); err != nil {
		t.Fatal(err)
	}
	if err := NewSink(conn, SinkOptions{Log: quiet}).Commit(ctx, live); err != nil {
		t.Fatal(err)
	}
	// As an older worker left it: no trades for the first ledgers.
	if err := conn.Exec(ctx, "DELETE FROM trades WHERE ledger_sequence < ?", uint32(first+without)); err != nil {
		t.Fatal(err)
	}
	var wantTrades uint64
	for _, p := range live {
		wantTrades += uint64(len(p.Trades))
	}
	txs := count(t, conn, "SELECT count() FROM transactions")

	end, err := TradesFillEnd(ctx, conn, first)
	if err != nil || end != first+without-1 {
		t.Fatalf("fill end %d, %v", end, err)
	}
	o := BackfillOptions{Open: func(context.Context) (driver.Conn, error) { return noClose{conn}, nil }, Source: lakeSource{sep54test.NewMemSource(chain...)},
		From: first, To: end, Segment: 1_000_000, Parallel: 1, FirstLedger: first, TradesOnly: true, MaxBatchLedgers: 3, MaxBatchBytes: 64 << 20,
		Passphrase: network.PublicNetworkPassphrase, Log: quiet}
	if res, err := Backfill(ctx, o); err != nil || res.Done != 1 || res.Ledgers != without {
		t.Fatalf("trades-only: %+v %v", res, err)
	}
	if got := count(t, conn, "SELECT count() FROM trades"); got != wantTrades {
		t.Errorf("%d trades, want %d", got, wantTrades)
	}
	if got := count(t, conn, "SELECT count() FROM (SELECT ledger_sequence, tx_index, op_index, position FROM trades GROUP BY ALL HAVING count() > 1)"); got != 0 {
		t.Errorf("%d duplicate trades", got)
	}
	if got := count(t, conn, "SELECT count() FROM transactions"); got != txs {
		t.Errorf("%d transactions after a trades-only fill, were %d", got, txs)
	}
	if r, err := Check(ctx, conn, Pipeline, first, first+n-1); err != nil || len(r.Problems()) > 0 {
		t.Errorf("the live pipeline's check: %s (%v)", r, err)
	}
	// The fill's end is now its own pipeline's, not the first trade's.
	if again, err := TradesFillEnd(ctx, conn, first); err != nil || again != end {
		t.Errorf("fill end after the fill: %d, %v", again, err)
	}
	// Below the live pipeline's first ledger a trades-only fill is refused.
	o.From = first - 5
	if _, err := Backfill(ctx, o); err == nil || !strings.Contains(err.Error(), "trades-only fill starts at") {
		t.Errorf("a trades-only fill below go-live: %v", err)
	}
}

// noClose shares the test's connection with the backfill's segments.
type noClose struct{ driver.Conn }

func (noClose) Close() error { return nil }
