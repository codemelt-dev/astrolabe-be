package reindex

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/network"

	"stellar-explorer/worker/explorer"
	"stellar-explorer/worker/explorer/explorertest"
	"stellar-explorer/worker/sep54"
	"stellar-explorer/worker/sep54/sep54test"
	"stellar-explorer/worker/sequencer"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

type namedMem struct {
	*sep54test.MemSource
	name string
}

func (m namedMem) Name() string { return m.name }

// ingested returns a database holding the chain [start, start+n) committed
// by the explorer pipeline, and the chain's source.
func ingested(t *testing.T, start uint32, n int) (*explorertest.DB, namedMem) {
	t.Helper()
	db := explorertest.New(t, explorertest.Options{WithRoles: true})
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, db.OwnerDSN)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := explorer.Migrate(ctx, conn, quiet); err != nil {
		t.Fatal(err)
	}
	conn.Close(ctx)
	db.PostMigrateGrants()

	src := namedMem{sep54test.NewMemSource(sep54test.Rechain(start, n)...), sep54.SourceLake}
	first, err := src.GetLedger(ctx, start)
	if err != nil {
		t.Fatal(err)
	}
	_, h, err := sep54.DecodeLedger(start, first.Body)
	if err != nil {
		t.Fatal(err)
	}
	wconn, err := pgx.Connect(ctx, db.WriterDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer wconn.Close(ctx)
	if err := explorer.SeedCursor(ctx, wconn, explorer.Pipeline, start-1, h.PreviousHash); err != nil {
		t.Fatal(err)
	}
	sink, err := explorer.NewSink(db.WriterDSN, explorer.SinkOptions{Lock: true, Log: quiet})
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	if err := sink.Start(ctx); err != nil {
		t.Fatal(err)
	}
	s, err := sequencer.New(sequencer.Options[*explorer.Prepared]{
		Pipeline: explorer.Pipeline, Sink: sink, Process: explorer.Processor(network.PublicNetworkPassphrase),
		Fixed: &sequencer.Fixed{Source: src, To: start + uint32(n) - 1}, Log: quiet, MaxBatchLedgers: 7,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Run(ctx); err != nil {
		t.Fatal(err)
	}
	return db, src
}

// checksums hashes the ordered content of the history PostgreSQL keeps,
// the ledgers (and the state tables, which reindex never touches), and
// returns the row counts.
func checksums(t *testing.T, db *explorertest.DB) (string, string) {
	t.Helper()
	conn := db.Conn()
	var sums, counts []string
	for _, tbl := range append([]string{"ledgers"}, explorer.StateTableNames()...) {
		var sum string
		var n int64
		q := fmt.Sprintf("SELECT md5(coalesce(string_agg(r::text, '|' ORDER BY r::text), '')), count(*) FROM explorer.%s r", tbl)
		if err := conn.QueryRow(context.Background(), q).Scan(&sum, &n); err != nil {
			t.Fatal(err)
		}
		sums = append(sums, tbl+"="+sum)
		counts = append(counts, fmt.Sprintf("%s=%d", tbl, n))
	}
	return strings.Join(sums, " "), strings.Join(counts, " ")
}

func cursorOf(t *testing.T, db *explorertest.DB, pipeline string) uint32 {
	t.Helper()
	seq, _, err := explorer.ReadCursor(context.Background(), db.Conn(), pipeline)
	if err != nil {
		t.Fatal(err)
	}
	return seq
}

// Reindex replaces rows ledger by ledger: row counts, content and the
// explorer cursor are unchanged, nothing conflicts; a finished range can run
// again; an interrupted one resumes (the M3 acceptance test, hermetic
// version).
func TestReindexLeavesRowsUnchanged(t *testing.T) {
	const start, n = 64_700_000, 30
	db, src := ingested(t, start, n)
	before, counts := checksums(t, db)
	t.Logf("rows: %s", counts)
	ctx := context.Background()
	o := Options{DSN: db.WriterDSN, From: start + 5, To: start + 24, Source: src, Log: quiet, MaxBatchLedgers: 4}
	res, err := Run(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	if res.Ledgers != 20 || res.Conflicts != 0 || res.From != start+5 || res.Pipeline != "reindex:64700005-64700024" {
		t.Fatalf("result %+v", res)
	}
	after, counts2 := checksums(t, db)
	if after != before || counts2 != counts {
		t.Fatalf("reindex changed rows:\n%s\n%s", counts, counts2)
	}
	if c := cursorOf(t, db, explorer.Pipeline); c != start+n-1 {
		t.Fatalf("explorer cursor moved to %d", c)
	}
	if c := cursorOf(t, db, res.Pipeline); c != start+24 {
		t.Fatalf("reindex cursor at %d", c)
	}
	// The same range again.
	if res, err := Run(ctx, o); err != nil || res.Ledgers != 20 {
		t.Fatalf("second run: %+v %v", res, err)
	}
	// Interrupted: the cursor row says 9 ledgers were done; a rerun resumes.
	db.Exec(fmt.Sprintf("UPDATE public.ingest_state SET ledger_sequence = %d, ledger_hash = (SELECT encode(hash, 'hex') FROM explorer.ledgers WHERE sequence = %d) WHERE pipeline = '%s'",
		start+13, start+13, res.Pipeline))
	res, err = Run(ctx, o)
	if err != nil || res.From != start+14 || res.Ledgers != 11 {
		t.Fatalf("resumed run: %+v %v", res, err)
	}
	if after, _ := checksums(t, db); after != before {
		t.Fatal("rows changed")
	}
}

func TestReindexRefuses(t *testing.T) {
	const start, n = 64_700_000, 6
	db, src := ingested(t, start, n)
	ctx := context.Background()
	// Beyond the explorer cursor.
	_, err := Run(ctx, Options{DSN: db.WriterDSN, From: start, To: start + n, Source: src, Log: quiet})
	if err == nil || !strings.Contains(err.Error(), "committed ledgers") {
		t.Fatalf("beyond the cursor: %v", err)
	}
	// Another chain at the same sequences (wrong network / mixed sources).
	other := namedMem{sep54test.NewMemSource(sep54test.Rechain(start+2, 2)...), sep54.SourceAWS}
	_, err = Run(ctx, Options{DSN: db.WriterDSN, From: start + 2, To: start + 3, Source: other, Log: quiet})
	if err == nil || !strings.Contains(err.Error(), "previous hash") {
		t.Fatalf("foreign chain: %v", err)
	}
	before, _ := checksums(t, db)
	// A hole in the source.
	src.Remove(start + 3)
	_, err = Run(ctx, Options{DSN: db.WriterDSN, From: start + 1, To: start + 4, Source: src, Log: quiet})
	if !errors.Is(err, sep54.ErrNotFound) {
		t.Fatalf("hole: %v", err)
	}
	if after, _ := checksums(t, db); after != before {
		t.Fatal("a failed reindex changed rows")
	}
	if _, err := Run(ctx, Options{From: 5, To: 4}); err == nil {
		t.Fatal("bad range accepted")
	}
}

// --dry-run validates without a database and reports every gap and
// invalid ledger.
func TestDryRun(t *testing.T) {
	const start = 1000
	chain := sep54test.Chain(start, 300)
	src := namedMem{sep54test.NewMemSource(chain...), sep54.SourceArchive}
	res, err := Run(context.Background(), Options{From: start, To: start + 299, Source: src, DryRun: true, Log: quiet})
	if err != nil || res.Ledgers != 300 {
		t.Fatalf("clean range: %+v %v", res, err)
	}
	src.Remove(1100)
	src.Remove(1101)
	src.Remove(1250)
	corrupt := chain[150]
	corrupt.Body = append([]byte(nil), corrupt.Body...)
	corrupt.Body[len(corrupt.Body)/2] ^= 0xff
	src.Add(corrupt)
	foreign := sep54test.Chain(1200, 1)[0] // valid alone, breaks the chain
	src.Add(foreign)
	res, err = Run(context.Background(), Options{From: start, To: start + 299, Source: src, DryRun: true, Log: quiet, Workers: 4})
	if err == nil {
		t.Fatal("no error")
	}
	if fmt.Sprint(res.Missing) != "[1100 1101 1250]" || len(res.Invalid) != 3 || res.Ledgers != 296 {
		t.Fatalf("missing %v invalid %v ledgers %d", res.Missing, res.Invalid, res.Ledgers)
	}
	for i, want := range []string{"1150: ", "1200: ", "1201: "} {
		if !strings.HasPrefix(res.Invalid[i], want) {
			t.Fatalf("invalid %v", res.Invalid)
		}
	}
}
