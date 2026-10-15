package clickhouse

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stellar/go-stellar-sdk/xdr"

	"stellar-explorer/worker/explorer"
	"stellar-explorer/worker/sequencer"
)

// testConn connects to the ClickHouse of TEST_CLICKHOUSE_URL
// (clickhouse://user:password@host:9000/default, for example through an SSH
// tunnel to the clickhouse host) in a database of its own, dropped after
// the test. Without the variable the test is skipped.
func testConn(t *testing.T) driver.Conn {
	t.Helper()
	url := os.Getenv("TEST_CLICKHOUSE_URL")
	if url == "" {
		t.Skip("TEST_CLICKHOUSE_URL is not set")
	}
	opts, err := ch.ParseDSN(url)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	admin, err := ch.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	db := fmt.Sprintf("stellar_test_%d", time.Now().UnixNano())
	if err := admin.Exec(ctx, "CREATE DATABASE "+db); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db+" SYNC")
		_ = admin.Close()
	})
	opts.Auth.Database = db
	conn, err := ch.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// migrated returns a test database with the schema and the cursor seeded
// before the real batch.
func migrated(t *testing.T) (driver.Conn, []*explorer.Prepared) {
	t.Helper()
	conn := testConn(t)
	ctx := context.Background()
	if _, err := Migrate(ctx, conn, quiet); err != nil {
		t.Fatal(err)
	}
	batch := realBatch(t)
	h := batch[0].Header()
	if created, err := Init(ctx, conn, Pipeline, h.Sequence-1, h.PreviousHash); err != nil || !created {
		t.Fatalf("Init: %v, %v", created, err)
	}
	return conn, batch
}

func checkClean(t *testing.T, conn driver.Conn, batch []*explorer.Prepared) {
	t.Helper()
	first, last := batch[0].Header().Sequence, batch[len(batch)-1].Header().Sequence
	r, err := Check(context.Background(), conn, Pipeline, first, last)
	if err != nil {
		t.Fatal(err)
	}
	if p := r.Problems(); len(p) > 0 || r.Transactions == 0 || r.Cursor != last {
		t.Fatalf("check: %s", r)
	}
}

func TestIntegrationMigrateTwice(t *testing.T) {
	conn := testConn(t)
	ctx := context.Background()
	applied, err := Migrate(ctx, conn, quiet)
	if err != nil || len(applied) != 13 {
		t.Fatalf("first migrate: %d applied, %v", len(applied), err)
	}
	if applied, err := Migrate(ctx, conn, quiet); err != nil || len(applied) != 0 {
		t.Fatalf("second migrate: %d applied, %v", len(applied), err)
	}
	if p, err := Pending(ctx, conn); err != nil || len(p) != 0 {
		t.Fatalf("pending: %d, %v", len(p), err)
	}
}

func TestIntegrationCommit(t *testing.T) {
	conn, batch := migrated(t)
	ctx := context.Background()
	s := NewSink(conn, SinkOptions{Log: quiet})
	if err := s.Commit(ctx, batch); err != nil {
		t.Fatal(err)
	}
	checkClean(t, conn, batch)
	// transfers_by_account holds every non-empty side of every transfer.
	var sides, byAccount uint64
	for _, p := range batch {
		for _, tr := range p.Transfers {
			for _, a := range []string{tr.From, tr.To} {
				if a != "" {
					sides++
				}
			}
		}
	}
	if err := conn.QueryRow(ctx, "SELECT count() FROM transfers_by_account").Scan(&byAccount); err != nil || byAccount != sides {
		t.Fatalf("transfers_by_account: %d rows, want %d (%v)", byAccount, sides, err)
	}
	// A transaction by its hash: the 8-byte key, then the whole hash.
	var feeBump *explorer.TransactionRow
	for _, tx := range batch[1].Transactions {
		if tx.InnerHash != nil {
			feeBump = &tx
			break
		}
	}
	const byHash = `SELECT ledger_sequence, tx_index FROM transactions
		WHERE (ledger_sequence, tx_index) IN (SELECT ledger_sequence, tx_index FROM tx_by_hash WHERE prefix = substring(?, 1, 8))
		AND (hash = ? OR inner_hash = ?)`
	for _, h := range []xdr.Hash{feeBump.Hash, *feeBump.InnerHash} {
		var seq, idx uint32
		if err := conn.QueryRow(ctx, byHash, string(h[:]), string(h[:]), string(h[:])).Scan(&seq, &idx); err != nil ||
			seq != feeBump.LedgerSequence || idx != feeBump.TxIndex {
			t.Fatalf("by hash %x: %d/%d %v", h, seq, idx, err)
		}
	}
	other := feeBump.Hash
	other[31]++ // same 8 bytes, another hash: nothing
	if err := conn.QueryRow(ctx, byHash, string(other[:]), string(other[:]), string(other[:])).Scan(new(uint32), new(uint32)); err == nil {
		t.Fatal("a hash sharing the first 8 bytes found a transaction")
	}
	// account_activity and contract_calls hold the batch's rows.
	for table, want := range map[string]int{"account_activity": 0, "contract_calls": 0} {
		for _, p := range batch {
			want += map[string]int{"account_activity": len(p.Activity), "contract_calls": len(p.Calls)}[table]
		}
		var n uint64
		if err := conn.QueryRow(ctx, "SELECT count() FROM "+table).Scan(&n); err != nil || n != uint64(want) {
			t.Fatalf("%s: %d rows, want %d (%v)", table, n, want, err)
		}
	}
	// daily_stats: a refresh computes the batch's day from the history.
	if err := conn.Exec(ctx, "SYSTEM REFRESH VIEW daily_stats_mv"); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, "SYSTEM WAIT VIEW daily_stats_mv"); err != nil {
		t.Fatal(err)
	}
	var ledgers, txs, calls uint64
	var types map[string]uint64
	if err := conn.QueryRow(ctx, "SELECT sum(ledgers), sum(transactions), sum(contract_calls), sumMap(operations_by_type) FROM daily_stats FINAL").
		Scan(&ledgers, &txs, &calls, &types); err != nil {
		t.Fatal(err)
	}
	var wantTxs, wantCalls uint64
	for _, p := range batch {
		wantTxs += uint64(len(p.Transactions))
		wantCalls += uint64(len(p.Calls))
	}
	if ledgers != uint64(len(batch)) || txs != wantTxs || calls != wantCalls || types["invoke_host_function"] == 0 {
		t.Fatalf("daily_stats: %d ledgers, %d transactions, %d calls, %v", ledgers, txs, calls, types)
	}
	// The same batch again (a retry after an ambiguous cursor write): no-op.
	if err := NewSink(conn, SinkOptions{Log: quiet}).Commit(ctx, batch); err != nil {
		t.Fatalf("recommit: %v", err)
	}
	checkClean(t, conn, batch)
}

// A run that stopped after writing rows but before the cursor: the next
// sink deletes the rows above the cursor and writes the batch once.
func TestIntegrationRecoversAfterACrash(t *testing.T) {
	conn, batch := migrated(t)
	ctx := context.Background()
	if err := NewSink(conn, SinkOptions{Log: quiet}).write(ctx, batch, batch[0].Header().Sequence); err != nil {
		t.Fatal(err)
	}
	if err := NewSink(conn, SinkOptions{Log: quiet}).Commit(ctx, batch); err != nil {
		t.Fatal(err)
	}
	checkClean(t, conn, batch)
}

func TestIntegrationHashMismatchHalts(t *testing.T) {
	conn := testConn(t)
	ctx := context.Background()
	if _, err := Migrate(ctx, conn, quiet); err != nil {
		t.Fatal(err)
	}
	batch := realBatch(t)
	if _, err := Init(ctx, conn, Pipeline, batch[0].Header().Sequence-1, xdr.Hash{1}); err != nil {
		t.Fatal(err)
	}
	if err := NewSink(conn, SinkOptions{Log: quiet}).Commit(ctx, batch); !sequencer.IsHalt(err) {
		t.Fatalf("commit after a foreign cursor hash: %v, want a halt", err)
	}
}
