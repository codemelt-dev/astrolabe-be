package clickhouse

import (
	"math/big"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"

	"stellar-explorer/worker/explorer"
	"stellar-explorer/worker/sep54"
	"stellar-explorer/worker/sep54/sep54test"
	"stellar-explorer/worker/sequencer"
)

func TestMigrations(t *testing.T) {
	all, err := Migrations()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 13 || all[len(all)-1].Name != "assets" {
		t.Fatalf("%d migrations, last %q", len(all), all[len(all)-1].Name)
	}
	stmts := 0
	for _, m := range all {
		for _, s := range Statements(m.SQL) {
			stmts++
			// Run again after a half-applied migration, every statement must
			// be a no-op the second time: a CREATE … IF NOT EXISTS.
			if !regexp.MustCompile(`^CREATE (TABLE|MATERIALIZED VIEW) IF NOT EXISTS `).MatchString(s) {
				t.Errorf("migration %d: not idempotent: %.60s", m.Version, s)
			}
			// Nothing expires (clickhouse/README.md §9.5).
			if strings.Contains(s, "TTL") {
				t.Errorf("migration %d sets a TTL: %.60s", m.Version, s)
			}
			if strings.Contains(s, "stellar.") {
				t.Errorf("migration %d names the database; tables go to the connection's: %.60s", m.Version, s)
			}
		}
	}
	if stmts != 25 { // 22 tables and 2 materialized views, plus daily_stats' refreshable one
		t.Errorf("%d statements, want 25", stmts)
	}
}

func TestStatements(t *testing.T) {
	got := Statements("-- comment\nCREATE TABLE a (x UInt8); -- not the end; really\nCREATE TABLE b\n(\n    y UInt8 -- 0-based; -1 for none\n);\n")
	if len(got) != 2 || !strings.HasPrefix(got[0], "CREATE TABLE a") || !strings.HasSuffix(got[1], ")") {
		t.Fatalf("%q", got)
	}
}

// Every operation type has a distinct snake-case name, Horizon's.
func TestOperationTypeNames(t *testing.T) {
	for n, want := range map[int32]string{0: "create_account", 1: "payment", 2: "path_payment_strict_receive", 3: "manage_sell_offer",
		13: "path_payment_strict_send", 22: "liquidity_pool_deposit", 24: "invoke_host_function", 25: "extend_footprint_ttl", 26: "restore_footprint"} {
		if got := OperationTypeName(n); got != want {
			t.Errorf("type %d: %q, want %q", n, got, want)
		}
	}
	seen := map[string]bool{}
	for n := int32(0); xdr.OperationType(0).ValidEnum(n); n++ {
		name := OperationTypeName(n)
		if seen[name] || !regexp.MustCompile(`^[a-z]+(_[a-z]+)*$`).MatchString(name) {
			t.Errorf("type %d: name %q", n, name)
		}
		seen[name] = true
	}
	if len(seen) < 27 {
		t.Errorf("%d operation types", len(seen))
	}
}

// realBatch processes the real pubnet ledgers of sep54test, history only,
// as the pipeline does.
func realBatch(t testing.TB) []*explorer.Prepared {
	t.Helper()
	process := explorer.HistoryProcessor(network.PublicNetworkPassphrase)
	var out []*explorer.Prepared
	for _, rl := range sep54test.RealLedgers() {
		obj := sep54test.RealObject(rl.Seq)
		lcm, h, err := sep54.DecodeLedger(rl.Seq, obj.Body)
		if err != nil {
			t.Fatal(err)
		}
		p, err := process(sequencer.Ledger{Header: h, LCM: lcm, Source: sep54.SourceArchive, Metadata: obj.Metadata, Size: len(obj.Body)})
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, p)
	}
	if len(out) < 2 {
		t.Fatalf("%d real ledgers", len(out))
	}
	return out
}

// Every table's columns: one per insert column, all of the same length,
// and that length is the batch's row count.
func TestColumnsOfRealLedgers(t *testing.T) {
	batch := realBatch(t)
	want := map[string]int{"ledgers": len(batch)}
	for _, p := range batch {
		want["transactions"] += len(p.Transactions)
		want["operations"] += len(p.Operations)
		want["events"] += len(p.Events)
		want["token_transfers"] += len(p.Transfers)
		want["tx_by_hash"] += len(p.Transactions)
		for _, tx := range p.Transactions {
			if tx.InnerHash != nil {
				want["tx_by_hash"]++
			}
		}
		want["account_activity"] += len(p.Activity)
		want["contract_calls"] += len(p.Calls)
		want["contract_creations"] += len(p.Creations)
		if p.State != nil {
			t.Fatal("history-only processing returned state changes")
		}
	}
	for _, tb := range tables {
		if tb.build == nil {
			continue
		}
		cols, n, err := tb.build(batch)
		if err != nil {
			t.Fatalf("%s: %v", tb.name, err)
		}
		// The fixtures created no contract.
		if n != want[tb.name] || n == 0 && tb.name != "contract_creations" {
			t.Errorf("%s: %d rows, want %d (non-zero)", tb.name, n, want[tb.name])
		}
		if len(cols) != len(tb.cols) {
			t.Fatalf("%s: %d columns for %d names", tb.name, len(cols), len(tb.cols))
		}
		for i, c := range cols {
			if l := reflect.ValueOf(c).Len(); l != n {
				t.Errorf("%s.%s: %d values for %d rows", tb.name, tb.cols[i], l, n)
			}
		}
	}
	// Spot checks: the header's counts, the close time, amounts as Int128.
	cols, _, _ := tables[0].build(batch)
	if got := cols[5].([]uint32)[0]; got != uint32(len(batch[0].Transactions)) {
		t.Errorf("ledgers.tx_count %d, %d transaction rows", got, len(batch[0].Transactions))
	}
	if ct := cols[3].([]time.Time)[0]; ct.Unix() != batch[0].Header().CloseTime {
		t.Errorf("closed_at %s, header close time %d", ct, batch[0].Header().CloseTime)
	}
	// Only fee events go negative: a refund (CAP-67).
	tc, _, _ := tables[4].build(batch)
	refunds := 0
	for i, a := range tc[10].([]*big.Int) {
		if kind := tc[5].([]string)[i]; a.Sign() < 0 {
			if kind != "fee" {
				t.Errorf("negative %s amount %s", kind, a)
			}
			refunds++
		}
	}
	if refunds == 0 {
		t.Error("no fee refund (negative fee) in the real ledgers")
	}
}

// tx_by_hash: each transaction by the first 8 bytes of its hash, a fee
// bump also by its inner hash's; contract_creations: zeros for a Stellar
// Asset Contract's Wasm hash.
func TestDerivedColumns(t *testing.T) {
	inner := xdr.Hash{9, 8, 7, 6, 5, 4, 3, 2, 1}
	p := &explorer.Prepared{
		Ledger: explorer.LedgerRow{Sequence: 70, ClosedAt: time.Unix(1700000000, 0).UTC()},
		Transactions: []explorer.TransactionRow{
			{LedgerSequence: 70, TxIndex: 1, Hash: xdr.Hash{1, 2, 3, 4, 5, 6, 7, 8, 9}},
			{LedgerSequence: 70, TxIndex: 2, Hash: xdr.Hash{2}, InnerHash: &inner},
		},
		Creations: []explorer.ContractCreationRow{
			{TxIndex: 2, ContractID: "CA", Executable: "wasm", WasmHash: inner[:]},
			{TxIndex: 2, ContractID: "CB", Executable: "stellar_asset", Asset: "native"},
		},
	}
	cols, n, err := txByHashColumns([]*explorer.Prepared{p})
	if err != nil || n != 3 {
		t.Fatalf("%d rows, %v", n, err)
	}
	want := []string{string([]byte{1, 2, 3, 4, 5, 6, 7, 8}), string([]byte{2, 0, 0, 0, 0, 0, 0, 0}), string(inner[:8])}
	if !reflect.DeepEqual(cols[0], want) || !reflect.DeepEqual(cols[1], []uint32{70, 70, 70}) || !reflect.DeepEqual(cols[2], []uint32{1, 2, 2}) {
		t.Fatalf("columns %q %v %v", cols[0], cols[1], cols[2])
	}
	cols, n, err = creationColumns([]*explorer.Prepared{p})
	if err != nil || n != 2 {
		t.Fatalf("%d rows, %v", n, err)
	}
	if hashes := cols[8].([]string); hashes[0] != string(inner[:]) || hashes[1] != string(make([]byte, 32)) {
		t.Fatalf("wasm hashes %q", hashes)
	}
	if closed := cols[2].([]time.Time); !closed[0].Equal(p.Ledger.ClosedAt) {
		t.Fatalf("closed_at %v", closed)
	}
}

func TestBadAmountHalts(t *testing.T) {
	batch := realBatch(t)
	for _, p := range batch {
		if len(p.Transfers) > 0 {
			p.Transfers[0].Amount = "not a number"
			if _, _, err := transferColumns(batch); err == nil || !strings.Contains(err.Error(), "not an Int128") {
				t.Fatalf("bad amount: %v", err)
			}
			return
		}
	}
	t.Skip("no transfers in the real ledgers")
}
