package statecopy

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"os"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/jackc/pgx/v5"

	"stellar-explorer/worker/clickhouse"
	"stellar-explorer/worker/explorer"
	"stellar-explorer/worker/explorer/explorertest"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// testCH connects to TEST_CLICKHOUSE_URL in a database of its own with the
// clickhouse pipeline's schema, dropped after the test.
func testCH(t *testing.T) driver.Conn {
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
		admin.Close()
	})
	opts.Auth.Database = db
	conn, err := ch.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	if _, err := clickhouse.Migrate(ctx, conn, quiet); err != nil {
		t.Fatal(err)
	}
	return conn
}

func TestRun(t *testing.T) {
	pg := explorertest.New(t, explorertest.Options{})
	chc := testCH(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, pg.DSN)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err := explorer.Migrate(ctx, conn, quiet); err != nil {
		t.Fatal(err)
	}
	const usd = "USD:GISSUER"
	pg.Exec(
		`INSERT INTO public.ingest_state VALUES ('explorer', 100, repeat('ab', 32), now())`,
		`INSERT INTO explorer.ledgers (sequence, hash, prev_hash, closed_at, protocol_version, tx_count, failed_tx_count, op_count,
			successful_op_count, base_fee, base_reserve, max_tx_set_size, total_coins, fee_pool, source)
			VALUES (100, '\x01', '\x02', '2026-10-09 12:00:00+00', 23, 0, 0, 0, 0, 100, 5000000, 1000, 1000000000, 0, 'galexie')`,
		`INSERT INTO explorer.accounts (account_id, balance, seq_num, num_subentries, flags, home_domain, master_weight, threshold_low,
			threshold_medium, threshold_high, signers, last_modified_ledger) VALUES
			('GA', 500, 1, 1, 0, '', 1, 0, 0, 0, '[]', 90), ('GB', 300, 1, 1, 0, '', 1, 0, 0, 0, '[]', 90),
			('GISSUER', 100, 1, 0, 3, 'x.org', 1, 0, 0, 0, '[]', 90)`,
		`INSERT INTO explorer.trustlines (account_id, asset, liquidity_pool_id, balance, trust_limit, flags, last_modified_ledger) VALUES
			('GA', 'USD:GISSUER', NULL, 70, 1000, 1, 90), ('GB', 'USD:GISSUER', NULL, 30, 1000, 1, 90),
			('GC', 'USD:GISSUER', NULL, 0, 1000, 1, 90), ('GA', 'abcd', '\xabcd', 5, 1000, 1, 90)`,
		`INSERT INTO explorer.contract_data (key_hash, contract_id, durability, key_xdr, val_xdr, key, val, balance_holder, balance,
			executable, asset, last_modified_ledger) VALUES
			('\x01', 'CSAC', 'persistent', '\x00', '\x00', '"instance"', '{}', NULL, NULL, 'stellar_asset', 'USD:GISSUER', 90),
			('\x02', 'CSAC', 'persistent', '\x00', '\x00', '{}', '{}', 'CHOLDER', 50, NULL, NULL, 90),
			('\x03', 'CXLM', 'persistent', '\x00', '\x00', '"instance"', '{}', NULL, NULL, 'stellar_asset', 'native', 90),
			('\x04', 'CXLM', 'persistent', '\x00', '\x00', '{}', '{}', 'CHOLDER', 40, NULL, NULL, 90)`,
		`INSERT INTO explorer.claimable_balances VALUES ('B1', 'USD:GISSUER', 5, '[]', 0, NULL, 90)`,
		`INSERT INTO explorer.liquidity_pools VALUES ('\xabcd', 'native', 'USD:GISSUER', 30, 1000, 10, 100, 1, 90)`,
	)
	// A Soroban-only token, created since go-live: minted to GA, half sent to GB.
	if err := chc.Exec(ctx, `INSERT INTO token_transfers (ledger_sequence, closed_at, tx_index, op_index, position, kind, asset, contract_id,
		from_address, to_address, amount) VALUES (99, now(), 1, 0, 0, 'mint', '', 'CTOKEN', '', 'GA', 8),
		(99, now(), 1, 0, 1, 'transfer', '', 'CTOKEN', 'GA', 'GB', 4)`); err != nil {
		t.Fatal(err)
	}
	if err := chc.Exec(ctx, `INSERT INTO contract_creations (contract_id, ledger_sequence, closed_at, tx_index, op_index, creator, via_contract,
		executable, wasm_hash, asset) VALUES ('CTOKEN', 98, now(), 1, 0, 'GA', '', 'wasm', '', '')`); err != nil {
		t.Fatal(err)
	}

	now := func() time.Time { return time.Date(2026, 10, 10, 0, 15, 0, 0, time.UTC) }
	for run := range 2 {
		res, err := Run(ctx, Options{DSN: pg.DSN, CH: chc, Snapshot: true, Log: quiet, Now: now})
		if err != nil {
			t.Fatalf("run %d: %v", run, err)
		}
		if res.Ledger != 100 || res.Rows["state_accounts"] != 3 || res.Rows["state_trustlines"] != 3 || res.Rows["state_sac_balances"] != 2 ||
			res.Snapshot.Format(time.DateOnly) != "2026-10-09" {
			t.Fatalf("run %d: %+v", run, res)
		}
	}

	type asset struct {
		holders, trustlines                         uint64
		supply, inAccounts, inCB, inLP, inContracts *big.Int
		contract, homeDomain                        string
		flags                                       uint32
		complete                                    bool
	}
	read := func(name string) asset {
		var a asset
		var s, ia, icb, ilp, ic big.Int
		err := chc.QueryRow(ctx, `SELECT holders, trustlines, supply, in_accounts, in_claimable_balances, in_liquidity_pools, in_contracts,
			contract_id, home_domain, issuer_flags, complete FROM assets WHERE asset = ?`, name).
			Scan(&a.holders, &a.trustlines, &s, &ia, &icb, &ilp, &ic, &a.contract, &a.homeDomain, &a.flags, &a.complete)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		a.supply, a.inAccounts, a.inCB, a.inLP, a.inContracts = &s, &ia, &icb, &ilp, &ic
		return a
	}
	if a := read(usd); a.holders != 3 || a.trustlines != 3 || a.supply.Int64() != 165 || a.inAccounts.Int64() != 100 || a.inCB.Int64() != 5 ||
		a.inLP.Int64() != 10 || a.inContracts.Int64() != 50 || a.contract != "CSAC" || a.homeDomain != "x.org" || a.flags != 3 || !a.complete {
		t.Errorf("USD: %+v", a)
	}
	if a := read("native"); a.holders != 4 || a.trustlines != 3 || a.supply.Int64() != 1000000000 || a.inAccounts.Int64() != 900 ||
		a.inLP.Int64() != 1000 || a.inContracts.Int64() != 40 || a.contract != "CXLM" {
		t.Errorf("XLM: %+v", a)
	}
	if a := read("CTOKEN"); a.holders != 2 || a.supply.Int64() != 8 || a.inAccounts.Int64() != 8 || !a.complete {
		t.Errorf("CTOKEN: %+v", a)
	}

	holders := func(table, asset string) string {
		rows, err := chc.Query(ctx, fmt.Sprintf("SELECT rank, holder, balance FROM %s WHERE asset = ? ORDER BY rank", table), asset)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var out string
		for rows.Next() {
			var rank uint16
			var holder string
			var balance big.Int
			if err := rows.Scan(&rank, &holder, &balance); err != nil {
				t.Fatal(err)
			}
			out += fmt.Sprintf("%d:%s:%s ", rank, holder, balance.String())
		}
		return out
	}
	if got, want := holders("top_holders", usd), "1:GA:70 2:CHOLDER:50 3:GB:30 "; got != want {
		t.Errorf("USD holders %q, want %q", got, want)
	}
	if got, want := holders("top_holders", "native"), "1:GA:500 2:GB:300 3:GISSUER:100 4:CHOLDER:40 "; got != want {
		t.Errorf("XLM holders %q, want %q", got, want)
	}
	// Two runs on the same day: the second replaced the first's snapshots.
	if got, want := holders("holder_snapshots", usd), "1:GA:70 2:CHOLDER:50 3:GB:30 "; got != want {
		t.Errorf("USD snapshot %q, want %q", got, want)
	}
	var runs, leftovers uint64
	if err := chc.QueryRow(ctx, "SELECT count() FROM state_copy_runs WHERE ledger = 100 AND snapshot = '2026-10-09'").Scan(&runs); err != nil || runs != 2 {
		t.Errorf("state_copy_runs: %d %v", runs, err)
	}
	if err := chc.QueryRow(ctx, "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '%\\_new'").Scan(&leftovers); err != nil || leftovers != 0 {
		t.Errorf("%d _new tables left: %v", leftovers, err)
	}
}
