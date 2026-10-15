// Package statecopy is `worker state-copy` (clickhouse/README.md §10), run once a day on
// the psql host: it copies PostgreSQL's current state into ClickHouse's
// state_* tables (clickhouse/state.sql), computes assets and top_holders
// from the copy (clickhouse/assets.sql), swaps them all in, and appends the
// day's snapshots.
//
// PostgreSQL is read in one REPEATABLE READ transaction, so the copy is the
// state at one ledger: the explorer cursor read in it. Each ClickHouse table
// is filled as <name>_new and swapped in with EXCHANGE TABLES, so readers see
// a whole copy, the last or the next. A run that fails leaves the last copy
// in place; the next one starts over.
package statecopy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/jackc/pgx/v5"
)

// Options configure Run.
type Options struct {
	// DSN reads PostgreSQL: on the psql host, state_reader over the Unix
	// socket with peer authentication.
	DSN string
	// CH writes ClickHouse as stellar_jobs.
	CH driver.Conn
	// Snapshot appends assets and top_holders to the snapshots, dated the
	// day before the run started (UTC): the daily timer runs at 00:15.
	Snapshot bool
	Log      *slog.Logger
	// Now is the clock (tests).
	Now func() time.Time
}

// Result describes a run.
type Result struct {
	Ledger   uint32    // the explorer cursor the copy is as of
	ClosedAt time.Time // when that ledger closed
	Rows     map[string]uint64
	Snapshot time.Time // the snapshots' date; zero without
	Duration time.Duration
}

// Top holders kept per asset, and for XLM.
const (
	TopHolders    = 500
	TopXLMHolders = 1000
)

// batchRows bounds one ClickHouse insert.
const batchRows = 200_000

// kind is how a PostgreSQL column (cast in its query) becomes a ClickHouse one.
type kind uint8

const (
	str  kind = iota // text → String
	i64              // bigint → Int64
	u32              // bigint → UInt32
	u64              // bigint → UInt64
	i128             // text (a numeric) → Int128
)

// copySpec is one state_* table: the PostgreSQL query that reads it, with
// the ClickHouse columns it fills, in order.
type copySpec struct {
	table string
	query string
	cols  []string
	kinds []kind
}

var copies = []copySpec{
	{"state_accounts", `SELECT account_id, balance, buying_liabilities, selling_liabilities, num_subentries::bigint, flags::bigint,
		home_domain, last_modified_ledger FROM explorer.accounts`,
		[]string{"account_id", "balance", "buying_liabilities", "selling_liabilities", "num_subentries", "flags", "home_domain", "last_modified_ledger"},
		[]kind{str, i64, i64, i64, u32, u32, str, u32}},
	{"state_trustlines", `SELECT asset, account_id, balance, trust_limit, flags::bigint, buying_liabilities, selling_liabilities,
		last_modified_ledger FROM explorer.trustlines WHERE liquidity_pool_id IS NULL`,
		[]string{"asset", "account_id", "balance", "trust_limit", "flags", "buying_liabilities", "selling_liabilities", "last_modified_ledger"},
		[]kind{str, str, i64, i64, u32, i64, i64, u32}},
	// Balances of Stellar Asset Contracts, held by contracts; archived
	// (evicted) entries still hold their tokens.
	{"state_sac_balances", `SELECT i.asset, d.contract_id, d.balance_holder, d.balance::text, d.last_modified_ledger
		FROM explorer.contract_data d JOIN explorer.contract_data i ON i.contract_id = d.contract_id AND i.executable = 'stellar_asset'
		WHERE d.balance_holder IS NOT NULL AND i.asset IS NOT NULL`,
		[]string{"asset", "contract_id", "holder", "balance", "last_modified_ledger"},
		[]kind{str, str, str, i128, u32}},
	{"state_asset_contracts", `SELECT asset, contract_id FROM explorer.contract_data WHERE executable = 'stellar_asset' AND asset IS NOT NULL`,
		[]string{"asset", "contract_id"},
		[]kind{str, str}},
	{"state_claimable_balances", `SELECT asset, balance_id, amount, last_modified_ledger FROM explorer.claimable_balances`,
		[]string{"asset", "balance_id", "amount", "last_modified_ledger"},
		[]kind{str, str, i64, u32}},
	{"state_liquidity_pools", `SELECT encode(pool_id, 'hex'), asset_a, asset_b, reserve_a, reserve_b, total_shares, trustlines,
		last_modified_ledger FROM explorer.liquidity_pools`,
		[]string{"pool_id", "asset_a", "asset_b", "reserve_a", "reserve_b", "total_shares", "trustlines", "last_modified_ledger"},
		[]kind{str, str, str, i64, i64, i64, u64, u32}},
}

// Tables lists every table a run replaces, in swap order: the copies, then
// what is computed from them.
func Tables() []string {
	out := make([]string, 0, len(copies)+2)
	for _, c := range copies {
		out = append(out, c.table)
	}
	return append(out, "assets", "top_holders")
}

// Run copies the state, computes the rankings, swaps them in and, with
// Snapshot, appends the snapshots.
func Run(ctx context.Context, o Options) (Result, error) {
	if o.Log == nil {
		o.Log = slog.Default()
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	started := o.Now().UTC()
	res := Result{Rows: map[string]uint64{}}

	conn, err := pgx.Connect(ctx, o.DSN)
	if err != nil {
		return res, fmt.Errorf("connect to postgres: %w", err)
	}
	defer conn.Close(context.WithoutCancel(ctx))
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return res, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var seq int64
	var totalCoins int64
	err = tx.QueryRow(ctx, `SELECT s.ledger_sequence, l.closed_at, l.total_coins FROM public.ingest_state s
		JOIN explorer.ledgers l ON l.sequence = s.ledger_sequence WHERE s.pipeline = 'explorer'`).Scan(&seq, &res.ClosedAt, &totalCoins)
	if errors.Is(err, pgx.ErrNoRows) {
		return res, errors.New("the explorer pipeline has no cursor (or its ledger row) yet: nothing to copy")
	}
	if err != nil {
		return res, fmt.Errorf("read the explorer cursor: %w", err)
	}
	res.Ledger = uint32(seq)
	o.Log.Info("copying the state", "ledger", res.Ledger, "closed_at", res.ClosedAt.UTC())

	for _, c := range Tables() {
		if err := o.CH.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s_new SYNC", c)); err != nil {
			return res, fmt.Errorf("drop %s_new: %w", c, err)
		}
		if err := o.CH.Exec(ctx, fmt.Sprintf("CREATE TABLE %s_new AS %s", c, c)); err != nil {
			return res, fmt.Errorf("create %s_new: %w", c, err)
		}
	}
	for _, c := range copies {
		t := time.Now()
		n, err := copyTable(ctx, tx, o.CH, c)
		if err != nil {
			return res, fmt.Errorf("copy %s: %w", c.table, err)
		}
		res.Rows[c.table] = n
		o.Log.Info("copied", "table", c.table, "rows", n, "duration", time.Since(t).Round(time.Millisecond))
	}
	if err := tx.Commit(ctx); err != nil {
		return res, err
	}

	t := time.Now()
	for _, q := range rankings(res.Ledger, totalCoins, started) {
		if err := o.CH.Exec(ctx, q); err != nil {
			return res, fmt.Errorf("compute the rankings: %w (%.80s)", err, strings.TrimSpace(q))
		}
	}
	for _, c := range []string{"assets", "top_holders"} {
		var n uint64
		if err := o.CH.QueryRow(ctx, "SELECT count() FROM "+c+"_new").Scan(&n); err != nil {
			return res, err
		}
		res.Rows[c] = n
	}
	o.Log.Info("computed assets and top holders", "assets", res.Rows["assets"], "top_holders", res.Rows["top_holders"],
		"duration", time.Since(t).Round(time.Millisecond))

	for _, c := range Tables() {
		if err := o.CH.Exec(ctx, fmt.Sprintf("EXCHANGE TABLES %s_new AND %s", c, c)); err != nil {
			return res, fmt.Errorf("swap %s in: %w", c, err)
		}
	}
	for _, c := range Tables() {
		if err := o.CH.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s_new SYNC", c)); err != nil {
			return res, fmt.Errorf("drop the previous %s: %w", c, err)
		}
	}

	snapshot := time.Time{}
	if o.Snapshot {
		snapshot = time.Date(started.Year(), started.Month(), started.Day()-1, 0, 0, 0, 0, time.UTC)
		if err := appendSnapshots(ctx, o.CH, snapshot); err != nil {
			return res, err
		}
		res.Snapshot = snapshot
		o.Log.Info("appended the snapshots", "date", snapshot.Format(time.DateOnly))
	}
	res.Duration = o.Now().UTC().Sub(started)
	if err := recordRun(ctx, o.CH, started, started.Add(res.Duration), res); err != nil {
		return res, fmt.Errorf("record the run: %w", err)
	}
	return res, nil
}

// recordRun appends the run to state_copy_runs.
func recordRun(ctx context.Context, ch driver.Conn, started, finished time.Time, res Result) error {
	b, err := ch.PrepareBatch(ctx, "INSERT INTO state_copy_runs (started_at, finished_at, ledger, closed_at, rows, snapshot)")
	if err != nil {
		return err
	}
	snapshot := res.Snapshot
	if snapshot.IsZero() {
		snapshot = time.Unix(0, 0).UTC()
	}
	if err := b.Append(started, finished, res.Ledger, res.ClosedAt.UTC(), res.Rows, snapshot); err != nil {
		_ = b.Abort()
		return err
	}
	return b.Send()
}

// copyTable streams one query's rows into <table>_new, batchRows at a time.
func copyTable(ctx context.Context, tx pgx.Tx, ch driver.Conn, c copySpec) (uint64, error) {
	rows, err := tx.Query(ctx, c.query)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	insert := fmt.Sprintf("INSERT INTO %s_new (%s)", c.table, strings.Join(c.cols, ", "))
	cols := newColumns(c.kinds)
	var total uint64
	flush := func() error {
		if cols.n == 0 {
			return nil
		}
		b, err := ch.PrepareBatch(ctx, insert)
		if err != nil {
			return err
		}
		for i, col := range cols.values() {
			if err := b.Column(i).Append(col); err != nil {
				_ = b.Abort()
				return fmt.Errorf("%s: %w", c.cols[i], err)
			}
		}
		if err := b.Send(); err != nil {
			return err
		}
		total += uint64(cols.n)
		cols = newColumns(c.kinds)
		return nil
	}
	dest := make([]any, len(c.kinds))
	for i, k := range c.kinds {
		switch k {
		case str, i128:
			dest[i] = new(string)
		default:
			dest[i] = new(int64)
		}
	}
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return total, err
		}
		if err := cols.add(dest); err != nil {
			return total, err
		}
		if cols.n >= batchRows {
			if err := flush(); err != nil {
				return total, err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return total, err
	}
	return total, flush()
}

// columns buffers rows column by column, in ClickHouse's types.
type columns struct {
	kinds []kind
	strs  map[int][]string
	i64s  map[int][]int64
	u32s  map[int][]uint32
	u64s  map[int][]uint64
	bigs  map[int][]*big.Int
	n     int
}

func newColumns(kinds []kind) *columns {
	return &columns{kinds: kinds, strs: map[int][]string{}, i64s: map[int][]int64{}, u32s: map[int][]uint32{},
		u64s: map[int][]uint64{}, bigs: map[int][]*big.Int{}}
}

func (c *columns) add(dest []any) error {
	for i, k := range c.kinds {
		switch k {
		case str:
			c.strs[i] = append(c.strs[i], *dest[i].(*string))
		case i64:
			c.i64s[i] = append(c.i64s[i], *dest[i].(*int64))
		case u32:
			c.u32s[i] = append(c.u32s[i], uint32(*dest[i].(*int64)))
		case u64:
			c.u64s[i] = append(c.u64s[i], uint64(*dest[i].(*int64)))
		case i128:
			v, ok := new(big.Int).SetString(*dest[i].(*string), 10)
			if !ok || v.BitLen() > 127 {
				return fmt.Errorf("%q is not an Int128", *dest[i].(*string))
			}
			c.bigs[i] = append(c.bigs[i], v)
		}
	}
	c.n++
	return nil
}

func (c *columns) values() []any {
	out := make([]any, len(c.kinds))
	for i, k := range c.kinds {
		switch k {
		case str:
			out[i] = c.strs[i]
		case i64:
			out[i] = c.i64s[i]
		case u32:
			out[i] = c.u32s[i]
		case u64:
			out[i] = c.u64s[i]
		case i128:
			out[i] = c.bigs[i]
		}
	}
	return out
}

// appendSnapshots appends assets and top_holders with the date, replacing
// that date's rows if a run already wrote them.
func appendSnapshots(ctx context.Context, ch driver.Conn, date time.Time) error {
	d := date.Format(time.DateOnly)
	for _, s := range []struct{ table, from string }{{"asset_snapshots", "assets"}, {"holder_snapshots", "top_holders"}} {
		var n uint64
		if err := ch.QueryRow(ctx, fmt.Sprintf("SELECT count() FROM %s WHERE date = toDate('%s')", s.table, d)).Scan(&n); err != nil {
			return fmt.Errorf("%s: %w", s.table, err)
		}
		if n > 0 {
			if err := ch.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE date = toDate('%s')", s.table, d)); err != nil {
				return fmt.Errorf("replace %s of %s: %w", s.table, d, err)
			}
		}
		if err := ch.Exec(ctx, fmt.Sprintf("INSERT INTO %s SELECT toDate('%s') AS date, * FROM %s", s.table, d, s.from)); err != nil {
			return fmt.Errorf("append %s of %s: %w", s.table, d, err)
		}
	}
	return nil
}
