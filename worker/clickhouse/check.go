package clickhouse

import (
	"context"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// CheckResult is a range's consistency, as `worker clickhouse-check`
// reports it.
type CheckResult struct {
	From, To uint32
	Cursor   uint32
	// Ledgers rows, and distinct sequences among them, in [From, To].
	Ledgers, Distinct uint64
	// Transaction and operation rows against the ledgers' own counts.
	Transactions, TxCount uint64
	Operations, OpCount   uint64
	// AboveCursor counts each table's rows above the cursor: a commit in
	// flight, or what a failed one left (the next commit deletes them).
	AboveCursor map[string]uint64
}

// Problems lists what is inconsistent; none means the range is complete.
func (r CheckResult) Problems() []string {
	var out []string
	if want := uint64(r.To-r.From) + 1; r.Distinct != want {
		out = append(out, fmt.Sprintf("%d of %d ledgers present", r.Distinct, want))
	}
	if r.Ledgers != r.Distinct {
		out = append(out, fmt.Sprintf("%d duplicate ledger rows", r.Ledgers-r.Distinct))
	}
	if r.Transactions != r.TxCount {
		out = append(out, fmt.Sprintf("%d transaction rows, the ledgers count %d", r.Transactions, r.TxCount))
	}
	if r.Operations != r.OpCount {
		out = append(out, fmt.Sprintf("%d operation rows, the ledgers count %d", r.Operations, r.OpCount))
	}
	for t, n := range r.AboveCursor {
		if n > 0 {
			out = append(out, fmt.Sprintf("%s: %d rows above the cursor %d", t, n, r.Cursor))
		}
	}
	return out
}

func (r CheckResult) String() string {
	return fmt.Sprintf("ledgers %d-%d: %d ledgers, %d transactions (header %d), %d operations (header %d); cursor %d",
		r.From, r.To, r.Distinct, r.Transactions, r.TxCount, r.Operations, r.OpCount, r.Cursor) +
		map[bool]string{true: "; ok", false: "; " + strings.Join(r.Problems(), "; ")}[len(r.Problems()) == 0]
}

// Check reads the consistency of [from, to] (at most the cursor).
func Check(ctx context.Context, conn driver.Conn, pipeline string, from, to uint32) (CheckResult, error) {
	r := CheckResult{From: from, To: to, AboveCursor: map[string]uint64{}}
	cur, _, err := ReadCursor(ctx, conn, pipeline)
	if err != nil {
		return r, err
	}
	r.Cursor = cur
	if r.To > cur {
		r.To = cur
	}
	if r.From > r.To {
		return r, fmt.Errorf("nothing to check: [%d, %d] is above the cursor %d", from, to, cur)
	}
	q := func(sql string, dest ...any) error { return conn.QueryRow(ctx, sql, r.From, r.To).Scan(dest...) }
	if err := q("SELECT count(), uniqExact(sequence), sum(tx_count), sum(op_count) FROM ledgers WHERE sequence BETWEEN ? AND ?",
		&r.Ledgers, &r.Distinct, &r.TxCount, &r.OpCount); err != nil {
		return r, fmt.Errorf("ledgers: %w", err)
	}
	if err := q("SELECT count() FROM transactions WHERE ledger_sequence BETWEEN ? AND ?", &r.Transactions); err != nil {
		return r, fmt.Errorf("transactions: %w", err)
	}
	if err := q("SELECT count() FROM operations WHERE ledger_sequence BETWEEN ? AND ?", &r.Operations); err != nil {
		return r, fmt.Errorf("operations: %w", err)
	}
	for _, t := range tables {
		var n uint64
		if err := conn.QueryRow(ctx, fmt.Sprintf("SELECT count() FROM %s WHERE %s > ?", t.name, t.seqCol), cur).Scan(&n); err != nil {
			return r, fmt.Errorf("%s: %w", t.name, err)
		}
		r.AboveCursor[t.name] = n
	}
	return r, nil
}
