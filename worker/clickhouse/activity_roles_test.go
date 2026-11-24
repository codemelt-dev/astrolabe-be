package clickhouse

import (
	"context"
	"fmt"
	"testing"

	"stellar-explorer/worker/explorer"
)

// The roles the processor sets and those the tables give are the same.
// With the bits taken out, the fill puts back exactly the processor's
// roles, without a row more or less; a rerun does nothing, and without
// Current the month the pipeline writes is left out.
func TestIntegrationActivityRoles(t *testing.T) {
	conn, batch := migrated(t)
	ctx := context.Background()
	if err := NewSink(conn, SinkOptions{Log: quiet}).Commit(ctx, batch); err != nil {
		t.Fatal(err)
	}
	first, last := batch[0].Header().Sequence, batch[len(batch)-1].Header().Sequence
	want := map[string]uint8{}
	var trades, calls int
	for _, p := range batch {
		for _, a := range p.Activity {
			want[fmt.Sprint(a.Account, p.Ledger.Sequence, a.TxIndex)] = a.Roles
			if a.Roles&explorer.RoleTrade != 0 {
				trades++
			}
			if a.Roles&explorer.RoleContractCall != 0 {
				calls++
			}
		}
	}
	if trades == 0 || calls == 0 {
		t.Fatalf("the batch has %d rows with a trade and %d with a contract call", trades, calls)
	}
	check := func(differ bool) {
		t.Helper()
		c, err := CheckActivityRoles(ctx, conn, first, last)
		if err != nil {
			t.Fatal(err)
		}
		if c.Rows != uint64(len(want)) || (c.Differ != 0) != differ {
			t.Fatalf("check: %+v, %d rows", c, len(want))
		}
	}
	check(false)

	if err := conn.Exec(ctx, fmt.Sprintf("ALTER TABLE account_activity UPDATE roles = bitAnd(roles, %d) WHERE 1 SETTINGS mutations_sync = 2", activityBaseRoles)); err != nil {
		t.Fatal(err)
	}
	check(true)
	res, err := FillActivityRoles(ctx, conn, ActivityRolesOptions{Log: quiet})
	if err != nil || res.Skipped == "" || res.Done != res.Months {
		t.Fatalf("fill without the current month: %+v, %v", res, err)
	}
	res, err = FillActivityRoles(ctx, conn, ActivityRolesOptions{Current: true, Log: quiet})
	if err != nil || res.Done != 1 || res.Skipped != "" || res.Rows == 0 {
		t.Fatalf("fill with the current month: %+v, %v", res, err)
	}
	check(false)
	rows, err := conn.Query(ctx, "SELECT account, ledger_sequence, tx_index, roles FROM account_activity")
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]uint8{}
	for rows.Next() {
		var a string
		var seq, tx uint32
		var r uint8
		if err := rows.Scan(&a, &seq, &tx, &r); err != nil {
			t.Fatal(err)
		}
		got[fmt.Sprint(a, seq, tx)] = r
	}
	rows.Close()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("roles after the fill differ from the processor's: %d rows, want %d", len(got), len(want))
	}
	res, err = FillActivityRoles(ctx, conn, ActivityRolesOptions{Current: true, Log: quiet})
	if err != nil || res.Done != 0 || res.DoneBefore != res.Months {
		t.Fatalf("rerun: %+v, %v", res, err)
	}
	if n := count(t, conn, "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name IN (?, ?)", activityRolesTable, activityRolesMerge); n != 0 {
		t.Fatalf("%d helper tables left behind", n)
	}
}
