package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stellar/go-stellar-sdk/xdr"

	"stellar-explorer/worker/explorer"
)

// The roles RoleTrade and RoleContractCall (explorer/activity.go) came after
// the history was loaded. FillActivityRoles adds them to the rows written
// before, from the operations and trades tables, which hold what the
// processor sets them from: the operations' types and sources, and the
// fills' sellers and buyers.
//
// It rebuilds account_activity one month (partition) at a time, newest
// first. The month's rows and those that set the bits go into
// account_activity_roles_merge, an AggregatingMergeTree that ORs the roles
// of each address and transaction; read with FINAL, it merges them as it
// reads, in little memory however large the month. (A GROUP BY over the
// biggest months spilled hundreds of files and ran out of memory merging
// them.) The merged rows go into account_activity_roles, the month is
// checked to have as many rows as before, and REPLACE PARTITION swaps it in. A month done gets the marker
// "done:activity-roles:<YYYYMM>" in ingest_state, and a rerun skips it.
// The bits only ever add to a row's roles, so a month can be done again.
//
// The month the pipeline is writing is left out unless current is set:
// stop the pipeline first, or its rows of the meantime would be lost.

const (
	activityRolesTable  = "account_activity_roles"
	activityRolesMerge  = "account_activity_roles_merge"
	activityRolesPrefix = "activity-roles:"
	// activityBaseRoles are the roles every row has at least one of.
	activityBaseRoles = explorer.RoleTrade - 1
)

// ActivityRolesOptions are FillActivityRoles' options.
type ActivityRolesOptions struct {
	Current bool // also the month the pipeline is writing
	Log     *slog.Logger
}

// ActivityRolesResult is what a fill did.
type ActivityRolesResult struct {
	Months, DoneBefore, Done int
	Rows                     uint64
	Skipped                  string // the month the pipeline is writing, when left out
}

func opTypes(ts []xdr.OperationType) string {
	s := make([]string, len(ts))
	for i, t := range ts {
		s[i] = fmt.Sprint(int32(t))
	}
	return strings.Join(s, ", ")
}

// activityRolesUnion is every row that gives an address a role in the rows
// that match where: account_activity's own rows (roles as rolesExpr, and
// as stored), and those of the operations and fills that give RoleTrade and
// RoleContractCall. Its columns: account, ledger_sequence, closed_at,
// tx_index, rl (the roles) and stored. Not "roles": ClickHouse would read
// "roles AS stored" as the alias, not the column.
func activityRolesUnion(where, rolesExpr string) string {
	trade, call := opTypes(explorer.TradeOperations), opTypes(explorer.ContractCallOperations)
	return fmt.Sprintf(`SELECT account, ledger_sequence, closed_at, tx_index, %[2]s AS rl, roles AS stored FROM account_activity WHERE %[1]s
UNION ALL
SELECT source_account, ledger_sequence, closed_at, tx_index, toUInt8(if(type IN (%[4]s), %[6]d, %[5]d)), toUInt8(0)
FROM operations WHERE %[1]s AND type IN (%[3]s, %[4]s)
UNION ALL
SELECT seller, ledger_sequence, closed_at, tx_index, toUInt8(%[5]d), toUInt8(0) FROM trades WHERE %[1]s
UNION ALL
SELECT buyer, ledger_sequence, closed_at, tx_index, toUInt8(%[5]d), toUInt8(0) FROM trades WHERE %[1]s`,
		where, rolesExpr, trade, call, explorer.RoleTrade, explorer.RoleContractCall)
}

// activityRolesGrouped is one row per address and transaction that has an
// account_activity row (a base role): its roles from all sources, and the
// roles stored.
func activityRolesGrouped(where, rolesExpr string) string {
	return fmt.Sprintf(`SELECT account, ledger_sequence, max(closed_at) AS closed, tx_index, groupBitOr(rl) AS r, groupBitOr(stored) AS s
FROM (%s)
GROUP BY account, ledger_sequence, tx_index
HAVING bitAnd(r, %d) != 0`, activityRolesUnion(where, rolesExpr), activityBaseRoles)
}

// The fill's settings: a month is up to ~900 million rows from the four
// sources, inserted in blocks of 256 MiB.
const activityRolesSettings = `SETTINGS max_memory_usage = 6000000000, max_threads = 4, max_execution_time = 0,
	min_insert_block_size_rows = 0, min_insert_block_size_bytes = 268435456`

const createActivityRolesMerge = `CREATE TABLE IF NOT EXISTS ` + activityRolesMerge + ` (
	account         String,
	ledger_sequence UInt32,
	closed_at       SimpleAggregateFunction(max, DateTime('UTC')),
	tx_index        UInt32,
	roles           SimpleAggregateFunction(groupBitOr, UInt8)
)
ENGINE = AggregatingMergeTree
ORDER BY (account, ledger_sequence, tx_index)`

func monthWhere(month uint32) string {
	start := time.Date(int(month/100), time.Month(month%100), 1, 0, 0, 0, 0, time.UTC)
	return fmt.Sprintf("closed_at >= toDateTime('%s', 'UTC') AND closed_at < toDateTime('%s', 'UTC')",
		start.Format(time.DateTime), start.AddDate(0, 1, 0).Format(time.DateTime))
}

// FillActivityRoles adds RoleTrade and RoleContractCall to every month of
// account_activity not done yet. It runs as the owner: it creates and drops
// account_activity_roles, and replaces partitions.
func FillActivityRoles(ctx context.Context, conn driver.Conn, o ActivityRolesOptions) (ActivityRolesResult, error) {
	var res ActivityRolesResult
	var months []uint32
	rows, err := conn.Query(ctx, "SELECT DISTINCT toYYYYMM(closed_at) AS m FROM ledgers ORDER BY m DESC")
	if err != nil {
		return res, err
	}
	for rows.Next() {
		var m uint32
		if err := rows.Scan(&m); err != nil {
			rows.Close()
			return res, err
		}
		months = append(months, m)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return res, err
	}
	if len(months) == 0 {
		return res, errors.New("no ledgers")
	}
	if !o.Current {
		res.Skipped = fmt.Sprint(months[0])
		months = months[1:]
	}
	res.Months = len(months)
	if err := conn.Exec(ctx, "CREATE TABLE IF NOT EXISTS "+activityRolesTable+" AS account_activity"); err != nil {
		return res, err
	}
	if err := conn.Exec(ctx, createActivityRolesMerge); err != nil {
		return res, err
	}
	for _, m := range months {
		marker := donePrefix + activityRolesPrefix + fmt.Sprint(m)
		if _, _, err := ReadCursor(ctx, conn, marker); err == nil {
			res.DoneBefore++
			continue
		} else if !errors.Is(err, ErrNoCursor) {
			return res, err
		}
		start := time.Now()
		n, err := fillActivityRolesMonth(ctx, conn, m)
		if err != nil {
			return res, fmt.Errorf("month %d: %w", m, err)
		}
		if err := writeCursor(ctx, conn, marker, 0, xdr.Hash{}); err != nil {
			return res, err
		}
		res.Done++
		res.Rows += n
		o.Log.Info("activity roles", "month", m, "rows", n, "took", time.Since(start).Round(time.Second),
			"months_left", res.Months-res.DoneBefore-res.Done)
	}
	for _, t := range []string{activityRolesTable, activityRolesMerge} {
		if err := conn.Exec(ctx, "DROP TABLE IF EXISTS "+t+" SYNC"); err != nil {
			return res, err
		}
	}
	return res, nil
}

func fillActivityRolesMonth(ctx context.Context, conn driver.Conn, m uint32) (uint64, error) {
	where, part := monthWhere(m), fmt.Sprintf("PARTITION ID '%d'", m)
	countIn := func(table string) (uint64, error) {
		var n uint64
		err := conn.QueryRow(ctx, "SELECT count() FROM "+table+" WHERE "+where).Scan(&n)
		return n, err
	}
	if err := conn.Exec(ctx, "ALTER TABLE "+activityRolesTable+" DROP "+part); err != nil {
		return 0, err
	}
	if err := conn.Exec(ctx, "TRUNCATE TABLE "+activityRolesMerge); err != nil {
		return 0, err
	}
	before, err := countIn("account_activity")
	if err != nil {
		return 0, err
	}
	for _, q := range []string{
		fmt.Sprintf("INSERT INTO %s (account, ledger_sequence, closed_at, tx_index, roles)\nSELECT account, ledger_sequence, closed_at, tx_index, rl FROM (%s)\n%s",
			activityRolesMerge, activityRolesUnion(where, "roles"), activityRolesSettings),
		fmt.Sprintf("INSERT INTO %s (account, ledger_sequence, closed_at, tx_index, roles)\nSELECT account, ledger_sequence, closed_at, tx_index, roles FROM %s FINAL WHERE bitAnd(roles, %d) != 0\n%s",
			activityRolesTable, activityRolesMerge, activityBaseRoles, activityRolesSettings),
	} {
		if err := conn.Exec(ctx, q); err != nil {
			return 0, err
		}
	}
	if err := conn.Exec(ctx, "TRUNCATE TABLE "+activityRolesMerge); err != nil {
		return 0, err
	}
	after, err := countIn("account_activity")
	if err != nil {
		return 0, err
	}
	rebuilt, err := countIn(activityRolesTable)
	if err != nil {
		return 0, err
	}
	if after != before {
		return 0, fmt.Errorf("account_activity went from %d to %d rows during the fill: is the pipeline writing this month? Stop it first", before, after)
	}
	if rebuilt != before {
		return 0, fmt.Errorf("rebuilt %d rows of %d", rebuilt, before)
	}
	if err := conn.Exec(ctx, "ALTER TABLE account_activity REPLACE "+part+" FROM "+activityRolesTable); err != nil {
		return 0, err
	}
	return before, conn.Exec(ctx, "ALTER TABLE "+activityRolesTable+" DROP "+part)
}

// ActivityRolesCheck is what CheckActivityRoles found.
type ActivityRolesCheck struct {
	From, To uint32
	Rows     uint64 // account_activity's rows in the range
	Differ   uint64 // rows whose roles differ from what the tables give
	Example  string // one of them
}

// CheckActivityRoles compares the roles stored for the ledgers [from, to]
// with those the operations and trades tables give: on ledgers the
// processor wrote with RoleTrade and RoleContractCall, they must be equal.
func CheckActivityRoles(ctx context.Context, conn driver.Conn, from, to uint32) (ActivityRolesCheck, error) {
	c := ActivityRolesCheck{From: from, To: to}
	var first, last time.Time
	if err := conn.QueryRow(ctx, "SELECT min(closed_at), max(closed_at) FROM ledgers WHERE sequence BETWEEN ? AND ?", from, to).Scan(&first, &last); err != nil {
		return c, err
	}
	// closed_at lets ClickHouse skip the other months.
	where := fmt.Sprintf("ledger_sequence BETWEEN %d AND %d AND closed_at BETWEEN toDateTime('%s', 'UTC') AND toDateTime('%s', 'UTC')",
		from, to, first.UTC().Format(time.DateTime), last.UTC().Format(time.DateTime))
	q := fmt.Sprintf(`SELECT count(), countIf(r != s), anyIf(concat(account, ' ledger ', toString(ledger_sequence), ' tx ', toString(tx_index),
	': stored ', toString(s), ', from the tables ', toString(r)), r != s)
FROM (%s) SETTINGS max_memory_usage = 6000000000, max_bytes_before_external_group_by = 2000000000, max_threads = 4`,
		activityRolesGrouped(where, fmt.Sprintf("bitAnd(roles, %d)", activityBaseRoles)))
	err := conn.QueryRow(ctx, q).Scan(&c.Rows, &c.Differ, &c.Example)
	return c, err
}
