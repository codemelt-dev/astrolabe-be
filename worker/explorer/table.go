package explorer

import (
	"fmt"
	"strings"
)

// table describes one table as the pipeline writes it: the columns
// in the order each row's values() returns them, and the deterministic
// primary key used for ON CONFLICT DO NOTHING. Each entity file
// declares its table next to its row type; the DDL is in the entity's .sql.
type table struct {
	name   string // in schema explorer
	seqCol string // the ledger-sequence column (range deletes)
	key    []string
	cols   []column

	// Built once by init.
	insertUnnest string // INSERT … SELECT * FROM unnest($1::t[], …) ON CONFLICT DO NOTHING
	insertTemp   string // INSERT … SELECT … FROM <temp> ON CONFLICT DO NOTHING
	createTemp   string // CREATE TEMP TABLE <temp> (LIKE …) ON COMMIT DROP
	deleteRange  string // DELETE … WHERE seq BETWEEN $1 AND $2 (reindex)
	colNames     []string
}

type column struct {
	name string
	typ  string // SQL type, for the unnest casts
}

// rowSet is one ledger's rows for one table.
type rowSet interface {
	Len() int
	Values(i int) []any
}

// The history tables, in commit order: PostgreSQL keeps only the ledgers
// (the clickhouse pipeline has the rest). Prepared.rows(i) returns the rows
// of historyTables[i].
var historyTables = []*table{&ledgersTable}

func init() {
	for _, t := range historyTables {
		t.build()
	}
}

func (t *table) qualified() string { return "explorer." + t.name }
func (t *table) tempName() string  { return "tmp_" + t.name }

func (t *table) build() {
	names := make([]string, len(t.cols))
	casts := make([]string, len(t.cols))
	for i, c := range t.cols {
		names[i] = c.name
		casts[i] = fmt.Sprintf("$%d::%s[]", i+1, c.typ)
	}
	t.colNames = names
	cols := strings.Join(names, ", ")
	onConflict := fmt.Sprintf("ON CONFLICT (%s) DO NOTHING", strings.Join(t.key, ", "))
	t.insertUnnest = fmt.Sprintf("INSERT INTO %s (%s) SELECT * FROM unnest(%s) %s",
		t.qualified(), cols, strings.Join(casts, ", "), onConflict)
	t.insertTemp = fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s %s",
		t.qualified(), cols, cols, t.tempName(), onConflict)
	t.createTemp = fmt.Sprintf("CREATE TEMP TABLE %s (LIKE %s) ON COMMIT DROP", t.tempName(), t.qualified())
	t.deleteRange = fmt.Sprintf("DELETE FROM %s WHERE %s BETWEEN $1 AND $2", t.qualified(), t.seqCol)
}

// unnestArgs transposes the rows of a batch into one array per column.
func (t *table) unnestArgs(sets []rowSet) (args []any, n int) {
	for _, s := range sets {
		n += s.Len()
	}
	cols := make([][]any, len(t.cols))
	for j := range cols {
		cols[j] = make([]any, 0, n)
	}
	for _, s := range sets {
		for i := range s.Len() {
			for j, v := range s.Values(i) {
				cols[j] = append(cols[j], v)
			}
		}
	}
	args = make([]any, len(cols))
	for j := range cols {
		args[j] = cols[j]
	}
	return args, n
}

// nullString returns nil for "" so that optional text columns are NULL.
func nullString(s string) any {
	if s == "" {
		return nil
	}
	return s
}
