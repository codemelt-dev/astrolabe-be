package explorer

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// Current state: accounts, trustlines, data_entries, offers,
// claimable_balances, liquidity_pools, contract_data, contract_code and ttl.
//
//   - Order: state is applied per ledger, in ledger order. Each ledger's
//     changes go through their own ChangeCompactor (never across ledgers),
//     then that ledger's evictions (lcm.EvictedLedgerKeys(), which the change
//     reader never yields) follow.
//   - Changes: CREATED, UPDATED and RESTORED upsert the full post-state with
//     archived = false, guarded by last_modified_ledger; REMOVED deletes
//     (0 rows is fine).
//   - Evictions: a TTL key deletes the ttl row, TEMPORARY contract data is
//     deleted, PERSISTENT contract data and contract code get
//     archived = true. An eviction that affects 0 rows is state drift and
//     raises an alert.
//   - Batches: the per-ledger results are merged last-op-wins per key, so
//     each key appears once per statement.
//
// The state tables are maintained only once they were loaded from a
// checkpoint (explorer.state_checkpoint has its row): after a
// `bootstrap --history-only` (dev and tests) they stay empty and untouched.

// stateTable is one current-state table: its columns in the order the
// row builders return them, and its identity (primary key).
type stateTable struct {
	table
	// archivable tables have an `archived` column (contract data and code).
	archivable bool

	keyIdx     []int  // positions of the key columns in cols
	archiveCol int    // position of `archived` (archivable only)
	upsertSQL  string // INSERT … unnest … ON CONFLICT DO UPDATE … WHERE last_modified_ledger <= EXCLUDED
	deleteSQL  string // DELETE … USING unnest(keys)
	archiveSQL string // UPDATE … SET archived = true FROM unnest(keys)
}

// stateTables in apply order.
var stateTables = []*stateTable{&accountsTable, &trustlinesTable, &dataEntriesTable, &offersTable, &claimableBalancesTable,
	&liquidityPoolsTable, &contractDataTable, &contractCodeTable, &ttlTable}

// StateTableNames are the state tables, in the order TRUNCATE and the
// checkpoint load use.
func StateTableNames() []string {
	names := make([]string, len(stateTables))
	for i, t := range stateTables {
		names[i] = t.name
	}
	return names
}

func init() {
	for _, t := range stateTables {
		t.buildState()
	}
}

func (t *stateTable) buildState() {
	t.build()
	t.archiveCol = -1
	var keyCasts, keyNames, keyMatch, updates []string
	for _, k := range t.key {
		for i, c := range t.cols {
			if c.name == k {
				t.keyIdx = append(t.keyIdx, i)
				keyCasts = append(keyCasts, fmt.Sprintf("$%d::%s[]", len(t.keyIdx), c.typ))
				keyNames = append(keyNames, c.name)
				keyMatch = append(keyMatch, fmt.Sprintf("t.%s = k.%s", c.name, c.name))
			}
		}
	}
	if len(t.keyIdx) != len(t.key) {
		panic("state table " + t.name + ": key column missing from cols")
	}
	for i, c := range t.cols {
		if c.name == "archived" {
			t.archiveCol = i
		}
		if !slices.Contains(t.key, c.name) {
			updates = append(updates, fmt.Sprintf("%s = EXCLUDED.%s", c.name, c.name))
		}
	}
	if t.archivable != (t.archiveCol >= 0) {
		panic("state table " + t.name + ": archivable does not match its columns")
	}
	casts := make([]string, len(t.cols))
	for i, c := range t.cols {
		casts[i] = fmt.Sprintf("$%d::%s[]", i+1, c.typ)
	}
	t.upsertSQL = fmt.Sprintf("INSERT INTO %s AS t (%s) SELECT * FROM unnest(%s) ON CONFLICT (%s) DO UPDATE SET %s WHERE t.last_modified_ledger <= EXCLUDED.last_modified_ledger",
		t.qualified(), strings.Join(t.colNames, ", "), strings.Join(casts, ", "), strings.Join(t.key, ", "), strings.Join(updates, ", "))
	t.deleteSQL = fmt.Sprintf("DELETE FROM %s t USING unnest(%s) AS k(%s) WHERE %s",
		t.qualified(), strings.Join(keyCasts, ", "), strings.Join(keyNames, ", "), strings.Join(keyMatch, " AND "))
	if t.archivable {
		t.archiveSQL = fmt.Sprintf("UPDATE %s t SET archived = true FROM unnest(%s) AS k(%s) WHERE %s",
			t.qualified(), strings.Join(keyCasts, ", "), strings.Join(keyNames, ", "), strings.Join(keyMatch, " AND "))
	}
}

// stateOp is what a change does to its row.
type stateOp uint8

const (
	// opUpsert writes the full post-state (CREATED, UPDATED, RESTORED).
	opUpsert stateOp = iota
	// opRemove deletes the row (REMOVED); 0 rows affected is fine.
	opRemove
	// opEvictDelete deletes an evicted TTL or temporary entry; the row must
	// exist.
	opEvictDelete
	// opEvictArchive sets archived = true on an evicted persistent entry;
	// the row must exist.
	opEvictArchive
)

func (o stateOp) String() string {
	return [...]string{"upsert", "remove", "evict_delete", "evict_archive"}[o]
}

// StateChange is one change to one row of a state table.
type StateChange struct {
	table *stateTable
	op    stateOp
	id    string // table-local identity, for merging
	key   []any  // key column values, in key order
	vals  []any  // the full row (opUpsert only)
	// Ledger is where the change happened.
	Ledger uint32
}

// Table returns the state table the change applies to.
func (c StateChange) Table() string { return c.table.name }

// Op returns upsert, remove, evict_delete or evict_archive.
func (c StateChange) Op() string { return c.op.String() }

func (c StateChange) size() int64 {
	n := int64(64 + len(c.id))
	for _, v := range c.vals {
		switch v := v.(type) {
		case string:
			n += int64(len(v))
		case []byte:
			n += int64(len(v))
		default:
			n += 8
		}
	}
	return n
}

// identity encodes key values as a map key.
func identity(key []any) string {
	var b strings.Builder
	for i, v := range key {
		if i > 0 {
			b.WriteByte(0)
		}
		switch v := v.(type) {
		case string:
			b.WriteString(v)
		case []byte:
			b.Write(v)
		default:
			fmt.Fprint(&b, v)
		}
	}
	return b.String()
}

func newChange(t *stateTable, op stateOp, ledger uint32, key, vals []any) StateChange {
	return StateChange{table: t, op: op, id: identity(key), key: key, vals: vals, Ledger: ledger}
}

// upsertChange builds the upsert of a post-state entry, or reports false
// for entry types without a state table (config settings).
func upsertChange(e xdr.LedgerEntry, ledger uint32, passphrase string) (StateChange, bool, error) {
	var t *stateTable
	var vals []any
	var err error
	switch e.Data.Type {
	case xdr.LedgerEntryTypeAccount:
		t = &accountsTable
		vals, err = accountValues(e)
	case xdr.LedgerEntryTypeTrustline:
		t = &trustlinesTable
		vals, err = trustlineValues(e)
	case xdr.LedgerEntryTypeData:
		t = &dataEntriesTable
		vals, err = dataValues(e)
	case xdr.LedgerEntryTypeOffer:
		t = &offersTable
		vals, err = offerValues(e)
	case xdr.LedgerEntryTypeClaimableBalance:
		t = &claimableBalancesTable
		vals, err = claimableBalanceValues(e)
	case xdr.LedgerEntryTypeLiquidityPool:
		t = &liquidityPoolsTable
		vals, err = liquidityPoolValues(e)
	case xdr.LedgerEntryTypeContractData:
		t = &contractDataTable
		vals, err = contractDataValues(e, passphrase)
	case xdr.LedgerEntryTypeContractCode:
		t = &contractCodeTable
		vals, err = contractCodeValues(e)
	case xdr.LedgerEntryTypeTtl:
		t = &ttlTable
		vals, err = ttlValues(e)
	default:
		return StateChange{}, false, nil
	}
	if err != nil {
		return StateChange{}, false, fmt.Errorf("%s entry: %w", t.name, err)
	}
	key := make([]any, len(t.keyIdx))
	for i, idx := range t.keyIdx {
		key[i] = vals[idx]
	}
	return newChange(t, opUpsert, ledger, key, vals), true, nil
}

// keyChange builds a removal or an eviction of a ledger key, or reports
// false for key types without a state table.
func keyChange(k xdr.LedgerKey, ledger uint32, evicted bool) (StateChange, bool, error) {
	var t *stateTable
	var key []any
	op := opRemove
	switch k.Type {
	case xdr.LedgerEntryTypeAccount:
		id, err := accountKey(k.MustAccount())
		if err != nil {
			return StateChange{}, false, err
		}
		t, key = &accountsTable, []any{id}
	case xdr.LedgerEntryTypeTrustline:
		tk, err := trustlineKey(k.MustTrustLine())
		if err != nil {
			return StateChange{}, false, err
		}
		t, key = &trustlinesTable, tk
	case xdr.LedgerEntryTypeData:
		dk, err := dataKey(k.MustData())
		if err != nil {
			return StateChange{}, false, err
		}
		t, key = &dataEntriesTable, dk
	case xdr.LedgerEntryTypeOffer:
		t, key = &offersTable, []any{int64(k.MustOffer().OfferId)}
	case xdr.LedgerEntryTypeClaimableBalance:
		id, err := k.MustClaimableBalance().BalanceId.EncodeToStrkey()
		if err != nil {
			return StateChange{}, false, err
		}
		t, key = &claimableBalancesTable, []any{id}
	case xdr.LedgerEntryTypeLiquidityPool:
		id := k.MustLiquidityPool().LiquidityPoolId
		t, key = &liquidityPoolsTable, []any{id[:]}
	case xdr.LedgerEntryTypeContractData, xdr.LedgerEntryTypeContractCode:
		kh, err := keyHash(k)
		if err != nil {
			return StateChange{}, false, err
		}
		t, key = &contractCodeTable, []any{kh}
		if k.Type == xdr.LedgerEntryTypeContractData {
			t = &contractDataTable
		}
		if evicted {
			op = opEvictArchive
			if k.Type == xdr.LedgerEntryTypeContractData && k.MustContractData().Durability == xdr.ContractDataDurabilityTemporary {
				op = opEvictDelete
			}
		}
	case xdr.LedgerEntryTypeTtl:
		kh := k.MustTtl().KeyHash
		t, key = &ttlTable, []any{kh[:]}
		if evicted {
			op = opEvictDelete
		}
	default:
		return StateChange{}, false, nil
	}
	if evicted && op == opRemove {
		// Only Soroban entries are ever evicted; anything else would be a
		// protocol change worth noticing, but deleting it is the closest
		// meaning.
		op = opEvictDelete
	}
	return newChange(t, op, ledger, key, nil), true, nil
}

// stateChangesOf extracts the state changes of one ledger: its
// changes compacted by a ChangeCompactor of its own, then its evictions.
// The result is sorted by table and key, so it is deterministic.
func stateChangesOf(passphrase string, lcm xdr.LedgerCloseMeta) ([]StateChange, error) {
	seq := lcm.LedgerSequence()
	reader, err := ingest.NewLedgerChangeReaderFromLedgerCloseMeta(passphrase, lcm)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	compactor := ingest.NewChangeCompactor(ingest.ChangeCompactorConfig{SuppressRemoveAfterRestoreChange: false})
	for {
		ch, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read changes: %w", err)
		}
		if err := compactor.AddChange(ch); err != nil {
			return nil, fmt.Errorf("compact changes: %w", err)
		}
	}
	var out []StateChange
	for _, ch := range compactor.GetChanges() {
		var c StateChange
		var ok bool
		switch ch.ChangeType {
		case xdr.LedgerEntryChangeTypeLedgerEntryCreated, xdr.LedgerEntryChangeTypeLedgerEntryUpdated,
			xdr.LedgerEntryChangeTypeLedgerEntryRestored:
			c, ok, err = upsertChange(*ch.Post, seq, passphrase)
		case xdr.LedgerEntryChangeTypeLedgerEntryRemoved:
			var k xdr.LedgerKey
			if k, err = ch.Pre.LedgerKey(); err == nil {
				c, ok, err = keyChange(k, seq, false)
			}
		default:
			err = fmt.Errorf("unexpected change type %v", ch.ChangeType)
		}
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, c)
		}
	}
	sortChanges(out)
	evicted, err := lcm.EvictedLedgerKeys()
	if err != nil {
		return nil, err
	}
	var evictions []StateChange
	for _, k := range evicted {
		c, ok, err := keyChange(k, seq, true)
		if err != nil {
			return nil, fmt.Errorf("evicted key: %w", err)
		}
		if ok {
			evictions = append(evictions, c)
		}
	}
	sortChanges(evictions)
	return append(out, evictions...), nil
}

func sortChanges(cs []StateChange) {
	order := func(t *stateTable) int { return slices.Index(stateTables, t) }
	slices.SortStableFunc(cs, func(a, b StateChange) int {
		if c := cmp.Compare(order(a.table), order(b.table)); c != 0 {
			return c
		}
		return strings.Compare(a.id, b.id)
	})
}

// mergeState merges the state changes of consecutive ledgers, in order,
// into one change per key ("last-op-wins"). Where the last op
// alone would lose information, the merge keeps it:
//
//   - an eviction of an entry written earlier in the batch: TTL and
//     temporary entries become a plain removal (the row may not exist in
//     the database yet, so 0 rows is no drift); persistent entries become
//     the upsert of the written row with archived = true;
//   - any other op replaces the previous one.
func mergeState(ledgers [][]StateChange) []StateChange {
	index := map[*stateTable]map[string]int{}
	var out []StateChange
	for _, changes := range ledgers {
		for _, c := range changes {
			m := index[c.table]
			if m == nil {
				m = map[string]int{}
				index[c.table] = m
			}
			i, seen := m[c.id]
			if !seen {
				m[c.id] = len(out)
				out = append(out, c)
				continue
			}
			prev := out[i]
			switch {
			case c.op == opEvictDelete && prev.op == opUpsert:
				c.op = opRemove
			case c.op == opEvictArchive && prev.op == opUpsert:
				vals := slices.Clone(prev.vals)
				vals[c.table.archiveCol] = true
				c = StateChange{table: c.table, op: opUpsert, id: c.id, key: prev.key, vals: vals, Ledger: c.Ledger}
			}
			out[i] = c
		}
	}
	return out
}

// stateChunk bounds the rows of one statement (one array parameter per
// column).
const stateChunk = 20_000

// stateDrift describes evictions that found no row.
type stateDrift struct {
	table    string
	op       stateOp
	expected int64
	affected int64
	first    uint32 // first ledger involved
}

// applyState writes merged changes in tx, one statement per table, op and
// chunk, pipelined. It returns the evictions that affected fewer rows than
// keys (state drift).
func applyState(ctx context.Context, tx pgx.Tx, changes []StateChange) ([]stateDrift, error) {
	type group struct {
		t   *stateTable
		op  stateOp
		cs  []StateChange
		min uint32
	}
	var groups []*group
	byKey := map[[2]any]*group{}
	for _, c := range changes {
		k := [2]any{c.table, c.op}
		g := byKey[k]
		if g == nil {
			g = &group{t: c.table, op: c.op, min: c.Ledger}
			byKey[k] = g
			groups = append(groups, g)
		}
		g.cs = append(g.cs, c)
		g.min = min(g.min, c.Ledger)
	}
	type queued struct {
		g *group
		n int
	}
	b := &pgx.Batch{}
	var qs []queued
	for _, g := range groups {
		for start := 0; start < len(g.cs); start += stateChunk {
			chunk := g.cs[start:min(start+stateChunk, len(g.cs))]
			switch g.op {
			case opUpsert:
				b.Queue(g.t.upsertSQL, columnArrays(len(g.t.cols), chunk, func(c StateChange) []any { return c.vals })...)
			case opRemove, opEvictDelete:
				b.Queue(g.t.deleteSQL, columnArrays(len(g.t.keyIdx), chunk, func(c StateChange) []any { return c.key })...)
			case opEvictArchive:
				b.Queue(g.t.archiveSQL, columnArrays(len(g.t.keyIdx), chunk, func(c StateChange) []any { return c.key })...)
			}
			qs = append(qs, queued{g, len(chunk)})
		}
	}
	if len(qs) == 0 {
		return nil, nil
	}
	res := tx.SendBatch(ctx, b)
	drift := map[*group]*stateDrift{}
	var drifts []stateDrift
	for _, q := range qs {
		tag, err := res.Exec()
		if err != nil {
			_ = res.Close()
			return nil, fmt.Errorf("state %s %s: %w", q.g.t.name, q.g.op, err)
		}
		if q.g.op != opEvictDelete && q.g.op != opEvictArchive {
			continue
		}
		d := drift[q.g]
		if d == nil {
			d = &stateDrift{table: q.g.t.name, op: q.g.op, first: q.g.min}
			drift[q.g] = d
		}
		d.expected += int64(q.n)
		d.affected += tag.RowsAffected()
	}
	if err := res.Close(); err != nil {
		return nil, err
	}
	for _, g := range groups {
		if d := drift[g]; d != nil && d.affected < d.expected {
			drifts = append(drifts, *d)
		}
	}
	return drifts, nil
}

// columnArrays transposes rows into one array per column.
func columnArrays(ncols int, cs []StateChange, row func(StateChange) []any) []any {
	cols := make([][]any, ncols)
	for j := range cols {
		cols[j] = make([]any, len(cs))
	}
	for i, c := range cs {
		for j, v := range row(c) {
			cols[j][i] = v
		}
	}
	args := make([]any, ncols)
	for j := range cols {
		args[j] = cols[j]
	}
	return args
}

// ---------------------------------------------------------- checkpoint load

// StateLoader writes the entries of a checkpoint into the state tables
// with COPY, inside the caller's transaction. Entries
// of other types are skipped. Rows are buffered per table and copied in
// chunks, so memory stays bounded.
type StateLoader struct {
	tx         pgx.Tx
	passphrase string
	buf        map[*stateTable][][]any
	counts     map[string]int64
	chunk      int
}

// NewStateLoader returns a loader writing in tx for the given network.
func NewStateLoader(tx pgx.Tx, passphrase string) *StateLoader {
	return &StateLoader{tx: tx, passphrase: passphrase, buf: map[*stateTable][][]any{}, counts: map[string]int64{}, chunk: 10_000}
}

// TruncateState empties every state table (bootstrap and --force replace
// them as a whole). It needs TRUNCATE on the state tables,
// which ingest_writer has.
func TruncateState(ctx context.Context, tx pgx.Tx) error {
	names := make([]string, len(stateTables))
	for i, t := range stateTables {
		names[i] = t.qualified()
	}
	_, err := tx.Exec(ctx, "TRUNCATE "+strings.Join(names, ", "))
	return err
}

// Add buffers one checkpoint entry.
func (l *StateLoader) Add(ctx context.Context, e xdr.LedgerEntry) error {
	c, ok, err := upsertChange(e, uint32(e.LastModifiedLedgerSeq), l.passphrase)
	if err != nil || !ok {
		return err
	}
	l.buf[c.table] = append(l.buf[c.table], c.vals)
	if len(l.buf[c.table]) >= l.chunk {
		return l.flush(ctx, c.table)
	}
	return nil
}

// Flush copies every buffered row.
func (l *StateLoader) Flush(ctx context.Context) error {
	for _, t := range stateTables {
		if err := l.flush(ctx, t); err != nil {
			return err
		}
	}
	return nil
}

func (l *StateLoader) flush(ctx context.Context, t *stateTable) error {
	rows := l.buf[t]
	if len(rows) == 0 {
		return nil
	}
	n, err := l.tx.CopyFrom(ctx, pgx.Identifier{"explorer", t.name}, t.colNames, pgx.CopyFromRows(rows))
	if err != nil {
		return fmt.Errorf("copy %s: %w", t.name, err)
	}
	l.counts[t.name] += n
	l.buf[t] = rows[:0]
	return nil
}

// Counts returns the rows copied per table so far.
func (l *StateLoader) Counts() map[string]int64 {
	out := make(map[string]int64, len(l.counts))
	for k, v := range l.counts {
		out[k] = v
	}
	return out
}
