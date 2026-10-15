package bootstrap

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/historyarchive"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"

	"stellar-explorer/worker/explorer"
	"stellar-explorer/worker/explorer/explorertest"
	"stellar-explorer/worker/reindex"
	"stellar-explorer/worker/sep54"
	"stellar-explorer/worker/sep54/sep54test"
	"stellar-explorer/worker/sequencer"
)

// The fixture is a real futurenet history archive (protocol 25, HAS v2 with
// a hot archive) shipped with the SDK: its latest checkpoint 247487 has
// every bucket, and the ledger headers of checkpoints 247423 and 247487.
const (
	fixtureCheckpoint = 247487
	fixturePrevious   = 247423
)

var fixtureCounts = map[string]int64{"accounts": 2704, "trustlines": 49, "offers": 2, "contract_data": 2325, "contract_code": 41, "ttl": 2366}

var sdkDir = sync.OnceValues(func() (string, error) {
	out, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", "github.com/stellar/go-stellar-sdk").Output()
	return strings.TrimSpace(string(out)), err
})

func fixtureArchive(t *testing.T) historyarchive.ArchiveInterface {
	t.Helper()
	dir, err := sdkDir()
	if err != nil || dir == "" {
		t.Fatalf("locate the SDK module: %v", err)
	}
	a, err := historyarchive.Connect("file://"+filepath.Join(dir, "historyarchive/testdata/futurenet-2025-12-10-last-100"),
		historyarchive.ArchiveOptions{NetworkPassphrase: network.FutureNetworkPassphrase, CheckpointFrequency: 64})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func testLog(t *testing.T) *slog.Logger {
	return slog.New(slog.NewTextHandler(tWriter{t}, nil))
}

type tWriter struct{ t *testing.T }

func (w tWriter) Write(p []byte) (int, error) {
	w.t.Log(strings.TrimRight(string(p), "\n"))
	return len(p), nil
}

// migrated is a scratch database migrated by its owner, with the writer's
// post-migrate grants (TRUNCATE on the state tables).
func migrated(t *testing.T) *explorertest.DB {
	t.Helper()
	db := explorertest.New(t, explorertest.Options{WithRoles: true})
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, db.OwnerDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err := explorer.Migrate(ctx, conn, quiet); err != nil {
		t.Fatal(err)
	}
	db.PostMigrateGrants()
	return db
}

func newLakeDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "state"), 0o750); err != nil {
		t.Fatal(err)
	}
	return dir
}

func options(t *testing.T, db *explorertest.DB, arch historyarchive.ArchiveInterface, lakeDir string) Options {
	return Options{DSN: db.WriterDSN, Archive: arch, Passphrase: network.FutureNetworkPassphrase, LakeDir: lakeDir,
		Log: testLog(t), CheckpointPoll: 10 * time.Millisecond}
}

func tableCounts(t *testing.T, conn *pgx.Conn) map[string]int64 {
	t.Helper()
	out := map[string]int64{}
	for _, tbl := range explorer.StateTableNames() {
		var n int64
		if err := conn.QueryRow(context.Background(), "SELECT count(*) FROM explorer."+tbl).Scan(&n); err != nil {
			t.Fatal(err)
		}
		out[tbl] = n
	}
	return out
}

// sameCounts compares rows per table; a table missing from one counts 0.
func sameCounts(a, b map[string]int64) bool {
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	for k, v := range b {
		if a[k] != v {
			return false
		}
	}
	return true
}

func cursorOf(t *testing.T, conn *pgx.Conn) (uint32, xdr.Hash) {
	t.Helper()
	seq, h, err := explorer.ReadCursor(context.Background(), conn, explorer.Pipeline)
	if err != nil {
		t.Fatal(err)
	}
	return seq, h
}

func headerOf(t *testing.T, arch historyarchive.ArchiveInterface, seq uint32) xdr.LedgerHeaderHistoryEntry {
	t.Helper()
	h, err := arch.GetLedgerHeader(seq)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// The first bootstrap loads the checkpoint's state, seeds the cursor at
// (C, hash_C), records the checkpoint and its duration, and writes
// bootstrap.json; a second run refuses.
func TestBootstrapLoadsCheckpoint(t *testing.T) {
	db := migrated(t)
	arch := fixtureArchive(t)
	lakeDir := newLakeDir(t)
	ctx := context.Background()
	res, err := Run(ctx, options(t, db, arch, lakeDir))
	if err != nil {
		t.Fatal(err)
	}
	hdr := headerOf(t, arch, fixtureCheckpoint)
	if res.Checkpoint != fixtureCheckpoint || res.Hash != hdr.Hash || !sameCounts(res.Entries, fixtureCounts) {
		t.Fatalf("result %+v", res)
	}
	if res.Duration <= 0 || res.LoadDuration <= 0 {
		t.Fatalf("durations %s %s", res.Duration, res.LoadDuration)
	}
	conn := db.Conn()
	if seq, h := cursorOf(t, conn); seq != fixtureCheckpoint || h != hdr.Hash {
		t.Fatalf("cursor %d %x", seq, h)
	}
	if got := tableCounts(t, conn); !sameCounts(got, fixtureCounts) {
		t.Fatalf("rows %v, want %v", got, fixtureCounts)
	}
	sc, ok, err := explorer.ReadStateCheckpoint(ctx, conn)
	if err != nil || !ok || sc.Checkpoint != fixtureCheckpoint || sc.Hash != hdr.Hash || sc.LoadSeconds <= 0 || !sameCounts(sc.Entries, fixtureCounts) {
		t.Fatalf("state checkpoint %+v %v %v", sc, ok, err)
	}
	// key_hash = sha256(XDR(LedgerKey)) = TtlEntry.KeyHash: every TTL row
	// belongs to a contract data or code row.
	var orphans int64
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM explorer.ttl t WHERE NOT EXISTS (SELECT 1 FROM explorer.contract_data d WHERE d.key_hash = t.key_hash)
		AND NOT EXISTS (SELECT 1 FROM explorer.contract_code c WHERE c.key_hash = t.key_hash)`).Scan(&orphans); err != nil {
		t.Fatal(err)
	}
	if orphans != 0 {
		t.Fatalf("%d ttl rows match no contract entry", orphans)
	}
	origin, err := ReadOrigin(lakeDir)
	want := NewOrigin(network.FutureNetworkPassphrase, fixtureCheckpoint, hdr.Hash)
	if err != nil || origin != want || !res.OriginWritten || origin.ArchiveOrigin != fixtureCheckpoint+1 {
		t.Fatalf("origin %+v %v, want %+v", origin, err, want)
	}

	_, err = Run(ctx, options(t, db, arch, lakeDir))
	if !errors.Is(err, explorer.ErrCursorExists) {
		t.Fatalf("second run: %v", err)
	}
}

// bootstrap.json is only written if absent.
func TestBootstrapKeepsExistingOrigin(t *testing.T) {
	db := migrated(t)
	lakeDir := newLakeDir(t)
	earlier := NewOrigin(network.FutureNetworkPassphrase, 191, xdr.Hash{7})
	if written, _, err := WriteOriginIfAbsent(lakeDir, earlier); err != nil || !written {
		t.Fatal(err)
	}
	res, err := Run(context.Background(), options(t, db, fixtureArchive(t), lakeDir))
	if err != nil {
		t.Fatal(err)
	}
	if res.OriginWritten || res.Origin != earlier {
		t.Fatalf("origin %+v written %v", res.Origin, res.OriginWritten)
	}
	if o, err := ReadOrigin(lakeDir); err != nil || o != earlier {
		t.Fatalf("bootstrap.json rewritten: %+v %v", o, err)
	}
}

// tampered serves the fixture with a corrupted HAS or header.
type tampered struct {
	historyarchive.ArchiveInterface
	has    func(historyarchive.HistoryArchiveState) historyarchive.HistoryArchiveState
	header func(xdr.LedgerHeaderHistoryEntry) xdr.LedgerHeaderHistoryEntry
}

func (a tampered) GetCheckpointHAS(c uint32) (historyarchive.HistoryArchiveState, error) {
	has, err := a.ArchiveInterface.GetCheckpointHAS(c)
	if err == nil && a.has != nil {
		has = a.has(has)
	}
	return has, err
}

func (a tampered) GetLedgerHeader(c uint32) (xdr.LedgerHeaderHistoryEntry, error) {
	h, err := a.ArchiveInterface.GetLedgerHeader(c)
	if err == nil && a.header != nil {
		h = a.header(h)
	}
	return h, err
}

// The bucket list is verified against the header before anything is
// streamed, and the header against its own hash: either failure leaves the
// database untouched.
func TestBootstrapVerifies(t *testing.T) {
	db := migrated(t)
	arch := fixtureArchive(t)
	for name, a := range map[string]tampered{
		"bucket list": {ArchiveInterface: arch, has: func(h historyarchive.HistoryArchiveState) historyarchive.HistoryArchiveState {
			h.CurrentBuckets[3].Snap = strings.Repeat("ab", 32)
			return h
		}},
		"hot archive bucket list": {ArchiveInterface: arch, has: func(h historyarchive.HistoryArchiveState) historyarchive.HistoryArchiveState {
			h.HotArchiveBuckets[0].Curr = strings.Repeat("cd", 32)
			return h
		}},
		"header": {ArchiveInterface: arch, header: func(h xdr.LedgerHeaderHistoryEntry) xdr.LedgerHeaderHistoryEntry {
			h.Header.TotalCoins++
			return h
		}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Run(context.Background(), options(t, db, a, ""))
			if err == nil {
				t.Fatal("bootstrap accepted a tampered archive")
			}
			t.Logf("refused: %v", err)
			conn := db.Conn()
			if _, _, err := explorer.ReadCursor(context.Background(), conn, explorer.Pipeline); !errors.Is(err, explorer.ErrNoCursor) {
				t.Fatalf("cursor after a refused bootstrap: %v", err)
			}
			if got := tableCounts(t, conn); !sameCounts(got, map[string]int64{}) {
				t.Fatalf("rows after a refused bootstrap: %v", got)
			}
		})
	}
}

// --force at C = P: the state tables are replaced from the checkpoint, the
// cursor stays, bootstrap.json is untouched.
func TestForceAtCursor(t *testing.T) {
	db := migrated(t)
	arch := fixtureArchive(t)
	lakeDir := newLakeDir(t)
	ctx := context.Background()
	if _, err := Run(ctx, options(t, db, arch, lakeDir)); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(OriginFile(lakeDir))
	conn := db.Conn()
	seq0, h0 := cursorOf(t, conn)
	// Corrupt the state: what --force repairs.
	db.Exec("DELETE FROM explorer.accounts WHERE account_id IN (SELECT account_id FROM explorer.accounts LIMIT 100)",
		"UPDATE explorer.trustlines SET balance = -1", "UPDATE explorer.contract_data SET archived = true")

	res, err := Force(ctx, options(t, db, arch, lakeDir))
	if err != nil {
		t.Fatal(err)
	}
	if res.PreviousCursor != seq0 || res.Checkpoint != seq0 || res.HistoryLedgers != 0 {
		t.Fatalf("result %+v", res)
	}
	if seq, h := cursorOf(t, conn); seq != seq0 || h != h0 {
		t.Fatalf("cursor moved: %d %x", seq, h)
	}
	if got := tableCounts(t, conn); !sameCounts(got, fixtureCounts) {
		t.Fatalf("rows %v", got)
	}
	var bad int64
	_ = conn.QueryRow(ctx, "SELECT (SELECT count(*) FROM explorer.trustlines WHERE balance < 0) + (SELECT count(*) FROM explorer.contract_data WHERE archived)").Scan(&bad)
	if bad != 0 {
		t.Fatalf("%d corrupt rows left", bad)
	}
	after, _ := os.ReadFile(OriginFile(lakeDir))
	if string(after) != string(before) {
		t.Fatal("bootstrap.json rewritten")
	}
}

// headerSource serves ledgers built around the archive's real headers, so
// they chain into the checkpoint like Galexie's would.
func headerSource(t *testing.T, arch historyarchive.ArchiveInterface, from, to uint32) *namedMem {
	t.Helper()
	ledgers, err := arch.GetLedgers(from, to)
	if err != nil {
		t.Fatal(err)
	}
	var objs []sep54.Object
	for seq := from; seq <= to; seq++ {
		l, ok := ledgers[seq]
		if !ok {
			t.Fatalf("fixture lacks ledger %d", seq)
		}
		objs = append(objs, sep54test.Encode(sep54test.HeaderLCM(l.Header)))
	}
	return &namedMem{MemSource: sep54test.NewMemSource(objs...), name: sep54.SourceLake}
}

type namedMem struct {
	*sep54test.MemSource
	name string
}

func (m *namedMem) Name() string { return m.name }

// --force with C > P writes the history P+1..C through the reindex path,
// checks it ends at the checkpoint's hash, then moves the cursor to C with
// the state load. The cursor never moves backwards and no history is
// skipped; a lost bootstrap.json is recreated from Postgres.
func TestForceWritesHistoryToCheckpoint(t *testing.T) {
	db := migrated(t)
	arch := fixtureArchive(t)
	lakeDir := newLakeDir(t)
	ctx := context.Background()
	prev := headerOf(t, arch, fixturePrevious)
	db.Exec("INSERT INTO public.ingest_state (pipeline, ledger_sequence, ledger_hash) VALUES ('explorer', 247423, '" + hashHex(prev.Hash) + "')")

	empty := &namedMem{MemSource: sep54test.NewMemSource(), name: sep54.SourceArchive}
	lake := headerSource(t, arch, fixturePrevious+1, fixtureCheckpoint)
	o := options(t, db, arch, lakeDir)
	o.Sources = []sequencer.Source{empty, lake}
	res, err := Force(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	if res.PreviousCursor != fixturePrevious || res.Checkpoint != fixtureCheckpoint || res.HistoryLedgers != 64 || res.HistorySource != sep54.SourceLake {
		t.Fatalf("result %+v", res)
	}
	conn := db.Conn()
	hdr := headerOf(t, arch, fixtureCheckpoint)
	if seq, h := cursorOf(t, conn); seq != fixtureCheckpoint || h != hdr.Hash {
		t.Fatalf("cursor %d %x", seq, h)
	}
	var n, lo, hi int64
	if err := conn.QueryRow(ctx, "SELECT count(*), min(sequence), max(sequence) FROM explorer.ledgers").Scan(&n, &lo, &hi); err != nil {
		t.Fatal(err)
	}
	if n != 64 || lo != fixturePrevious+1 || hi != fixtureCheckpoint {
		t.Fatalf("history %d rows %d-%d", n, lo, hi)
	}
	if _, _, err := explorer.ReadCursor(ctx, conn, reindex.Pipeline(fixturePrevious+1, fixtureCheckpoint)); !errors.Is(err, explorer.ErrNoCursor) {
		t.Fatalf("reindex row left: %v", err)
	}
	if got := tableCounts(t, conn); !sameCounts(got, fixtureCounts) {
		t.Fatalf("rows %v", got)
	}
	// bootstrap.json did not exist: it is recreated from Postgres (the
	// archive starts after the oldest committed ledger's predecessor).
	want := NewOrigin(network.FutureNetworkPassphrase, fixturePrevious, prev.Hash)
	if !res.OriginWritten || res.Origin != want {
		t.Fatalf("origin %+v written %v, want %+v", res.Origin, res.OriginWritten, want)
	}
	if err := os.Remove(OriginFile(lakeDir)); err != nil {
		t.Fatal(err)
	}
	got, written, err := RecreateOrigin(ctx, Options{DSN: db.WriterDSN, LakeDir: lakeDir, Passphrase: network.FutureNetworkPassphrase})
	if err != nil || !written || got != want {
		t.Fatalf("recreated %+v %v %v", got, written, err)
	}
	if got, written, err := RecreateOrigin(ctx, Options{DSN: db.WriterDSN, LakeDir: lakeDir}); err != nil || written || got != want {
		t.Fatalf("recreate over an existing file: %+v %v %v", got, written, err)
	}
}

// History that does not end at the checkpoint's hash (a wrong or mixed
// source) is refused before the state or the cursor change.
func TestForceRefusesForeignHistory(t *testing.T) {
	db := migrated(t)
	arch := fixtureArchive(t)
	ctx := context.Background()
	prev := headerOf(t, arch, fixturePrevious)
	db.Exec("INSERT INTO public.ingest_state (pipeline, ledger_sequence, ledger_hash) VALUES ('explorer', 247423, '" + hashHex(prev.Hash) + "')")
	// A synthetic chain linked to the cursor but not to the checkpoint.
	objs := make([]sep54.Object, 0, 64)
	ph := prev.Hash
	for seq := uint32(fixturePrevious + 1); seq <= fixtureCheckpoint; seq++ {
		lcm := sep54test.SyntheticLCM(seq, ph, sep54test.BaseCloseTime+5*int64(seq))
		objs = append(objs, sep54test.Encode(lcm))
		ph = lcm.LedgerHash()
	}
	o := options(t, db, arch, "")
	o.Sources = []sequencer.Source{&namedMem{MemSource: sep54test.NewMemSource(objs...), name: sep54.SourceLake}}
	_, err := Force(ctx, o)
	if err == nil || !strings.Contains(err.Error(), "checkpoint 247487 has hash") {
		t.Fatalf("foreign history: %v", err)
	}
	conn := db.Conn()
	if seq, _ := cursorOf(t, conn); seq != fixturePrevious {
		t.Fatalf("cursor moved to %d", seq)
	}
	if _, ok, _ := explorer.ReadStateCheckpoint(ctx, conn); ok {
		t.Fatal("state loaded from a checkpoint the history does not reach")
	}
}

// slowArchive publishes the latest checkpoint only after a few polls.
type slowArchive struct {
	historyarchive.ArchiveInterface
	polls atomic.Int32
	after int32
	older uint32
}

func (a *slowArchive) GetRootHAS() (historyarchive.HistoryArchiveState, error) {
	if a.polls.Add(1) <= a.after {
		return a.ArchiveInterface.GetCheckpointHAS(a.older)
	}
	return a.ArchiveInterface.GetRootHAS()
}

// --force waits for a checkpoint at or above the cursor; it refuses while
// ingest holds the pipeline's lock, and without a cursor.
func TestForceWaitsAndRefuses(t *testing.T) {
	db := migrated(t)
	arch := fixtureArchive(t)
	ctx := context.Background()

	if _, err := Force(ctx, options(t, db, arch, "")); !errors.Is(err, explorer.ErrNoCursor) {
		t.Fatalf("without a cursor: %v", err)
	}
	hdr := headerOf(t, arch, fixtureCheckpoint)
	db.Exec("INSERT INTO public.ingest_state (pipeline, ledger_sequence, ledger_hash) VALUES ('explorer', 247487, '" + hashHex(hdr.Hash) + "')")

	// Ingest running: it holds the advisory lock.
	lock, err := pgx.Connect(ctx, db.WriterDSN)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lock.Exec(ctx, "SELECT pg_advisory_lock(hashtext('explorer'))"); err != nil {
		t.Fatal(err)
	}
	if _, err := Force(ctx, options(t, db, arch, "")); !errors.Is(err, ErrIngestRunning) {
		t.Fatalf("with ingest running: %v", err)
	}
	lock.Close(ctx)

	// The archive is behind the cursor for 3 polls.
	slow := &slowArchive{ArchiveInterface: arch, after: 3, older: fixturePrevious}
	res, err := Force(ctx, options(t, db, slow, ""))
	if err != nil {
		t.Fatal(err)
	}
	if slow.polls.Load() < 4 || res.Checkpoint != fixtureCheckpoint {
		t.Fatalf("polls %d, result %+v", slow.polls.Load(), res)
	}

	// Waiting is cancellable and changes nothing.
	never := &slowArchive{ArchiveInterface: arch, after: 1 << 30, older: fixturePrevious}
	cctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	if _, err := Force(cctx, options(t, db, never, "")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancelled wait: %v", err)
	}
	if seq, _ := cursorOf(t, db.Conn()); seq != fixtureCheckpoint {
		t.Fatalf("cursor %d", seq)
	}
}

// The first ledger after the checkpoint must link to hash_C (the guarded
// cursor); once it does, its state changes apply on top of the loaded
// state.
func TestFirstLedgerAfterBootstrap(t *testing.T) {
	db := migrated(t)
	arch := fixtureArchive(t)
	ctx := context.Background()
	res, err := Run(ctx, options(t, db, arch, ""))
	if err != nil {
		t.Fatal(err)
	}
	conn := db.Conn()
	var id string
	if err := conn.QueryRow(ctx, "SELECT account_id FROM explorer.accounts ORDER BY account_id LIMIT 1").Scan(&id); err != nil {
		t.Fatal(err)
	}
	next := res.Checkpoint + 1
	ledger := func(prev xdr.Hash) sequencer.Ledger {
		acct := xdr.LedgerEntry{LastModifiedLedgerSeq: xdr.Uint32(next), Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeAccount, Account: &xdr.AccountEntry{
				AccountId: xdr.MustAddress(id), Balance: 4242, Thresholds: xdr.Thresholds{1, 0, 0, 0}},
		}}
		pre := acct
		lcm := sep54test.SyntheticLCM(next, prev, sep54test.BaseCloseTime+5*int64(next))
		fee := xdr.Uint32(100)
		lcm.V2.UpgradesProcessing = []xdr.UpgradeEntryMeta{{
			Upgrade: xdr.LedgerUpgrade{Type: xdr.LedgerUpgradeTypeLedgerUpgradeBaseFee, NewBaseFee: &fee},
			Changes: xdr.LedgerEntryChanges{
				{Type: xdr.LedgerEntryChangeTypeLedgerEntryState, State: &pre},
				{Type: xdr.LedgerEntryChangeTypeLedgerEntryUpdated, Updated: &acct},
			},
		}}
		h, err := sep54.CheckLCM(next, lcm)
		if err != nil {
			t.Fatal(err)
		}
		return sequencer.Ledger{Header: h, LCM: lcm, Source: sep54.SourceLake}
	}
	sink, err := explorer.NewSink(db.WriterDSN, explorer.SinkOptions{Log: quiet})
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	if err := sink.Start(ctx); err != nil || !sink.StateLoaded() {
		t.Fatalf("start: %v, state loaded %v", err, sink.StateLoaded())
	}
	commit := func(l sequencer.Ledger) error {
		p, err := explorer.Process(network.FutureNetworkPassphrase, l)
		if err != nil {
			t.Fatal(err)
		}
		return sink.Commit(ctx, []*explorer.Prepared{p})
	}
	if err := commit(ledger(xdr.Hash{1})); !sequencer.IsHalt(err) {
		t.Fatalf("ledger C+1 not linked to hash_C: %v", err)
	}
	if err := commit(ledger(res.Hash)); err != nil {
		t.Fatal(err)
	}
	var balance, lml int64
	if err := conn.QueryRow(ctx, "SELECT balance, last_modified_ledger FROM explorer.accounts WHERE account_id = $1", id).Scan(&balance, &lml); err != nil {
		t.Fatal(err)
	}
	if balance != 4242 || lml != int64(next) {
		t.Fatalf("account after C+1: balance %d, last modified %d", balance, lml)
	}
}
