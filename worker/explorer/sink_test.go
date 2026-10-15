package explorer

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/network"

	"stellar-explorer/worker/explorer/explorertest"
	"stellar-explorer/worker/sep54"
	"stellar-explorer/worker/sep54/sep54test"
	"stellar-explorer/worker/sequencer"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// migrated returns a scratch database migrated by its owner role, with the
// post-migrate grants applied, like production.
func migrated(t *testing.T) *explorertest.DB {
	t.Helper()
	db := explorertest.New(t, explorertest.Options{WithRoles: true})
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, db.OwnerDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err := Migrate(ctx, conn, quiet); err != nil {
		t.Fatal(err)
	}
	db.PostMigrateGrants()
	return db
}

func seedCursor(t *testing.T, db *explorertest.DB, pipeline string, seq uint32, hash [32]byte) {
	t.Helper()
	db.Exec(fmt.Sprintf("INSERT INTO public.ingest_state (pipeline, ledger_sequence, ledger_hash) VALUES ('%s', %d, '%s')",
		pipeline, seq, hex.EncodeToString(hash[:])))
}

func count(t *testing.T, conn *pgx.Conn, q string) int64 {
	t.Helper()
	var n int64
	if err := conn.QueryRow(context.Background(), q).Scan(&n); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return n
}

type alerts struct {
	mu    sync.Mutex
	kinds []string
}

func (a *alerts) alert(kind, msg string, args ...any) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.kinds = append(a.kinds, kind)
}

func (a *alerts) list() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.kinds...)
}

func newSink(t *testing.T, dsn string, o SinkOptions) *Sink {
	t.Helper()
	if o.Log == nil {
		o.Log = quiet
	}
	s, err := NewSink(dsn, o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s
}

func prepare(t *testing.T, objs []sep54.Object, source string) []*Prepared {
	t.Helper()
	var out []*Prepared
	for _, o := range objs {
		seq, _ := sep54.ParseObjectPath(o.Key)
		lcm, h, err := sep54.DecodeLedger(seq, o.Body)
		if err != nil {
			t.Fatal(err)
		}
		p, err := Process(network.PublicNetworkPassphrase, sequencer.Ledger{Header: h, LCM: lcm, Source: source, Metadata: o.Metadata, Size: len(o.Body)})
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, p)
	}
	return out
}

func TestParseMigrations(t *testing.T) {
	all, err := Migrations()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 12 || all[0].Name != "ingest_state" || all[1].Name != "ledgers" || all[11].Name != "state_liquidity_pools" {
		var names []string
		for _, m := range all {
			names = append(names, fmt.Sprintf("%d %s", m.Version, m.Name))
		}
		t.Fatalf("migrations: %v", names)
	}
	bad := map[string]fstest.MapFS{
		"gap":       {"a.sql": {Data: []byte("-- +migration 1 a\nSELECT 1;\n-- +migration 3 b\nSELECT 1;\n")}},
		"duplicate": {"a.sql": {Data: []byte("-- +migration 1 a\nSELECT 1;\n")}, "b.sql": {Data: []byte("-- +migration 1 b\nSELECT 1;\n")}},
		"malformed": {"a.sql": {Data: []byte("-- +migration one\nSELECT 1;\n")}},
		"empty":     {"a.sql": {Data: []byte("-- +migration 1 a\n\n")}},
	}
	for name, fsys := range bad {
		if _, err := parseMigrations(fsys); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	ms, err := parseMigrations(fstest.MapFS{
		"b.sql": {Data: []byte("-- header\n-- +migration 2 b\nSELECT 2;\n")},
		"a.sql": {Data: []byte("-- +migration 1 a\nSELECT 1;\n-- +migration 3 c\nSELECT 3;\n")},
	})
	if err != nil || len(ms) != 3 || ms[1].File != "b.sql" || ms[1].SQL != "SELECT 2;\n" || ms[2].Name != "c" {
		t.Fatalf("%+v %v", ms, err)
	}
}

func TestMigrateAndCheckSchema(t *testing.T) {
	db := explorertest.New(t, explorertest.Options{WithRoles: true})
	ctx := context.Background()
	writer, err := pgx.Connect(ctx, db.WriterDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close(ctx)
	if err := CheckSchema(ctx, writer); !errors.Is(err, ErrSchemaBehind) {
		t.Fatalf("empty database: %v", err)
	}
	owner, err := pgx.Connect(ctx, db.OwnerDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close(ctx)
	applied, err := Migrate(ctx, owner, quiet)
	if err != nil || len(applied) != 12 {
		t.Fatalf("migrate: %d applied, %v", len(applied), err)
	}
	if again, err := Migrate(ctx, owner, quiet); err != nil || len(again) != 0 {
		t.Fatalf("second migrate: %d applied, %v", len(again), err)
	}
	if err := CheckSchema(ctx, writer); err != nil {
		t.Fatal(err)
	}
	// The owner role owns everything it created.
	if n := count(t, owner, fmt.Sprintf(`SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE (n.nspname = 'explorer' OR c.relname = 'ingest_state') AND c.relkind IN ('r','p') AND pg_get_userbyid(c.relowner) <> '%s'`, db.Owner)); n != 0 {
		t.Fatalf("%d tables not owned by the owner role", n)
	}
	// A database migrated by a newer build is fine; a missing or edited
	// migration is not.
	db.Exec("INSERT INTO explorer.schema_migrations (version, name, checksum) VALUES (99, 'future', 'x')")
	if err := CheckSchema(ctx, writer); err != nil {
		t.Fatalf("newer schema: %v", err)
	}
	db.Exec("DELETE FROM explorer.schema_migrations WHERE version = 7")
	if err := CheckSchema(ctx, writer); !errors.Is(err, ErrSchemaBehind) || !strings.Contains(err.Error(), "7 state_ttl") {
		t.Fatalf("missing migration: %v", err)
	}
	db.Exec("UPDATE explorer.schema_migrations SET checksum = 'edited' WHERE version = 3")
	if err := CheckSchema(ctx, writer); err == nil || !strings.Contains(err.Error(), "never edit") {
		t.Fatalf("edited migration: %v", err)
	}
	if _, err := Migrate(ctx, owner, quiet); err == nil {
		t.Fatal("migrate over an edited migration succeeded")
	}
}

// The Go column lists (history and state tables) match the migrated tables: every column exists with
// the declared type, and every NOT NULL column without a default is written.
func TestColumnsMatchSchema(t *testing.T) {
	db := migrated(t)
	conn := db.Conn()
	tables := append([]*table(nil), historyTables...)
	for _, st := range stateTables {
		tables = append(tables, &st.table)
	}
	for _, tbl := range tables {
		rows, err := conn.Query(context.Background(), `SELECT column_name, format_type(a.atttypid, a.atttypmod), a.attnotnull, a.atthasdef
			FROM information_schema.columns c
			JOIN pg_attribute a ON a.attrelid = ('explorer.' || c.table_name)::regclass AND a.attname = c.column_name
			WHERE c.table_schema = 'explorer' AND c.table_name = $1`, tbl.name)
		if err != nil {
			t.Fatal(err)
		}
		dbCols := map[string]string{}
		required := map[string]bool{}
		for rows.Next() {
			var name, typ string
			var notNull, hasDefault bool
			if err := rows.Scan(&name, &typ, &notNull, &hasDefault); err != nil {
				t.Fatal(err)
			}
			dbCols[name] = typ
			if notNull && !hasDefault {
				required[name] = true
			}
		}
		if len(dbCols) == 0 {
			t.Fatalf("table %s not found", tbl.name)
		}
		written := map[string]bool{}
		for _, c := range tbl.cols {
			written[c.name] = true
			if got, ok := dbCols[c.name]; !ok || got != c.typ {
				t.Errorf("%s.%s: database type %q, Go writes %q", tbl.name, c.name, got, c.typ)
			}
		}
		for name := range required {
			if !written[name] {
				t.Errorf("%s.%s is NOT NULL but never written", tbl.name, name)
			}
		}
	}
}

// Commits through both write paths produce identical rows; the cursor
// advances with them; nothing conflicts.
func TestSinkCommitPaths(t *testing.T) {
	objs := sep54test.Rechain(64_700_000, 6)
	prepared := prepare(t, objs, sep54.SourceLake)
	prev := prepared[0].Header().PreviousHash

	checksums := map[string]string{}
	for _, mode := range []string{"unnest", "copy"} {
		t.Run(mode, func(t *testing.T) {
			db := migrated(t)
			seedCursor(t, db, Pipeline, 64_699_999, prev)
			al := &alerts{}
			s := newSink(t, db.WriterDSN, SinkOptions{Lock: true, Alert: al.alert, COPYThreshold: map[string]int{"unnest": 100, "copy": 1}[mode]})
			ctx := context.Background()
			if err := s.Start(ctx); err != nil {
				t.Fatal(err)
			}
			// One ledger, then a batch of the rest.
			if err := s.Commit(ctx, prepared[:1]); err != nil {
				t.Fatal(err)
			}
			if err := s.Commit(ctx, prepared[1:]); err != nil {
				t.Fatal(err)
			}
			seq, hash, err := s.Cursor(ctx)
			if err != nil || seq != 64_700_005 || hash != prepared[5].Header().Hash {
				t.Fatalf("cursor %d %x %v", seq, hash, err)
			}
			if s.Conflicts() != 0 || len(al.list()) != 0 {
				t.Fatalf("conflicts %d alerts %v", s.Conflicts(), al.list())
			}
			conn := db.Conn()
			for i, tbl := range historyTables {
				want := 0
				for _, p := range prepared {
					want += p.rows(i).Len()
				}
				if n := count(t, conn, "SELECT count(*) FROM "+tbl.qualified()); n != int64(want) {
					t.Fatalf("%s: %d rows, want %d", tbl.name, n, want)
				}
			}
			checksums[mode] = tableChecksums(t, conn)
		})
	}
	if checksums["unnest"] != checksums["copy"] {
		t.Fatalf("the write paths differ:\nunnest %s\ncopy   %s", checksums["unnest"], checksums["copy"])
	}
}

// tableChecksums hashes the full, ordered content of every history table
// (the ledgers).
func tableChecksums(t *testing.T, conn *pgx.Conn) string {
	t.Helper()
	var parts []string
	for _, tbl := range historyTables {
		var sum string
		q := fmt.Sprintf("SELECT md5(coalesce(string_agg(r::text, '|' ORDER BY r::text), '')) FROM %s r", tbl.qualified())
		if err := conn.QueryRow(context.Background(), q).Scan(&sum); err != nil {
			t.Fatal(err)
		}
		parts = append(parts, tbl.name+"="+sum)
	}
	return strings.Join(parts, " ")
}

// The guarded cursor: a retried commit is a no-op, a batch that
// does not follow the cursor is refused, a hash mismatch halts.
func TestSinkGuardedCursor(t *testing.T) {
	objs := sep54test.Rechain(64_700_000, 4)
	prepared := prepare(t, objs, sep54.SourceLake)
	db := migrated(t)
	seedCursor(t, db, Pipeline, 64_699_999, prepared[0].Header().PreviousHash)
	al := &alerts{}
	s := newSink(t, db.WriterDSN, SinkOptions{Alert: al.alert})
	ctx := context.Background()
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit(ctx, prepared[:2]); err != nil {
		t.Fatal(err)
	}
	// The same batch again (a retry after an ambiguous COMMIT): no-op.
	if err := s.Commit(ctx, prepared[:2]); err != nil {
		t.Fatalf("retried commit: %v", err)
	}
	// A batch that does not follow the cursor.
	if err := s.Commit(ctx, prepared[3:]); !errors.Is(err, ErrCursorMoved) {
		t.Fatalf("gap: %v", err)
	}
	if err := s.Commit(ctx, prepared[:1]); !errors.Is(err, ErrCursorMoved) {
		t.Fatalf("behind: %v", err)
	}
	// A suffix of what is committed is committed too.
	if err := s.Commit(ctx, prepared[1:2]); err != nil {
		t.Fatalf("committed suffix: %v", err)
	}
	// Another chain at the right sequence: the hash does not link.
	other := prepare(t, sep54test.Rechain(64_700_002, 1), sep54.SourceLake)
	if err := s.Commit(ctx, other); !sequencer.IsHalt(err) || !strings.Contains(err.Error(), "hash mismatch") {
		t.Fatalf("foreign chain: %v", err)
	}
	// A batch that is not a chain never reaches the database.
	if err := s.Commit(ctx, []*Prepared{prepared[2], other[0]}); !sequencer.IsHalt(err) {
		t.Fatalf("broken batch: %v", err)
	}
	conn := db.Conn()
	if n := count(t, conn, "SELECT count(*) FROM explorer.ledgers"); n != 2 {
		t.Fatalf("%d ledgers", n)
	}
	// Rows that are already there (never possible through the cursor) are
	// skipped and counted as conflicts.
	db.Exec(`INSERT INTO explorer.ledgers (sequence, hash, prev_hash, closed_at, protocol_version, tx_count, failed_tx_count, op_count,
		successful_op_count, base_fee, base_reserve, max_tx_set_size, total_coins, fee_pool, source)
		VALUES (64700002, '\x00', '\x00', now(), 23, 0, 0, 0, 0, 100, 5000000, 1000, 0, 0, 'galexie')`)
	if err := s.Commit(ctx, prepared[2:3]); err != nil {
		t.Fatal(err)
	}
	if s.Conflicts() != 1 || fmt.Sprint(al.list()) != "[conflict_rows]" {
		t.Fatalf("conflicts %d alerts %v", s.Conflicts(), al.list())
	}
}

func TestSinkStartChecks(t *testing.T) {
	db := migrated(t)
	ctx := context.Background()
	s := newSink(t, db.WriterDSN, SinkOptions{Lock: true})
	if err := s.Start(ctx); !errors.Is(err, ErrNoCursor) {
		t.Fatalf("no cursor: %v", err)
	}
	seedCursor(t, db, Pipeline, 64_699_999, [32]byte{1})
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	// A second instance cannot take the lock (it exits).
	s2 := newSink(t, db.WriterDSN, SinkOptions{Lock: true})
	if err := s2.Start(ctx); !errors.Is(err, ErrLocked) {
		t.Fatalf("second instance: %v", err)
	}
	// Losing the lock connection: the next commit re-acquires it first.
	db.Exec("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = 'worker explorer (lock)'")
	if err := s.ensureLock(ctx); err != nil {
		t.Fatalf("re-acquire: %v", err)
	}
	if err := s2.ensureLock(ctx); !errors.Is(err, ErrLocked) {
		t.Fatalf("second instance after re-acquire: %v", err)
	}
	s.Close()
	if err := s2.Start(ctx); err != nil {
		t.Fatalf("after the first instance stopped: %v", err)
	}
	// Schema behind.
	db.Exec("DELETE FROM explorer.schema_migrations WHERE version = 5")
	if err := s2.Start(ctx); !errors.Is(err, ErrSchemaBehind) {
		t.Fatalf("schema behind: %v", err)
	}
}

// Postgres unreachable: every operation fails as Transient, never as a halt.
func TestSinkUnreachable(t *testing.T) {
	s := newSink(t, "postgres://nobody@127.0.0.1:1/none?sslmode=disable&connect_timeout=2", SinkOptions{Lock: true})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p := prepare(t, sep54test.Chain(100, 1), sep54.SourceLake)
	for name, err := range map[string]error{
		"start":  s.Start(ctx),
		"commit": s.Commit(ctx, p),
	} {
		if !sequencer.IsTransient(err) {
			t.Errorf("%s: %v is not transient", name, err)
		}
	}
	if _, _, err := s.Cursor(ctx); !sequencer.IsTransient(err) {
		t.Errorf("cursor: %v", err)
	}
}

// Replace (reindex): the same ledgers again leave identical rows; different
// ledgers at the same sequences halt.
func TestSinkReplace(t *testing.T) {
	objs := sep54test.Rechain(64_700_000, 5)
	prepared := prepare(t, objs, sep54.SourceLake)
	db := migrated(t)
	seedCursor(t, db, Pipeline, 64_699_999, prepared[0].Header().PreviousHash)
	ctx := context.Background()
	live := newSink(t, db.WriterDSN, SinkOptions{Lock: true})
	if err := live.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := live.Commit(ctx, prepared); err != nil {
		t.Fatal(err)
	}
	conn := db.Conn()
	before := tableChecksums(t, conn)

	pipeline := "reindex:64700001-64700003"
	seedCursor(t, db, pipeline, 64_700_000, prepared[0].Header().Hash)
	re := newSink(t, db.WriterDSN, SinkOptions{Pipeline: pipeline, Replace: true, Lock: true})
	if err := re.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := re.Commit(ctx, prepared[1:2]); err != nil {
		t.Fatal(err)
	}
	if err := re.Commit(ctx, prepared[2:4]); err != nil {
		t.Fatal(err)
	}
	if re.Conflicts() != 0 {
		t.Fatalf("%d conflicts", re.Conflicts())
	}
	if after := tableChecksums(t, conn); after != before {
		t.Fatalf("reindex changed rows:\n%s\n%s", before, after)
	}
	if seq, _, _ := live.Cursor(ctx); seq != 64_700_004 {
		t.Fatalf("explorer cursor moved to %d", seq)
	}
	// Another chain at the same sequences: refused, nothing replaced.
	pipeline2 := "reindex:64700002-64700002"
	foreign := prepare(t, sep54test.Rechain(64_700_002, 1), sep54.SourceLake)
	seedCursor(t, db, pipeline2, 64_700_001, foreign[0].Header().PreviousHash)
	re2 := newSink(t, db.WriterDSN, SinkOptions{Pipeline: pipeline2, Replace: true})
	if err := re2.Commit(ctx, foreign); !sequencer.IsHalt(err) || !strings.Contains(err.Error(), "explorer.ledgers has hash") {
		t.Fatalf("foreign ledger: %v", err)
	}
	if after := tableChecksums(t, conn); after != before {
		t.Fatal("a refused reindex changed rows")
	}
}

// BenchmarkCommitOneLedger compares the two write paths for a single real
// pubnet ledger (the tip case): INSERT … unnest(…) and COPY into temporary
// tables. Run with TEST_DATABASE_URL and -bench.
func BenchmarkCommitOneLedger(b *testing.B) {
	for _, mode := range []struct {
		name      string
		threshold int
	}{{"unnest", 100}, {"copy", -1}} {
		b.Run(mode.name, func(b *testing.B) {
			objs := sep54test.Rechain(64_700_000, b.N)
			var prepared []*Prepared
			for _, o := range objs {
				seq, _ := sep54.ParseObjectPath(o.Key)
				lcm, h, err := sep54.DecodeLedger(seq, o.Body)
				if err != nil {
					b.Fatal(err)
				}
				p, err := Process(network.PublicNetworkPassphrase, sequencer.Ledger{Header: h, LCM: lcm, Source: sep54.SourceLake})
				if err != nil {
					b.Fatal(err)
				}
				prepared = append(prepared, p)
			}
			db := explorertest.New(b, explorertest.Options{})
			ctx := context.Background()
			conn, err := pgx.Connect(ctx, db.DSN)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := Migrate(ctx, conn, quiet); err != nil {
				b.Fatal(err)
			}
			if err := SeedCursor(ctx, conn, Pipeline, 64_699_999, prepared[0].Header().PreviousHash); err != nil {
				b.Fatal(err)
			}
			conn.Close(ctx)
			s, err := NewSink(db.DSN, SinkOptions{COPYThreshold: mode.threshold, Log: quiet})
			if err != nil {
				b.Fatal(err)
			}
			defer s.Close()
			if err := s.Start(ctx); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for i := range b.N {
				if err := s.Commit(ctx, prepared[i:i+1]); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
