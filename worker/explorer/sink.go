package explorer

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stellar/go-stellar-sdk/xdr"

	"stellar-explorer/worker/sep54"
	"stellar-explorer/worker/sequencer"
)

// Pipeline is the explorer pipeline's name in ingest_state.
const Pipeline = "explorer"

var (
	// ErrNoCursor means ingest_state has no row for the pipeline: seed it
	// with `worker bootstrap`.
	ErrNoCursor = errors.New("ingest_state has no row for this pipeline")
	// ErrLocked means another session holds the pipeline's advisory lock:
	// another ingest is running.
	ErrLocked = errors.New("another session holds the pipeline's advisory lock")
	// ErrCursorMoved means the cursor changed under us: another committer.
	ErrCursorMoved = errors.New("the pipeline cursor was moved by another committer")
)

// SinkOptions configure a Sink.
type SinkOptions struct {
	// Pipeline names the ingest_state row: Pipeline, or "reindex:<A>-<B>".
	Pipeline string
	// Replace deletes the batch's ledger rows before inserting them
	// (`worker reindex`). It also requires every existing
	// explorer.ledgers row in the range to have the same hash.
	Replace bool
	// Lock holds pg_try_advisory_lock(hashtext(pipeline)) on a dedicated
	// connection and re-acquires it after a reconnect.
	Lock bool
	// COPYThreshold: a commit with more ledgers than this uses COPY into
	// temporary tables; smaller ones INSERT … unnest(…). 0 means the
	// default (1); a negative value always uses COPY.
	COPYThreshold int
	Log           *slog.Logger
	// Alert reports conditions that need an operator.
	Alert func(kind, msg string, args ...any)
}

// Sink is the explorer pipeline's Postgres sink. It implements
// sequencer.Sink[*Prepared].
type Sink struct {
	o    SinkOptions
	dsn  string
	pool *pgxpool.Pool

	lockMu   sync.Mutex
	lockConn *pgx.Conn

	cursor    atomic.Int64 // last committed ledger, as seen by this sink
	conflicts atomic.Uint64
	commits   atomic.Uint64
	ledgers   atomic.Uint64
	// stateLoaded: the state tables were loaded from a checkpoint, so
	// commits maintain them. Set by Start.
	stateLoaded atomic.Bool
	drift       atomic.Uint64
}

var _ sequencer.Sink[*Prepared] = (*Sink)(nil)

// NewSink returns a sink for dsn. It does not connect yet.
func NewSink(dsn string, o SinkOptions) (*Sink, error) {
	if o.Pipeline == "" {
		o.Pipeline = Pipeline
	}
	switch {
	case o.COPYThreshold == 0:
		o.COPYThreshold = 1
	case o.COPYThreshold < 0:
		o.COPYThreshold = 0
	}
	if o.Log == nil {
		o.Log = slog.Default()
	}
	if o.Alert == nil {
		log := o.Log
		o.Alert = func(kind, msg string, args ...any) {
			log.Error(msg, append([]any{"alert", kind}, args...)...)
		}
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres dsn: %w", err)
	}
	cfg.MaxConns = 4
	cfg.MaxConnIdleTime = 5 * time.Minute
	setRuntimeParams(cfg.ConnConfig, "worker "+o.Pipeline)
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		return nil, err
	}
	return &Sink{o: o, dsn: dsn, pool: pool}, nil
}

func setRuntimeParams(c *pgx.ConnConfig, app string) {
	if c.RuntimeParams == nil {
		c.RuntimeParams = map[string]string{}
	}
	c.RuntimeParams["application_name"] = app
	// A process frozen mid-transaction must not hold the cursor's row lock
	// forever.
	c.RuntimeParams["idle_in_transaction_session_timeout"] = "300000"
}

// Pool exposes the connection pool (reindex, tests).
func (s *Sink) Pool() *pgxpool.Pool { return s.pool }

// Conflicts returns explorer_conflict_rows_total: rows skipped by ON
// CONFLICT DO NOTHING. It should always be 0.
func (s *Sink) Conflicts() uint64 { return s.conflicts.Load() }

// Commits returns the number of committed transactions and ledgers.
func (s *Sink) Commits() (commits, ledgers uint64) { return s.commits.Load(), s.ledgers.Load() }

// StateLoaded reports whether commits maintain the state tables: they were
// loaded from a checkpoint (explorer.state_checkpoint), and the sink is not
// a reindex.
func (s *Sink) StateLoaded() bool { return s.stateLoaded.Load() && !s.o.Replace }

// LastCursor returns the cursor as last read or committed by this sink;
// false until Start (or Cursor) has read it.
func (s *Sink) LastCursor() (uint32, bool) {
	c := s.cursor.Load()
	return uint32(c), c > 0
}

// Ping checks that Postgres answers.
func (s *Sink) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// StateDrift returns the number of evictions that found no row; it should
// always be 0.
func (s *Sink) StateDrift() uint64 { return s.drift.Load() }

// Close closes every connection.
func (s *Sink) Close() {
	s.lockMu.Lock()
	if s.lockConn != nil {
		_ = s.lockConn.Close(context.Background())
		s.lockConn = nil
	}
	s.lockMu.Unlock()
	s.pool.Close()
}

// Start performs the checks before a pipeline may commit: the
// schema is current (ErrSchemaBehind), the pipeline has a cursor
// (ErrNoCursor) and the advisory lock is free (ErrLocked). Connection
// failures are sequencer.Transient.
func (s *Sink) Start(ctx context.Context) error {
	if err := CheckSchema(ctx, s.pool); err != nil {
		if errors.Is(err, ErrSchemaBehind) || errors.Is(err, ErrSchemaMismatch) {
			return err
		}
		return classify(err)
	}
	if _, _, err := s.Cursor(ctx); err != nil {
		return err
	}
	if !s.o.Replace {
		sc, ok, err := ReadStateCheckpoint(ctx, s.pool)
		if err != nil {
			return err
		}
		s.stateLoaded.Store(ok)
		if ok {
			s.o.Log.Info("maintaining the state tables", "pipeline", s.o.Pipeline, "state_checkpoint", sc.Checkpoint)
		} else {
			s.o.Log.Warn("the state tables were never loaded from a checkpoint (bootstrap --history-only): only history is written",
				"pipeline", s.o.Pipeline)
		}
	}
	if s.o.Lock {
		if err := s.ensureLock(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Cursor implements sequencer.Sink.
func (s *Sink) Cursor(ctx context.Context) (uint32, xdr.Hash, error) {
	seq, h, err := ReadCursor(ctx, s.pool, s.o.Pipeline)
	if err != nil {
		return 0, xdr.Hash{}, err
	}
	s.cursor.Store(int64(seq))
	return seq, h, nil
}

func parseHash(s string) (xdr.Hash, error) {
	var h xdr.Hash
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != len(h) {
		return h, fmt.Errorf("bad ledger hash %q", s)
	}
	copy(h[:], b)
	return h, nil
}

// ensureLock makes sure this process holds the pipeline's advisory lock on
// its dedicated connection, reconnecting (and re-acquiring) if that
// connection broke.
func (s *Sink) ensureLock(ctx context.Context) error {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	if s.lockConn != nil {
		if err := s.lockConn.Ping(ctx); err == nil {
			return nil
		}
		_ = s.lockConn.Close(context.Background())
		s.lockConn = nil
		s.o.Log.Warn("advisory-lock connection lost; re-acquiring", "pipeline", s.o.Pipeline)
	}
	cfg, err := pgx.ParseConfig(s.dsn)
	if err != nil {
		return err
	}
	setRuntimeParams(cfg, "worker "+s.o.Pipeline+" (lock)")
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return classify(err)
	}
	var got bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock(hashtext($1))", s.o.Pipeline).Scan(&got); err != nil {
		_ = conn.Close(context.Background())
		return classify(err)
	}
	if !got {
		_ = conn.Close(context.Background())
		return fmt.Errorf("%w (pipeline %q)", ErrLocked, s.o.Pipeline)
	}
	s.lockConn = conn
	return nil
}

const guardedCursorUpdate = `UPDATE public.ingest_state
   SET ledger_sequence = $1, ledger_hash = $2, updated_at = now()
 WHERE pipeline = $3 AND ledger_sequence = $4 AND ledger_hash = $5`

// Commit implements sequencer.Sink: one transaction that first
// takes the cursor row with the guarded UPDATE, then writes the ledger rows
// with INSERT … ON CONFLICT DO NOTHING (COPY into temporary tables for
// batches) and the state changes, then commits. Skipped rows are counted as
// conflicts.
func (s *Sink) Commit(ctx context.Context, batch []*Prepared) error {
	if len(batch) == 0 {
		return nil
	}
	first, last := batch[0].header, batch[len(batch)-1].header
	for i := 1; i < len(batch); i++ {
		if err := sep54.CheckChain(batch[i-1].header, batch[i].header); err != nil {
			return sequencer.Halt(batch[i].header.Sequence, fmt.Errorf("batch is not a chain: %w", err))
		}
	}
	if s.o.Lock {
		if err := s.ensureLock(ctx); err != nil {
			return err
		}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return classify(err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	tag, err := tx.Exec(ctx, guardedCursorUpdate, int64(last.Sequence), hex.EncodeToString(last.Hash[:]),
		s.o.Pipeline, int64(first.Sequence)-1, hex.EncodeToString(first.PreviousHash[:]))
	if err != nil {
		return atLedger(first.Sequence, classify(err))
	}
	if tag.RowsAffected() != 1 {
		_ = tx.Rollback(ctx)
		return s.cursorMismatch(ctx, first, last)
	}
	if s.o.Replace {
		if err := s.replaceRange(ctx, tx, batch); err != nil {
			return atLedger(first.Sequence, err)
		}
	}
	var offered, inserted int64
	if len(batch) > s.o.COPYThreshold {
		offered, inserted, err = s.writeCopy(ctx, tx, batch)
	} else {
		offered, inserted, err = s.writeUnnest(ctx, tx, batch)
	}
	if err != nil {
		return atLedger(first.Sequence, classify(err))
	}
	var drift []stateDrift
	if s.StateLoaded() {
		changes := make([][]StateChange, len(batch))
		for i, p := range batch {
			changes[i] = p.State
		}
		if drift, err = applyState(ctx, tx, mergeState(changes)); err != nil {
			return atLedger(first.Sequence, classify(err))
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return atLedger(first.Sequence, classify(err))
	}
	s.cursor.Store(int64(last.Sequence))
	s.commits.Add(1)
	s.ledgers.Add(uint64(len(batch)))
	for _, d := range drift {
		s.drift.Add(uint64(d.expected - d.affected))
		s.o.Alert("state_drift", "an eviction found no row: the state tables drifted from the ledger",
			"pipeline", s.o.Pipeline, "table", d.table, "op", d.op.String(), "evicted", d.expected, "rows", d.affected,
			"first", first.Sequence, "last", last.Sequence)
	}
	if c := offered - inserted; c > 0 {
		s.conflicts.Add(uint64(c))
		s.o.Alert("conflict_rows", "rows already present were skipped (explorer_conflict_rows_total should stay 0)",
			"pipeline", s.o.Pipeline, "first", first.Sequence, "last", last.Sequence, "conflicts", c)
	}
	return nil
}

// cursorMismatch explains a guarded UPDATE that matched no row:
// the batch was already committed (a retry after an ambiguous COMMIT), the
// hash chain does not match (halt), or another committer moved the cursor.
func (s *Sink) cursorMismatch(ctx context.Context, first, last sep54.Header) error {
	seq, hash, err := s.Cursor(ctx)
	switch {
	case errors.Is(err, ErrNoCursor):
		return sequencer.Halt(first.Sequence, err)
	case err != nil:
		return err
	case seq == last.Sequence && hash == last.Hash:
		s.o.Log.Info("batch was already committed", "pipeline", s.o.Pipeline, "first", first.Sequence, "last", last.Sequence)
		return nil
	case seq == first.Sequence-1:
		return sequencer.Halt(first.Sequence, fmt.Errorf("hash mismatch: ledger %d has previous hash %x, the cursor holds %x",
			first.Sequence, first.PreviousHash, hash))
	default:
		return fmt.Errorf("%w: cursor is at %d, the batch starts at %d", ErrCursorMoved, seq, first.Sequence)
	}
}

// replaceRange implements reindex's delete-then-insert after
// checking that the ledgers being replaced are the same ledgers.
func (s *Sink) replaceRange(ctx context.Context, tx pgx.Tx, batch []*Prepared) error {
	first, last := batch[0].header.Sequence, batch[len(batch)-1].header.Sequence
	rows, err := tx.Query(ctx, "SELECT sequence, hash FROM explorer.ledgers WHERE sequence BETWEEN $1 AND $2", int64(first), int64(last))
	if err != nil {
		return classify(err)
	}
	for rows.Next() {
		var seq int64
		var h []byte
		if err := rows.Scan(&seq, &h); err != nil {
			return classify(err)
		}
		want := batch[seq-int64(first)].header.Hash
		if string(h) != string(want[:]) {
			rows.Close()
			return sequencer.Halt(uint32(seq), fmt.Errorf("explorer.ledgers has hash %x for ledger %d, the source has %x", h, seq, want))
		}
	}
	if err := rows.Err(); err != nil {
		return classify(err)
	}
	b := &pgx.Batch{}
	for _, t := range historyTables {
		b.Queue(t.deleteRange, int64(first), int64(last))
	}
	if err := tx.SendBatch(ctx, b).Close(); err != nil {
		return classify(err)
	}
	return nil
}

// writeUnnest writes a small commit with one INSERT … SELECT * FROM
// unnest(…) per table, pipelined in one round trip.
func (s *Sink) writeUnnest(ctx context.Context, tx pgx.Tx, batch []*Prepared) (offered, inserted int64, err error) {
	b := &pgx.Batch{}
	var queued []int64
	for i, t := range historyTables {
		sets := rowSetsOf(batch, i)
		args, n := t.unnestArgs(sets)
		if n == 0 {
			continue
		}
		b.Queue(t.insertUnnest, args...)
		queued = append(queued, int64(n))
	}
	res := tx.SendBatch(ctx, b)
	for _, n := range queued {
		tag, err := res.Exec()
		if err != nil {
			_ = res.Close()
			return 0, 0, err
		}
		offered += n
		inserted += tag.RowsAffected()
	}
	return offered, inserted, res.Close()
}

// writeCopy writes a batch through COPY into temporary tables, then
// INSERT … SELECT … ON CONFLICT DO NOTHING into each history table.
func (s *Sink) writeCopy(ctx context.Context, tx pgx.Tx, batch []*Prepared) (offered, inserted int64, err error) {
	create := &pgx.Batch{}
	for _, t := range historyTables {
		create.Queue(t.createTemp)
	}
	if err := tx.SendBatch(ctx, create).Close(); err != nil {
		return 0, 0, err
	}
	insert := &pgx.Batch{}
	var copied []int64
	for i, t := range historyTables {
		it := &rowIter{sets: rowSetsOf(batch, i)}
		n, err := tx.CopyFrom(ctx, pgx.Identifier{t.tempName()}, t.colNames, it)
		if err != nil {
			return 0, 0, fmt.Errorf("copy %s: %w", t.name, err)
		}
		if n == 0 {
			continue
		}
		insert.Queue(t.insertTemp)
		copied = append(copied, n)
	}
	res := tx.SendBatch(ctx, insert)
	for _, n := range copied {
		tag, err := res.Exec()
		if err != nil {
			_ = res.Close()
			return 0, 0, err
		}
		offered += n
		inserted += tag.RowsAffected()
	}
	return offered, inserted, res.Close()
}

func rowSetsOf(batch []*Prepared, table int) []rowSet {
	sets := make([]rowSet, len(batch))
	for i, p := range batch {
		sets[i] = p.rows(table)
	}
	return sets
}

// rowIter feeds the rows of several ledgers to COPY.
type rowIter struct {
	sets []rowSet
	set  int
	row  int
	cur  []any
}

func (it *rowIter) Next() bool {
	for it.set < len(it.sets) {
		if it.row < it.sets[it.set].Len() {
			it.cur = it.sets[it.set].Values(it.row)
			it.row++
			return true
		}
		it.set++
		it.row = 0
	}
	return false
}

func (it *rowIter) Values() ([]any, error) { return it.cur, nil }
func (it *rowIter) Err() error             { return nil }

// classify marks database errors: deterministic ones (a data or schema
// error reported by the server) halt the pipeline, because retrying the same
// rows cannot help; everything else (connection failures, shutdowns,
// timeouts, driver errors) is sequencer.Transient.
func classify(err error) error {
	if err == nil || errors.Is(err, context.Canceled) || sequencer.IsTransient(err) || sequencer.IsHalt(err) {
		return err
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && !isTransientPG(err) {
		return sequencer.Halt(0, err)
	}
	return sequencer.Transient(err)
}

// atLedger sets the ledger of a halt that classify could not know.
func atLedger(seq uint32, err error) error {
	var he *sequencer.HaltError
	if errors.As(err, &he) && he.Seq == 0 {
		he.Seq = seq
	}
	return err
}

func isTransientPG(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code[:2] {
		case "08", // connection exception
			"40", // serialization failure, deadlock
			"53", // insufficient resources
			"57", // operator intervention: shutdown, query cancelled
			"58": // system error
			return true
		}
		return pgErr.Code == "55P03" // lock_not_available (lock_timeout)
	}
	var ce *pgconn.ConnectError
	var ne net.Error
	return errors.As(err, &ce) || errors.As(err, &ne) || pgconn.SafeToRetry(err) || pgconn.Timeout(err) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, net.ErrClosed)
}
