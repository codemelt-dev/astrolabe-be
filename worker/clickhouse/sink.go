package clickhouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stellar/go-stellar-sdk/xdr"

	"stellar-explorer/worker/explorer"
	"stellar-explorer/worker/sep54"
	"stellar-explorer/worker/sequencer"
)

// ErrNoCursor means the pipeline has no cursor yet: run
// `worker clickhouse-init`.
var ErrNoCursor = errors.New("no clickhouse cursor yet: run `worker clickhouse-init`")

// ErrCursorMoved means another writer moved the cursor under this one.
var ErrCursorMoved = errors.New("the clickhouse cursor moved")

// ReadCursor returns the pipeline's cursor: its last committed ledger and
// that ledger's hash.
func ReadCursor(ctx context.Context, conn driver.Conn, pipeline string) (uint32, xdr.Hash, error) {
	var seq uint32
	var hash string
	err := conn.QueryRow(ctx, "SELECT ledger_sequence, ledger_hash FROM ingest_state WHERE pipeline = ? ORDER BY ledger_sequence DESC LIMIT 1",
		pipeline).Scan(&seq, &hash)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, xdr.Hash{}, ErrNoCursor
	case err != nil:
		return 0, xdr.Hash{}, err
	case len(hash) != len(xdr.Hash{}):
		return 0, xdr.Hash{}, fmt.Errorf("ingest_state: a %d-byte hash for ledger %d", len(hash), seq)
	}
	var h xdr.Hash
	copy(h[:], hash)
	return seq, h, nil
}

// writeCursor records a committed ledger.
func writeCursor(ctx context.Context, conn driver.Conn, pipeline string, seq uint32, hash xdr.Hash) error {
	return conn.Exec(ctx, "INSERT INTO ingest_state (pipeline, ledger_sequence, ledger_hash, updated_at) VALUES (?, ?, ?, ?)",
		pipeline, seq, string(hash[:]), time.Now().UTC())
}

// Init seeds the pipeline's cursor once: the ledger before the first one
// to write, and its hash. A cursor that is already there is kept; asking
// for another one is an error.
func Init(ctx context.Context, conn driver.Conn, pipeline string, seq uint32, hash xdr.Hash) (bool, error) {
	cur, h, err := ReadCursor(ctx, conn, pipeline)
	switch {
	case errors.Is(err, ErrNoCursor):
		return true, writeCursor(ctx, conn, pipeline, seq, hash)
	case err != nil:
		return false, err
	case cur != seq || h != hash:
		return false, fmt.Errorf("the %s cursor is already at %d (%x): it is never moved by init", pipeline, cur, h)
	}
	return false, nil
}

// SinkOptions configure a Sink.
type SinkOptions struct {
	Pipeline string // default Pipeline
	Log      *slog.Logger
}

// Sink writes the clickhouse pipeline's batches (sequencer.Sink).
type Sink struct {
	conn driver.Conn
	o    SinkOptions

	mu    sync.Mutex
	seq   uint32
	hash  xdr.Hash
	known bool
	// dirty: rows above the cursor may exist (a start, or a failed commit)
	// and must be deleted before the next batch is written.
	dirty bool
}

// NewSink returns a sink on conn. It starts dirty: the first commit checks
// for rows a previous run left above the cursor.
func NewSink(conn driver.Conn, o SinkOptions) *Sink {
	if o.Pipeline == "" {
		o.Pipeline = Pipeline
	}
	if o.Log == nil {
		o.Log = slog.Default()
	}
	return &Sink{conn: conn, o: o, dirty: true}
}

// Cursor implements sequencer.Sink.
func (s *Sink) Cursor(ctx context.Context) (uint32, xdr.Hash, error) {
	seq, hash, err := ReadCursor(ctx, s.conn, s.o.Pipeline)
	if err != nil {
		if errors.Is(err, ErrNoCursor) {
			return 0, xdr.Hash{}, sequencer.Halt(0, err)
		}
		return 0, xdr.Hash{}, classify(0, err)
	}
	s.mu.Lock()
	s.seq, s.hash, s.known = seq, hash, true
	s.mu.Unlock()
	return seq, hash, nil
}

// LastCursor is the last cursor read or written, for the metrics.
func (s *Sink) LastCursor() (uint32, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.seq, s.known
}

// Ping checks the connection, for the health endpoint.
func (s *Sink) Ping(ctx context.Context) error { return s.conn.Ping(ctx) }

// Commit implements sequencer.Sink: the batch's rows, table by table, then
// the cursor. A failure leaves the sink dirty; the next commit re-reads the
// cursor, which tells whether the failed one landed, and deletes whatever
// it left above the cursor before writing.
func (s *Sink) Commit(ctx context.Context, batch []*explorer.Prepared) error {
	if len(batch) == 0 {
		return nil
	}
	first, last := batch[0].Header(), batch[len(batch)-1].Header()
	for i := 1; i < len(batch); i++ {
		if err := sep54.CheckChain(batch[i-1].Header(), batch[i].Header()); err != nil {
			return sequencer.Halt(batch[i].Header().Sequence, fmt.Errorf("batch is not a chain: %w", err))
		}
	}
	s.mu.Lock()
	dirty, known := s.dirty, s.known
	s.mu.Unlock()
	if dirty || !known {
		if _, _, err := s.Cursor(ctx); err != nil {
			return err
		}
	}
	s.mu.Lock()
	cur, hash := s.seq, s.hash
	s.mu.Unlock()
	switch {
	case cur == last.Sequence && hash == last.Hash:
		s.o.Log.Info("batch was already committed", "pipeline", s.o.Pipeline, "first", first.Sequence, "last", last.Sequence)
		return nil
	case cur+1 != first.Sequence:
		return fmt.Errorf("%w: cursor is at %d, the batch starts at %d", ErrCursorMoved, cur, first.Sequence)
	case hash != first.PreviousHash:
		return sequencer.Halt(first.Sequence, fmt.Errorf("hash mismatch: ledger %d has previous hash %x, the cursor holds %x",
			first.Sequence, first.PreviousHash, hash))
	}

	if dirty {
		if err := s.cleanAbove(ctx, cur); err != nil {
			return classify(first.Sequence, err)
		}
	}
	if err := s.write(ctx, batch, first.Sequence); err != nil {
		s.setDirty()
		return err
	}
	if err := writeCursor(ctx, s.conn, s.o.Pipeline, last.Sequence, last.Hash); err != nil {
		s.setDirty()
		return classify(first.Sequence, err)
	}
	s.mu.Lock()
	s.seq, s.hash, s.known, s.dirty = last.Sequence, last.Hash, true, false
	s.mu.Unlock()
	return nil
}

func (s *Sink) setDirty() {
	s.mu.Lock()
	s.dirty = true
	s.mu.Unlock()
}

// write inserts the batch into every table, one columnar insert each.
func (s *Sink) write(ctx context.Context, batch []*explorer.Prepared, first uint32) error {
	for _, t := range tables {
		if t.build == nil {
			continue // filled by a materialized view
		}
		cols, n, err := t.build(batch)
		if err != nil {
			return sequencer.Halt(first, fmt.Errorf("%s: %w", t.name, err))
		}
		if n == 0 {
			continue
		}
		b, err := s.conn.PrepareBatch(ctx, t.insert())
		if err != nil {
			return classify(first, fmt.Errorf("%s: %w", t.name, err))
		}
		for i, c := range cols {
			if err := b.Column(i).Append(c); err != nil {
				_ = b.Abort()
				// The rows don't fit the table: retrying cannot help.
				return sequencer.Halt(first, fmt.Errorf("%s.%s: %w", t.name, t.cols[i], err))
			}
		}
		if err := b.Send(); err != nil {
			return classify(first, fmt.Errorf("%s: %w", t.name, err))
		}
	}
	return nil
}

// cleanAbove deletes every history row above the cursor: what a commit that
// failed half-way, or a run that stopped in one, left behind. Only tables
// that hold such rows get a (lightweight) DELETE.
func (s *Sink) cleanAbove(ctx context.Context, cursor uint32) error {
	for _, t := range tables {
		var n uint64
		if err := s.conn.QueryRow(ctx, fmt.Sprintf("SELECT count() FROM %s WHERE %s > ?", t.name, t.seqCol), cursor).Scan(&n); err != nil {
			return fmt.Errorf("%s: count rows above the cursor: %w", t.name, err)
		}
		if n == 0 {
			continue
		}
		s.o.Log.Warn("deleting rows above the cursor", "pipeline", s.o.Pipeline, "table", t.name, "cursor", cursor, "rows", n)
		if err := s.conn.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE %s > ?", t.name, t.seqCol), cursor); err != nil {
			return fmt.Errorf("%s: delete rows above the cursor: %w", t.name, err)
		}
	}
	s.mu.Lock()
	s.dirty = false
	s.mu.Unlock()
	return nil
}

var _ sequencer.Sink[*explorer.Prepared] = (*Sink)(nil)
