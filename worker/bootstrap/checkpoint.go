package bootstrap

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/historyarchive"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/support/storage"
	"github.com/stellar/go-stellar-sdk/xdr"

	"stellar-explorer/worker/explorer"
	"stellar-explorer/worker/reindex"
	"stellar-explorer/worker/sep54"
	"stellar-explorer/worker/sequencer"
)

// Options configure Run (the first bootstrap), Force (state repair) and
// RecreateOrigin.
type Options struct {
	// DSN connects as ingest_writer.
	DSN string
	// Archives are the history archive URLs, tried in order. Archive
	// replaces them (tests).
	Archives []string
	Archive  historyarchive.ArchiveInterface
	// Passphrase is the network (default pubnet). The archives' HAS must
	// match it.
	Passphrase string
	// LakeDir holds state/bootstrap.json.
	LakeDir string
	// Sources (lake, archive, AWS lake) supply the history P+1..C that
	// `--force` writes before it moves the cursor to C.
	Sources []sequencer.Source
	// CheckpointPoll is how often `--force` re-reads the archive while it
	// waits for a checkpoint at or above the cursor (default 30 s).
	CheckpointPoll time.Duration
	// MaxBatchLedgers / MaxBatchBytes bound the history commits of --force.
	MaxBatchLedgers int
	MaxBatchBytes   int64
	Log             *slog.Logger
	// ProgressEvery is the interval of progress logs (default 30 s).
	ProgressEvery time.Duration
}

func (o Options) withDefaults() Options {
	if o.Passphrase == "" {
		o.Passphrase = network.PublicNetworkPassphrase
	}
	if o.CheckpointPoll <= 0 {
		o.CheckpointPoll = 30 * time.Second
	}
	if o.Log == nil {
		o.Log = slog.Default()
	}
	if o.ProgressEvery <= 0 {
		o.ProgressEvery = 30 * time.Second
	}
	return o
}

// StateResult describes a checkpoint load.
type StateResult struct {
	// Checkpoint is C and Hash its ledger hash.
	Checkpoint uint32
	Hash       xdr.Hash
	// PreviousCursor is the cursor before `--force` (P).
	PreviousCursor uint32
	// HistoryLedgers is how many ledgers (P+1..C) `--force` wrote.
	HistoryLedgers int
	// HistorySource is where those ledgers were read.
	HistorySource string
	// Entries are the rows loaded per state table.
	Entries map[string]int64
	// Duration is the whole run; LoadDuration the state load alone.
	Duration, LoadDuration time.Duration
	// Origin is state/bootstrap.json; OriginWritten tells whether this run
	// created it.
	Origin        Origin
	OriginWritten bool
}

// ErrIngestRunning means another session holds the explorer pipeline's
// advisory lock: stop stellar-ingest first.
var ErrIngestRunning = errors.New("another session holds the explorer pipeline's advisory lock: stop stellar-ingest first")

// Run is the first bootstrap: it loads the current state from
// the latest history-archive checkpoint C, seeds the explorer cursor at
// (C, hash_C) in the same transaction, and writes state/bootstrap.json if it
// is absent.
func Run(ctx context.Context, o Options) (StateResult, error) {
	o = o.withDefaults()
	start := time.Now()
	db, err := openDB(ctx, o.DSN)
	if err != nil {
		return StateResult{}, err
	}
	defer db.close()
	if seq, _, err := explorer.ReadCursor(ctx, db.conn, explorer.Pipeline); err == nil {
		return StateResult{}, fmt.Errorf("%w %q (cursor at %d): bootstrap runs once; use --force to repair the state", explorer.ErrCursorExists, explorer.Pipeline, seq)
	} else if !errors.Is(err, explorer.ErrNoCursor) {
		return StateResult{}, err
	}
	arch, has, err := o.connect(ctx)
	if err != nil {
		return StateResult{}, err
	}
	c := has.CurrentLedger
	o.Log.Info("latest checkpoint", "checkpoint", c, "server", has.Server)
	cp, err := openCheckpoint(ctx, arch, c, o.Log)
	if err != nil {
		return StateResult{}, err
	}
	defer cp.reader.Close()

	res := StateResult{Checkpoint: c, Hash: cp.header.Hash}
	err = load(ctx, db.conn, cp, o, &res, func(tx pgx.Tx) error {
		return explorer.SeedCursor(ctx, tx, explorer.Pipeline, c, cp.header.Hash)
	})
	if err != nil {
		return res, err
	}
	return finish(o, start, &res, NewOrigin(o.Passphrase, c, cp.header.Hash))
}

// Force repairs the state tables (`--force`). Ingest must be
// stopped (Force takes the pipeline's advisory lock).
//
//  1. Read the cursor P, then wait until the latest published checkpoint C
//     satisfies C ≥ P.
//  2. Write the ledger rows of P+1..C with the reindex code path
//     (skipped when C = P).
//  3. Truncate only the state tables and load checkpoint C.
//  4. Move the cursor from P to (C, hash_C) in the transaction of the
//     load.
//
// The cursor never moves backwards, no history is skipped, and
// bootstrap.json is never rewritten (it is only recreated if lost).
func Force(ctx context.Context, o Options) (StateResult, error) {
	o = o.withDefaults()
	start := time.Now()
	db, err := openDB(ctx, o.DSN)
	if err != nil {
		return StateResult{}, err
	}
	defer db.close()
	p, pHash, err := explorer.ReadCursor(ctx, db.conn, explorer.Pipeline)
	if errors.Is(err, explorer.ErrNoCursor) {
		return StateResult{}, fmt.Errorf("%w: run `worker bootstrap` (without --force) first", err)
	}
	if err != nil {
		return StateResult{}, err
	}
	res := StateResult{PreviousCursor: p}
	o.Log.Info("repairing the state tables", "cursor", p)

	arch, has, err := o.connect(ctx)
	if err != nil {
		return res, err
	}
	for has.CurrentLedger < p {
		o.Log.Info("waiting for a checkpoint at or above the cursor", "cursor", p, "latest_checkpoint", has.CurrentLedger,
			"poll", o.CheckpointPoll)
		select {
		case <-ctx.Done():
			return res, ctx.Err()
		case <-time.After(o.CheckpointPoll):
		}
		if has, err = arch.GetRootHAS(); err != nil {
			o.Log.Warn("read the latest checkpoint", "err", err)
			has.CurrentLedger = 0
		}
	}
	c := has.CurrentLedger
	res.Checkpoint = c
	cp, err := openCheckpoint(ctx, arch, c, o.Log)
	if err != nil {
		return res, err
	}
	defer cp.reader.Close()
	res.Hash = cp.header.Hash

	// History P+1..C, then check it ends at the checkpoint's ledger.
	var reindexPipeline string
	if c == p {
		if pHash != cp.header.Hash {
			return res, fmt.Errorf("checkpoint %d has hash %x, the explorer cursor holds %x: the archive and the pipeline disagree",
				c, cp.header.Hash, pHash)
		}
	} else {
		reindexPipeline = reindex.Pipeline(p+1, c)
		if err := o.writeHistory(ctx, db, p, pHash, c, cp.header.Hash, &res); err != nil {
			return res, err
		}
	}

	err = load(ctx, db.conn, cp, o, &res, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `UPDATE public.ingest_state SET ledger_sequence = $1, ledger_hash = $2, updated_at = now()
			WHERE pipeline = $3 AND ledger_sequence = $4 AND ledger_hash = $5`,
			int64(c), hashHex(cp.header.Hash), explorer.Pipeline, int64(p), hashHex(pHash))
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("%w: the explorer cursor is no longer at %d", explorer.ErrCursorMoved, p)
		}
		if reindexPipeline != "" {
			return explorer.DeleteCursor(ctx, tx, reindexPipeline)
		}
		return nil
	})
	if err != nil {
		return res, err
	}
	origin, err := originFromDB(ctx, db.conn, o.Passphrase)
	if err != nil {
		return res, err
	}
	return finish(o, start, &res, origin)
}

// writeHistory writes ledgers p+1..c through the reindex code path
// (history rows only) from the first source that holds both ends, after
// checking that p+1 links to the cursor; it then checks that ledger c is
// the checkpoint's.
func (o Options) writeHistory(ctx context.Context, db *dbConn, p uint32, pHash xdr.Hash, c uint32, cHash xdr.Hash, res *StateResult) error {
	src, first, err := pickSource(ctx, o.Sources, p+1, c, o.Log)
	if err != nil {
		return err
	}
	if first.PreviousHash != pHash {
		return fmt.Errorf("ledger %d from %s has previous hash %x, the explorer cursor holds %x", p+1, src.Name(), first.PreviousHash, pHash)
	}
	o.Log.Info("writing history up to the checkpoint", "from", p+1, "to", c, "source", src.Name())
	_, err = reindex.Run(ctx, reindex.Options{
		DSN: o.DSN, From: p + 1, To: c, Source: src, AboveCursor: true, Log: o.Log,
		MaxBatchLedgers: o.MaxBatchLedgers, MaxBatchBytes: o.MaxBatchBytes,
	})
	if err != nil {
		return fmt.Errorf("write history %d-%d: %w", p+1, c, err)
	}
	seq, h, err := explorer.ReadCursor(ctx, db.conn, reindex.Pipeline(p+1, c))
	if err != nil {
		return err
	}
	if seq != c || h != cHash {
		return fmt.Errorf("history from %s ends at ledger %d with hash %x; checkpoint %d has hash %x", src.Name(), seq, h, c, cHash)
	}
	res.HistoryLedgers, res.HistorySource = int(c-p), src.Name()
	return nil
}

// pickSource returns the first source that holds ledgers from and to, and
// ledger from's validated header.
func pickSource(ctx context.Context, sources []sequencer.Source, from, to uint32, log *slog.Logger) (sequencer.Source, sep54.Header, error) {
	var errs []error
	for _, src := range sources {
		h, err := readValidated(ctx, src, from)
		if err == nil {
			_, err = readValidated(ctx, src, to)
		}
		if err == nil {
			return src, h, nil
		}
		if ctx.Err() != nil {
			return nil, sep54.Header{}, ctx.Err()
		}
		log.Info("source does not hold the range; trying the next", "source", src.Name(), "from", from, "to", to, "err", err)
		errs = append(errs, fmt.Errorf("%s: %w", src.Name(), err))
	}
	return nil, sep54.Header{}, fmt.Errorf("no source holds ledgers %d-%d: %w", from, to, errors.Join(errs...))
}

func readValidated(ctx context.Context, src sequencer.Source, seq uint32) (sep54.Header, error) {
	obj, err := src.GetLedger(ctx, seq)
	if err != nil {
		return sep54.Header{}, err
	}
	_, h, err := sep54.DecodeLedger(seq, obj.Body)
	return h, err
}

// checkpoint is an opened, verified checkpoint.
type checkpoint struct {
	header xdr.LedgerHeaderHistoryEntry
	reader *ingest.CheckpointChangeReader
}

// openCheckpoint fetches checkpoint c's header, checks its self-hash, opens
// the checkpoint reader and verifies the bucket list against the header
// before anything is streamed.
func openCheckpoint(ctx context.Context, arch historyarchive.ArchiveInterface, c uint32, log *slog.Logger) (checkpoint, error) {
	if !arch.GetCheckpointManager().IsCheckpoint(c) {
		return checkpoint{}, fmt.Errorf("ledger %d is not a checkpoint ((C+1) %% 64 != 0)", c)
	}
	hdr, err := arch.GetLedgerHeader(c)
	if err != nil {
		return checkpoint{}, fmt.Errorf("checkpoint %d header: %w", c, err)
	}
	if uint32(hdr.Header.LedgerSeq) != c {
		return checkpoint{}, fmt.Errorf("checkpoint %d: the archive returned the header of ledger %d", c, hdr.Header.LedgerSeq)
	}
	b, err := hdr.Header.MarshalBinary()
	if err != nil {
		return checkpoint{}, err
	}
	if sum := xdr.Hash(sha256.Sum256(b)); sum != hdr.Hash {
		return checkpoint{}, fmt.Errorf("checkpoint %d: sha256(header) %x does not match its hash %x", c, sum, hdr.Hash)
	}
	reader, err := ingest.NewCheckpointChangeReader(ctx, arch, c)
	if err != nil {
		return checkpoint{}, fmt.Errorf("checkpoint %d: %w", c, err)
	}
	if err := reader.VerifyBucketList(hdr.Header.BucketListHash); err != nil {
		reader.Close()
		return checkpoint{}, fmt.Errorf("checkpoint %d: %w", c, err)
	}
	log.Info("checkpoint verified", "checkpoint", c, "hash", hashHex(hdr.Hash), "protocol", hdr.Header.LedgerVersion)
	return checkpoint{header: hdr, reader: reader}, nil
}

// load runs the state load transaction: truncate the state tables, COPY
// every checkpoint entry, record the checkpoint, and run cursor (seed or
// move the cursor) before COMMIT.
func load(ctx context.Context, conn *pgx.Conn, cp checkpoint, o Options, res *StateResult, cursor func(pgx.Tx) error) error {
	start := time.Now()
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := explorer.TruncateState(ctx, tx); err != nil {
		return fmt.Errorf("truncate the state tables: %w", err)
	}
	loader := explorer.NewStateLoader(tx, o.Passphrase)
	var n int64
	lastLog := time.Now()
	for {
		ch, err := cp.reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("read checkpoint %d: %w", res.Checkpoint, err)
		}
		if ch.Post == nil {
			return fmt.Errorf("checkpoint %d: a change without a post-state", res.Checkpoint)
		}
		if err := loader.Add(ctx, *ch.Post); err != nil {
			return err
		}
		n++
		if time.Since(lastLog) >= o.ProgressEvery {
			lastLog = time.Now()
			o.Log.Info("loading state", "checkpoint", res.Checkpoint, "entries_read", n,
				"progress_pct", fmt.Sprintf("%.1f", cp.reader.Progress()), "rows", loader.Counts())
		}
	}
	if err := loader.Flush(ctx); err != nil {
		return err
	}
	if err := cursor(tx); err != nil {
		return err
	}
	res.Entries = loader.Counts()
	res.LoadDuration = time.Since(start)
	if err := explorer.WriteStateCheckpoint(ctx, tx, explorer.StateCheckpoint{
		Checkpoint: res.Checkpoint, Hash: res.Hash, LoadSeconds: res.LoadDuration.Seconds(), Entries: res.Entries,
	}); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit the state load: %w", err)
	}
	o.Log.Info("state loaded", "checkpoint", res.Checkpoint, "entries_read", n, "rows", res.Entries, "duration", res.LoadDuration.Round(time.Millisecond))
	return nil
}

// finish writes bootstrap.json if absent.
func finish(o Options, start time.Time, res *StateResult, origin Origin) (StateResult, error) {
	if o.LakeDir != "" {
		written, existing, err := WriteOriginIfAbsent(o.LakeDir, origin)
		if err != nil {
			return *res, fmt.Errorf("state loaded, but %s: %w", OriginFile(o.LakeDir), err)
		}
		res.OriginWritten = written
		res.Origin = origin
		if !written {
			res.Origin = existing
			if existing != origin {
				o.Log.Warn("state/bootstrap.json already exists and is kept (archive_origin is never rewritten)",
					"existing", existing, "this_run", origin)
			}
		}
	}
	res.Duration = time.Since(start)
	return *res, nil
}

// originFromDB rebuilds bootstrap.json's content from Postgres:
// C = min(explorer.ledgers.sequence) − 1 and hash_C its previous
// hash, or the cursor if no ledger has been committed.
func originFromDB(ctx context.Context, conn *pgx.Conn, passphrase string) (Origin, error) {
	var seq *int64
	var prev []byte
	err := conn.QueryRow(ctx, "SELECT sequence, prev_hash FROM explorer.ledgers ORDER BY sequence LIMIT 1").Scan(&seq, &prev)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		c, h, err := explorer.ReadCursor(ctx, conn, explorer.Pipeline)
		if err != nil {
			return Origin{}, err
		}
		return NewOrigin(passphrase, c, h), nil
	case err != nil:
		return Origin{}, err
	}
	var h xdr.Hash
	if len(prev) != len(h) {
		return Origin{}, fmt.Errorf("explorer.ledgers %d: bad prev_hash length %d", *seq, len(prev))
	}
	copy(h[:], prev)
	return NewOrigin(passphrase, uint32(*seq-1), h), nil
}

// RecreateOrigin writes state/bootstrap.json from Postgres if it was lost
// (galexie/README.md §7.4.15). An existing file is returned unchanged.
func RecreateOrigin(ctx context.Context, o Options) (Origin, bool, error) {
	o = o.withDefaults()
	if cur, err := ReadOrigin(o.LakeDir); err == nil {
		return cur, false, nil
	} else if !errors.Is(err, ErrNoOrigin) {
		return Origin{}, false, err
	}
	conn, err := pgx.Connect(ctx, o.DSN)
	if err != nil {
		return Origin{}, false, fmt.Errorf("connect to postgres: %w", err)
	}
	defer conn.Close(context.WithoutCancel(ctx))
	origin, err := originFromDB(ctx, conn, o.Passphrase)
	if err != nil {
		return Origin{}, false, err
	}
	written, existing, err := WriteOriginIfAbsent(o.LakeDir, origin)
	if !written && err == nil {
		return existing, false, nil
	}
	return origin, written, err
}

// connect returns the history archive and its latest checkpoint: the
// configured Archive, or the first archive URL that answers.
func (o Options) connect(ctx context.Context) (historyarchive.ArchiveInterface, historyarchive.HistoryArchiveState, error) {
	if o.Archive != nil {
		has, err := o.Archive.GetRootHAS()
		return o.Archive, has, err
	}
	var errs []error
	for _, u := range o.Archives {
		a, err := historyarchive.Connect(u, historyarchive.ArchiveOptions{
			NetworkPassphrase:   o.Passphrase,
			CheckpointFrequency: historyarchive.DefaultCheckpointFrequency,
			ConnectOptions:      storage.ConnectOptions{Context: ctx, UserAgent: "stellar-explorer-worker"},
		})
		if err == nil {
			var has historyarchive.HistoryArchiveState
			if has, err = a.GetRootHAS(); err == nil {
				return a, has, nil
			}
		}
		o.Log.Warn("history archive unavailable; trying the next", "archive", u, "err", err)
		errs = append(errs, fmt.Errorf("%s: %w", u, err))
	}
	return nil, historyarchive.HistoryArchiveState{}, fmt.Errorf("no history archive answered: %w", errors.Join(errs...))
}

// dbConn is the load connection plus the pipeline's advisory lock, held on
// a second connection for the whole run so ingest cannot commit meanwhile.
type dbConn struct {
	conn, lock *pgx.Conn
}

func openDB(ctx context.Context, dsn string) (*dbConn, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}
	db := &dbConn{conn: conn}
	if err := explorer.CheckSchema(ctx, conn); err != nil {
		db.close()
		return nil, err
	}
	if db.lock, err = pgx.Connect(ctx, dsn); err != nil {
		db.close()
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}
	var got bool
	if err := db.lock.QueryRow(ctx, "SELECT pg_try_advisory_lock(hashtext($1))", explorer.Pipeline).Scan(&got); err != nil {
		db.close()
		return nil, err
	}
	if !got {
		db.close()
		return nil, ErrIngestRunning
	}
	return db, nil
}

func (db *dbConn) close() {
	if db.lock != nil {
		_ = db.lock.Close(context.Background())
	}
	_ = db.conn.Close(context.Background())
}

func hashHex(h xdr.Hash) string { return fmt.Sprintf("%x", h[:]) }
