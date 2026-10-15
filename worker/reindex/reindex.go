// Package reindex implements `worker reindex --from A --to B [--source …]
// [--dry-run]`: it re-runs processLedger over a closed,
// already committed range and replaces the ledger rows ledger by ledger
// (delete, then insert, in the commit that advances its own cursor
// `reindex:<A>-<B>`). Current-state tables are never touched: only
// `worker bootstrap` rebuilds them.
//
// With --dry-run it only reads, decodes and validates the range (sequence,
// self-hash, hash chain, gaps) and reports every problem; nothing is
// written. `worker gapcheck` falls back to it against the archive.
package reindex

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stellar/go-stellar-sdk/network"

	"stellar-explorer/worker/explorer"
	"stellar-explorer/worker/sep54"
	"stellar-explorer/worker/sequencer"
)

// Options configure Run.
type Options struct {
	// DSN connects as ingest_writer (unused with DryRun).
	DSN      string
	From, To uint32
	Source   sequencer.Source
	DryRun   bool
	Log      *slog.Logger
	Alert    func(kind, msg string, args ...any)
	OnCommit func(sequencer.CommitInfo)
	// MaxBatchLedgers / MaxBatchBytes bound one replacement commit.
	MaxBatchLedgers int
	MaxBatchBytes   int64
	// Workers is the dry run's read concurrency (default 16).
	Workers int
	// AboveCursor writes the history of a range that starts right after
	// the explorer cursor instead of a committed one: `bootstrap --force`
	// writes P+1..C this way before it moves the cursor to C.
	// The explorer cursor must be at From − 1 and ingest must be stopped.
	AboveCursor bool
}

// Result summarises a run.
type Result struct {
	Pipeline string
	// From is where this run started (a resumed run starts after the
	// ledgers an earlier run already replaced).
	From      uint32
	Ledgers   int
	Conflicts uint64
	// Dry run only: ledgers missing from the source, and validation
	// failures ("<seq>: <error>").
	Missing []uint32
	Invalid []string
}

// Pipeline returns the ingest_state row name of a reindex of [from, to].
func Pipeline(from, to uint32) string { return fmt.Sprintf("reindex:%d-%d", from, to) }

// Run reindexes (or with DryRun, validates) [From, To].
func Run(ctx context.Context, o Options) (Result, error) {
	if o.From < 2 || o.To < o.From {
		return Result{}, fmt.Errorf("bad range %d-%d: want 2 ≤ from ≤ to", o.From, o.To)
	}
	if o.Log == nil {
		o.Log = slog.Default()
	}
	if o.DryRun {
		return dryRun(ctx, o)
	}
	return replace(ctx, o)
}

func replace(ctx context.Context, o Options) (Result, error) {
	log := o.Log
	pipeline := Pipeline(o.From, o.To)
	res := Result{Pipeline: pipeline}
	sink, err := explorer.NewSink(o.DSN, explorer.SinkOptions{Pipeline: pipeline, Replace: true, Lock: true, Log: log, Alert: o.Alert})
	if err != nil {
		return res, err
	}
	defer sink.Close()
	pool := sink.Pool()
	if err := explorer.CheckSchema(ctx, pool); err != nil {
		return res, err
	}
	// Only committed ledgers: the live pipeline writes only above its
	// cursor, a reindex only at or below it, so they never overlap.
	cursor, _, err := explorer.ReadCursor(ctx, pool, explorer.Pipeline)
	if err != nil {
		return res, err
	}
	switch {
	case o.AboveCursor && cursor != o.From-1:
		return res, fmt.Errorf("the explorer cursor is at %d, not %d: the range must start right after it", cursor, o.From-1)
	case !o.AboveCursor && cursor < o.To:
		return res, fmt.Errorf("reindex covers only committed ledgers: the explorer cursor is at %d, below %d", cursor, o.To)
	}

	start := o.From
	seq, _, err := explorer.ReadCursor(ctx, pool, pipeline)
	switch {
	case errors.Is(err, explorer.ErrNoCursor):
		if err := seed(ctx, o, pool); err != nil {
			return res, err
		}
	case err != nil:
		return res, err
	case seq == o.To:
		log.Info("a previous reindex of this range finished; running it again", "pipeline", pipeline)
		if err := explorer.DeleteCursor(ctx, pool, pipeline); err != nil {
			return res, err
		}
		if err := seed(ctx, o, pool); err != nil {
			return res, err
		}
	case seq >= o.From-1 && seq < o.To:
		start = seq + 1
		log.Info("resuming an interrupted reindex", "pipeline", pipeline, "from", start)
	default:
		return res, fmt.Errorf("ingest_state %q is at %d, outside %d-%d", pipeline, seq, o.From-1, o.To)
	}
	res.From = start
	if err := sink.Start(ctx); err != nil {
		return res, err
	}
	s, err := sequencer.New(sequencer.Options[*explorer.Prepared]{
		Pipeline:        pipeline,
		Sink:            sink,
		Process:         explorer.LedgerProcessor(network.PublicNetworkPassphrase),
		Fixed:           &sequencer.Fixed{Source: o.Source, To: o.To},
		MaxBatchLedgers: o.MaxBatchLedgers,
		MaxBatchBytes:   o.MaxBatchBytes,
		Log:             log,
		Alert:           o.Alert,
		OnCommit:        o.OnCommit,
	})
	if err != nil {
		return res, err
	}
	err = s.Run(ctx)
	st := s.Stats()
	res.Ledgers, res.Conflicts = int(st.Committed), sink.Conflicts()
	if err != nil {
		return res, err
	}
	if ctx.Err() != nil {
		return res, ctx.Err()
	}
	if done, _, err := explorer.ReadCursor(ctx, pool, pipeline); err != nil || done != o.To {
		return res, fmt.Errorf("reindex stopped at %d before %d: %v", done, o.To, err)
	}
	return res, nil
}

// seed creates the reindex cursor at From−1, linked by ledger From's
// previous hash, after checking that hash against explorer.ledgers.
func seed(ctx context.Context, o Options, pool *pgxpool.Pool) error {
	l, err := readValidated(ctx, o.Source, o.From)
	if err != nil {
		return err
	}
	if h, ok, err := explorer.LedgerHash(ctx, pool, o.From-1); err != nil {
		return err
	} else if ok && h != l.PreviousHash {
		return fmt.Errorf("ledger %d from %s has previous hash %x, explorer.ledgers has %x for ledger %d",
			o.From, o.Source.Name(), l.PreviousHash, h, o.From-1)
	}
	return explorer.SeedCursor(ctx, pool, Pipeline(o.From, o.To), o.From-1, l.PreviousHash)
}

// readValidated reads and fully decodes one ledger, retrying while the
// source is unavailable.
func readValidated(ctx context.Context, src sequencer.Source, seq uint32) (sep54.Header, error) {
	delay := 200 * time.Millisecond
	for attempt := 1; ; attempt++ {
		obj, err := src.GetLedger(ctx, seq)
		if err == nil {
			_, h, err := sep54.DecodeLedger(seq, obj.Body)
			return h, err
		}
		if !sep54.IsUnavailable(err) || attempt == 8 || ctx.Err() != nil {
			return sep54.Header{}, fmt.Errorf("read ledger %d from %s: %w", seq, src.Name(), err)
		}
		select {
		case <-ctx.Done():
			return sep54.Header{}, ctx.Err()
		case <-time.After(delay):
		}
		delay = min(2*delay, 5*time.Second)
	}
}
