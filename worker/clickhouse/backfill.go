package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/processors/token_transfer"
	"github.com/stellar/go-stellar-sdk/xdr"

	"stellar-explorer/worker/explorer"
	"stellar-explorer/worker/sep54"
	"stellar-explorer/worker/sequencer"
)

// Backfill is `worker clickhouse-backfill`: it writes the history of a
// closed range of ledgers below go-live, from the AWS public lake, into
// the tables the live pipeline writes.
//
//	the range is split into segments of Segment ledgers, aligned to its
//	multiples, newest first; Parallel run at once. Each segment [a, b]:
//	    pipeline "backfill:<a>-<b>", its own cursor in ingest_state,
//	        seeded at a − 1 with ledger a's previous hash from the lake
//	    a sequencer over the fixed range, reading LedgerCloseMeta V0, V1
//	        and V2, with the live pipeline's processor
//	    a sink whose Ceiling is b: its cleanup after a failed commit
//	        deletes rows in (cursor, b] only, never a newer segment's or
//	        the live pipeline's
//	    every VerifyEvery-th ledger: the SDK's check of the transfers
//	        against the balance changes, and of the trades against the
//	        token movements
//	    at the end: ledger b's hash must be ledger b + 1's previous hash;
//	        the ledgers, transactions and operations of [a, b] are counted
//	        against the ledgers' own counts; tx_by_hash's partitions move
//	        to the data volume; the marker "done:backfill:<a>-<b>" is set
//
// A rerun skips the segments with a marker and resumes the others from
// their cursors. Any invalid ledger, processing error or failed check
// stops the run: nothing is skipped.
//
// With TradesOnly, the segments are pipelines "trades:<a>-<b>" that write
// and clean up the trades table alone, for ledgers whose other history is
// already stored: the ones the live pipeline wrote before it knew trades.

// BackfillOptions configure Backfill.
type BackfillOptions struct {
	// Open returns a connection for one segment, and for the planning.
	Open func(ctx context.Context) (driver.Conn, error)
	// Source is the AWS public lake.
	Source sequencer.Source
	// From and To are the range, inclusive.
	From, To uint32
	// Segment is the size of a segment: a multiple of 1,000,000, the size
	// of tx_by_hash's partitions.
	Segment uint32
	// Parallel is how many segments run at once.
	Parallel int
	// FirstLedger is the first ledger the live pipeline wrote (go-live).
	// A backfill stays below it; a trades-only fill starts at it.
	FirstLedger uint32
	TradesOnly  bool
	// MaxBatchLedgers and MaxBatchBytes bound one commit of one segment.
	MaxBatchLedgers int
	MaxBatchBytes   int64
	// ReadWorkers is how many ledgers a segment downloads at once
	// (default: the sequencer's 8). A read from the AWS lake takes about a
	// quarter of a second from Europe whatever the ledger's size, so this
	// bounds a segment's speed.
	ReadWorkers int
	// VerifyEvery: check the ledgers whose sequence is a multiple of it
	// (0: none).
	VerifyEvery uint32
	Passphrase  string
	Log         *slog.Logger
}

// BackfillResult summarises a run.
type BackfillResult struct {
	Segments, DoneBefore, Done int
	Ledgers                    uint64
}

const (
	backfillPrefix = "backfill:"
	tradesPrefix   = "trades:"
	donePrefix     = "done:"
	// txByHashPartition is the ledgers per partition of tx_by_hash.
	txByHashPartition = 1_000_000
)

// Segments splits [from, to] into segments of size ledgers aligned to its
// multiples, newest first.
func Segments(from, to, size uint32) [][2]uint32 {
	var out [][2]uint32
	for end := to; ; {
		start := max(end/size*size, from)
		out = append(out, [2]uint32{start, end})
		if start == from {
			return out
		}
		end = start - 1
	}
}

func (o BackfillOptions) prefix() string {
	if o.TradesOnly {
		return tradesPrefix
	}
	return backfillPrefix
}

func (o BackfillOptions) pipeline(s [2]uint32) string {
	return fmt.Sprintf("%s%d-%d", o.prefix(), s[0], s[1])
}

// TradesFillEnd is where a trades-only fill ends: an earlier fill's end if
// there is one, else one below the first ledger the live pipeline wrote
// trades for, so that the two meet without a gap or a duplicate.
func TradesFillEnd(ctx context.Context, conn driver.Conn, firstLedger uint32) (uint32, error) {
	ranges, err := pipelineRanges(ctx, conn, tradesPrefix)
	if err != nil {
		return 0, err
	}
	var end uint32
	for _, r := range ranges {
		end = max(end, r[1])
	}
	if end != 0 {
		return end, nil
	}
	var n uint64
	var first uint32
	if err := conn.QueryRow(ctx, "SELECT count(), min(ledger_sequence) FROM trades WHERE ledger_sequence >= ?", firstLedger).Scan(&n, &first); err != nil {
		return 0, fmt.Errorf("the live pipeline's first trade: %w", err)
	}
	if n == 0 {
		return 0, errors.New("the live pipeline has written no trades yet: deploy the worker that writes them first")
	}
	if first <= firstLedger {
		return 0, fmt.Errorf("trades start at ledger %d: nothing to fill from %d on", first, firstLedger)
	}
	return first - 1, nil
}

// pipelineRanges are the ranges of the pipelines "<prefix><a>-<b>" in
// ingest_state.
func pipelineRanges(ctx context.Context, conn driver.Conn, prefix string) ([][2]uint32, error) {
	rows, err := conn.Query(ctx, "SELECT DISTINCT pipeline FROM ingest_state WHERE startsWith(pipeline, ?)", prefix)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out [][2]uint32
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		var a, b uint32
		if _, err := fmt.Sscanf(strings.TrimPrefix(name, prefix), "%d-%d", &a, &b); err != nil {
			return nil, fmt.Errorf("pipeline %q: %w", name, err)
		}
		out = append(out, [2]uint32{a, b})
	}
	return out, rows.Err()
}

// Backfill runs until every segment is done, ctx ends or an error stops it.
func Backfill(ctx context.Context, o BackfillOptions) (BackfillResult, error) {
	var res BackfillResult
	if o.Log == nil {
		o.Log = slog.Default()
	}
	switch {
	case o.Open == nil || o.Source == nil:
		return res, errors.New("backfill: a connection and a source are required")
	case o.From < 2 || o.From > o.To:
		return res, fmt.Errorf("backfill: bad range %d-%d", o.From, o.To)
	case o.Segment == 0 || o.Segment%txByHashPartition != 0:
		return res, fmt.Errorf("backfill: the segment size must be a multiple of %d", txByHashPartition)
	case o.Parallel < 1:
		return res, errors.New("backfill: parallel must be at least 1")
	case o.FirstLedger < 2:
		return res, errors.New("backfill: the first ledger of the live pipeline is required")
	case !o.TradesOnly && o.To >= o.FirstLedger:
		return res, fmt.Errorf("backfill: the range ends at %d, but the live pipeline owns everything from %d on", o.To, o.FirstLedger)
	case o.TradesOnly && o.From < o.FirstLedger:
		return res, fmt.Errorf("backfill: a trades-only fill starts at %d or above; below it a backfill writes the trades", o.FirstLedger)
	}

	conn, err := o.Open(ctx)
	if err != nil {
		return res, err
	}
	defer conn.Close()
	segments := Segments(o.From, o.To, o.Segment)
	res.Segments = len(segments)
	// An earlier run with another range or segment size left pipelines
	// that overlap these: running both would write ledgers twice.
	existing, err := pipelineRanges(ctx, conn, o.prefix())
	if err != nil {
		return res, err
	}
	for _, s := range segments {
		for _, e := range existing {
			if e != s && e[0] <= s[1] && s[0] <= e[1] {
				return res, fmt.Errorf("backfill: segment %d-%d overlaps the earlier pipeline %s%d-%d: use the range and segment size of that run",
					s[0], s[1], o.prefix(), e[0], e[1])
			}
		}
	}
	var todo [][2]uint32
	for _, s := range segments {
		if _, _, err := ReadCursor(ctx, conn, donePrefix+o.pipeline(s)); err == nil {
			res.DoneBefore++
			continue
		} else if !errors.Is(err, ErrNoCursor) {
			return res, err
		}
		todo = append(todo, s)
	}
	o.Log.Info("backfill", "from", o.From, "to", o.To, "segments", len(segments), "done_before", res.DoneBefore, "parallel", o.Parallel,
		"trades_only", o.TradesOnly)

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		jobs = make(chan [2]uint32)
	)
	for range o.Parallel {
		wg.Go(func() {
			for s := range jobs {
				if ctx.Err() != nil {
					continue
				}
				n, err := o.runSegment(ctx, s)
				mu.Lock()
				res.Ledgers += n
				if err == nil {
					res.Done++
				}
				mu.Unlock()
				if err != nil {
					cancel(fmt.Errorf("segment %d-%d: %w", s[0], s[1], err))
					continue
				}
				o.Log.Info("segment done", "segment", o.pipeline(s), "segments_done", res.DoneBefore+res.Done, "segments", res.Segments)
			}
		})
	}
feed:
	for _, s := range todo {
		select {
		case jobs <- s:
		case <-ctx.Done():
			break feed
		}
	}
	close(jobs)
	wg.Wait()
	if err := context.Cause(ctx); err != nil {
		return res, err
	}
	return res, nil
}

// runSegment runs one segment to its end and returns the ledgers it
// committed in this run.
func (o BackfillOptions) runSegment(ctx context.Context, s [2]uint32) (uint64, error) {
	a, b, pipeline := s[0], s[1], o.pipeline(s)
	log := o.Log.With("segment", pipeline)
	conn, err := o.Open(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	cur, _, err := ReadCursor(ctx, conn, pipeline)
	switch {
	case errors.Is(err, ErrNoCursor):
		h, err := o.header(ctx, a)
		if err != nil {
			return 0, fmt.Errorf("read ledger %d to seed the cursor: %w", a, err)
		}
		if _, err := Init(ctx, conn, pipeline, a-1, h.PreviousHash); err != nil {
			return 0, err
		}
		cur = a - 1
	case err != nil:
		return 0, err
	case cur < a-1 || cur > b:
		return 0, fmt.Errorf("the cursor of %s is at %d, outside the segment", pipeline, cur)
	}
	log.Info("segment", "cursor", cur, "ledgers_left", b-cur)

	var committed uint64
	if cur < b {
		process := o.processor()
		lastLog, lastCursor := time.Now(), cur
		seq, err := sequencer.New(sequencer.Options[*explorer.Prepared]{
			Pipeline:        pipeline,
			Sink:            NewSink(conn, SinkOptions{Pipeline: pipeline, Log: log, Ceiling: b, TradesOnly: o.TradesOnly}),
			Process:         process,
			Fixed:           &sequencer.Fixed{Source: o.Source, To: b},
			AnyLCMVersion:   true,
			BSB:             o.bsb(),
			MaxBatchLedgers: o.MaxBatchLedgers,
			MaxBatchBytes:   o.MaxBatchBytes,
			Log:             log,
			Alert:           func(kind, msg string, args ...any) { log.Error(msg, append([]any{"alert", kind}, args...)...) },
			OnCommit: func(c sequencer.CommitInfo) {
				committed += uint64(c.Ledgers)
				if since := time.Since(lastLog); since >= time.Minute {
					speed := float64(c.Last-lastCursor) / since.Seconds()
					log.Info("committed", "cursor", c.Last, "ledgers_left", b-c.Last, "per_second", fmt.Sprintf("%.0f", speed),
						"eta", (time.Duration(float64(b-c.Last)/max(speed, 1e-9)) * time.Second).Round(time.Minute))
					lastLog, lastCursor = time.Now(), c.Last
				}
			},
		})
		if err != nil {
			return 0, err
		}
		if err := seq.Run(ctx); err != nil {
			return committed, err
		}
		if err := ctx.Err(); err != nil {
			return committed, context.Cause(ctx)
		}
	}

	// The segment's end: its last ledger chains to the next one.
	cur, hash, err := ReadCursor(ctx, conn, pipeline)
	if err != nil {
		return committed, err
	}
	if cur != b {
		return committed, fmt.Errorf("the sequencer stopped at %d before %d", cur, b)
	}
	next, err := o.previousHashOf(ctx, conn, b+1)
	if err != nil {
		return committed, fmt.Errorf("ledger %d's previous hash: %w", b+1, err)
	}
	if next != hash {
		return committed, fmt.Errorf("ledger %d (hash %x) does not chain to ledger %d (previous hash %x)", b, hash, b+1, next)
	}
	if !o.TradesOnly {
		r, err := CheckSegment(ctx, conn, a, b)
		if err != nil {
			return committed, err
		}
		if p := r.Problems(); len(p) > 0 {
			return committed, fmt.Errorf("check: %s", r)
		}
		log.Info("segment checked", "ledgers", r.Distinct, "transactions", r.Transactions, "operations", r.Operations)
		// tx_by_hash has no time column, so its parts don't move to the
		// data volume by age. A segment covers whole partitions of it.
		for p := a / txByHashPartition; p <= b/txByHashPartition; p++ {
			if err := conn.Exec(ctx, fmt.Sprintf("ALTER TABLE tx_by_hash MOVE PARTITION ID '%d' TO VOLUME 'cold'", p)); err != nil {
				log.Warn("tx_by_hash: the partition was not moved to the data volume (the local disk's move_factor moves it when space runs low)",
					"partition", p, "err", err)
			}
		}
	}
	return committed, writeCursor(ctx, conn, donePrefix+pipeline, b, hash)
}

// bsb is the catch-up reader's configuration: ReadWorkers downloads, and a
// buffer of twice as many ledgers.
func (o BackfillOptions) bsb() ledgerbackend.BufferedStorageBackendConfig {
	cfg := sequencer.DefaultBSB
	if o.ReadWorkers > 0 {
		cfg.NumWorkers, cfg.BufferSize = uint32(o.ReadWorkers), uint32(2*o.ReadWorkers)
	}
	return cfg
}

// processor is the live pipeline's processor, or the trades-only one, with
// the sampled checks.
func (o BackfillOptions) processor() sequencer.Processor[*explorer.Prepared] {
	if o.TradesOnly {
		return explorer.TradesProcessor(o.Passphrase)
	}
	history := explorer.HistoryProcessor(o.Passphrase)
	return func(l sequencer.Ledger) (*explorer.Prepared, error) {
		p, err := history(l)
		if err != nil {
			return nil, err
		}
		if o.VerifyEvery != 0 && l.Header.Sequence%o.VerifyEvery == 0 {
			if err := token_transfer.VerifyEvents(l.LCM, o.Passphrase, true); err != nil {
				return nil, fmt.Errorf("ledger %d: the transfers don't match the balance changes: %w", l.Header.Sequence, err)
			}
			if err := explorer.CheckTrades(p); err != nil {
				return nil, fmt.Errorf("the trades don't match the token movements: %w", err)
			}
		}
		return p, nil
	}
}

// header reads ledger seq's header from the source.
func (o BackfillOptions) header(ctx context.Context, seq uint32) (sep54.Header, error) {
	obj, err := o.Source.GetLedger(ctx, seq)
	if err != nil {
		return sep54.Header{}, err
	}
	_, h, err := sep54.DecodeHistoryLedger(seq, obj.Body)
	return h, err
}

// previousHashOf is ledger seq's previous hash: from ledgers if it is
// stored (the live pipeline's first ledger, or a finished segment's),
// else from the source.
func (o BackfillOptions) previousHashOf(ctx context.Context, conn driver.Conn, seq uint32) (xdr.Hash, error) {
	rows, err := conn.Query(ctx, "SELECT prev_hash FROM ledgers WHERE sequence = ? LIMIT 1", seq)
	if err != nil {
		return xdr.Hash{}, err
	}
	defer rows.Close()
	if rows.Next() {
		var prev string
		if err := rows.Scan(&prev); err != nil {
			return xdr.Hash{}, err
		}
		if len(prev) != len(xdr.Hash{}) {
			return xdr.Hash{}, fmt.Errorf("ledgers: a %d-byte previous hash for ledger %d", len(prev), seq)
		}
		var h xdr.Hash
		copy(h[:], prev)
		return h, nil
	}
	if err := rows.Err(); err != nil {
		return xdr.Hash{}, err
	}
	h, err := o.header(ctx, seq)
	return h.PreviousHash, err
}
