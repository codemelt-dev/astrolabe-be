// Package sequencer is the ingest loop of one pipeline: it
// processes cursor + 1, woken by the lake's commit notification, and
// commits through a Sink. Catch-up and live are the same loop: when the tip
// is already ahead of the cursor, the loop just doesn't wait.
//
// Reads go to the lake while it holds the ledger, else to the archive, and
// to the AWS public lake as the last resort (with an alert). A NotFound
// re-routes to the next source; an unavailable lake is retried for a grace
// period before failing over; a validation failure tries the next source
// and halts the pipeline if every source fails. The cursor never advances
// past a ledger that did not validate and commit.
//
// Internally a producer goroutine routes, reads, decodes, checks the hash
// chain and runs the Processor; the committer batches what is ready (one
// ledger at the tip, up to MaxBatchLedgers / MaxBatchBytes in catch-up) and
// commits, so processing overlaps the previous commit.
package sequencer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/xdr"

	"stellar-explorer/worker/sep54"
)

// Tip is the lake's control endpoint (*sep54.TipClient).
type Tip interface {
	// Get returns the lake's current tip at once.
	Get(ctx context.Context) (sep54.Tip, error)
	// WaitAbove blocks until the tip exceeds after (long-poll), retrying
	// while the lake is unavailable for at most grace.
	WaitAbove(ctx context.Context, after uint32, grace time.Duration) (sep54.Tip, error)
}

// Sources are where ledgers are read from, in routing order.
type Sources struct {
	// Lake and LakeTip are required unless the sequencer runs Fixed.
	Lake    Source
	LakeTip Tip
	// Archive is optional; ArchiveTip returns its newest ledger when known
	// (the archiver's in-memory tip).
	Archive    Source
	ArchiveTip func() (uint32, bool)
	// AWS is the last-resort source (optional).
	AWS Source
}

// Fixed makes the sequencer read every ledger from one source, with no
// re-routing, and stop after To (`worker reindex`).
type Fixed struct {
	Source Source
	To     uint32
}

// Defaults.
const (
	DefaultCatchupThreshold = 64
	DefaultMaxBatchLedgers  = 500
	DefaultMaxBatchBytes    = 512 << 20
	DefaultLakeGrace        = 2 * time.Minute
	DefaultArchiveGrace     = 10 * time.Minute
	DefaultMaxBackoff       = 5 * time.Second
	DefaultCommitMaxBackoff = 30 * time.Second
	DefaultDrainTimeout     = 30 * time.Second
)

// archiveRetryInterval: while failed over from the archive to the AWS lake,
// the archive is retried this often.
const archiveRetryInterval = time.Minute

// DefaultBSB is the catch-up reader's explicit configuration.
var DefaultBSB = ledgerbackend.BufferedStorageBackendConfig{BufferSize: 64, NumWorkers: 8, RetryLimit: 2, RetryWait: time.Second}

// Options configure a Sequencer. Zero values take the defaults.
type Options[P Prepared] struct {
	// Pipeline names the pipeline in logs and alerts.
	Pipeline string
	Sink     Sink[P]
	Process  Processor[P]
	Sources  Sources
	Fixed    *Fixed

	// CatchupThreshold: when a source holds more than this many ledgers
	// beyond the cursor, reads go through a BufferedStorageBackend.
	CatchupThreshold uint32
	BSB              ledgerbackend.BufferedStorageBackendConfig
	MaxBatchLedgers  int
	MaxBatchBytes    int64
	// LakeGrace: how long the lake may be unavailable before failing over.
	LakeGrace time.Duration
	// ArchiveGrace: how long archive reads may fail before the AWS lake.
	ArchiveGrace time.Duration
	// MaxBackoff caps retries of sources; CommitMaxBackoff of commits.
	MaxBackoff       time.Duration
	CommitMaxBackoff time.Duration
	// DrainTimeout bounds the in-flight commit after ctx is cancelled.
	DrainTimeout time.Duration
	// CommitAlertAfter: alert when commits keep failing this long (5 min).
	CommitAlertAfter time.Duration

	Log *slog.Logger
	// Alert reports conditions that need an operator.
	Alert func(kind, msg string, args ...any)
	// OnCommit is called after every successful commit (metrics, tests).
	OnCommit func(CommitInfo)
}

// CommitInfo describes one committed batch.
type CommitInfo struct {
	First, Last uint32
	Ledgers     int
	Bytes       int64
	// Duration is the commit's; Read (GET and decode) and Process
	// (processLedger) are summed over the batch's ledgers.
	Duration      time.Duration
	Read, Process time.Duration
	// Sources counts the batch's ledgers per read source.
	Sources map[string]int
	// Latency is lake commit → sink commit, set only when the batch holds
	// the tip the sequencer was woken for; catch-up ledgers
	// have none.
	Latency time.Duration
}

// Stats is a snapshot of a sequencer's counters.
type Stats struct {
	Cursor    uint32
	Commits   uint64
	Committed uint64
	// Reads counts ledgers read per source.
	Reads map[string]uint64
	// Alerts counts alerts raised, per kind.
	Alerts map[string]uint64
}

// Sequencer runs one pipeline.
type Sequencer[P Prepared] struct {
	o   Options[P]
	log *slog.Logger

	statsMu sync.Mutex
	reads   map[string]uint64
	alerts  map[string]uint64
	cursor  atomic.Uint32
	commits atomic.Uint64
	ledgers atomic.Uint64
}

// New validates the options and returns a sequencer.
func New[P Prepared](o Options[P]) (*Sequencer[P], error) {
	if o.Sink == nil || o.Process == nil {
		return nil, errors.New("sequencer: Sink and Process are required")
	}
	if o.Fixed == nil && (o.Sources.Lake == nil || o.Sources.LakeTip == nil) {
		return nil, errors.New("sequencer: the lake and its tip are required")
	}
	if o.Fixed != nil && o.Fixed.Source == nil {
		return nil, errors.New("sequencer: Fixed needs a source")
	}
	if o.Sources.Archive != nil && o.Sources.ArchiveTip == nil {
		return nil, errors.New("sequencer: the archive needs ArchiveTip")
	}
	if o.Pipeline == "" {
		o.Pipeline = "pipeline"
	}
	if o.CatchupThreshold == 0 {
		o.CatchupThreshold = DefaultCatchupThreshold
	}
	if o.BSB == (ledgerbackend.BufferedStorageBackendConfig{}) {
		o.BSB = DefaultBSB
	}
	if o.MaxBatchLedgers <= 0 {
		o.MaxBatchLedgers = DefaultMaxBatchLedgers
	}
	if o.MaxBatchBytes <= 0 {
		o.MaxBatchBytes = DefaultMaxBatchBytes
	}
	if o.LakeGrace <= 0 {
		o.LakeGrace = DefaultLakeGrace
	}
	if o.ArchiveGrace <= 0 {
		o.ArchiveGrace = DefaultArchiveGrace
	}
	if o.MaxBackoff <= 0 {
		o.MaxBackoff = DefaultMaxBackoff
	}
	if o.CommitMaxBackoff <= 0 {
		o.CommitMaxBackoff = DefaultCommitMaxBackoff
	}
	if o.DrainTimeout <= 0 {
		o.DrainTimeout = DefaultDrainTimeout
	}
	if o.CommitAlertAfter <= 0 {
		o.CommitAlertAfter = 5 * time.Minute
	}
	if o.Log == nil {
		o.Log = slog.Default()
	}
	s := &Sequencer[P]{o: o, log: o.Log.With("pipeline", o.Pipeline), reads: map[string]uint64{}, alerts: map[string]uint64{}}
	return s, nil
}

// Stats returns a snapshot of the counters.
func (s *Sequencer[P]) Stats() Stats {
	s.statsMu.Lock()
	defer s.statsMu.Unlock()
	st := Stats{Cursor: s.cursor.Load(), Commits: s.commits.Load(), Committed: s.ledgers.Load(),
		Reads: map[string]uint64{}, Alerts: map[string]uint64{}}
	for k, v := range s.reads {
		st.Reads[k] = v
	}
	for k, v := range s.alerts {
		st.Alerts[k] = v
	}
	return st
}

func (s *Sequencer[P]) alert(kind, msg string, args ...any) {
	s.statsMu.Lock()
	s.alerts[kind]++
	s.statsMu.Unlock()
	args = append([]any{"pipeline", s.o.Pipeline}, args...)
	if s.o.Alert != nil {
		s.o.Alert(kind, msg, args...)
		return
	}
	s.o.Log.Error(msg, append([]any{"alert", kind}, args...)...)
}

// item is one prepared ledger on its way to the committer.
type item[P Prepared] struct {
	p      P
	source string
	size   int64
	// tipCommittedAt is the lake's commit time (Unix ns) of the tip the
	// sequencer was woken for, when this ledger is that tip.
	tipCommittedAt int64
	read, process  time.Duration
}

// Run processes ledgers until ctx is cancelled (nil), the Fixed range is
// done (nil), the pipeline halts (*HaltError) or a fatal error occurs.
func (s *Sequencer[P]) Run(ctx context.Context) (err error) {
	seq, hash, err := s.readCursor(ctx)
	if err != nil {
		return err
	}
	s.cursor.Store(seq)
	s.log.Info("sequencer starting", "cursor", seq)

	pctx, cancel := context.WithCancel(ctx)
	defer cancel()
	items := make(chan item[P], s.o.MaxBatchLedgers)
	budget := newBudget(2 * s.o.MaxBatchBytes)
	p := &producer[P]{s: s, next: seq + 1, prev: hash, out: items, budget: budget, backoff: backoff{max: s.o.MaxBackoff}}
	var prodErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer close(items)
		defer func() {
			if r := recover(); r != nil {
				prodErr = fmt.Errorf("sequencer producer panicked: %v", r)
			}
		}()
		prodErr = p.run(pctx)
	}()
	err = s.commitLoop(ctx, items, budget)
	cancel()
	<-done
	if err == nil && prodErr != nil && !errors.Is(prodErr, context.Canceled) {
		err = prodErr
	}
	if IsHalt(err) {
		s.alert("pipeline_halted", "pipeline halted", "err", err)
	}
	return err
}

func (s *Sequencer[P]) readCursor(ctx context.Context) (uint32, xdr.Hash, error) {
	b := backoff{min: time.Second, max: s.o.CommitMaxBackoff}
	for {
		seq, hash, err := s.o.Sink.Cursor(ctx)
		if err == nil || !IsTransient(err) {
			return seq, hash, err
		}
		s.log.Warn("read cursor; retrying", "err", err)
		if err := b.sleep(ctx); err != nil {
			return 0, xdr.Hash{}, err
		}
	}
}

// commitLoop batches ready ledgers and commits them. It returns when the
// producer is done (items closed), on a halt or fatal commit error, or when
// ctx is cancelled (after finishing or rolling back the in-flight commit).
func (s *Sequencer[P]) commitLoop(ctx context.Context, items <-chan item[P], budget *budget) error {
	var pending *item[P]
	for {
		var batch []item[P]
		var bytes int64
		if pending != nil {
			batch, bytes, pending = append(batch, *pending), pending.size, nil
		} else {
			select {
			case it, ok := <-items:
				if !ok {
					return nil
				}
				batch, bytes = append(batch, it), it.size
			case <-ctx.Done():
				return nil
			}
		}
	drain:
		for len(batch) < s.o.MaxBatchLedgers && bytes < s.o.MaxBatchBytes {
			select {
			case it, ok := <-items:
				if !ok {
					break drain
				}
				if bytes+it.size > s.o.MaxBatchBytes {
					pending = &it
					break drain
				}
				batch, bytes = append(batch, it), bytes+it.size
			default:
				break drain
			}
		}
		err := s.commit(ctx, batch, bytes)
		budget.release(bytes)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
	}
}

// commit commits one batch, retrying transient failures with backoff.
func (s *Sequencer[P]) commit(ctx context.Context, batch []item[P], bytes int64) error {
	ps := make([]P, len(batch))
	sources := map[string]int{}
	var tipAt int64
	var readTime, processTime time.Duration
	for i, it := range batch {
		ps[i] = it.p
		sources[it.source]++
		if it.tipCommittedAt != 0 {
			tipAt = it.tipCommittedAt
		}
		readTime += it.read
		processTime += it.process
	}
	first, last := ps[0].Header().Sequence, ps[len(ps)-1].Header().Sequence
	b := backoff{min: time.Second, max: s.o.CommitMaxBackoff}
	var failingSince time.Time
	alerted := false
	for {
		start := time.Now()
		cctx, cancel := drainContext(ctx, s.o.DrainTimeout)
		err := s.safeCommit(cctx, ps, first)
		cancel()
		if err == nil {
			now := time.Now()
			s.cursor.Store(last)
			s.commits.Add(1)
			s.ledgers.Add(uint64(len(ps)))
			info := CommitInfo{First: first, Last: last, Ledgers: len(ps), Bytes: bytes, Duration: now.Sub(start),
				Read: readTime, Process: processTime, Sources: sources}
			if tipAt != 0 {
				info.Latency = now.Sub(time.Unix(0, tipAt))
			}
			if alerted {
				s.log.Info("commits succeed again", "failed_for", now.Sub(failingSince).Round(time.Second))
			}
			s.log.Debug("committed", "first", first, "last", last, "ledgers", len(ps), "bytes", bytes,
				"duration", info.Duration, "latency", info.Latency, "sources", sources)
			if s.o.OnCommit != nil {
				s.o.OnCommit(info)
			}
			return nil
		}
		if IsHalt(err) {
			return err
		}
		if !IsTransient(err) {
			if ctx.Err() != nil {
				// Shut down mid-commit: rolled back, nothing committed.
				return context.Canceled
			}
			return fmt.Errorf("commit %d-%d: %w", first, last, err)
		}
		if failingSince.IsZero() {
			failingSince = start
		}
		s.log.Warn("commit failed; retrying", "first", first, "last", last, "err", err)
		if !alerted && time.Since(failingSince) >= s.o.CommitAlertAfter {
			alerted = true
			s.alert("sink_unavailable", "commits keep failing", "since", failingSince, "err", err)
		}
		if err := b.sleep(ctx); err != nil {
			return context.Canceled
		}
	}
}

// safeCommit calls the sink under recover(): a panic halts the pipeline
// (a deterministic bug would otherwise crash ingest in a restart loop).
func (s *Sequencer[P]) safeCommit(ctx context.Context, ps []P, first uint32) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = Halt(first, fmt.Errorf("sink panicked: %v", r))
		}
	}()
	return s.o.Sink.Commit(ctx, ps)
}

// drainContext returns a context for an in-flight commit: it outlives ctx
// by at most d, so a shutdown finishes the commit when it can and rolls it
// back otherwise.
func drainContext(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	cctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	var timer *time.Timer
	var mu sync.Mutex
	stop := context.AfterFunc(ctx, func() {
		mu.Lock()
		timer = time.AfterFunc(d, cancel)
		mu.Unlock()
	})
	return cctx, func() {
		stop()
		mu.Lock()
		if timer != nil {
			timer.Stop()
		}
		mu.Unlock()
		cancel()
	}
}

// ---------------------------------------------------------------- producer

// failure kinds recorded per source for the ledger being read.
type failure int

const (
	failNotFound failure = iota + 1
	failInvalid
)

type producer[P Prepared] struct {
	s      *Sequencer[P]
	out    chan<- item[P]
	budget *budget

	next uint32
	prev xdr.Hash // hash of next-1

	tip           sep54.Tip // the lake's tip as last seen
	tipRefreshed  time.Time
	lakeDownSince time.Time // zero while the lake answers
	failedOver    bool      // lake down ≥ LakeGrace (alerted)
	lastProbe     time.Time
	archDownSince time.Time
	archLastTry   time.Time
	backoff       backoff

	// Per ledger: sources that failed it, and the last validation error.
	excluded       map[string]failure
	lastInvalid    error
	missingAlerted uint32

	reader   *rangeReader
	bsbAvoid uint32 // a ledger known missing ahead: stop BSB ranges before it

	awsReason string // why the current AWS stretch started (alerted once)
	woken     struct {
		seq uint32
		at  int64
	}
}

// route is where to read the next ledger.
type route struct {
	src    Source
	role   role
	end    uint32 // the source's known bound (BSB range end)
	reason string // for AWS reads
}

// role is the slot a route's source fills (Sources.Lake, Archive or AWS):
// the grace periods and alerts follow the slot, not the source's name, so
// the clickhouse pipeline can read the AWS lake as its lake.
type role int

const (
	roleFixed role = iota
	roleLake
	roleArchive
	roleAWS
)

func (p *producer[P]) run(ctx context.Context) error {
	defer p.closeReader()
	o := &p.s.o
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if o.Fixed != nil && p.next > o.Fixed.To {
			return nil
		}
		r, ok, err := p.route(ctx)
		if err != nil {
			return err
		}
		if !ok {
			continue // waited, retried or refreshed: route again
		}
		readStart := time.Now()
		l, err := p.read(ctx, r)
		readTime := time.Since(readStart)
		if err != nil {
			if err := p.readFailed(ctx, r, err); err != nil {
				return err
			}
			continue
		}
		p.readOK(r)
		if l.Header.PreviousHash != p.prev {
			err := &sep54.InvalidError{Seq: p.next, Err: fmt.Errorf("previous hash %x does not match ledger %d hash %x (the cursor chain)",
				l.Header.PreviousHash, p.next-1, p.prev)}
			if err := p.invalid(ctx, r, err); err != nil {
				return err
			}
			continue
		}
		processStart := time.Now()
		prepared, err := p.process(l)
		processTime := time.Since(processStart)
		if err != nil {
			if err := p.invalid(ctx, r, &sep54.InvalidError{Seq: p.next, Err: fmt.Errorf("processLedger: %w", err)}); err != nil {
				return err
			}
			continue
		}
		p.used(r)
		it := item[P]{p: prepared, source: l.Source, size: prepared.Size(), read: readTime, process: processTime}
		if p.woken.seq == p.next {
			it.tipCommittedAt = p.woken.at
		}
		if err := p.budget.acquire(ctx, it.size); err != nil {
			return err
		}
		select {
		case p.out <- it:
		case <-ctx.Done():
			p.budget.release(it.size)
			return ctx.Err()
		}
		p.s.statsMu.Lock()
		p.s.reads[l.Source]++
		p.s.statsMu.Unlock()
		p.prev = l.Header.Hash
		p.next++
		p.excluded, p.lastInvalid = nil, nil
		p.backoff.reset()
	}
}

// process runs the Processor; a panic counts as a validation failure.
func (p *producer[P]) process(l Ledger) (prepared P, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	return p.s.o.Process(l)
}

// route decides where ledger p.next comes from. ok=false means
// it waited or refreshed state instead; the caller routes again.
func (p *producer[P]) route(ctx context.Context) (route, bool, error) {
	o := &p.s.o
	next := p.next
	if o.Fixed != nil {
		return route{src: o.Fixed.Source, end: o.Fixed.To}, true, nil
	}
	src := &o.Sources

	// A failing lake: within the grace period keep retrying it (back off,
	// re-fetch the tip, read again); after it, fail over and only probe it
	// now and then. Only a successful read (or a completed wait) ends the
	// outage, so a lake whose tip answers while its reads fail still fails
	// over.
	lakeOK := true
	if !p.lakeDownSince.IsZero() {
		down := time.Since(p.lakeDownSince)
		if down < o.LakeGrace {
			if err := p.backoff.sleep(ctx); err != nil {
				return route{}, false, err
			}
			if !p.refreshTip(ctx, 0) {
				return route{}, false, nil
			}
		} else {
			if !p.failedOver {
				p.failedOver = true
				p.s.alert("lake_unavailable", "lake unavailable beyond the grace period; failing over", "down_for", down.Round(time.Second))
			}
			lakeOK = false
			if time.Since(p.lastProbe) >= o.MaxBackoff {
				p.lastProbe = time.Now()
				lakeOK = p.refreshTip(ctx, 2*time.Second)
			}
		}
	}
	// Prefer the lake: when the cached tip is unknown or behind, look again
	// before routing elsewhere (at most once a second; waitTip refreshes it
	// at the tip).
	if lakeOK && (p.tip.Tip == nil || next > *p.tip.Tip) && time.Since(p.tipRefreshed) >= time.Second {
		if !p.refreshTip(ctx, 0) {
			return route{}, false, nil
		}
	}
	tipSet := p.tip.Tip != nil && p.tip.Oldest != nil

	var candidates []route
	awsReason := ""
	// 1. The lake holds it.
	if lakeOK && tipSet && *p.tip.Oldest <= next && next <= *p.tip.Tip {
		candidates = append(candidates, route{src: src.Lake, role: roleLake, end: *p.tip.Tip})
	}
	// 2. The archive holds it; after ArchiveGrace of failures, the AWS lake
	// (retrying the archive once a minute).
	if src.Archive != nil {
		if archTip, ok := src.ArchiveTip(); ok && next <= archTip {
			failing := !p.archDownSince.IsZero() && time.Since(p.archDownSince) >= o.ArchiveGrace
			switch {
			case !failing:
				candidates = append(candidates, route{src: src.Archive, role: roleArchive, end: archTip})
			case time.Since(p.archLastTry) >= archiveRetryInterval:
				p.archLastTry = time.Now()
				candidates = append(candidates, route{src: src.Archive, role: roleArchive, end: archTip})
			default:
				awsReason = "archive unavailable"
			}
		}
	}
	// 3. The AWS lake: a gap that was never archived, a re-route after the
	// others failed this ledger, or a lake that is down.
	switch {
	case awsReason != "":
	case lakeOK && tipSet && next < *p.tip.Oldest:
		awsReason = "ledger below the lake's oldest and not archived"
	case len(candidates) > 0:
		awsReason = "not readable from " + candidates[len(candidates)-1].src.Name()
	case !lakeOK:
		awsReason = "lake unavailable"
	}
	if src.AWS != nil && awsReason != "" {
		end := next
		if lakeOK && tipSet && next < *p.tip.Oldest {
			end = *p.tip.Oldest - 1
		} else if archTip, ok := p.archiveTip(); ok && next <= archTip {
			end = archTip
		}
		candidates = append(candidates, route{src: src.AWS, role: roleAWS, end: end, reason: awsReason})
	}

	for _, c := range candidates {
		if _, bad := p.excluded[c.src.Name()]; !bad {
			return c, true, nil
		}
	}
	if len(candidates) > 0 {
		return route{}, false, p.allFailed(ctx, lakeOK)
	}
	if !lakeOK {
		// Failed over, but nothing else can serve this ledger yet.
		return route{}, false, p.backoff.sleep(ctx)
	}
	// 4. Wait for the lake: long-poll for the next commit.
	return route{}, false, p.waitTip(ctx)
}

func (p *producer[P]) archiveTip() (uint32, bool) {
	if f := p.s.o.Sources.ArchiveTip; f != nil {
		return f()
	}
	return 0, false
}

// allFailed handles a ledger that every eligible source failed: halt if any
// failed validation, otherwise (missing everywhere) alert, back off and
// retry.
func (p *producer[P]) allFailed(ctx context.Context, lakeOK bool) error {
	for _, f := range p.excluded {
		if f == failInvalid {
			return Halt(p.next, fmt.Errorf("every source failed ledger %d; last validation error: %w", p.next, p.lastInvalid))
		}
	}
	if lakeOK && p.missingAlerted != p.next {
		p.missingAlerted = p.next
		p.s.alert("ledger_missing", "ledger not found in any source", "ledger", p.next, "sources", fmt.Sprint(p.excluded))
	}
	p.excluded = nil
	if err := p.backoff.sleep(ctx); err != nil {
		return err
	}
	p.refreshTip(ctx, 2*time.Second)
	return nil
}

// waitTip long-polls the lake until its tip passes p.next - 1.
func (p *producer[P]) waitTip(ctx context.Context) error {
	o := &p.s.o
	p.closeReader()
	start := time.Now()
	tip, err := o.Sources.LakeTip.WaitAbove(ctx, p.next-1, o.LakeGrace)
	switch {
	case err == nil:
		p.tip, p.tipRefreshed = tip, time.Now()
		p.lakeUp()
		// Woken by the commit of exactly the ledger we wait for: its
		// latency is measured.
		if tip.Tip != nil && *tip.Tip == p.next && tip.TipCommittedAt != nil && *tip.TipCommittedAt >= start.UnixNano() {
			p.woken.seq, p.woken.at = p.next, *tip.TipCommittedAt
		}
		return nil
	case ctx.Err() != nil:
		return ctx.Err()
	case sep54.IsUnavailable(err):
		// Unavailable for the whole grace period: fail over.
		p.lakeDown(time.Now().Add(-o.LakeGrace), err)
		return nil
	default:
		p.s.log.Error("lake tip", "err", err)
		return p.backoff.sleep(ctx)
	}
}

// refreshTip re-reads the lake's tip; with timeout > 0 it is a quick probe.
// It reports whether the lake answered.
func (p *producer[P]) refreshTip(ctx context.Context, timeout time.Duration) bool {
	c := ctx
	if timeout > 0 {
		var cancel context.CancelFunc
		c, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	tip, err := p.s.o.Sources.LakeTip.Get(c)
	if err != nil {
		if ctx.Err() == nil {
			p.lakeDown(time.Now(), err)
		}
		return false
	}
	p.tip, p.tipRefreshed = tip, time.Now()
	return true
}

// lakeUp ends a lake outage.
func (p *producer[P]) lakeUp() {
	if !p.lakeDownSince.IsZero() {
		p.s.log.Info("lake answers again", "down_for", time.Since(p.lakeDownSince).Round(time.Millisecond), "failed_over", p.failedOver)
		p.lakeDownSince, p.failedOver = time.Time{}, false
	}
}

func (p *producer[P]) lakeDown(since time.Time, err error) {
	if p.lakeDownSince.IsZero() {
		p.lakeDownSince = since
		p.s.log.Warn("lake unavailable; retrying", "err", err, "grace", p.s.o.LakeGrace)
	}
}

func fmtTip(t *uint32) any {
	if t == nil {
		return nil
	}
	return *t
}

// read reads p.next from r: through a BufferedStorageBackend when the
// source holds more than CatchupThreshold ledgers past the cursor, else with
// one GET. When the backend fails for any reason, it is closed and the
// ledger is read with one GET, whose error is precise.
func (p *producer[P]) read(ctx context.Context, r route) (Ledger, error) {
	o := &p.s.o
	if p.reader != nil && (p.reader.src.Name() != r.src.Name() || p.reader.next != p.next) {
		p.closeReader()
	}
	if p.bsbAvoid != 0 && p.next > p.bsbAvoid {
		p.bsbAvoid = 0
	}
	end := r.end
	if p.bsbAvoid != 0 {
		end = min(end, p.bsbAvoid-1)
	}
	if p.reader == nil && end >= p.next && end-p.next >= o.CatchupThreshold {
		rr, err := openRange(ctx, r.src, p.next, end, o.BSB)
		if err != nil {
			p.s.log.Warn("open catch-up reader", "source", r.src.Name(), "err", err)
		} else {
			p.reader = rr
			p.s.log.Info("catching up", "source", r.src.Name(), "from", p.next, "to", end)
		}
	}
	if p.reader != nil {
		l, err := p.reader.read(ctx, p.next)
		if err == nil {
			if p.reader.next > p.reader.to {
				p.closeReader()
			}
			return l, nil
		}
		if ctx.Err() != nil {
			return Ledger{}, ctx.Err()
		}
		if sep54.IsInvalid(err) {
			p.closeReader()
			return Ledger{}, err
		}
		if missing, ok := p.reader.store.firstMissing(); ok && missing >= p.next {
			p.bsbAvoid = missing
		}
		p.s.log.Info("catch-up reader stopped; reading with single GETs", "source", r.src.Name(), "ledger", p.next, "err", err)
		p.closeReader()
	}
	return readOne(ctx, r.src, p.next)
}

func (p *producer[P]) closeReader() {
	if p.reader != nil {
		p.reader.close()
		p.reader = nil
	}
}

// readOK updates the source state after a successful read.
func (p *producer[P]) readOK(r route) {
	switch r.role {
	case roleLake:
		p.lakeUp()
		p.awsReason = ""
	case roleArchive:
		p.archDownSince = time.Time{}
		p.awsReason = ""
	}
}

// used records that ledger p.next from r passed every check and is on its
// way to the sink. Reading from the AWS lake alerts once per stretch.
func (p *producer[P]) used(r route) {
	if r.role == roleAWS && p.awsReason == "" {
		p.awsReason = r.reason
		p.s.alert("reading_from_aws", "reading from the AWS public lake", "ledger", p.next, "reason", r.reason)
	}
}

// readFailed classifies a read error: NotFound re-routes,
// unavailability is retried (the lake for a grace period), invalid data
// tries the next source.
func (p *producer[P]) readFailed(ctx context.Context, r route, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	p.closeReader()
	o := &p.s.o
	name := r.src.Name()
	switch {
	case errors.Is(err, os.ErrNotExist):
		if o.Fixed != nil {
			return Halt(p.next, fmt.Errorf("ledger %d is missing from %s: %w", p.next, name, err))
		}
		p.s.log.Info("ledger not found; re-routing", "ledger", p.next, "source", name)
		p.exclude(name, failNotFound)
		return nil
	case sep54.IsInvalid(err):
		return p.invalid(ctx, r, err)
	default:
		// Unavailable (or an unexpected error): retry, never a validation
		// failure.
		switch {
		case o.Fixed != nil:
			p.s.log.Warn("read failed; retrying", "ledger", p.next, "source", name, "err", err)
			return p.backoff.sleep(ctx)
		case r.role == roleLake:
			p.lakeDown(time.Now(), err)
			return nil
		case r.role == roleArchive:
			if p.archDownSince.IsZero() {
				p.archDownSince = time.Now()
			}
			p.s.log.Warn("archive read failed; retrying", "ledger", p.next, "err", err)
			return p.backoff.sleep(ctx)
		default:
			p.s.log.Warn("read failed; retrying", "ledger", p.next, "source", name, "err", err)
			return p.backoff.sleep(ctx)
		}
	}
}

// invalid handles a validation failure of ledger p.next from r's source:
// never advance, alert, try the next source.
func (p *producer[P]) invalid(ctx context.Context, r route, err error) error {
	p.closeReader()
	if p.s.o.Fixed != nil {
		return Halt(p.next, err)
	}
	p.lastInvalid = err
	p.s.alert("validation_failed", "ledger failed validation; trying the next source", "ledger", p.next, "source", r.src.Name(), "err", err)
	p.exclude(r.src.Name(), failInvalid)
	return nil
}

func (p *producer[P]) exclude(source string, f failure) {
	if p.excluded == nil {
		p.excluded = map[string]failure{}
	}
	p.excluded[source] = f
}

// ------------------------------------------------------------------ helpers

type backoff struct {
	min, max, cur time.Duration
}

func (b *backoff) reset() { b.cur = 0 }

func (b *backoff) sleep(ctx context.Context) error {
	lo := b.min
	if lo <= 0 {
		lo = 100 * time.Millisecond
	}
	if b.cur < lo {
		b.cur = lo
	} else {
		b.cur = min(2*b.cur, b.max)
	}
	t := time.NewTimer(b.cur)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// budget bounds the bytes of prepared ledgers held between the producer
// and the sink (one batch being committed plus one being gathered).
type budget struct {
	mu    sync.Mutex
	used  int64
	max   int64
	freed chan struct{}
}

func newBudget(max int64) *budget { return &budget{max: max, freed: make(chan struct{})} }

// acquire waits until n bytes fit. A single item larger than the whole
// budget is admitted when nothing else is held.
func (b *budget) acquire(ctx context.Context, n int64) error {
	for {
		b.mu.Lock()
		if b.used == 0 || b.used+n <= b.max {
			b.used += n
			b.mu.Unlock()
			return nil
		}
		ch := b.freed
		b.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (b *budget) release(n int64) {
	b.mu.Lock()
	b.used -= n
	close(b.freed)
	b.freed = make(chan struct{})
	b.mu.Unlock()
}
