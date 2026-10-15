package sequencer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"stellar-explorer/worker/sep54"
	"stellar-explorer/worker/sep54/sep54test"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

type alertLog struct {
	mu    sync.Mutex
	kinds []string
}

func (a *alertLog) alert(kind, msg string, args ...any) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.kinds = append(a.kinds, kind)
}

func (a *alertLog) list() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.kinds...)
}

// env is a pipeline over in-memory sources: the chain [start, start+n).
type env struct {
	t       *testing.T
	chain   []sep54.Object
	start   uint32
	lake    *memSource
	archive *memSource
	aws     *memSource
	tip     *fakeTip
	archTip uint32
	archMu  sync.Mutex
	sink    *fakeSink
	alerts  *alertLog
	commits chan CommitInfo
}

func newEnv(t *testing.T, start uint32, n int) *env {
	e := &env{
		t:       t,
		chain:   sep54test.Chain(start, n),
		start:   start,
		lake:    newMemSource(sep54.SourceLake),
		archive: newMemSource(sep54.SourceArchive),
		aws:     newMemSource(sep54.SourceAWS),
		tip:     newFakeTip(),
		alerts:  &alertLog{},
		commits: make(chan CommitInfo, 1000),
	}
	first, _, err := sep54.DecodeLedger(start, e.chain[0].Body)
	if err != nil {
		t.Fatal(err)
	}
	e.sink = newFakeSink(start-1, first.LedgerHeaderHistoryEntry().Header.PreviousLedgerHash)
	return e
}

func (e *env) obj(seq uint32) sep54.Object { return e.chain[seq-e.start] }

// toLake puts [from, to] into the lake and publishes the tip.
func (e *env) toLake(from, to uint32) {
	for s := from; s <= to; s++ {
		e.lake.Add(e.obj(s))
	}
	oldest := from
	if t := e.tip.snapshot(); t.Oldest != nil {
		oldest = *t.Oldest
	}
	e.tip.set(oldest, to)
}

func (e *env) toArchive(from, to uint32) {
	for s := from; s <= to; s++ {
		e.archive.Add(e.obj(s))
	}
	e.archMu.Lock()
	e.archTip = max(e.archTip, to)
	e.archMu.Unlock()
}

func (e *env) toAWS(from, to uint32) {
	for s := from; s <= to; s++ {
		e.aws.Add(e.obj(s))
	}
}

func (e *env) options() Options[fakePrepared] {
	return Options[fakePrepared]{
		Pipeline: "test",
		Sink:     e.sink,
		Process:  fakeProcess(1000),
		Sources: Sources{
			Lake: e.lake, LakeTip: e.tip, Archive: e.archive, AWS: e.aws,
			ArchiveTip: func() (uint32, bool) {
				e.archMu.Lock()
				defer e.archMu.Unlock()
				return e.archTip, e.archTip != 0
			},
		},
		MaxBackoff: 50 * time.Millisecond,
		Log:        quiet,
		Alert:      e.alerts.alert,
		OnCommit:   func(c CommitInfo) { e.commits <- c },
	}
}

// run starts the sequencer; stop cancels it and returns Run's result.
func (e *env) run(o Options[fakePrepared]) (stop func() error, s *Sequencer[fakePrepared]) {
	e.t.Helper()
	s, err := New(o)
	if err != nil {
		e.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	var once sync.Once
	var result error
	stop = func() error {
		once.Do(func() {
			cancel()
			select {
			case result = <-done:
			case <-time.After(20 * time.Second):
				result = errors.New("sequencer did not stop")
			}
		})
		return result
	}
	e.t.Cleanup(func() { _ = stop() })
	return stop, s
}

// waitCursor waits until the sink's cursor reaches seq.
func (e *env) waitCursor(seq uint32, within time.Duration) {
	e.t.Helper()
	deadline := time.Now().Add(within)
	for e.sink.cursor() < seq {
		if time.Now().After(deadline) {
			e.t.Fatalf("cursor at %d, want %d", e.sink.cursor(), seq)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// checkCommitted verifies the sink holds exactly [start, to], in order, and
// returns the read source of each ledger.
func (e *env) checkCommitted(to uint32) map[uint32]string {
	e.t.Helper()
	committed, _ := e.sink.snapshot()
	if len(committed) != int(to-e.start+1) {
		e.t.Fatalf("%d ledgers committed, want %d", len(committed), to-e.start+1)
	}
	sources := map[uint32]string{}
	for i, p := range committed {
		if p.h.Sequence != e.start+uint32(i) {
			e.t.Fatalf("committed[%d] = ledger %d", i, p.h.Sequence)
		}
		sources[p.h.Sequence] = p.source
	}
	return sources
}

func sourceCounts(m map[uint32]string) map[string]int {
	out := map[string]int{}
	for _, s := range m {
		out[s]++
	}
	return out
}

// Catch-up through the BufferedStorageBackend in batches, then the tip one
// ledger per commit, woken by each lake commit, with latency measured.
func TestCatchUpThenLive(t *testing.T) {
	e := newEnv(t, 1000, 320)
	e.toLake(1000, 1299)
	o := e.options()
	o.MaxBatchLedgers = 50
	e.sink.commitDelay = 3 * time.Millisecond // a sink that takes time, like Postgres
	stop, s := e.run(o)
	e.waitCursor(1299, 20*time.Second)
	_, batches := e.sink.snapshot()
	big := 0
	for _, b := range batches {
		if b > 50 {
			t.Fatalf("batch of %d > MaxBatchLedgers", b)
		}
		if b > 1 {
			big++
		}
	}
	if big == 0 {
		t.Fatalf("catch-up never batched: %v", batches)
	}
	// Live: one ledger at a time.
	for len(e.commits) > 0 {
		<-e.commits
	}
	for seq := uint32(1300); seq < 1320; seq++ {
		e.toLake(seq, seq)
		select {
		case c := <-e.commits:
			if c.First != seq || c.Last != seq || c.Latency <= 0 || c.Latency > 2*time.Second {
				t.Fatalf("live commit %+v", c)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("ledger %d not committed", seq)
		}
	}
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if got := sourceCounts(e.checkCommitted(1319)); got[sep54.SourceLake] != 320 {
		t.Fatalf("sources %v", got)
	}
	if st := s.Stats(); st.Cursor != 1319 || st.Reads[sep54.SourceLake] != 320 || len(e.alerts.list()) != 0 {
		t.Fatalf("stats %+v alerts %v", st, e.alerts.list())
	}
}

// NotFound re-routes: a hole in the lake is read from the
// archive; a hole in both from the AWS lake (with an alert). Holes are found
// both at the tip and inside a catch-up range.
func TestNotFoundReroutes(t *testing.T) {
	e := newEnv(t, 5000, 200)
	e.toLake(5000, 5199)
	e.toArchive(5000, 5100)
	e.toAWS(5000, 5199)
	// Holes: 5050 (archived), 5150 (not archived), 5198 (near the tip).
	for _, h := range []uint32{5050, 5150, 5198} {
		e.lake.Remove(h)
	}
	stop, _ := e.run(e.options())
	e.waitCursor(5199, 20*time.Second)
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	src := e.checkCommitted(5199)
	if src[5050] != sep54.SourceArchive || src[5150] != sep54.SourceAWS || src[5198] != sep54.SourceAWS || src[5051] != sep54.SourceLake {
		t.Fatalf("sources: 5050=%s 5150=%s 5198=%s 5051=%s", src[5050], src[5150], src[5198], src[5051])
	}
	if got := sourceCounts(src); got[sep54.SourceLake] != 197 {
		t.Fatalf("sources %v", got)
	}
	for _, k := range e.alerts.list() {
		if k != "reading_from_aws" {
			t.Fatalf("alerts %v", e.alerts.list())
		}
	}
}

// A gap below the lake's oldest that was never archived comes from the AWS
// lake, with an alert; the archive is preferred when it has it.
func TestGapBelowOldest(t *testing.T) {
	e := newEnv(t, 7000, 100)
	e.toLake(7060, 7099)
	e.toArchive(7000, 7019)
	e.toAWS(7000, 7099)
	stop, _ := e.run(e.options())
	e.waitCursor(7099, 20*time.Second)
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	got := sourceCounts(e.checkCommitted(7099))
	if got[sep54.SourceArchive] != 20 || got[sep54.SourceAWS] != 40 || got[sep54.SourceLake] != 40 {
		t.Fatalf("sources %v", got)
	}
	if fmt.Sprint(e.alerts.list()) != "[reading_from_aws]" {
		t.Fatalf("alerts %v", e.alerts.list())
	}
}

// An empty lake: read from the archive up to its tip, then wait for Galexie.
func TestEmptyLakeReadsArchiveThenWaits(t *testing.T) {
	e := newEnv(t, 100, 80)
	e.toArchive(100, 169)
	stop, _ := e.run(e.options())
	e.waitCursor(169, 10*time.Second)
	e.toLake(170, 179)
	e.waitCursor(179, 10*time.Second)
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if got := sourceCounts(e.checkCommitted(179)); got[sep54.SourceArchive] != 70 || got[sep54.SourceLake] != 10 {
		t.Fatalf("sources %v", got)
	}
	if len(e.alerts.list()) != 0 {
		t.Fatalf("alerts %v", e.alerts.list())
	}
}

// A corrupt object is a validation failure: the next source is tried and an
// alert raised; the cursor never passes a ledger that did not validate.
func TestInvalidTriesNextSource(t *testing.T) {
	e := newEnv(t, 300, 100)
	e.toLake(300, 399)
	e.toArchive(300, 399)
	bad := e.obj(350)
	bad.Body = append([]byte(nil), bad.Body...)
	bad.Body[len(bad.Body)/2] ^= 0xff
	e.lake.Add(bad)
	stop, _ := e.run(e.options())
	e.waitCursor(399, 20*time.Second)
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	src := e.checkCommitted(399)
	if src[350] != sep54.SourceArchive || src[351] != sep54.SourceLake {
		t.Fatalf("sources 350=%s 351=%s", src[350], src[351])
	}
	if fmt.Sprint(e.alerts.list()) != "[validation_failed]" {
		t.Fatalf("alerts %v", e.alerts.list())
	}
}

// Every source fails validation: halt without advancing.
func TestAllSourcesInvalidHalts(t *testing.T) {
	e := newEnv(t, 300, 20)
	e.toLake(300, 319)
	e.toAWS(300, 319)
	foreign := sep54test.Chain(310, 1)[0] // another chain's ledger 310, everywhere
	e.lake.Add(foreign)
	e.aws.Add(foreign)
	stop, s := e.run(e.options())
	deadline := time.Now().Add(10 * time.Second)
	for s.Stats().Alerts["pipeline_halted"] == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	err := stop()
	var he *HaltError
	if !errors.As(err, &he) || he.Seq != 310 {
		t.Fatalf("Run returned %v", err)
	}
	e.checkCommitted(309) // the valid prefix is committed
	if got := e.alerts.list(); fmt.Sprint(got) != "[validation_failed validation_failed pipeline_halted]" {
		t.Fatalf("alerts %v", got)
	}
}

// The cursor's hash does not link to any source's ledger: halt, commit
// nothing.
func TestCursorHashMismatchHalts(t *testing.T) {
	e := newEnv(t, 300, 5)
	e.toLake(300, 304)
	e.sink.hash[0] ^= 1
	stop, _ := e.run(e.options())
	time.Sleep(300 * time.Millisecond)
	if err := stop(); !IsHalt(err) || !strings.Contains(err.Error(), "cursor chain") {
		t.Fatalf("Run returned %v", err)
	}
	if c := e.sink.cursor(); c != 299 {
		t.Fatalf("cursor moved to %d", c)
	}
}

// A lake restart shorter than the grace period: retries, no failover, no
// alert (M3 acceptance).
func TestLakeRestartNoFailover(t *testing.T) {
	e := newEnv(t, 900, 200)
	e.toLake(900, 999)
	e.toAWS(900, 1099)
	stop, _ := e.run(e.options())
	e.waitCursor(999, 10*time.Second)
	// The lake goes away (both ports) while ingest waits at the tip, and
	// while it catches up.
	e.lake.down.Store(true)
	e.tip.setDown(true)
	time.Sleep(700 * time.Millisecond)
	e.lake.down.Store(false)
	e.tip.setDown(false)
	e.toLake(1000, 1099)
	time.Sleep(20 * time.Millisecond)
	e.lake.down.Store(true)
	e.tip.setDown(true)
	time.Sleep(500 * time.Millisecond)
	e.lake.down.Store(false)
	e.tip.setDown(false)
	e.waitCursor(1099, 20*time.Second)
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if got := sourceCounts(e.checkCommitted(1099)); got[sep54.SourceLake] != 200 || e.aws.Gets.Load() != 0 {
		t.Fatalf("sources %v, AWS gets %d", got, e.aws.Gets.Load())
	}
	if len(e.alerts.list()) != 0 {
		t.Fatalf("alerts %v", e.alerts.list())
	}
}

// A lake down for longer than the grace period: fail over to the archive or
// the AWS lake with an alert; back to the lake when it returns.
func TestLakeFailover(t *testing.T) {
	e := newEnv(t, 900, 60)
	e.toLake(900, 919)
	e.toArchive(900, 929)
	e.toAWS(900, 939)
	o := e.options()
	o.LakeGrace = 300 * time.Millisecond
	stop, _ := e.run(o)
	e.waitCursor(919, 10*time.Second)
	e.lake.down.Store(true)
	e.tip.setDown(true)
	// Galexie keeps exporting into the lake meanwhile (unreadable).
	for s := uint32(920); s < 940; s++ {
		e.lake.Add(e.obj(s))
	}
	e.waitCursor(939, 10*time.Second) // 920-929 archive, 930-939 AWS
	e.lake.down.Store(false)
	e.tip.setDown(false)
	e.toLake(940, 959)
	e.waitCursor(959, 10*time.Second)
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	src := e.checkCommitted(959)
	if src[925] != sep54.SourceArchive || src[935] != sep54.SourceAWS || src[950] != sep54.SourceLake {
		t.Fatalf("sources 925=%s 935=%s 950=%s", src[925], src[935], src[950])
	}
	if got := e.alerts.list(); fmt.Sprint(got) != "[lake_unavailable reading_from_aws]" {
		t.Fatalf("alerts %v", got)
	}
}

// The AWS lake as the pipeline's lake (the clickhouse pipeline): its reads
// are lake reads, with no reading_from_aws alert, and an outage shorter
// than the grace period is ridden out.
func TestAWSAsLake(t *testing.T) {
	e := newEnv(t, 300, 40)
	e.lake = newMemSource(sep54.SourceAWS)
	e.toLake(300, 319)
	o := e.options()
	o.Sources = Sources{Lake: e.lake, LakeTip: e.tip}
	o.LakeGrace = 10 * time.Second
	stop, _ := e.run(o)
	e.waitCursor(319, 10*time.Second)
	e.lake.down.Store(true)
	e.toLake(320, 339)
	time.Sleep(300 * time.Millisecond)
	e.lake.down.Store(false)
	e.waitCursor(339, 10*time.Second)
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if got := sourceCounts(e.checkCommitted(339)); got[sep54.SourceAWS] != 40 {
		t.Fatalf("sources %v", got)
	}
	if got := e.alerts.list(); len(got) != 0 {
		t.Fatalf("alerts %v", got)
	}
}

// The archive failing for ArchiveGrace: the AWS lake, with an alert.
func TestArchiveUnavailableFallsBackToAWS(t *testing.T) {
	e := newEnv(t, 100, 40)
	e.toArchive(100, 139)
	e.toAWS(100, 139)
	e.archive.down.Store(true)
	o := e.options()
	o.ArchiveGrace = 300 * time.Millisecond
	stop, _ := e.run(o)
	e.waitCursor(139, 10*time.Second)
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if got := sourceCounts(e.checkCommitted(139)); got[sep54.SourceAWS] != 40 {
		t.Fatalf("sources %v", got)
	}
	if got := e.alerts.list(); fmt.Sprint(got) != "[reading_from_aws]" {
		t.Fatalf("alerts %v", got)
	}
}

// Transient commit failures (Postgres down) are retried with the same
// batch; nothing is lost or duplicated.
func TestCommitRetries(t *testing.T) {
	e := newEnv(t, 10, 100)
	e.toLake(10, 109)
	e.sink.failures = []error{Transient(errors.New("connection refused")), Transient(errors.New("admin shutdown"))}
	o := e.options()
	o.CommitMaxBackoff = 50 * time.Millisecond
	stop, _ := e.run(o)
	e.waitCursor(109, 20*time.Second)
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	e.checkCommitted(109)
	if calls := e.sink.commitCalls.Load(); calls < 3 {
		t.Fatalf("%d commit calls", calls)
	}
}

// A sink halt (e.g. the cursor's hash does not match) stops the pipeline;
// any other non-transient error is fatal.
func TestSinkErrors(t *testing.T) {
	for name, fail := range map[string]error{
		"halt":  Halt(10, errors.New("hash mismatch")),
		"fatal": errors.New("the pipeline cursor was moved by another committer"),
	} {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t, 10, 5)
			e.toLake(10, 14)
			e.sink.failures = []error{fail}
			s, err := New(e.options())
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			err = s.Run(ctx)
			if !errors.Is(err, fail) && !strings.Contains(fmt.Sprint(err), fail.Error()) {
				t.Fatalf("Run returned %v", err)
			}
			if IsHalt(err) != (name == "halt") {
				t.Fatalf("halt=%v for %v", IsHalt(err), err)
			}
			if e.sink.cursor() != 9 {
				t.Fatalf("cursor %d", e.sink.cursor())
			}
		})
	}
}

// Batches respect MaxBatchBytes.
func TestBatchBytes(t *testing.T) {
	e := newEnv(t, 10, 200)
	e.toLake(10, 209)
	o := e.options()
	o.Process = fakeProcess(1000)
	o.MaxBatchBytes = 5000 // at most 5 ledgers
	e.sink.commitDelay = 5 * time.Millisecond
	stop, _ := e.run(o)
	e.waitCursor(209, 20*time.Second)
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	_, batches := e.sink.snapshot()
	for _, b := range batches {
		if b > 5 {
			t.Fatalf("batch of %d ledgers over the byte bound: %v", b, batches)
		}
	}
	e.checkCommitted(209)
}

// Fixed (reindex): one source, a closed range, no re-routing.
func TestFixedRange(t *testing.T) {
	e := newEnv(t, 400, 150)
	e.toAWS(400, 549)
	o := e.options()
	o.Fixed = &Fixed{Source: e.aws, To: 499}
	s, err := New(o)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := s.Run(ctx); err != nil {
		t.Fatal(err)
	}
	e.checkCommitted(499)
	// A missing ledger ends the run with an error at that ledger.
	e2 := newEnv(t, 400, 150)
	e2.toAWS(400, 549)
	e2.aws.Remove(450)
	o2 := e2.options()
	o2.Fixed = &Fixed{Source: e2.aws, To: 549}
	s2, _ := New(o2)
	err = s2.Run(ctx)
	var he *HaltError
	if !errors.As(err, &he) || he.Seq != 450 || !errors.Is(err, sep54.ErrNotFound) {
		t.Fatalf("Run returned %v", err)
	}
	e2.checkCommitted(449)
}

// Shutdown lets the in-flight commit finish.
func TestShutdownFinishesInFlightCommit(t *testing.T) {
	e := newEnv(t, 10, 1)
	e.toLake(10, 10)
	e.sink.commitDelay = 400 * time.Millisecond
	stop, _ := e.run(e.options())
	deadline := time.Now().Add(5 * time.Second)
	for e.sink.commitCalls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	e.checkCommitted(10)
}

// A processLedger panic is a validation failure of that source's bytes.
func TestProcessorPanic(t *testing.T) {
	e := newEnv(t, 10, 20)
	e.toLake(10, 29)
	e.toAWS(10, 29)
	o := e.options()
	o.Process = func(l Ledger) (fakePrepared, error) {
		if l.Header.Sequence == 20 && l.Source == sep54.SourceLake {
			panic("boom")
		}
		return fakePrepared{h: l.Header, source: l.Source, size: 1}, nil
	}
	stop, _ := e.run(o)
	e.waitCursor(29, 10*time.Second)
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if src := e.checkCommitted(29); src[20] != sep54.SourceAWS {
		t.Fatalf("ledger 20 from %s", src[20])
	}
	if got := e.alerts.list(); fmt.Sprint(got) != "[validation_failed reading_from_aws]" {
		t.Fatalf("alerts %v", got)
	}
}

// A sink that panics halts the pipeline instead of crashing the process.
func TestSinkPanicHalts(t *testing.T) {
	e := newEnv(t, 10, 3)
	e.toLake(10, 12)
	o := e.options()
	o.Sink = panicSink{e.sink}
	s, err := New(o)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.Run(ctx); !IsHalt(err) || !strings.Contains(err.Error(), "sink panicked") {
		t.Fatalf("Run returned %v", err)
	}
	if got := e.alerts.list(); fmt.Sprint(got) != "[pipeline_halted]" {
		t.Fatalf("alerts %v", got)
	}
}

type panicSink struct{ *fakeSink }

func (panicSink) Commit(context.Context, []fakePrepared) error { panic("bug") }
