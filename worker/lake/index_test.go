package lake

import (
	"context"
	"math"
	"testing"
	"time"

	"stellar-explorer/worker/sep54"
)

func tipVals(t sep54.Tip) (tip, oldest uint32, ok bool) {
	if t.Tip == nil || t.Oldest == nil || t.TipCommittedAt == nil {
		return 0, 0, false
	}
	return *t.Tip, *t.Oldest, true
}

func TestIndexEmptyIsNull(t *testing.T) {
	x := newIndex()
	if s := x.snapshot(); s.Tip != nil || s.Oldest != nil || s.TipCommittedAt != nil {
		t.Fatalf("empty index = %s", fmtTip(s))
	}
	x.load(0, 0, time.Time{}, false)
	if s := x.snapshot(); s.Tip != nil {
		t.Fatalf("load(ok=false) set the tip: %s", fmtTip(s))
	}
}

func TestIndexTipMonotonicAndOldest(t *testing.T) {
	x := newIndex()
	t0 := time.Unix(100, 0)
	x.committed(10, t0) // first commit into an empty lake sets oldest
	if tip, oldest, _ := tipVals(x.snapshot()); tip != 10 || oldest != 10 {
		t.Fatalf("tip %d oldest %d", tip, oldest)
	}
	x.committed(12, t0.Add(time.Second))
	x.committed(11, t0.Add(2*time.Second)) // below the tip: a repair, or a 412
	x.committed(5, t0.Add(3*time.Second))  // below oldest: oldest is only set when unset
	s := x.snapshot()
	if tip, oldest, _ := tipVals(s); tip != 12 || oldest != 10 {
		t.Fatalf("tip %d oldest %d, want 12 10", tip, oldest)
	}
	if *s.TipCommittedAt != t0.Add(time.Second).UnixNano() {
		t.Fatalf("tip_committed_at moved on a commit below the tip")
	}
}

func TestIndexDeleted(t *testing.T) {
	x := newIndex()
	x.load(20, 10, time.Unix(1, 0), true)
	x.deleted(15) // not the oldest: unchanged
	if _, oldest, _ := tipVals(x.snapshot()); oldest != 10 {
		t.Fatalf("oldest %d after deleting a non-oldest ledger", oldest)
	}
	x.deleted(10)
	x.deleted(11)
	if _, oldest, _ := tipVals(x.snapshot()); oldest != 12 {
		t.Fatalf("oldest %d, want 12", oldest)
	}
	x.deleted(9) // below oldest: unchanged
	if tip, oldest, _ := tipVals(x.snapshot()); oldest != 12 || tip != 20 {
		t.Fatalf("tip %d oldest %d", tip, oldest)
	}
	y := newIndex()
	y.load(math.MaxUint32, math.MaxUint32, time.Unix(1, 0), true)
	y.deleted(math.MaxUint32) // must not wrap to 0
	if _, oldest, _ := tipVals(y.snapshot()); oldest != math.MaxUint32 {
		t.Fatalf("oldest wrapped to %d", oldest)
	}
}

func TestIndexWait(t *testing.T) {
	x := newIndex()
	x.load(100, 90, time.Unix(1, 0), true)
	ctx := context.Background()

	// Already past: immediate.
	start := time.Now()
	if tip, _, _ := tipVals(x.wait(ctx, 99, time.Minute)); tip != 100 || time.Since(start) > time.Second {
		t.Fatalf("wait(after=99) = %d after %v", tip, time.Since(start))
	}

	// Timeout returns the current values.
	start = time.Now()
	if tip, _, _ := tipVals(x.wait(ctx, 100, 50*time.Millisecond)); tip != 100 {
		t.Fatalf("timed-out wait tip = %d", tip)
	}
	if d := time.Since(start); d < 50*time.Millisecond {
		t.Fatalf("wait returned after %v, before the timeout", d)
	}

	// A commit that does not pass `after` keeps waiting; one that does wakes it.
	got := make(chan sep54.Tip, 1)
	go func() { got <- x.wait(ctx, 101, time.Minute) }()
	time.Sleep(20 * time.Millisecond)
	x.committed(101, time.Now())
	select {
	case s := <-got:
		t.Fatalf("woke at tip %s, want > 101", fmtTip(s))
	case <-time.After(50 * time.Millisecond):
	}
	x.committed(102, time.Now())
	select {
	case s := <-got:
		if tip, _, _ := tipVals(s); tip != 102 {
			t.Fatalf("woke with tip %d", tip)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("wait did not wake on the commit")
	}

	// An empty lake waits for the first commit.
	e := newIndex()
	go func() { got <- e.wait(ctx, 0, time.Minute) }()
	time.Sleep(20 * time.Millisecond)
	e.committed(1, time.Now())
	select {
	case s := <-got:
		if tip, oldest, _ := tipVals(s); tip != 1 || oldest != 1 {
			t.Fatalf("got %s", fmtTip(s))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("wait on an empty lake did not wake on the first commit")
	}

	// A cancelled request returns.
	cctx, cancel := context.WithCancel(ctx)
	go func() { got <- x.wait(cctx, 1000, time.Minute) }()
	cancel()
	select {
	case <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("wait ignored its context")
	}
}

func TestIndexRelease(t *testing.T) {
	x := newIndex()
	x.load(5, 1, time.Unix(1, 0), true)
	const n = 3
	got := make(chan sep54.Tip, n)
	for range n {
		go func() { got <- x.wait(context.Background(), 5, time.Minute) }()
	}
	time.Sleep(20 * time.Millisecond)
	start := time.Now()
	x.release()
	for range n {
		select {
		case s := <-got:
			if tip, _, _ := tipVals(s); tip != 5 {
				t.Fatalf("released with tip %d", tip)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("release did not wake a waiter")
		}
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("release took %v", d)
	}
	// Later waits return immediately too; a second release is harmless.
	x.release()
	start = time.Now()
	x.wait(context.Background(), 5, time.Minute)
	if d := time.Since(start); d > time.Second {
		t.Fatalf("wait after release took %v", d)
	}
}

func TestInflightPhases(t *testing.T) {
	now := time.Unix(1000, 0)
	fl := newInflight(func() time.Time { return now })
	if p := fl.start(false); p != nil {
		t.Fatal("non-ledger PUTs must not be tracked")
	}
	var nilPut *inflightPut
	nilPut.enter(phaseSyncing) // nil-safe
	nilPut.done()

	a := fl.start(true)
	now = now.Add(3 * time.Second)
	b := fl.start(true)
	now = now.Add(2 * time.Second)
	ages := fl.ages()
	if ages[phaseReceiving] != 5*time.Second || ages[phaseSyncing] != 0 {
		t.Fatalf("ages %v", ages)
	}
	a.enter(phaseSyncing) // the phase age restarts
	now = now.Add(time.Second)
	a.enter(phaseSyncing) // re-entering the same phase keeps its start
	ages = fl.ages()
	if ages[phaseReceiving] != 3*time.Second || ages[phaseSyncing] != time.Second {
		t.Fatalf("ages %v", ages)
	}
	a.done()
	b.done()
	ages = fl.ages()
	if ages[phaseReceiving] != 0 || ages[phaseSyncing] != 0 || len(ages) != 2 {
		t.Fatalf("ages after done %v", ages)
	}
}
