package lake

import (
	"context"
	"math"
	"sync"
	"time"

	"stellar-explorer/worker/sep54"
)

// index tracks the commit tip and the oldest ledger (rule 6, tap.go) and
// wakes long-polls when the tip grows. It only ever holds uint32 sequences,
// never request strings (rule 5).
type index struct {
	mu        sync.Mutex
	tip       uint32
	hasTip    bool
	tipAt     int64 // Unix ns when tip was committed
	oldest    uint32
	hasOldest bool
	grew      chan struct{} // closed (and replaced) whenever the tip grows
	released  chan struct{} // closed once at shutdown: every wait returns at once
	isRelease bool
}

func newIndex() *index {
	return &index{grew: make(chan struct{}), released: make(chan struct{})}
}

// load seeds the index from the partition directories at startup.
func (x *index) load(tip, oldest uint32, tipAt time.Time, ok bool) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if !ok {
		return
	}
	x.tip, x.hasTip, x.tipAt = tip, true, tipAt.UnixNano()
	x.oldest, x.hasOldest = oldest, true
}

// committed records a durable ledger object: tip = max(tip, seq), so the
// tip never moves backwards (e.g. on a repair below the tip); oldest is set
// only if unset; waiters wake only if the tip grew.
func (x *index) committed(seq uint32, at time.Time) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if !x.hasOldest {
		x.oldest, x.hasOldest = seq, true
	}
	if x.hasTip && seq <= x.tip {
		return
	}
	x.tip, x.hasTip, x.tipAt = seq, true, at.UnixNano()
	close(x.grew)
	x.grew = make(chan struct{})
}

// deleted records a DeleteObject of ledger seq: deleting the oldest advances
// oldest by one; deleting any other ledger leaves it unchanged (a hole at the
// new oldest is handled by the reader's NotFound re-route).
func (x *index) deleted(seq uint32) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.hasOldest && seq == x.oldest && seq < math.MaxUint32 {
		x.oldest = seq + 1
	}
}

func (x *index) snapshot() sep54.Tip {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.snapshotLocked()
}

func (x *index) snapshotLocked() sep54.Tip {
	var t sep54.Tip
	if x.hasTip {
		tip, at := x.tip, x.tipAt
		t.Tip, t.TipCommittedAt = &tip, &at
	}
	if x.hasOldest {
		oldest := x.oldest
		t.Oldest = &oldest
	}
	return t
}

// wait returns as soon as the tip exceeds after, or with the current values
// once timeout passes, ctx ends, or the index is released at shutdown.
func (x *index) wait(ctx context.Context, after uint32, timeout time.Duration) sep54.Tip {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		x.mu.Lock()
		if x.isRelease || (x.hasTip && x.tip > after) {
			t := x.snapshotLocked()
			x.mu.Unlock()
			return t
		}
		grew := x.grew
		x.mu.Unlock()

		select {
		case <-grew:
		case <-x.released:
		case <-timer.C:
			return x.snapshot()
		case <-ctx.Done():
			return x.snapshot()
		}
	}
}

// release makes every current and future wait return immediately.
func (x *index) release() {
	x.mu.Lock()
	defer x.mu.Unlock()
	if !x.isRelease {
		x.isRelease = true
		close(x.released)
	}
}
