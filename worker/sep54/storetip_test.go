package sep54_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"stellar-explorer/worker/sep54"
)

// fakeStore answers NewestObject from a scripted state.
type fakeStore struct {
	mu    sync.Mutex
	seq   uint32
	err   error
	calls int
}

func (f *fakeStore) newest(context.Context) (uint32, time.Time, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.seq, time.Unix(1_700_000_000, 0), f.err
}

func (f *fakeStore) set(seq uint32, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seq, f.err = seq, err
}

func TestStoreTipGet(t *testing.T) {
	f := &fakeStore{seq: 100}
	tip := &sep54.StoreTip{Newest: f.newest, Oldest: 50, Poll: time.Hour}
	got, err := tip.Get(context.Background())
	if err != nil || got.Tip == nil || *got.Tip != 100 || *got.Oldest != 50 || *got.TipCommittedAt != 1_700_000_000e9 {
		t.Fatalf("Get = %+v, %v", got, err)
	}
	// Within Poll the tip is cached: no second LIST.
	f.set(101, nil)
	if got, _ := tip.Get(context.Background()); *got.Tip != 100 || f.calls != 1 {
		t.Fatalf("cached Get = %d after %d calls", *got.Tip, f.calls)
	}
}

func TestStoreTipNeverBackwards(t *testing.T) {
	f := &fakeStore{seq: 100}
	tip := &sep54.StoreTip{Newest: f.newest, Poll: time.Nanosecond}
	tip.Get(context.Background())
	f.set(90, nil) // a listing that lags (eventual consistency)
	if got, _ := tip.Get(context.Background()); *got.Tip != 100 {
		t.Fatalf("tip moved back to %d", *got.Tip)
	}
}

func TestStoreTipEmptyAndUnavailable(t *testing.T) {
	f := &fakeStore{err: sep54.ErrNoLedgers}
	tip := &sep54.StoreTip{Newest: f.newest, Poll: time.Nanosecond}
	if got, err := tip.Get(context.Background()); err != nil || got.Tip != nil {
		t.Fatalf("empty store: %+v, %v (want no tip, no error)", got, err)
	}
	f.set(0, errors.New("connection refused"))
	if _, err := tip.Get(context.Background()); !sep54.IsUnavailable(err) {
		t.Fatalf("a failed LIST is %v, want unavailability", err)
	}
}

func TestStoreTipWaitAbove(t *testing.T) {
	f := &fakeStore{seq: 100}
	tip := &sep54.StoreTip{Newest: f.newest, Poll: 10 * time.Millisecond}
	go func() { time.Sleep(30 * time.Millisecond); f.set(101, nil) }()
	got, err := tip.WaitAbove(context.Background(), 100, time.Second)
	if err != nil || *got.Tip != 101 {
		t.Fatalf("WaitAbove = %+v, %v", got, err)
	}
	// Unreachable for longer than grace: the outage is returned.
	f.set(0, errors.New("timeout"))
	start := time.Now()
	if _, err := tip.WaitAbove(context.Background(), 200, 50*time.Millisecond); !sep54.IsUnavailable(err) || time.Since(start) < 50*time.Millisecond {
		t.Fatalf("WaitAbove during an outage = %v after %s", err, time.Since(start))
	}
}
