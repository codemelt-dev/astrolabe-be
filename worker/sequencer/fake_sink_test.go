package sequencer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"

	"stellar-explorer/worker/sep54"
	"stellar-explorer/worker/sep54/sep54test"
)

// fakePrepared is what the fake processor produces.
type fakePrepared struct {
	h      sep54.Header
	source string
	size   int64
}

func (f fakePrepared) Header() sep54.Header { return f.h }
func (f fakePrepared) Size() int64          { return f.size }

func fakeProcess(size int64) Processor[fakePrepared] {
	return func(l Ledger) (fakePrepared, error) {
		return fakePrepared{h: l.Header, source: l.Source, size: size}, nil
	}
}

// fakeSink is an in-memory Sink with the guarded-cursor semantics of the
// Postgres sink.
type fakeSink struct {
	mu        sync.Mutex
	seq       uint32
	hash      xdr.Hash
	committed []fakePrepared
	batches   []int

	// failures injects errors: each Commit consumes one.
	failures    []error
	commitDelay time.Duration
	commitCalls atomic.Int64
}

func newFakeSink(seq uint32, hash xdr.Hash) *fakeSink { return &fakeSink{seq: seq, hash: hash} }

func (s *fakeSink) Cursor(ctx context.Context) (uint32, xdr.Hash, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.seq, s.hash, nil
}

func (s *fakeSink) Commit(ctx context.Context, batch []fakePrepared) error {
	s.commitCalls.Add(1)
	if s.commitDelay > 0 {
		select {
		case <-time.After(s.commitDelay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.failures) > 0 {
		err := s.failures[0]
		s.failures = s.failures[1:]
		return err
	}
	first, last := batch[0].h, batch[len(batch)-1].h
	if first.Sequence != s.seq+1 {
		return fmt.Errorf("cursor moved: at %d, batch starts at %d", s.seq, first.Sequence)
	}
	if first.PreviousHash != s.hash {
		return Halt(first.Sequence, errors.New("hash mismatch"))
	}
	for i := 1; i < len(batch); i++ {
		if err := sep54.CheckChain(batch[i-1].h, batch[i].h); err != nil {
			return Halt(batch[i].h.Sequence, err)
		}
	}
	s.committed = append(s.committed, batch...)
	s.batches = append(s.batches, len(batch))
	s.seq, s.hash = last.Sequence, last.Hash
	return nil
}

func (s *fakeSink) cursor() uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.seq
}

func (s *fakeSink) snapshot() ([]fakePrepared, []int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]fakePrepared(nil), s.committed...), append([]int(nil), s.batches...)
}

// memSource names a sep54test.MemSource.
type memSource struct {
	*sep54test.MemSource
	name string
	down atomic.Bool
}

func newMemSource(name string, objs ...sep54.Object) *memSource {
	m := &memSource{MemSource: sep54test.NewMemSource(objs...), name: name}
	m.Fail = func(uint32) error {
		if m.down.Load() {
			return &sep54.UnavailableError{Op: name, Err: errors.New("connection refused")}
		}
		return nil
	}
	return m
}

func (m *memSource) Name() string { return m.name }

// fakeTip is the lake's control endpoint.
type fakeTip struct {
	mu          sync.Mutex
	tip, oldest *uint32
	committedAt int64
	changed     chan struct{}
	down        bool
}

func newFakeTip() *fakeTip { return &fakeTip{changed: make(chan struct{})} }

func (f *fakeTip) snapshot() sep54.Tip {
	t := sep54.Tip{}
	if f.tip != nil {
		tip, oldest, at := *f.tip, *f.oldest, f.committedAt
		t.Tip, t.Oldest, t.TipCommittedAt = &tip, &oldest, &at
	}
	return t
}

func (f *fakeTip) Get(ctx context.Context) (sep54.Tip, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.down {
		return sep54.Tip{}, &sep54.UnavailableError{Op: "lake tip", Err: errors.New("connection refused")}
	}
	return f.snapshot(), nil
}

func (f *fakeTip) WaitAbove(ctx context.Context, after uint32, grace time.Duration) (sep54.Tip, error) {
	var downSince time.Time
	for {
		f.mu.Lock()
		down, t, ch := f.down, f.snapshot(), f.changed
		f.mu.Unlock()
		if down {
			if downSince.IsZero() {
				downSince = time.Now()
			}
			if grace > 0 && time.Since(downSince) >= grace {
				return sep54.Tip{}, &sep54.UnavailableError{Op: "lake tip", Err: errors.New("connection refused")}
			}
		} else {
			downSince = time.Time{}
			if t.Tip != nil && *t.Tip > after {
				return t, nil
			}
		}
		select {
		case <-ch:
		case <-time.After(20 * time.Millisecond):
		case <-ctx.Done():
			return sep54.Tip{}, ctx.Err()
		}
	}
}

// set publishes a new tip (and oldest) and wakes waiters.
func (f *fakeTip) set(oldest, tip uint32) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tip, f.oldest, f.committedAt = &tip, &oldest, time.Now().UnixNano()
	close(f.changed)
	f.changed = make(chan struct{})
}

func (f *fakeTip) setDown(down bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.down = down
	close(f.changed)
	f.changed = make(chan struct{})
}
