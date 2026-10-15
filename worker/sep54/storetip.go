package sep54

import (
	"context"
	"errors"
	"sync"
	"time"
)

// StoreTip follows a store's newest ledger by listing it (one LIST every
// Poll), for a pipeline that reads a store without a lake: the clickhouse
// pipeline follows the archive bucket. It satisfies the sequencer's Tip and
// the metrics' tip reader like TipClient does for the lake.
type StoreTip struct {
	// Newest lists the store's newest ledger and when its upload finished
	// (Source.NewestObject).
	Newest func(ctx context.Context) (uint32, time.Time, error)
	// Oldest is reported as the oldest ledger the store holds; the
	// sequencer reads ledgers from it in [Oldest, tip].
	Oldest uint32
	// Poll is how often the store is listed (default 5 s: about a ledger).
	Poll time.Duration

	mu     sync.Mutex
	last   Tip
	lastAt time.Time
}

// Get returns the tip, listing the store at most once per Poll. Errors are
// reported as unavailability, so the sequencer retries and fails over.
func (t *StoreTip) Get(ctx context.Context) (Tip, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.lastAt.IsZero() && time.Since(t.lastAt) < t.poll() {
		return t.last, nil
	}
	seq, at, err := t.Newest(ctx)
	switch {
	case errors.Is(err, ErrNoLedgers):
		t.last, t.lastAt = Tip{}, time.Now() // empty: no tip yet, not an outage
		return t.last, nil
	case err != nil:
		if IsUnavailable(err) {
			return Tip{}, err
		}
		return Tip{}, &UnavailableError{Op: "list the newest ledger", Err: err}
	}
	oldest, committed := t.Oldest, at.UnixNano()
	if t.last.Tip != nil && *t.last.Tip > seq {
		seq = *t.last.Tip // never backwards
	}
	t.last, t.lastAt = Tip{Tip: &seq, Oldest: &oldest, TipCommittedAt: &committed}, time.Now()
	return t.last, nil
}

// WaitAbove polls until the tip exceeds after. While the store stays
// unreachable for grace (> 0) it returns the last *UnavailableError, so the
// caller can fail over and alert, as TipClient.WaitAbove does.
func (t *StoreTip) WaitAbove(ctx context.Context, after uint32, grace time.Duration) (Tip, error) {
	var downSince time.Time
	for {
		start := time.Now()
		tip, err := t.Get(ctx)
		switch {
		case ctx.Err() != nil:
			return Tip{}, ctx.Err()
		case err != nil:
			if downSince.IsZero() {
				downSince = start
			}
			if grace > 0 && time.Since(downSince) >= grace {
				return Tip{}, err
			}
		default:
			downSince = time.Time{}
			if tip.Tip != nil && *tip.Tip > after {
				return tip, nil
			}
		}
		select {
		case <-ctx.Done():
			return Tip{}, ctx.Err()
		case <-time.After(t.poll()):
		}
	}
}

func (t *StoreTip) poll() time.Duration {
	if t.Poll <= 0 {
		return 5 * time.Second
	}
	return t.Poll
}
