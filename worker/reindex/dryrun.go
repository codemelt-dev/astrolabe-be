package reindex

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"stellar-explorer/worker/sep54"
)

// dryRun reads and fully decodes every ledger of [From, To] with parallel
// GETs and checks sequence, self-hash and the hash chain. It reports every
// missing and invalid ledger instead of stopping at the first.
func dryRun(ctx context.Context, o Options) (Result, error) {
	workers := o.Workers
	if workers <= 0 {
		workers = 16
	}
	res := Result{From: o.From}
	type result struct {
		h   sep54.Header
		err error
	}
	// A window of in-flight ledgers, consumed in order.
	window := uint32(4 * workers)
	results := make(map[uint32]chan result)
	var mu sync.Mutex
	sem := make(chan struct{}, workers)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	defer wg.Wait()
	launch := func(seq uint32) {
		ch := make(chan result, 1)
		mu.Lock()
		results[seq] = ch
		mu.Unlock()
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				ch <- result{err: ctx.Err()}
				return
			}
			defer func() { <-sem }()
			h, err := readValidated(ctx, o.Source, seq)
			ch <- result{h, err}
		}()
	}
	next := o.From
	for ; next <= o.To && next-o.From < window; next++ {
		launch(next)
	}
	var prev *sep54.Header
	lastLog := time.Now()
	for seq := o.From; seq <= o.To; seq++ {
		mu.Lock()
		ch := results[seq]
		delete(results, seq)
		mu.Unlock()
		r := <-ch
		if next <= o.To {
			launch(next)
			next++
		}
		switch {
		case r.err == nil:
			if prev != nil {
				if err := sep54.CheckChain(*prev, r.h); err != nil {
					res.Invalid = append(res.Invalid, fmt.Sprintf("%d: %v", seq, err))
				}
			}
			h := r.h
			prev = &h
			res.Ledgers++
		case ctx.Err() != nil:
			return res, ctx.Err()
		case errors.Is(r.err, os.ErrNotExist):
			res.Missing = append(res.Missing, seq)
			prev = nil
		case sep54.IsInvalid(r.err):
			res.Invalid = append(res.Invalid, fmt.Sprintf("%d: %v", seq, r.err))
			prev = nil
		default:
			return res, r.err
		}
		if time.Since(lastLog) > 30*time.Second {
			lastLog = time.Now()
			o.Log.Info("dry run progress", "ledger", seq, "to", o.To, "missing", len(res.Missing), "invalid", len(res.Invalid))
		}
	}
	if len(res.Missing) > 0 || len(res.Invalid) > 0 {
		return res, fmt.Errorf("%d-%d from %s: %d missing, %d invalid", o.From, o.To, o.Source.Name(), len(res.Missing), len(res.Invalid))
	}
	return res, nil
}
