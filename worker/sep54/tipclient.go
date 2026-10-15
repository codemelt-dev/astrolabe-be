package sep54

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"
)

// Tip client defaults.
const (
	// TipHTTPTimeout is longer than the lake's 30 s long-poll.
	TipHTTPTimeout = 40 * time.Second
	// TipLongPoll is how long the lake holds GET /v1/tip?after=N.
	TipLongPoll = 30 * time.Second
	// TipMaxBackoff caps the retry delay while the lake is unavailable or
	// returns long-polls early.
	TipMaxBackoff = 5 * time.Second
	// tipMinBackoff is the first retry delay.
	tipMinBackoff = 100 * time.Millisecond
)

// TipClient talks to the lake's control endpoint (GET /v1/tip). It has its
// own HTTP transport with no proxy: the endpoint is loopback only.
type TipClient struct {
	base string
	hc   *http.Client
	// LongPoll is the server's long-poll duration. A poll that returns
	// without progress in less than half of it counts as an early return
	// and is retried with backoff (a lake that is shutting down releases
	// its long-polls at once).
	LongPoll time.Duration
	// MaxBackoff caps retry delays.
	MaxBackoff time.Duration
	// sleep waits for d or until ctx ends; replaceable in tests.
	sleep func(ctx context.Context, d time.Duration) error
}

// NewTipClient returns a client for the control listener at addr
// ("127.0.0.1:9001").
func NewTipClient(addr string) *TipClient {
	tr := &http.Transport{
		Proxy:               nil,
		DialContext:         (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConnsPerHost: 4,
		IdleConnTimeout:     90 * time.Second,
	}
	return &TipClient{
		base:       "http://" + addr,
		hc:         &http.Client{Transport: tr, Timeout: TipHTTPTimeout},
		LongPoll:   TipLongPoll,
		MaxBackoff: TipMaxBackoff,
		sleep:      sleepCtx,
	}
}

// Get returns the lake's current tip at once.
func (c *TipClient) Get(ctx context.Context) (Tip, error) {
	return c.do(ctx, c.base+"/v1/tip")
}

// Poll long-polls once: it returns as soon as the lake's tip exceeds after,
// or with the current values when the lake's long-poll ends (30 s) or the
// lake shuts down.
func (c *TipClient) Poll(ctx context.Context, after uint32) (Tip, error) {
	return c.do(ctx, c.base+"/v1/tip?after="+strconv.FormatUint(uint64(after), 10))
}

// WaitAbove blocks until the lake's tip exceeds after and returns it.
//
//   - While the lake is unavailable it retries with backoff (at most
//     MaxBackoff). On reconnect it first re-fetches the tip with an
//     immediate GET, so it never long-polls for a commit that already
//     happened.
//   - A long-poll that returns early without progress (a lake shutting down
//     releases them at once) is retried with the same backoff, never in a
//     hot loop.
//   - If the lake stays unavailable for grace (> 0), it returns the last
//     *UnavailableError so the caller can fail over and alert.
//   - Protocol errors (4xx, bad JSON) are returned at once.
func (c *TipClient) WaitAbove(ctx context.Context, after uint32, grace time.Duration) (Tip, error) {
	var (
		backoff   time.Duration
		downSince time.Time
		immediate = true // start with a plain GET: the tip may already be there
	)
	for {
		start := time.Now()
		var tip Tip
		var err error
		if immediate {
			tip, err = c.Get(ctx)
		} else {
			tip, err = c.Poll(ctx, after)
		}
		switch {
		case ctx.Err() != nil:
			return Tip{}, ctx.Err()
		case err != nil && !IsUnavailable(err):
			return Tip{}, err
		case err != nil:
			if downSince.IsZero() {
				downSince = start
			}
			if grace > 0 && time.Since(downSince) >= grace {
				return Tip{}, err
			}
			immediate = true
		default:
			downSince = time.Time{}
			if tip.Tip != nil && *tip.Tip > after {
				return tip, nil
			}
			if immediate || time.Since(start) >= c.LongPoll/2 {
				// A plain GET, or a long-poll that ran its course: poll again now.
				backoff, immediate = 0, false
				continue
			}
			// The long-poll returned early without progress.
		}
		backoff = nextBackoff(backoff, c.MaxBackoff)
		if err := c.sleep(ctx, backoff); err != nil {
			return Tip{}, err
		}
	}
}

func nextBackoff(cur, max time.Duration) time.Duration {
	if cur < tipMinBackoff {
		return tipMinBackoff
	}
	return min(2*cur, max)
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func (c *TipClient) do(ctx context.Context, url string) (Tip, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return Tip{}, err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return Tip{}, ctx.Err()
		}
		return Tip{}, &UnavailableError{Op: "lake tip", Err: err}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if err != nil {
		if ctx.Err() != nil {
			return Tip{}, ctx.Err()
		}
		return Tip{}, &UnavailableError{Op: "lake tip", Err: err}
	}
	switch {
	case resp.StatusCode >= 500:
		return Tip{}, &UnavailableError{Op: "lake tip", Err: fmt.Errorf("%s: %s", resp.Status, body)}
	case resp.StatusCode != http.StatusOK:
		return Tip{}, fmt.Errorf("lake tip: %s: %s", resp.Status, body)
	}
	var tip Tip
	if err := json.Unmarshal(body, &tip); err != nil {
		return Tip{}, fmt.Errorf("lake tip: bad response %q: %w", body, err)
	}
	return tip, nil
}
