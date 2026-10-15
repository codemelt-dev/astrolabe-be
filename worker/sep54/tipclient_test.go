package sep54

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeLake serves /v1/tip from a scripted handler and records requests.
type fakeLake struct {
	mu     sync.Mutex
	reqs   []string
	handle func(w http.ResponseWriter, r *http.Request)
}

func (f *fakeLake) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.reqs = append(f.reqs, r.URL.RequestURI())
	h := f.handle
	f.mu.Unlock()
	h(w, r)
}

func (f *fakeLake) requests() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.reqs...)
}

func tipJSON(w http.ResponseWriter, tip *uint32) {
	t := Tip{Tip: tip}
	if tip != nil {
		o, at := uint32(1), int64(1)
		t.Oldest, t.TipCommittedAt = &o, &at
	}
	_ = json.NewEncoder(w).Encode(t)
}

func u32(v uint32) *uint32 { return &v }

func newTestTipClient(t *testing.T, h http.Handler) (*TipClient, *[]time.Duration) {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	c := NewTipClient(strings.TrimPrefix(srv.URL, "http://"))
	var mu sync.Mutex
	var sleeps []time.Duration
	c.sleep = func(ctx context.Context, d time.Duration) error {
		mu.Lock()
		sleeps = append(sleeps, d)
		mu.Unlock()
		return ctx.Err()
	}
	return c, &sleeps
}

func TestTipClientGetAndPoll(t *testing.T) {
	f := &fakeLake{handle: func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Has("after") {
			tipJSON(w, u32(11))
			return
		}
		tipJSON(w, nil)
	}}
	c, _ := newTestTipClient(t, f)
	tip, err := c.Get(context.Background())
	if err != nil || tip.Tip != nil || tip.Oldest != nil || tip.TipCommittedAt != nil {
		t.Fatalf("empty lake: %+v %v", tip, err)
	}
	tip, err = c.Poll(context.Background(), 10)
	if err != nil || tip.Tip == nil || *tip.Tip != 11 {
		t.Fatalf("poll: %+v %v", tip, err)
	}
	if got := f.requests(); len(got) != 2 || got[0] != "/v1/tip" || got[1] != "/v1/tip?after=10" {
		t.Fatalf("requests %v", got)
	}
	if c.hc.Timeout != 40*time.Second {
		t.Fatalf("HTTP timeout %v, want 40s (longer than the 30 s long-poll)", c.hc.Timeout)
	}
}

// WaitAbove starts with an immediate GET, then long-polls.
func TestWaitAboveReturnsOnCommit(t *testing.T) {
	var polls atomic.Int32
	f := &fakeLake{handle: func(w http.ResponseWriter, r *http.Request) {
		if !r.URL.Query().Has("after") {
			tipJSON(w, u32(5))
			return
		}
		polls.Add(1)
		tipJSON(w, u32(6)) // the commit the poll was waiting for
	}}
	c, sleeps := newTestTipClient(t, f)
	tip, err := c.WaitAbove(context.Background(), 5, 0)
	if err != nil || *tip.Tip != 6 {
		t.Fatalf("WaitAbove = %+v, %v", tip, err)
	}
	if got := f.requests(); len(got) != 2 || got[0] != "/v1/tip" || got[1] != "/v1/tip?after=5" {
		t.Fatalf("requests %v", got)
	}
	if len(*sleeps) != 0 {
		t.Fatalf("slept %v", *sleeps)
	}
	// Already above: the first GET answers.
	tip, err = c.WaitAbove(context.Background(), 4, 0)
	if err != nil || *tip.Tip != 5 || polls.Load() != 1 {
		t.Fatalf("already above: %+v, %v, polls %d", tip, err, polls.Load())
	}
}

// A lake that releases long-polls at once (shutting down) must not be
// hammered: early returns back off exponentially up to MaxBackoff.
func TestWaitAboveBacksOffOnEarlyReturns(t *testing.T) {
	var n atomic.Int32
	f := &fakeLake{handle: func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1) > 12 {
			tipJSON(w, u32(8))
			return
		}
		tipJSON(w, u32(7)) // no progress, returned immediately
	}}
	c, sleeps := newTestTipClient(t, f)
	tip, err := c.WaitAbove(context.Background(), 7, 0)
	if err != nil || *tip.Tip != 8 {
		t.Fatalf("WaitAbove = %+v, %v", tip, err)
	}
	got := *sleeps
	if len(got) != 11 {
		t.Fatalf("slept %d times (%v), want one backoff per early poll", len(got), got)
	}
	if got[0] != 100*time.Millisecond || got[1] != 200*time.Millisecond || got[len(got)-1] != TipMaxBackoff {
		t.Fatalf("backoff %v", got)
	}
	for _, d := range got {
		if d > TipMaxBackoff {
			t.Fatalf("backoff %v above the cap", d)
		}
	}
}

// A long-poll that ran its full course is re-issued at once.
func TestWaitAboveRepollsAfterFullLongPoll(t *testing.T) {
	var n atomic.Int32
	f := &fakeLake{handle: func(w http.ResponseWriter, r *http.Request) {
		if !r.URL.Query().Has("after") {
			tipJSON(w, u32(3))
			return
		}
		if n.Add(1) == 1 {
			time.Sleep(60 * time.Millisecond) // the whole (shortened) long-poll
			tipJSON(w, u32(3))
			return
		}
		tipJSON(w, u32(4))
	}}
	c, sleeps := newTestTipClient(t, f)
	c.LongPoll = 100 * time.Millisecond
	tip, err := c.WaitAbove(context.Background(), 3, 0)
	if err != nil || *tip.Tip != 4 || len(*sleeps) != 0 {
		t.Fatalf("WaitAbove = %+v, %v, slept %v", tip, err, *sleeps)
	}
}

// Connection refused is unavailability: retried with backoff until the
// grace period, then returned as *UnavailableError.
func TestWaitAboveGivesUpAfterGrace(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close() // nothing listens: connection refused
	c := NewTipClient(addr)
	c.MaxBackoff = 20 * time.Millisecond
	start := time.Now()
	_, err = c.WaitAbove(context.Background(), 1, 300*time.Millisecond)
	var ue *UnavailableError
	if !errors.As(err, &ue) || !IsUnavailable(err) {
		t.Fatalf("WaitAbove = %v, want *UnavailableError", err)
	}
	if d := time.Since(start); d < 300*time.Millisecond || d > 5*time.Second {
		t.Fatalf("gave up after %v, want just after the 300ms grace", d)
	}
	if _, err := c.Get(context.Background()); !IsUnavailable(err) {
		t.Fatalf("Get on a closed port = %v", err)
	}
}

// After a failure the client re-fetches with an immediate GET, so a commit
// made while the lake was unreachable is seen at once.
func TestWaitAboveRefetchesOnReconnect(t *testing.T) {
	var n atomic.Int32
	f := &fakeLake{handle: func(w http.ResponseWriter, r *http.Request) {
		switch n.Add(1) {
		case 1:
			tipJSON(w, u32(1)) // initial GET: no progress
		case 2, 3:
			// The long-poll dies mid-request (lake restart). Go's transport
			// retries a GET once on a reused connection, so kill both.
			hj, _ := w.(http.Hijacker)
			conn, _, _ := hj.Hijack()
			conn.Close()
		default:
			tipJSON(w, u32(2))
		}
	}}
	c, sleeps := newTestTipClient(t, f)
	tip, err := c.WaitAbove(context.Background(), 1, time.Minute)
	if err != nil || *tip.Tip != 2 {
		t.Fatalf("WaitAbove = %+v, %v", tip, err)
	}
	got := f.requests()
	if len(got) != 4 || got[3] != "/v1/tip" {
		t.Fatalf("requests %v, want the retry to be an immediate GET", got)
	}
	if len(*sleeps) != 1 {
		t.Fatalf("slept %v", *sleeps)
	}
}

func TestTipClientErrorClasses(t *testing.T) {
	status := http.StatusBadRequest
	body := "after must be a ledger sequence"
	f := &fakeLake{handle: func(w http.ResponseWriter, r *http.Request) {
		if status == http.StatusOK {
			_, _ = w.Write([]byte(body))
			return
		}
		http.Error(w, body, status)
	}}
	c, _ := newTestTipClient(t, f)
	_, err := c.WaitAbove(context.Background(), 1, time.Minute)
	if err == nil || IsUnavailable(err) || !strings.Contains(err.Error(), "400") {
		t.Fatalf("400: %v", err)
	}
	status = http.StatusServiceUnavailable
	if _, err := c.Get(context.Background()); !IsUnavailable(err) {
		t.Fatalf("503: %v", err)
	}
	status, body = http.StatusOK, "{not json"
	if _, err := c.WaitAbove(context.Background(), 1, time.Minute); err == nil || IsUnavailable(err) {
		t.Fatalf("bad JSON: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Get(ctx); !errors.Is(err, context.Canceled) || IsUnavailable(err) {
		t.Fatalf("cancelled: %v", err)
	}
}
