package lake

import (
	"sync"
	"time"
)

// phase is where an in-flight ledger PUT is (rule 8, tap.go). The
// watchdog restarts Galexie for a stuck sender ("receiving") but only pages
// for a stuck disk ("syncing"), which a restart cannot unblock.
type phase string

const (
	phaseReceiving phase = "receiving" // until the body is fully staged
	phaseSyncing   phase = "syncing"   // post-function fsync through syncPublished
)

var phases = []phase{phaseReceiving, phaseSyncing}

// inflight tracks the ledger PUTs currently in the handler.
type inflight struct {
	mu   sync.Mutex
	next uint64
	puts map[uint64]*inflightPut
	now  func() time.Time
}

type inflightPut struct {
	t     *inflight
	id    uint64
	phase phase
	since time.Time // when phase was entered
}

func newInflight(now func() time.Time) *inflight {
	return &inflight{puts: make(map[uint64]*inflightPut), now: now}
}

// start registers a PUT in phase "receiving". It returns nil for non-ledger
// keys; every method of *inflightPut is nil-safe.
func (t *inflight) start(isLedger bool) *inflightPut {
	if !isLedger {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.next++
	p := &inflightPut{t: t, id: t.next, phase: phaseReceiving, since: t.now()}
	t.puts[p.id] = p
	return p
}

func (p *inflightPut) enter(ph phase) {
	if p == nil {
		return
	}
	p.t.mu.Lock()
	defer p.t.mu.Unlock()
	if p.phase != ph {
		p.phase, p.since = ph, p.t.now()
	}
}

func (p *inflightPut) done() {
	if p == nil {
		return
	}
	p.t.mu.Lock()
	defer p.t.mu.Unlock()
	delete(p.t.puts, p.id)
}

// ages returns, per phase, how long the longest-waiting PUT has been in it
// (zero when none is).
func (t *inflight) ages() map[phase]time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()
	out := make(map[phase]time.Duration, len(phases))
	for _, ph := range phases {
		out[ph] = 0
	}
	for _, p := range t.puts {
		if age := now.Sub(p.since); age > out[p.phase] {
			out[p.phase] = age
		}
	}
	return out
}
