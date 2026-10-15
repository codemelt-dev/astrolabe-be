package watchdog

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"stellar-explorer/worker/config"
	"stellar-explorer/worker/lake/laketest"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// world is everything a watchdog run reads: the lake's phase ages,
// Galexie's two gauges and its unit, and the clock.
type world struct {
	mu        sync.Mutex
	receiving float64
	syncing   float64
	export    *float64
	uploader  *float64
	lakeDown  bool
	gxDown    bool
	unit      Unit
	restarts  []time.Time
	now       time.Time
	network   uint32
}

func f(v float64) *float64 { return &v }

func (w *world) lakeHandler(rw http.ResponseWriter, _ *http.Request) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.lakeDown {
		http.Error(rw, "down", http.StatusServiceUnavailable)
		return
	}
	fmt.Fprintf(rw, "# TYPE lake_put_phase_age_seconds gauge\nlake_put_phase_age_seconds{phase=\"receiving\"} %g\nlake_put_phase_age_seconds{phase=\"syncing\"} %g\nlake_tip 100\n",
		w.receiving, w.syncing)
}

func (w *world) galexieHandler(rw http.ResponseWriter, _ *http.Request) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.gxDown {
		http.Error(rw, "down", http.StatusServiceUnavailable)
		return
	}
	if w.export != nil {
		fmt.Fprintf(rw, "# TYPE galexie_export_manager_latest_ledger gauge\ngalexie_export_manager_latest_ledger %g\n", *w.export)
	}
	if w.uploader != nil {
		fmt.Fprintf(rw, "# TYPE galexie_uploader_latest_ledger gauge\ngalexie_uploader_latest_ledger %g\n", *w.uploader)
	}
	fmt.Fprint(rw, "galexie_process_start_time_seconds 1.7e9\n")
}

func (w *world) Unit(context.Context, string) (Unit, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.unit, nil
}

func (w *world) Restart(_ context.Context, name string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if name != "stellar-galexie.service" {
		return fmt.Errorf("restart of %s", name)
	}
	w.restarts = append(w.restarts, w.now)
	return nil
}

type alertLog struct{ kinds []string }

func (a *alertLog) alert(kind, msg string, args ...any) { a.kinds = append(a.kinds, kind) }

func (a *alertLog) count(kind string) int {
	n := 0
	for _, k := range a.kinds {
		if k == kind {
			n++
		}
	}
	return n
}

type harness struct {
	t      *testing.T
	w      *world
	o      Options
	alerts *alertLog
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	start := time.Unix(1_800_000_000, 0)
	w := &world{now: start, unit: Unit{Active: true, State: "active", Since: start}}
	lake := httptest.NewServer(http.HandlerFunc(w.lakeHandler))
	gx := httptest.NewServer(http.HandlerFunc(w.galexieHandler))
	t.Cleanup(lake.Close)
	t.Cleanup(gx.Close)
	cfg := config.Default().Watchdog
	cfg.StateFile = filepath.Join(t.TempDir(), "state.json")
	al := &alertLog{}
	h := &harness{t: t, w: w, alerts: al}
	h.o = Options{Config: cfg, Unit: "stellar-galexie.service", LakeMetricsURL: lake.URL, GalexieMetricsURL: gx.URL,
		Systemd: w, Now: func() time.Time { w.mu.Lock(); defer w.mu.Unlock(); return w.now }, Log: quiet, Alert: al.alert,
		NetworkTip: func(context.Context) (uint32, error) {
			w.mu.Lock()
			defer w.mu.Unlock()
			if w.network == 0 {
				return 0, errors.New("unknown")
			}
			return w.network, nil
		}}
	return h
}

// run advances the clock by d and runs the watchdog once.
func (h *harness) run(d time.Duration) State {
	h.t.Helper()
	h.w.mu.Lock()
	h.w.now = h.w.now.Add(d)
	h.w.mu.Unlock()
	st, err := Run(context.Background(), h.o)
	if err != nil {
		h.t.Fatal(err)
	}
	return st
}

// upload simulates Galexie making progress.
func (h *harness) upload(seq float64) {
	h.w.mu.Lock()
	h.w.export, h.w.uploader = f(seq), f(seq)
	h.w.mu.Unlock()
}

func (h *harness) restarts() int {
	h.w.mu.Lock()
	defer h.w.mu.Unlock()
	return len(h.w.restarts)
}

func (h *harness) set(fn func(w *world)) {
	h.w.mu.Lock()
	fn(h.w)
	h.w.mu.Unlock()
}

// Healthy: Galexie uploads every ledger; 30 s runs never restart it.
func TestHealthy(t *testing.T) {
	h := newHarness(t)
	for i := range 100 {
		h.upload(float64(1000 + i*6))
		h.set(func(w *world) { w.receiving = 0.2 })
		if st := h.run(30 * time.Second); st.LastDecision != DecisionOK {
			t.Fatalf("run %d: %s", i, st.LastDecision)
		}
	}
	if h.restarts() != 0 || len(h.alerts.kinds) != 0 {
		t.Fatalf("restarts %d alerts %v", h.restarts(), h.alerts.kinds)
	}
}

// Rule (a): a PUT receiving for > 120 s restarts Galexie, at most once per
// 15 min, and pages after 2 restarts in a row.
func TestRuleReceiving(t *testing.T) {
	h := newHarness(t)
	h.upload(1000)
	h.run(30 * time.Second)
	h.set(func(w *world) { w.receiving = 100 })
	if st := h.run(30 * time.Second); st.LastDecision != DecisionOK {
		t.Fatalf("100 s receiving: %s", st.LastDecision)
	}
	h.set(func(w *world) { w.receiving = 130 })
	if st := h.run(30 * time.Second); st.LastDecision != DecisionRestart || h.restarts() != 1 {
		t.Fatalf("130 s receiving: %s, %d restarts", st.LastDecision, h.restarts())
	}
	// Still stuck (the restart did not help): no second restart for 15 min.
	for range 20 {
		if st := h.run(30 * time.Second); st.LastDecision != DecisionRateLimit {
			t.Fatalf("within 15 min: %s", st.LastDecision)
		}
	}
	if h.restarts() != 1 || h.alerts.count("galexie_watchdog_restarts") != 0 {
		t.Fatalf("restarts %d alerts %v", h.restarts(), h.alerts.kinds)
	}
	h.run(6 * time.Minute)
	if h.restarts() != 2 || h.alerts.count("galexie_watchdog_restarts") != 1 {
		t.Fatalf("after 15 min: restarts %d alerts %v", h.restarts(), h.alerts.kinds)
	}
	// Progress resets the count.
	h.set(func(w *world) { w.receiving = 0; w.unit.Since = w.now })
	h.upload(1001)
	h.run(30 * time.Second)
	h.upload(1002)
	st := h.run(30 * time.Second)
	if st.RestartsInARow != 0 || st.RestartsTotal != 2 {
		t.Fatalf("state %+v", st)
	}
}

// Rule (b): export manager ahead of an uploader that has not moved for
// > 180 s.
func TestRuleUploaderStalled(t *testing.T) {
	h := newHarness(t)
	h.upload(1000)
	h.run(30 * time.Second)
	h.set(func(w *world) { w.export = f(1010) })
	for i := range 6 { // 180 s exactly: not yet
		if st := h.run(30 * time.Second); st.LastDecision != DecisionOK {
			t.Fatalf("run %d: %s", i, st.LastDecision)
		}
	}
	if st := h.run(30 * time.Second); st.LastDecision != DecisionRestart {
		t.Fatalf("after 210 s: %s", st.LastDecision)
	}
	// Equal values (a catchup, or an idle network) never trigger it.
	h2 := newHarness(t)
	h2.upload(500)
	for range 40 {
		if st := h2.run(30 * time.Second); st.LastDecision != DecisionOK {
			t.Fatalf("idle: %s", st.LastDecision)
		}
	}
}

// The watchdog never fires during a `syncing` stall (it pages
// instead), even while rules (a) and (b) hold.
func TestNeverRestartsWhileSyncing(t *testing.T) {
	h := newHarness(t)
	h.upload(1000)
	h.run(30 * time.Second)
	h.set(func(w *world) { w.syncing = 5; w.receiving = 500; w.export = f(1100) })
	for i := range 60 { // 30 min
		h.set(func(w *world) { w.syncing += 30 })
		if st := h.run(30 * time.Second); st.LastDecision != DecisionSyncing {
			t.Fatalf("run %d: %s", i, st.LastDecision)
		}
	}
	if h.restarts() != 0 {
		t.Fatalf("%d restarts during a syncing stall", h.restarts())
	}
	if h.alerts.count("lake_io_stall") != 1 {
		t.Fatalf("alerts %v: want one lake_io_stall page", h.alerts.kinds)
	}
	st, _ := ReadState(h.o.Config.StateFile)
	if !slices.Contains(st.Alerts, "lake_io_stall") || st.SyncingAge < 30 {
		t.Fatalf("state %+v", st)
	}
}

// No restart during a real catchup (new-db, catchup, checkpoint
// waits: no PUTs, export manager not ahead); only an alert when there is no
// first upload within N minutes.
func TestCatchupOnlyAlerts(t *testing.T) {
	h := newHarness(t)
	h.set(func(w *world) { w.export, w.uploader = f(0), f(0) })
	for i := range 120 { // 60 min
		if st := h.run(30 * time.Second); st.LastDecision != DecisionOK {
			t.Fatalf("run %d: %s", i, st.LastDecision)
		}
		if mins := float64(i+1) / 2; mins <= 45 && len(h.alerts.kinds) != 0 {
			t.Fatalf("alert after %.1f min: %v", mins, h.alerts.kinds)
		}
	}
	if h.restarts() != 0 || h.alerts.count("galexie_no_first_upload") != 1 {
		t.Fatalf("restarts %d alerts %v", h.restarts(), h.alerts.kinds)
	}
	// Uploads begin, but lag the network tip.
	h.set(func(w *world) { w.network = 2000 })
	h.upload(1500)
	h.run(30 * time.Second)
	if h.alerts.count("galexie_uploader_lag") != 1 {
		t.Fatalf("alerts %v", h.alerts.kinds)
	}
	h.upload(1990)
	if st := h.run(30 * time.Second); slices.Contains(st.Alerts, "galexie_uploader_lag") {
		t.Fatalf("lag alert still active: %v", st.Alerts)
	}
}

// A held (stopped) Galexie, or a lake whose metrics cannot be read, is
// never restarted.
func TestNoActionWhenHeldOrBlind(t *testing.T) {
	h := newHarness(t)
	h.upload(1000)
	h.run(30 * time.Second)
	h.set(func(w *world) { w.unit = Unit{State: "inactive"}; w.receiving = 1000; w.export = f(2000) })
	for range 40 {
		if st := h.run(30 * time.Second); st.LastDecision != DecisionInactive {
			t.Fatalf("held: %s", st.LastDecision)
		}
	}
	h.set(func(w *world) { w.unit = Unit{Active: true, State: "active", Since: w.now}; w.lakeDown = true })
	for range 40 {
		if st := h.run(30 * time.Second); st.LastDecision != DecisionBlind {
			t.Fatalf("blind: %s", st.LastDecision)
		}
	}
	if h.restarts() != 0 {
		t.Fatalf("%d restarts", h.restarts())
	}
}

// The watchdog reads the phases from a real lake's /metrics.
func TestScrapesRealLake(t *testing.T) {
	l := laketest.Start(t, laketest.Options{})
	h := newHarness(t)
	h.o.LakeMetricsURL = "http://" + l.ControlAddr() + "/metrics"
	h.upload(1)
	st := h.run(time.Second)
	if st.LastDecision != DecisionOK || st.ReceivingAge != 0 || st.SyncingAge != 0 {
		t.Fatalf("state %+v", st)
	}
}

func TestSystemctl(t *testing.T) {
	var calls [][]string
	s := Systemctl{Run: func(_ context.Context, name string, args ...string) ([]byte, error) {
		calls = append(calls, append([]string{name}, args...))
		if args[0] == "show" {
			return []byte("ActiveState=active\nActiveEnterTimestamp=@1790000000\nNRestarts=3\n"), nil
		}
		return nil, nil
	}}
	u, err := s.Unit(context.Background(), "stellar-galexie.service")
	if err != nil || !u.Active || u.Since.Unix() != 1_790_000_000 || u.NRestarts != 3 {
		t.Fatalf("%+v %v", u, err)
	}
	if err := s.Restart(context.Background(), "stellar-galexie.service"); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"/usr/bin/systemctl show --timestamp=unix -p ActiveState -p ActiveEnterTimestamp -p NRestarts stellar-galexie.service",
		"/usr/bin/systemctl restart --no-block stellar-galexie.service",
	}
	for i, c := range calls {
		if strings.Join(c, " ") != want[i] {
			t.Fatalf("call %d: %q", i, c)
		}
	}
	inactive := Systemctl{Run: func(context.Context, string, ...string) ([]byte, error) {
		return []byte("ActiveState=inactive\nActiveEnterTimestamp=\nNRestarts=0\n"), nil
	}}
	if u, err := inactive.Unit(context.Background(), "x"); err != nil || u.Active || !u.Since.IsZero() {
		t.Fatalf("%+v %v", u, err)
	}
}
