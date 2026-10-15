// Package watchdog implements `worker watchdog`, run by
// stellar-watchdog.timer every 30 s. It restarts stellar-galexie only when
// Galexie's upload is hung:
//
//	(a) the lake reports a ledger PUT in phase "receiving" for > 120 s (the
//	    sender is stuck); or
//	(b) galexie_export_manager_latest_ledger > galexie_uploader_latest_ledger
//	    and the uploader value has not changed for > 180 s.
//
// It never restarts Galexie
//
//   - while a PUT is in phase "syncing": the disk is stuck, and a restart
//     cannot unblock an fsync; > 30 s pages "lake Volume I/O stall";
//   - during captive-core new-db, catchup and checkpoint waits, which
//     produce no PUTs: it only alerts when there is no first upload within
//     N min of start, or the uploader lags the network tip by > 120
//     ledgers;
//   - when Galexie is not running (a held `systemctl stop` survives);
//   - when it cannot read the lake's phases (it never acts blind).
//
// At most one watchdog restart per 15 min; it pages after 2 in a row. The
// state that spans runs (the uploader's last value and since when, the
// restarts) lives in a small JSON file that ingest exports as metrics.
package watchdog

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"slices"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"

	"stellar-explorer/worker/config"
	"stellar-explorer/worker/statefile"
)

// Metric names the watchdog reads.
const (
	MetricPhaseAge = "lake_put_phase_age_seconds"
	MetricExport   = "galexie_export_manager_latest_ledger"
	MetricUploader = "galexie_uploader_latest_ledger"
)

// Unit is Galexie's systemd unit as the watchdog sees it.
type Unit struct {
	// Active is ActiveState == "active".
	Active bool
	State  string
	// Since is when the unit last entered the active state.
	Since time.Time
	// NRestarts counts systemd's automatic restarts of the unit.
	NRestarts uint64
}

// Systemd queries and restarts units.
type Systemd interface {
	Unit(ctx context.Context, name string) (Unit, error)
	// Restart restarts a unit without waiting for the job.
	Restart(ctx context.Context, name string) error
}

// Options configure one watchdog run.
type Options struct {
	Config config.Watchdog
	// Unit is Galexie's unit (stellar-galexie.service).
	Unit string
	// LakeMetricsURL is the lake's /metrics on its control listener;
	// GalexieMetricsURL Galexie's admin endpoint.
	LakeMetricsURL    string
	GalexieMetricsURL string
	Systemd           Systemd
	// NetworkTip returns the network's newest ledger (nil: not checked).
	NetworkTip func(ctx context.Context) (uint32, error)
	HTTP       *http.Client
	Now        func() time.Time
	Log        *slog.Logger
	Alert      func(kind, msg string, args ...any)
}

// State persists between runs; ingest exports it as metrics.
type State struct {
	// UploaderLatest is the last galexie_uploader_latest_ledger seen and
	// UploaderSince when it last changed.
	UploaderLatest *uint32   `json:"uploader_latest_ledger"`
	UploaderSince  time.Time `json:"uploader_since"`
	// ExportLatest is the last galexie_export_manager_latest_ledger seen.
	ExportLatest *uint32 `json:"export_manager_latest_ledger"`
	// LastRestart is the watchdog's last restart; RestartsInARow counts
	// restarts without upload progress in between.
	LastRestart    time.Time `json:"last_restart"`
	RestartsInARow int       `json:"restarts_in_a_row"`
	RestartsTotal  uint64    `json:"restarts_total"`
	// Galexie's unit, as of the last run.
	GalexieActive    bool      `json:"galexie_active"`
	GalexieSince     time.Time `json:"galexie_active_since"`
	GalexieNRestarts uint64    `json:"galexie_nrestarts"`
	// The lake's phase ages, as of the last run (-1: unknown).
	ReceivingAge float64 `json:"receiving_age_seconds"`
	SyncingAge   float64 `json:"syncing_age_seconds"`
	// NetworkTip is the network's newest ledger, if read.
	NetworkTip *uint32 `json:"network_tip"`
	// LastRun and LastDecision describe the last run; Alerts are the
	// conditions active then.
	LastRun      time.Time `json:"last_run"`
	LastDecision string    `json:"last_decision"`
	Alerts       []string  `json:"alerts"`
}

// Decisions.
const (
	DecisionOK         = "ok"
	DecisionInactive   = "galexie_not_running"
	DecisionBlind      = "lake_metrics_unavailable"
	DecisionSyncing    = "syncing_no_restart"
	DecisionRestart    = "restart"
	DecisionRateLimit  = "restart_suppressed_rate_limit"
	DecisionRestartErr = "restart_failed"
)

// ReadState reads the state file; a missing file is a fresh state.
func ReadState(path string) (State, error) {
	var s State
	err := statefile.ReadJSON(path, &s)
	if errors.Is(err, fs.ErrNotExist) {
		return State{}, nil
	}
	return s, err
}

// Run performs one watchdog run and returns the new state (also written
// to the state file).
func Run(ctx context.Context, o Options) (State, error) {
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.Log == nil {
		o.Log = slog.Default()
	}
	if o.Alert == nil {
		log := o.Log
		o.Alert = func(kind, msg string, args ...any) { log.Error(msg, append([]any{"alert", kind}, args...)...) }
	}
	if o.HTTP == nil {
		o.HTTP = &http.Client{Timeout: 5 * time.Second}
	}
	cfg := o.Config
	now := o.Now()
	st, err := ReadState(cfg.StateFile)
	if err != nil {
		o.Log.Warn("watchdog state unreadable; starting fresh", "file", cfg.StateFile, "err", err)
		st = State{}
	}
	prevAlerts := st.Alerts
	st.LastRun, st.Alerts = now, nil
	alert := func(kind, msg string, args ...any) {
		st.Alerts = append(st.Alerts, kind)
		if slices.Contains(prevAlerts, kind) {
			o.Log.Warn(msg, append([]any{"alert", kind, "still"}, args...)...)
			return
		}
		o.Alert(kind, msg, args...)
	}

	st.LastDecision = decide(ctx, o, &st, now, alert)
	o.Log.Info("watchdog", "decision", st.LastDecision, "receiving_age", st.ReceivingAge, "syncing_age", st.SyncingAge,
		"export", fmtPtr(st.ExportLatest), "uploader", fmtPtr(st.UploaderLatest), "alerts", st.Alerts)
	if err := statefile.WriteJSON(cfg.StateFile, st); err != nil {
		return st, fmt.Errorf("write %s: %w", cfg.StateFile, err)
	}
	return st, nil
}

func decide(ctx context.Context, o Options, st *State, now time.Time, alert func(kind, msg string, args ...any)) string {
	cfg := o.Config
	unit, err := o.Systemd.Unit(ctx, o.Unit)
	if err != nil {
		o.Log.Warn("cannot query Galexie's unit; doing nothing", "unit", o.Unit, "err", err)
		return DecisionInactive
	}
	st.GalexieActive, st.GalexieNRestarts = unit.Active, unit.NRestarts
	if !unit.Active {
		// Held, crashed or starting: systemd owns it. Forget the uploader's
		// progress so a fresh start gets the full grace.
		st.GalexieSince, st.UploaderLatest, st.ExportLatest, st.UploaderSince = time.Time{}, nil, nil, time.Time{}
		return DecisionInactive
	}
	if !unit.Since.Equal(st.GalexieSince) {
		// A new Galexie process: its metrics start over.
		st.GalexieSince, st.UploaderLatest, st.ExportLatest, st.UploaderSince = unit.Since, nil, nil, now
	}

	// Lake phases: without them the watchdog never restarts.
	st.ReceivingAge, st.SyncingAge = -1, -1
	lake, lakeErr := scrape(ctx, o.HTTP, o.LakeMetricsURL)
	if lakeErr == nil {
		st.ReceivingAge, _ = labeled(lake, MetricPhaseAge, "phase", "receiving")
		st.SyncingAge, _ = labeled(lake, MetricPhaseAge, "phase", "syncing")
	}

	// Galexie's progress.
	gx, gxErr := scrape(ctx, o.HTTP, o.GalexieMetricsURL)
	var export, uploader *uint32
	if gxErr == nil {
		export, uploader = gauge(gx, MetricExport), gauge(gx, MetricUploader)
	} else {
		o.Log.Warn("Galexie's metrics are unavailable", "url", o.GalexieMetricsURL, "err", gxErr)
	}
	if gxErr == nil {
		if !eqPtr(uploader, st.UploaderLatest) {
			if st.UploaderLatest != nil && uploader != nil && *uploader > *st.UploaderLatest {
				st.RestartsInARow = 0 // progress since the last restart
			}
			st.UploaderLatest, st.UploaderSince = uploader, now
		}
		st.ExportLatest = export
	}

	// Alert-only conditions (never a restart).
	uploaded := st.UploaderLatest != nil && *st.UploaderLatest > 0
	if !uploaded && now.Sub(unit.Since) > cfg.FirstUploadWithin {
		alert("galexie_no_first_upload", "Galexie has not uploaded a ledger since it started (captive-core catchup?)",
			"running_for", now.Sub(unit.Since).Round(time.Second), "limit", cfg.FirstUploadWithin)
	}
	if o.NetworkTip != nil {
		nctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		tip, err := o.NetworkTip(nctx)
		cancel()
		if err == nil {
			st.NetworkTip = &tip
			if uploaded && tip > *st.UploaderLatest && tip-*st.UploaderLatest > cfg.MaxUploaderLag {
				alert("galexie_uploader_lag", "Galexie's uploader lags the network tip",
					"uploader", *st.UploaderLatest, "network_tip", tip, "limit", cfg.MaxUploaderLag)
			}
		} else {
			o.Log.Warn("network tip unknown", "err", err)
		}
	}

	if lakeErr != nil {
		o.Log.Warn("the lake's metrics are unavailable; never restarting blind", "url", o.LakeMetricsURL, "err", lakeErr)
		return DecisionBlind
	}
	syncing := time.Duration(st.SyncingAge * float64(time.Second))
	if syncing > cfg.SyncingPage {
		alert("lake_io_stall", "a lake PUT is stuck in fsync (Volume I/O stall): fix the Volume; Galexie is not restarted",
			"syncing_for", syncing.Round(time.Second))
	}

	var reason string
	receiving := time.Duration(st.ReceivingAge * float64(time.Second))
	switch {
	case receiving > cfg.ReceivingRestart:
		reason = fmt.Sprintf("a ledger PUT has been receiving for %s (rule a)", receiving.Round(time.Second))
	case export != nil && uploader != nil && *export > *uploader && now.Sub(st.UploaderSince) > cfg.UploaderStall:
		reason = fmt.Sprintf("the uploader is stuck at %d for %s while the export manager is at %d (rule b)",
			*uploader, now.Sub(st.UploaderSince).Round(time.Second), *export)
	default:
		return DecisionOK
	}
	if st.SyncingAge > 0 {
		o.Log.Warn("upload looks hung, but a PUT is syncing: not restarting", "reason", reason)
		return DecisionSyncing
	}
	if !st.LastRestart.IsZero() && now.Sub(st.LastRestart) < cfg.MinRestartInterval {
		o.Log.Warn("upload looks hung; restart suppressed (at most one per interval)", "reason", reason,
			"last_restart", st.LastRestart, "interval", cfg.MinRestartInterval)
		alert("galexie_upload_hung", "Galexie's upload is hung and the watchdog may not restart it yet", "reason", reason)
		return DecisionRateLimit
	}
	o.Log.Warn("restarting Galexie: its upload is hung", "unit", o.Unit, "reason", reason)
	if err := o.Systemd.Restart(ctx, o.Unit); err != nil {
		alert("galexie_watchdog_restart_failed", "the watchdog could not restart Galexie", "err", err)
		return DecisionRestartErr
	}
	st.LastRestart = now
	st.RestartsInARow++
	st.RestartsTotal++
	st.UploaderSince = now
	if st.RestartsInARow >= cfg.PageAfterRestarts {
		alert("galexie_watchdog_restarts", "the watchdog restarted Galexie repeatedly without upload progress",
			"in_a_row", st.RestartsInARow, "reason", reason)
	}
	return DecisionRestart
}

// scrape fetches and parses a Prometheus text endpoint.
func scrape(ctx context.Context, hc *http.Client, url string) (map[string]*dto.MetricFamily, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil, fmt.Errorf("%s: %s", url, resp.Status)
	}
	var p expfmt.TextParser
	return p.TextToMetricFamilies(io.LimitReader(resp.Body, 16<<20))
}

func value(m *dto.Metric) float64 {
	switch {
	case m.GetGauge() != nil:
		return m.GetGauge().GetValue()
	case m.GetCounter() != nil:
		return m.GetCounter().GetValue()
	case m.GetUntyped() != nil:
		return m.GetUntyped().GetValue()
	}
	return 0
}

// labeled returns the value of family name with label=val.
func labeled(fams map[string]*dto.MetricFamily, name, label, val string) (float64, bool) {
	f := fams[name]
	if f == nil {
		return 0, false
	}
	for _, m := range f.GetMetric() {
		for _, l := range m.GetLabel() {
			if l.GetName() == label && l.GetValue() == val {
				return value(m), true
			}
		}
	}
	return 0, false
}

// gauge returns an unlabeled gauge as a ledger sequence, or nil.
func gauge(fams map[string]*dto.MetricFamily, name string) *uint32 {
	f := fams[name]
	if f == nil || len(f.GetMetric()) == 0 {
		return nil
	}
	v := value(f.GetMetric()[0])
	if v < 0 || v > float64(^uint32(0)) {
		return nil
	}
	u := uint32(v)
	return &u
}

func eqPtr(a, b *uint32) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func fmtPtr(p *uint32) any {
	if p == nil {
		return nil
	}
	return *p
}
