package config

import (
	"errors"
	"fmt"
	"net/netip"
	"net/url"
	"slices"
	"strings"
	"time"
)

// The sections of the M5–M8 commands: bootstrap, the pruner, ingest's
// metrics and health endpoints, the Galexie launcher and watchdog, and the
// weekly gap check.

// Bootstrap configures `worker bootstrap`.
type Bootstrap struct {
	// HistoryArchives are the pubnet history archives the checkpoint state
	// is loaded from, tried in order.
	HistoryArchives []string `toml:"history_archives"`
	// CheckpointPoll is how often `bootstrap --force` re-reads the
	// archives while it waits for a checkpoint at or above the cursor.
	CheckpointPoll time.Duration `toml:"checkpoint_poll"`
}

// PubnetHistoryArchives are SDF's pubnet history archives.
var PubnetHistoryArchives = []string{
	"https://history.stellar.org/prd/core-live/core_live_001",
	"https://history.stellar.org/prd/core-live/core_live_002",
	"https://history.stellar.org/prd/core-live/core_live_003",
}

func (b Bootstrap) validate() error {
	var errs []error
	if len(b.HistoryArchives) == 0 {
		errs = append(errs, errors.New("bootstrap.history_archives is empty"))
	}
	for _, a := range b.HistoryArchives {
		u, err := url.Parse(a)
		if err != nil || (u.Scheme != "https" && u.Scheme != "http" && u.Scheme != "file") {
			errs = append(errs, fmt.Errorf("bootstrap.history_archives: %q is not an http(s):// or file:// URL", a))
		}
	}
	if b.CheckpointPoll < time.Second {
		errs = append(errs, fmt.Errorf("bootstrap.checkpoint_poll %s: want at least 1s", b.CheckpointPoll))
	}
	return errors.Join(errs...)
}

// Prune configures the pruner of `worker ingest`.
type Prune struct {
	// Enabled runs the pruner (production: always).
	Enabled bool `toml:"enabled"`
	// RetentionLedgers is W: ledgers newer than lake.tip − W are kept
	// (17,280–51,840, default 51,840 = 72 h).
	RetentionLedgers uint32 `toml:"retention_ledgers"`
	// DiskHighWatermark is the used fraction of the lake filesystem above
	// which the pruner stops waiting for the local readers' cursors.
	DiskHighWatermark float64 `toml:"disk_high_watermark"`
	// Interval is how often the pruner runs.
	Interval time.Duration `toml:"interval"`
	// LocalReaders are the pipelines that read from the lake; their cursors
	// hold back pruning while the disk is below the watermark.
	LocalReaders []string `toml:"local_readers"`
}

// Retention bounds: 24–72 h at 5 s per ledger.
const (
	MinRetentionLedgers = 17_280
	MaxRetentionLedgers = 51_840
)

func (p Prune) validate() error {
	var errs []error
	if p.RetentionLedgers < MinRetentionLedgers || p.RetentionLedgers > MaxRetentionLedgers {
		errs = append(errs, fmt.Errorf("prune.retention_ledgers %d: want %d-%d", p.RetentionLedgers, MinRetentionLedgers, MaxRetentionLedgers))
	}
	if p.DiskHighWatermark < 0.5 || p.DiskHighWatermark > 0.95 {
		errs = append(errs, fmt.Errorf("prune.disk_high_watermark %v: want 0.5-0.95", p.DiskHighWatermark))
	}
	if p.Interval < time.Second {
		errs = append(errs, fmt.Errorf("prune.interval %s: want at least 1s", p.Interval))
	}
	for _, r := range p.LocalReaders {
		if r == "" || strings.HasPrefix(r, "reindex:") {
			errs = append(errs, fmt.Errorf("prune.local_readers: %q is not a pipeline", r))
		}
	}
	return errors.Join(errs...)
}

// Metrics configures ingest's /metrics, /healthz and /readyz.
type Metrics struct {
	// Listen is the loopback listener (127.0.0.1:9102).
	Listen string `toml:"listen"`
	// PrivateListen is "<private IP>:9102", bound best-effort in the
	// background (the private interface can come up late). Empty disables
	// it.
	PrivateListen string `toml:"private_listen"`
	// CoreDir is captive core's storage (/srv/core), whose disk usage is
	// exported.
	CoreDir string `toml:"core_dir"`
	// ReadyMaxLag: /readyz fails while the explorer cursor is more than
	// this many ledgers behind the lake tip.
	ReadyMaxLag uint32 `toml:"ready_max_lag"`
	// NetworkTip polls the AWS public lake's newest ledger as the network
	// tip (lag alerts tell a network halt from a pipeline stall).
	NetworkTip bool `toml:"network_tip"`
}

func (m Metrics) validate() error {
	var errs []error
	errs = append(errs, loopbackAddr("metrics.listen", m.Listen))
	if m.PrivateListen != "" {
		if _, err := netip.ParseAddrPort(m.PrivateListen); err != nil {
			errs = append(errs, fmt.Errorf("metrics.private_listen %q: want <ip>:<port>: %w", m.PrivateListen, err))
		}
	}
	errs = append(errs, absPath("metrics.core_dir", m.CoreDir))
	if m.ReadyMaxLag == 0 {
		errs = append(errs, errors.New("metrics.ready_max_lag must be > 0"))
	}
	return errors.Join(errs...)
}

// Galexie describes the Galexie service for the launcher, the watchdog and
// the gap check.
type Galexie struct {
	// Unit is Galexie's systemd unit.
	Unit string `toml:"unit"`
	// MetricsURL is Galexie's admin endpoint (admin_port 6061).
	MetricsURL string `toml:"metrics_url"`
	// Image is the pinned Galexie image, "stellar/stellar-galexie:<tag>@sha256:<digest>",
	// used by the gap check's detect-gaps container.
	Image string `toml:"image"`
	// Docker is the docker CLI.
	Docker string `toml:"docker"`
}

func (g Galexie) validate() error {
	var errs []error
	if g.Unit == "" {
		errs = append(errs, errors.New("galexie.unit is required"))
	}
	if u, err := url.Parse(g.MetricsURL); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		errs = append(errs, fmt.Errorf("galexie.metrics_url %q is not an http(s) URL", g.MetricsURL))
	}
	errs = append(errs, absPath("galexie.docker", g.Docker))
	return errors.Join(errs...)
}

// Watchdog configures `worker watchdog`.
type Watchdog struct {
	// StateFile keeps what the watchdog needs between runs (it runs every
	// 30 s): the uploader's last value and since when, and its restarts.
	// ingest exports it as metrics, so it must be world-readable.
	StateFile string `toml:"state_file"`
	// ReceivingRestart: rule (a), a ledger PUT in phase "receiving" for
	// longer restarts Galexie.
	ReceivingRestart time.Duration `toml:"receiving_restart"`
	// UploaderStall: rule (b), export manager ahead of the uploader whose
	// value has not changed for longer restarts Galexie.
	UploaderStall time.Duration `toml:"uploader_stall"`
	// SyncingPage: a PUT in phase "syncing" for longer pages (Volume I/O
	// stall); it never restarts Galexie.
	SyncingPage time.Duration `toml:"syncing_page"`
	// MinRestartInterval: at most one watchdog restart per interval.
	MinRestartInterval time.Duration `toml:"min_restart_interval"`
	// PageAfterRestarts: page after this many restarts in a row.
	PageAfterRestarts int `toml:"page_after_restarts"`
	// FirstUploadWithin: alert when Galexie has run this long without a
	// first upload (captive-core catchup; N from the M4 measurement).
	FirstUploadWithin time.Duration `toml:"first_upload_within"`
	// MaxUploaderLag: alert when the uploader is more ledgers than this
	// behind the network tip.
	MaxUploaderLag uint32 `toml:"max_uploader_lag"`
	// NetworkTip reads the network tip from the AWS public lake.
	NetworkTip bool `toml:"network_tip"`
}

func (w Watchdog) validate() error {
	var errs []error
	errs = append(errs, absPath("watchdog.state_file", w.StateFile))
	for name, d := range map[string]time.Duration{
		"receiving_restart": w.ReceivingRestart, "uploader_stall": w.UploaderStall, "syncing_page": w.SyncingPage,
		"min_restart_interval": w.MinRestartInterval, "first_upload_within": w.FirstUploadWithin,
	} {
		if d < time.Second {
			errs = append(errs, fmt.Errorf("watchdog.%s %s: want at least 1s", name, d))
		}
	}
	if w.PageAfterRestarts < 1 {
		errs = append(errs, errors.New("watchdog.page_after_restarts must be ≥ 1"))
	}
	if w.MaxUploaderLag == 0 {
		errs = append(errs, errors.New("watchdog.max_uploader_lag must be > 0"))
	}
	return errors.Join(errs...)
}

// Gapcheck configures `worker gapcheck`.
type Gapcheck struct {
	// Method is "detect-gaps" (Galexie's container against the archive) or
	// "reindex-dry-run" (`worker reindex --dry-run --source archive`, for
	// providers that reject path-style addressing).
	Method string `toml:"method"`
	// GalexieConfig is galexie.toml pointed at the archive, admin_port = 0.
	GalexieConfig string `toml:"galexie_config"`
	// EnvFile holds the uploader key under AWS_* names (root-only).
	EnvFile string `toml:"env_file"`
	// ResultFile records the last run; ingest exports it as metrics.
	ResultFile string `toml:"result_file"`
}

// Gap check methods.
const (
	GapcheckDetectGaps = "detect-gaps"
	GapcheckDryRun     = "reindex-dry-run"
)

func (g Gapcheck) validate() error {
	var errs []error
	if g.Method != GapcheckDetectGaps && g.Method != GapcheckDryRun {
		errs = append(errs, fmt.Errorf("gapcheck.method %q: want %s or %s", g.Method, GapcheckDetectGaps, GapcheckDryRun))
	}
	errs = append(errs,
		absPath("gapcheck.galexie_config", g.GalexieConfig),
		absPath("gapcheck.env_file", g.EnvFile),
		absPath("gapcheck.result_file", g.ResultFile),
	)
	return errors.Join(errs...)
}

func defaultOperations(c *Config) {
	c.Bootstrap = Bootstrap{
		HistoryArchives: slices.Clone(PubnetHistoryArchives),
		CheckpointPoll:  30 * time.Second,
	}
	c.Prune = Prune{
		Enabled:           true,
		RetentionLedgers:  MaxRetentionLedgers,
		DiskHighWatermark: 0.80,
		Interval:          time.Minute,
		LocalReaders:      []string{"explorer"},
	}
	c.Metrics = Metrics{
		Listen:      "127.0.0.1:9102",
		CoreDir:     "/srv/core",
		ReadyMaxLag: 120,
		NetworkTip:  true,
	}
	c.Galexie = Galexie{
		Unit:       "stellar-galexie.service",
		MetricsURL: "http://127.0.0.1:6061/metrics",
		Docker:     "/usr/bin/docker",
	}
	c.Watchdog = Watchdog{
		StateFile:          "/var/lib/stellar-watchdog/state.json",
		ReceivingRestart:   120 * time.Second,
		UploaderStall:      180 * time.Second,
		SyncingPage:        30 * time.Second,
		MinRestartInterval: 15 * time.Minute,
		PageAfterRestarts:  2,
		FirstUploadWithin:  45 * time.Minute,
		MaxUploaderLag:     120,
		NetworkTip:         true,
	}
	c.Gapcheck = Gapcheck{
		Method:        GapcheckDetectGaps,
		GalexieConfig: "/etc/stellar/galexie-archive.toml",
		EnvFile:       "/etc/stellar/archive-aws.env",
		ResultFile:    "/var/lib/stellar-gapcheck/result.json",
	}
}

func (c Config) validateOperations() []error {
	return []error{
		c.Bootstrap.validate(),
		c.Prune.validate(),
		c.Metrics.validate(),
		c.Galexie.validate(),
		c.Watchdog.validate(),
		c.Gapcheck.validate(),
	}
}
