// Package metrics holds ingest's metrics and its /metrics, /healthz and
// /readyz endpoints, served on 127.0.0.1:9102 and,
// best-effort, <private IP>:9102. The lake exports its own (lake/metrics.go)
// on :9001 and :9101; Galexie on its admin port 6061.
//
// ingest exports, besides its pipelines' progress, the archiver, the pruner,
// the network tip (the AWS public lake), the disks of /srv/core and
// /srv/lake, and the last results of the two oneshot commands that run as
// root from timers: the watchdog's state file and the gap check's result
// file. Alert rules (deploy/alerts.yml) are written against these names.
package metrics

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"golang.org/x/sys/unix"

	"stellar-explorer/worker/archive"
	"stellar-explorer/worker/prune"
	"stellar-explorer/worker/sep54"
	"stellar-explorer/worker/sequencer"
	"stellar-explorer/worker/watchdog"
)

// Ingest is the metrics registry of one `worker ingest` process.
type Ingest struct {
	Reg *prometheus.Registry
	log *slog.Logger

	cursor         *prometheus.GaugeVec
	commits        *prometheus.CounterVec
	committed      *prometheus.CounterVec
	read           *prometheus.CounterVec
	readSeconds    *prometheus.CounterVec
	processSeconds *prometheus.CounterVec
	commitDuration *prometheus.HistogramVec
	latency        *prometheus.HistogramVec
	alerts         *prometheus.CounterVec

	mu       sync.Mutex
	sources  Sources
	network  *uint32
	netAt    time.Time
	cursorOf map[string]func() (uint32, bool)
}

// Sources are the components whose state is read at scrape time. Any may
// be nil (not running).
type Sources struct {
	// LakeTip reads the lake's tip (lag is computed against it).
	LakeTip interface {
		Get(ctx context.Context) (sep54.Tip, error)
	}
	Archiver *archive.Archiver
	Pruner   *prune.Pruner
	// Sink reports the explorer sink's counters.
	Sink SinkStats
	// CoreDir and LakeDir are statfs'ed for disk usage.
	CoreDir, LakeDir string
	// WatchdogState and GapcheckResult are the oneshot commands' files.
	WatchdogState, GapcheckResult string
}

// SinkStats is the part of explorer.Sink the metrics read.
type SinkStats interface {
	Conflicts() uint64
	StateDrift() uint64
	LastCursor() (uint32, bool)
}

// New returns a registry with ingest's collectors.
func New(log *slog.Logger) *Ingest {
	if log == nil {
		log = slog.Default()
	}
	m := &Ingest{
		Reg:      prometheus.NewRegistry(),
		log:      log,
		cursorOf: map[string]func() (uint32, bool){},
		cursor: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "ingest_cursor", Help: "Last committed ledger of each pipeline.",
		}, []string{"pipeline"}),
		commits: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ingest_commits_total", Help: "Committed transactions per pipeline.",
		}, []string{"pipeline"}),
		committed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ingest_ledgers_committed_total", Help: "Ledgers committed per pipeline.",
		}, []string{"pipeline"}),
		read: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ingest_ledgers_read_total", Help: "Committed ledgers by the source they were read from (lake, archive, aws).",
		}, []string{"pipeline", "source"}),
		readSeconds: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ingest_read_seconds_total", Help: "Time spent reading and decoding committed ledgers (GET + decode).",
		}, []string{"pipeline"}),
		processSeconds: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ingest_process_seconds_total", Help: "Time spent in processLedger for committed ledgers.",
		}, []string{"pipeline"}),
		commitDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "ingest_commit_duration_seconds", Help: "Duration of each Postgres commit.",
			Buckets: prometheus.ExponentialBuckets(0.002, 2, 14),
		}, []string{"pipeline"}),
		latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "ingest_ledger_latency_seconds", Help: "Lake commit to Postgres commit, for the tip ingest was woken for (catch-up excluded).",
			Buckets: prometheus.ExponentialBuckets(0.005, 2, 14),
		}, []string{"pipeline"}),
		alerts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "stellar_alerts_total", Help: "Alerts raised by ingest, by kind (see the logs for details).",
		}, []string{"kind"}),
	}
	m.Reg.MustRegister(m.cursor, m.commits, m.committed, m.read, m.readSeconds, m.processSeconds, m.commitDuration,
		m.latency, m.alerts, &stateCollector{m: m},
		collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return m
}

// SetSources sets what is read at scrape time.
func (m *Ingest) SetSources(s Sources) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sources = s
}

// SetCursor registers a pipeline's cursor reader (lag is tip − cursor).
func (m *Ingest) SetCursor(pipeline string, f func() (uint32, bool)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cursorOf[pipeline] = f
}

// Commit records a sequencer commit.
func (m *Ingest) Commit(pipeline string, c sequencer.CommitInfo) {
	m.cursor.WithLabelValues(pipeline).Set(float64(c.Last))
	m.commits.WithLabelValues(pipeline).Inc()
	m.committed.WithLabelValues(pipeline).Add(float64(c.Ledgers))
	for src, n := range c.Sources {
		m.read.WithLabelValues(pipeline, src).Add(float64(n))
	}
	m.readSeconds.WithLabelValues(pipeline).Add(c.Read.Seconds())
	m.processSeconds.WithLabelValues(pipeline).Add(c.Process.Seconds())
	m.commitDuration.WithLabelValues(pipeline).Observe(c.Duration.Seconds())
	if c.Latency > 0 {
		m.latency.WithLabelValues(pipeline).Observe(c.Latency.Seconds())
	}
}

// Alert counts an alert (the caller logs it).
func (m *Ingest) Alert(kind string) { m.alerts.WithLabelValues(kind).Inc() }

// SetNetworkTip records the network's newest ledger.
func (m *Ingest) SetNetworkTip(tip uint32, at time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.network, m.netAt = &tip, at
}

// PollNetworkTip reads the network tip every interval until ctx ends.
func (m *Ingest) PollNetworkTip(ctx context.Context, newest func(ctx context.Context) (uint32, error), interval time.Duration) {
	for {
		tctx, cancel := context.WithTimeout(ctx, 20*time.Second)
		tip, err := newest(tctx)
		cancel()
		if err == nil {
			m.SetNetworkTip(tip, time.Now())
		} else if ctx.Err() == nil {
			m.log.Debug("network tip unavailable", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}

func (m *Ingest) snapshot() (Sources, map[string]func() (uint32, bool), *uint32, time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cursors := make(map[string]func() (uint32, bool), len(m.cursorOf))
	for k, v := range m.cursorOf {
		cursors[k] = v
	}
	return m.sources, cursors, m.network, m.netAt
}

// Scrape-time metrics.
var (
	descLakeTip    = prometheus.NewDesc("ingest_lake_tip", "The tip of the store the pipelines follow, as they see it: the lake (on the clickhouse host, the archive). Absent while it is empty or unreachable.", nil, nil)
	descLakeUp     = prometheus.NewDesc("ingest_lake_up", "1 if the lake's control endpoint answered the scrape.", nil, nil)
	descLag        = prometheus.NewDesc("ingest_lag_ledgers", "ingest_lake_tip minus the pipeline's cursor.", []string{"pipeline"}, nil)
	descNetwork    = prometheus.NewDesc("stellar_network_tip", "Newest ledger of the network (the AWS public lake).", nil, nil)
	descNetworkAt  = prometheus.NewDesc("stellar_network_tip_updated_seconds", "When the network tip was last read.", nil, nil)
	descConflicts  = prometheus.NewDesc("explorer_conflict_rows_total", "Rows skipped by ON CONFLICT DO NOTHING; should always be 0.", nil, nil)
	descDrift      = prometheus.NewDesc("explorer_state_drift_total", "Evictions that found no state row; should always be 0.", nil, nil)
	descDisk       = prometheus.NewDesc("stellar_disk_bytes", "Filesystem size, used and available bytes (statfs).", []string{"mount", "kind"}, nil)
	descArchTip    = prometheus.NewDesc("archive_tip", "Newest archived ledger (archive_origin − 1 while nothing is archived).", nil, nil)
	descArchKnown  = prometheus.NewDesc("archive_tip_known", "1 once the archiver knows the archive tip; the pruner deletes nothing before.", nil, nil)
	descArchived   = prometheus.NewDesc("archiver_ledgers_total", "Ledgers archived, by result (created, exists, differs).", []string{"result"}, nil)
	descArchBytes  = prometheus.NewDesc("archiver_bytes_total", "Bytes uploaded to the archive.", nil, nil)
	descArchLast   = prometheus.NewDesc("archiver_last_archived_seconds", "When the archive tip last advanced.", nil, nil)
	descArchBlock  = prometheus.NewDesc("archiver_blocked", "1 while the archiver cannot advance, by reason.", []string{"reason"}, nil)
	descPruned     = prometheus.NewDesc("prune_deleted_total", "Ledgers deleted from the lake.", nil, nil)
	descPruneBound = prometheus.NewDesc("prune_bound", "Highest ledger the last pruning pass could delete (0: none).", nil, nil)
	descPruneWM    = prometheus.NewDesc("prune_disk_watermark", "1 while the disk watermark lifts the readers' cursor gate.", nil, nil)
	descPruneErr   = prometheus.NewDesc("prune_errors_total", "Pruning passes that failed.", nil, nil)
	descPruneLast  = prometheus.NewDesc("prune_last_pass_seconds", "When the last pruning pass ended.", nil, nil)
	descWDRun      = prometheus.NewDesc("watchdog_last_run_seconds", "When the watchdog last ran.", nil, nil)
	descWDRestarts = prometheus.NewDesc("watchdog_restarts_total", "Galexie restarts by the watchdog.", nil, nil)
	descWDInARow   = prometheus.NewDesc("watchdog_restarts_in_a_row", "Watchdog restarts without upload progress in between.", nil, nil)
	descWDAlert    = prometheus.NewDesc("watchdog_alert", "1 for each condition the watchdog reported in its last run.", []string{"kind"}, nil)
	descGxActive   = prometheus.NewDesc("galexie_active", "1 if stellar-galexie was active at the watchdog's last run.", nil, nil)
	descGxRestarts = prometheus.NewDesc("galexie_systemd_restarts", "systemd's automatic restarts of stellar-galexie (NRestarts).", nil, nil)
	descGxUploader = prometheus.NewDesc("galexie_uploader_latest_ledger_seen", "Galexie's uploader ledger at the watchdog's last run.", nil, nil)
	descGapRun     = prometheus.NewDesc("gapcheck_last_run_seconds", "When the last gap check finished.", nil, nil)
	descGapMissing = prometheus.NewDesc("gapcheck_missing_ledgers", "Ledgers missing from the archive at the last gap check.", nil, nil)
	descGapInvalid = prometheus.NewDesc("gapcheck_invalid_objects", "Invalid archive objects at the last gap check (dry run only).", nil, nil)
	descGapFailed  = prometheus.NewDesc("gapcheck_failed", "1 if the last gap check could not run.", nil, nil)
	descGapTo      = prometheus.NewDesc("gapcheck_checked_to", "Upper bound (archive tip) of the last gap check.", nil, nil)
)

type stateCollector struct{ m *Ingest }

func (c *stateCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{descLakeTip, descLakeUp, descLag, descNetwork, descNetworkAt, descConflicts, descDrift,
		descDisk, descArchTip, descArchKnown, descArchived, descArchBytes, descArchLast, descArchBlock,
		descPruned, descPruneBound, descPruneWM, descPruneErr, descPruneLast, descWDRun, descWDRestarts, descWDInARow,
		descWDAlert, descGxActive, descGxRestarts, descGxUploader, descGapRun, descGapMissing, descGapInvalid, descGapFailed, descGapTo} {
		ch <- d
	}
}

func gaugeOf(d *prometheus.Desc, v float64, labels ...string) prometheus.Metric {
	return prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, labels...)
}

func counterOf(d *prometheus.Desc, v float64, labels ...string) prometheus.Metric {
	return prometheus.MustNewConstMetric(d, prometheus.CounterValue, v, labels...)
}

func unixSeconds(t time.Time) float64 { return float64(t.UnixNano()) / 1e9 }

func (c *stateCollector) Collect(ch chan<- prometheus.Metric) {
	s, cursors, network, netAt := c.m.snapshot()

	if s.LakeTip != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		tip, err := s.LakeTip.Get(ctx)
		cancel()
		up := 0.0
		if err == nil {
			up = 1
			if tip.Tip != nil {
				ch <- gaugeOf(descLakeTip, float64(*tip.Tip))
				for p, f := range cursors {
					if cur, ok := f(); ok {
						ch <- gaugeOf(descLag, float64(int64(*tip.Tip)-int64(cur)), p)
					}
				}
			}
		}
		ch <- gaugeOf(descLakeUp, up)
	}
	if network != nil {
		ch <- gaugeOf(descNetwork, float64(*network))
		ch <- gaugeOf(descNetworkAt, unixSeconds(netAt))
	}
	if s.Sink != nil {
		ch <- counterOf(descConflicts, float64(s.Sink.Conflicts()))
		ch <- counterOf(descDrift, float64(s.Sink.StateDrift()))
	}
	for mount, dir := range map[string]string{"core": s.CoreDir, "lake": s.LakeDir} {
		if dir == "" {
			continue
		}
		var st unix.Statfs_t
		if err := unix.Statfs(dir, &st); err != nil {
			continue
		}
		bsize := float64(st.Bsize)
		size, free, avail := float64(st.Blocks)*bsize, float64(st.Bfree)*bsize, float64(st.Bavail)*bsize
		ch <- gaugeOf(descDisk, size, mount, "size")
		ch <- gaugeOf(descDisk, size-free, mount, "used")
		ch <- gaugeOf(descDisk, avail, mount, "avail")
	}
	if a := s.Archiver; a != nil {
		tip, known := a.Tip()
		if known {
			ch <- gaugeOf(descArchTip, float64(tip))
		}
		ch <- gaugeOf(descArchKnown, boolf(known))
		st := a.Stats()
		ch <- counterOf(descArchived, float64(st.Created), "created")
		ch <- counterOf(descArchived, float64(st.Existed), "exists")
		ch <- counterOf(descArchived, float64(st.Differed), "differs")
		ch <- counterOf(descArchBytes, float64(st.Bytes))
		if !st.LastArchived.IsZero() {
			ch <- gaugeOf(descArchLast, unixSeconds(st.LastArchived))
		}
		for _, r := range []string{archive.BlockedUnavailable, archive.BlockedOrigin, archive.BlockedManifest,
			archive.BlockedMissing, archive.BlockedInvalid, archive.BlockedHalted} {
			ch <- gaugeOf(descArchBlock, boolf(st.Blocked == r), r)
		}
	}
	if p := s.Pruner; p != nil {
		st := p.Stats()
		ch <- counterOf(descPruned, float64(st.Deleted))
		ch <- gaugeOf(descPruneBound, float64(st.Bound))
		ch <- gaugeOf(descPruneWM, boolf(st.Watermark))
		ch <- counterOf(descPruneErr, float64(st.Errors))
		if !st.LastPass.IsZero() {
			ch <- gaugeOf(descPruneLast, unixSeconds(st.LastPass))
		}
	}
	if s.WatchdogState != "" {
		if st, err := watchdog.ReadState(s.WatchdogState); err == nil && !st.LastRun.IsZero() {
			ch <- gaugeOf(descWDRun, unixSeconds(st.LastRun))
			ch <- counterOf(descWDRestarts, float64(st.RestartsTotal))
			ch <- gaugeOf(descWDInARow, float64(st.RestartsInARow))
			for _, k := range st.Alerts {
				ch <- gaugeOf(descWDAlert, 1, k)
			}
			ch <- gaugeOf(descGxActive, boolf(st.GalexieActive))
			ch <- counterOf(descGxRestarts, float64(st.GalexieNRestarts))
			if st.UploaderLatest != nil {
				ch <- gaugeOf(descGxUploader, float64(*st.UploaderLatest))
			}
		}
	}
	if s.GapcheckResult != "" {
		if r, err := archive.ReadGapReport(s.GapcheckResult); err == nil {
			ch <- gaugeOf(descGapRun, unixSeconds(r.FinishedAt))
			ch <- gaugeOf(descGapMissing, float64(r.Missing))
			ch <- gaugeOf(descGapInvalid, float64(len(r.Invalid)))
			ch <- gaugeOf(descGapFailed, boolf(r.Error != ""))
			ch <- gaugeOf(descGapTo, float64(r.To))
		}
	}
}

func boolf(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
