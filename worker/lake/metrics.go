package lake

import (
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"golang.org/x/sys/unix"
)

// metrics are the lake's Prometheus metrics, in a registry
// owned by one Lake so that sequential lakes in one process never collide.
type metrics struct {
	reg           *prometheus.Registry
	puts          *prometheus.CounterVec
	putDuration   prometheus.Histogram
	fsyncDuration *prometheus.HistogramVec
	fsyncErrors   *prometheus.CounterVec
	fsyncFailed   prometheus.Gauge
	ready         prometheus.Gauge
	syncfsSeconds prometheus.Gauge
	scanChecked   prometheus.Gauge
	scanDeleted   prometheus.Gauge
	scanHoles     prometheus.Gauge
}

func newMetrics(idx *index, fl *inflight, diskPath string, log *slog.Logger) *metrics {
	m := &metrics{
		reg: prometheus.NewRegistry(),
		puts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "lake_puts_total",
			Help: "Object PUTs by result: created, exists (412, re-synced), error, fail_closed.",
		}, []string{"result"}),
		putDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "lake_put_duration_seconds",
			Help:    "Duration of committed ledger PUTs, from handler entry to commit.",
			Buckets: prometheus.ExponentialBuckets(0.0005, 2, 16),
		}),
		fsyncDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "lake_fsync_duration_seconds",
			Help:    "Duration of each fsync in the commit path, by stage (staged, published, dir).",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 18),
		}, []string{"stage"}),
		fsyncErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "lake_fsync_errors_total",
			Help: "fsync errors in the commit path, by stage.",
		}, []string{"stage"}),
		fsyncFailed: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "lake_fsync_failed",
			Help: "1 once an fsync after a link failed: the lake is fail-closed and answers every PUT with 503 until restarted.",
		}),
		ready: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "lake_ready",
			Help: "1 once both the control and the S3 listeners accept connections.",
		}),
		syncfsSeconds: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "lake_startup_syncfs_seconds",
			Help: "Duration of the startup syncfs of the lake filesystem.",
		}),
		scanChecked: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "lake_startup_scan_checked_objects",
			Help: "Objects checksum-verified by the post-reboot startup scan (0 if it did not run).",
		}),
		scanDeleted: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "lake_startup_scan_deleted_objects",
			Help: "Corrupt objects deleted by the post-reboot startup scan.",
		}),
		scanHoles: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "lake_startup_scan_holes",
			Help: "Corrupt objects deleted below a good one: holes to repair with scan-and-fill.",
		}),
	}
	m.reg.MustRegister(m.puts, m.putDuration, m.fsyncDuration, m.fsyncErrors, m.fsyncFailed,
		m.ready, m.syncfsSeconds, m.scanChecked, m.scanDeleted, m.scanHoles,
		&stateCollector{idx: idx, inflight: fl, diskPath: diskPath, log: log},
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return m
}

var (
	descTip = prometheus.NewDesc("lake_tip",
		"Newest committed (durable) ledger; absent while the lake is empty.", nil, nil)
	descOldest = prometheus.NewDesc("lake_oldest",
		"Oldest ledger the lake is expected to hold; absent while the lake is empty.", nil, nil)
	descTipAt = prometheus.NewDesc("lake_tip_committed_at_seconds",
		"Unix time at which the tip was committed.", nil, nil)
	descPhaseAge = prometheus.NewDesc("lake_put_phase_age_seconds",
		"Age of the longest-waiting in-flight ledger PUT in each phase (0 if none). "+
			"receiving: the sender is slow or stuck; syncing: the disk is.", []string{"phase"}, nil)
	descDisk = prometheus.NewDesc("lake_disk_bytes",
		"Lake filesystem size, used and available bytes (statfs).", []string{"kind"}, nil)
)

// stateCollector reports values read at scrape time.
type stateCollector struct {
	idx      *index
	inflight *inflight
	diskPath string
	log      *slog.Logger
}

func (c *stateCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- descTip
	ch <- descOldest
	ch <- descTipAt
	ch <- descPhaseAge
	ch <- descDisk
}

func (c *stateCollector) Collect(ch chan<- prometheus.Metric) {
	t := c.idx.snapshot()
	if t.Tip != nil {
		ch <- prometheus.MustNewConstMetric(descTip, prometheus.GaugeValue, float64(*t.Tip))
		ch <- prometheus.MustNewConstMetric(descTipAt, prometheus.GaugeValue, float64(*t.TipCommittedAt)/1e9)
	}
	if t.Oldest != nil {
		ch <- prometheus.MustNewConstMetric(descOldest, prometheus.GaugeValue, float64(*t.Oldest))
	}
	for ph, age := range c.inflight.ages() {
		ch <- prometheus.MustNewConstMetric(descPhaseAge, prometheus.GaugeValue, age.Seconds(), string(ph))
	}
	var st unix.Statfs_t
	if err := unix.Statfs(c.diskPath, &st); err != nil {
		c.log.Warn("statfs of the lake failed", "path", c.diskPath, "err", err)
		return
	}
	bsize := float64(st.Bsize)
	size, free, avail := float64(st.Blocks)*bsize, float64(st.Bfree)*bsize, float64(st.Bavail)*bsize
	ch <- prometheus.MustNewConstMetric(descDisk, prometheus.GaugeValue, size, "size")
	ch <- prometheus.MustNewConstMetric(descDisk, prometheus.GaugeValue, size-free, "used")
	ch <- prometheus.MustNewConstMetric(descDisk, prometheus.GaugeValue, avail, "avail")
}
