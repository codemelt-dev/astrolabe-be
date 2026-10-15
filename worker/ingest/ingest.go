// Package ingest runs `worker ingest`: one sequencer per
// pipeline (v1: explorer → Postgres), the archiver (lake → archive bucket),
// the pruner, and the metrics and health endpoints.
//
// The archiver and the pruner do not depend on Postgres: they keep running
// while it is down or while the pipeline is halted.
package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/stellar/go-stellar-sdk/network"
	"golang.org/x/sys/unix"

	"stellar-explorer/worker/archive"
	"stellar-explorer/worker/bootstrap"
	"stellar-explorer/worker/config"
	"stellar-explorer/worker/explorer"
	"stellar-explorer/worker/metrics"
	"stellar-explorer/worker/prune"
	"stellar-explorer/worker/sep54"
	"stellar-explorer/worker/sequencer"
)

// Options configure Run.
type Options struct {
	Config  config.Config
	Log     *slog.Logger
	Version string
	// Alert reports conditions that need an operator; the default logs at
	// ERROR with alert=<kind>. Every alert is also counted in
	// stellar_alerts_total{kind}.
	Alert func(kind, msg string, args ...any)
	// OnCommit observes every explorer commit (tests).
	OnCommit func(sequencer.CommitInfo)
	// Sources overrides the stores (tests); the default is OpenSources.
	Sources *Sources
	// StartRetry bounds the wait between startup checks (default 30 s).
	StartRetry time.Duration
	// Metrics overrides the registry (tests).
	Metrics *metrics.Ingest
	// NetworkTip reads the network tip for the metrics (default: the AWS
	// public lake's newest ledger, when metrics.network_tip is set).
	NetworkTip func(ctx context.Context) (uint32, error)
	// ArchiveOptions and PruneOptions adjust the archiver and the pruner
	// (tests: short intervals).
	ArchiveOptions func(*archive.Options)
	PruneOptions   func(*prune.Options)
}

// NetworkTipInterval is how often the network tip is read: about one ledger
// (one LIST request to the AWS public lake each time).
const NetworkTipInterval = 5 * time.Second

// Run runs ingest until ctx is cancelled. It returns an error only for
// conditions a restart may fix or an operator must see at once: another
// ingest holds the lock, the cursor moved under it, a configuration error.
// A halted pipeline keeps the process (and the archiver) running.
func Run(ctx context.Context, o Options) error {
	cfg := o.Config
	log := o.Log
	if log == nil {
		log = slog.Default()
	}
	m := o.Metrics
	if m == nil {
		m = metrics.New(log)
	}
	alert := func(kind, msg string, args ...any) {
		m.Alert(kind)
		if o.Alert != nil {
			o.Alert(kind, msg, args...)
			return
		}
		log.Error(msg, append([]any{"alert", kind}, args...)...)
	}
	if cfg.Postgres.DSN == "" {
		return errors.New("postgres.dsn is not set")
	}
	var srcs Sources
	if o.Sources != nil {
		srcs = *o.Sources
	} else {
		var err error
		if srcs, err = OpenSources(cfg); err != nil {
			return err
		}
	}

	var wg sync.WaitGroup
	defer wg.Wait()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Metrics and health first, so /healthz answers while ingest waits for
	// the lake or Postgres.
	var sinkRef atomic.Pointer[explorer.Sink]
	cursor := func() (uint32, bool) {
		if s := sinkRef.Load(); s != nil {
			return s.LastCursor()
		}
		return 0, false
	}
	m.SetCursor(explorer.Pipeline, cursor)
	health := metrics.Health{LakeTip: srcs.LakeTip, Cursor: cursor, MaxLag: cfg.Metrics.ReadyMaxLag,
		Database: func(ctx context.Context) error {
			if s := sinkRef.Load(); s != nil {
				return s.Ping(ctx)
			}
			return errors.New("not connected yet")
		}}
	srv, err := metrics.Start(ctx, cfg.Metrics.Listen, cfg.Metrics.PrivateListen, metrics.Handler(m, health), log)
	if err != nil {
		return err
	}
	defer func() { cancel(); _ = srv.Wait() }()
	if networkTip := o.NetworkTip; networkTip != nil || cfg.Metrics.NetworkTip {
		if networkTip == nil {
			aws := NewLazySource(sep54.SourceAWS, sep54.OpenAWSLake)
			networkTip = func(ctx context.Context) (uint32, error) {
				src, err := aws.Source(ctx)
				if err != nil {
					return 0, err
				}
				return src.Newest(ctx)
			}
		}
		goSafe(&wg, log, "network tip", func() { m.PollNetworkTip(ctx, networkTip, NetworkTipInterval) })
	}

	// Connect to the lake, retrying until it answers.
	if err := waitForLake(ctx, srcs, log); err != nil {
		return err
	}
	if ctx.Err() != nil {
		return nil
	}

	// Archiver and pruner.
	sources := metrics.Sources{LakeTip: srcs.LakeTip, CoreDir: cfg.Metrics.CoreDir, LakeDir: cfg.Lake.Dir,
		WatchdogState: cfg.Watchdog.StateFile, GapcheckResult: cfg.Gapcheck.ResultFile}
	archiveTip := func() (uint32, bool) { return 0, false }
	if srcs.Archive != nil {
		ao := archive.Options{
			Lake: srcs.Lake.Source, Archive: srcs.Archive.Source, LakeTip: srcs.LakeTip,
			Origin: func() (bootstrap.Origin, error) { return bootstrap.ReadOrigin(cfg.Lake.Dir) },
			Log:    log, Alert: alert,
		}
		if o.ArchiveOptions != nil {
			o.ArchiveOptions(&ao)
		}
		archiver := archive.New(ao)
		archiveTip = archiver.Tip
		sources.Archiver = archiver
		goSafe(&wg, log, "archiver", func() { _ = archiver.Run(ctx) })
	} else {
		log.Warn("archiving is disabled (archive.enabled = false): nothing is archived, so nothing is pruned")
	}
	switch {
	case !cfg.Prune.Enabled:
		log.Warn("pruning is disabled (prune.enabled = false)")
	case srcs.Archive == nil:
		log.Warn("pruning is off: without an archive nothing may be deleted from the lake")
	default:
		lakeDir := cfg.Lake.Dir
		po := prune.Options{
			Lake: srcs.Lake.Source, LakeTip: srcs.LakeTip, ArchiveTip: archiveTip,
			Readers: cfg.Prune.LocalReaders,
			Cursor: func(p string) (uint32, bool) {
				if p == explorer.Pipeline {
					return cursor()
				}
				return 0, false // not in this process: unknown, so "not passed"
			},
			Retention:     cfg.Prune.RetentionLedgers,
			DiskUsage:     func() (float64, error) { return diskUsed(lakeDir) },
			HighWatermark: cfg.Prune.DiskHighWatermark,
			Interval:      cfg.Prune.Interval,
			Log:           log, Alert: alert,
		}
		if o.PruneOptions != nil {
			o.PruneOptions(&po)
		}
		pruner, err := prune.New(po)
		if err != nil {
			return err
		}
		sources.Pruner = pruner
		goSafe(&wg, log, "pruner", func() { pruner.Run(ctx) })
	}

	sink, err := explorer.NewSink(cfg.Postgres.DSN, explorer.SinkOptions{
		Pipeline:      explorer.Pipeline,
		Lock:          true,
		COPYThreshold: 1,
		Log:           log,
		Alert:         alert,
	})
	if err != nil {
		return err
	}
	defer sink.Close()
	sinkRef.Store(sink)
	sources.Sink = sink
	m.SetSources(sources)
	if err := startSink(ctx, sink, o.StartRetry, log, alert); err != nil {
		if ctx.Err() != nil {
			return nil // stopped while waiting
		}
		return err
	}

	seqSources := sequencer.Sources{Lake: srcs.Lake, LakeTip: srcs.LakeTip, AWS: srcs.AWS}
	if srcs.Archive != nil {
		seqSources.Archive, seqSources.ArchiveTip = srcs.Archive, archiveTip
	}
	progress := newProgress(log)
	seq, err := sequencer.New(sequencer.Options[*explorer.Prepared]{
		Pipeline:        explorer.Pipeline,
		Sink:            sink,
		Process:         explorer.Processor(network.PublicNetworkPassphrase),
		Sources:         seqSources,
		MaxBatchLedgers: cfg.Ingest.MaxBatchLedgers,
		MaxBatchBytes:   int64(cfg.Ingest.MaxBatchMiB) << 20,
		LakeGrace:       cfg.Ingest.LakeGrace,
		ArchiveGrace:    cfg.Ingest.ArchiveGrace,
		Log:             log,
		Alert:           alert,
		OnCommit: func(c sequencer.CommitInfo) {
			m.Commit(explorer.Pipeline, c)
			progress.commit(c)
			if o.OnCommit != nil {
				o.OnCommit(c)
			}
		},
	})
	if err != nil {
		return err
	}
	goSafe(&wg, log, "progress", func() { progress.run(ctx, srcs.LakeTip, sink) })

	err = seq.Run(ctx)
	switch {
	case err == nil || ctx.Err() != nil:
		log.Info("ingest stopped", "cursor", seq.Stats().Cursor, "conflicts", sink.Conflicts())
		return nil
	case sequencer.IsHalt(err):
		// Halted without advancing: wait for an operator; the archiver and
		// pruner keep running (galexie/README.md §7.4.3).
		log.Error("explorer pipeline halted; waiting for an operator", "err", err)
		<-ctx.Done()
		return nil
	default:
		return err
	}
}

// diskUsed returns the used fraction of the filesystem holding dir.
func diskUsed(dir string) (float64, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(dir, &st); err != nil {
		return 0, err
	}
	if st.Blocks == 0 {
		return 0, fmt.Errorf("statfs %s: no blocks", dir)
	}
	// Like df: used / (used + available to unprivileged users).
	used := float64(st.Blocks - st.Bfree)
	return used / (used + float64(st.Bavail)), nil
}

// goSafe runs f in a goroutine under recover(): a panic in a
// helper is logged and ends only that helper.
func goSafe(wg *sync.WaitGroup, log *slog.Logger, name string, f func()) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				log.Error("goroutine panicked", "goroutine", name, "panic", r)
			}
		}()
		f()
	}()
}

// waitForLake retries until the lake's S3 endpoint and control endpoint
// both answer.
func waitForLake(ctx context.Context, srcs Sources, log *slog.Logger) error {
	delay := 100 * time.Millisecond
	var lastLog time.Time
	for {
		_, err := srcs.Lake.Source(ctx)
		if err == nil {
			_, err = srcs.LakeTip.Get(ctx)
		}
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return nil
		}
		if !sep54.IsUnavailable(err) {
			return fmt.Errorf("lake: %w", err)
		}
		if time.Since(lastLog) > 30*time.Second {
			lastLog = time.Now()
			log.Warn("waiting for the lake", "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(delay):
		}
		delay = min(2*delay, 5*time.Second)
	}
}

// startSink runs the pre-commit checks until they pass:
// Postgres down is retried; a schema that is behind or a missing cursor is
// alerted and retried (an operator runs `worker migrate` or `worker
// bootstrap`); a held advisory lock is fatal (another ingest runs).
func startSink(ctx context.Context, sink *explorer.Sink, maxWait time.Duration, log *slog.Logger, alert func(string, string, ...any)) error {
	if maxWait <= 0 {
		maxWait = 30 * time.Second
	}
	delay := time.Second
	alerted := map[string]bool{}
	for {
		err := sink.Start(ctx)
		if err == nil {
			return nil
		}
		kind := ""
		switch {
		case errors.Is(err, context.Canceled):
			return err
		case errors.Is(err, explorer.ErrLocked):
			return err
		case errors.Is(err, explorer.ErrSchemaBehind):
			kind = "schema_behind"
		case errors.Is(err, explorer.ErrSchemaMismatch):
			kind = "schema_mismatch"
		case errors.Is(err, explorer.ErrNoCursor):
			kind = "no_cursor"
		case sequencer.IsTransient(err):
			log.Warn("postgres not ready; retrying", "err", err)
		default:
			return err
		}
		if kind != "" && !alerted[kind] {
			alerted[kind] = true
			alert(kind, "ingest refuses to commit", "err", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		delay = min(2*delay, maxWait)
	}
}

// progress logs the pipeline's state every 30 s: cursor, lag behind the
// lake tip, rate, ledger latency.
type progress struct {
	log *slog.Logger

	mu        sync.Mutex
	ledgers   int
	commits   int
	last      uint32
	latencies []time.Duration
}

func newProgress(log *slog.Logger) *progress { return &progress{log: log} }

func (p *progress) commit(c sequencer.CommitInfo) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ledgers += c.Ledgers
	p.commits++
	p.last = c.Last
	if c.Latency > 0 {
		p.latencies = append(p.latencies, c.Latency)
	}
}

func (p *progress) run(ctx context.Context, tip *sep54.TipClient, sink *explorer.Sink) {
	const every = 30 * time.Second
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		p.mu.Lock()
		ledgers, commits, last, lat := p.ledgers, p.commits, p.last, p.latencies
		p.ledgers, p.commits, p.latencies = 0, 0, nil
		p.mu.Unlock()
		attrs := []any{"cursor", last, "ledgers", ledgers, "commits", commits,
			"rate_per_s", fmt.Sprintf("%.2f", float64(ledgers)/every.Seconds()), "conflicts", sink.Conflicts()}
		if tip != nil {
			tctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			if t, err := tip.Get(tctx); err == nil && t.Tip != nil && last > 0 {
				attrs = append(attrs, "lake_tip", *t.Tip, "lag", int64(*t.Tip)-int64(last))
			}
			cancel()
		}
		if len(lat) > 0 {
			var max, sum time.Duration
			for _, l := range lat {
				sum += l
				max = maxDuration(max, l)
			}
			attrs = append(attrs, "latency_avg", (sum / time.Duration(len(lat))).Round(time.Millisecond), "latency_max", max.Round(time.Millisecond))
		}
		p.log.Info("ingest progress", attrs...)
	}
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}
