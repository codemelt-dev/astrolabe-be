package clickhouse

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/stellar/go-stellar-sdk/network"

	"stellar-explorer/worker/config"
	"stellar-explorer/worker/explorer"
	"stellar-explorer/worker/ingest"
	"stellar-explorer/worker/metrics"
	"stellar-explorer/worker/sep54"
	"stellar-explorer/worker/sequencer"
)

// RunOptions configure Run.
type RunOptions struct {
	Config config.Config
	Log    *slog.Logger
	// StartRetry caps the wait between attempts to reach ClickHouse at
	// start (default 30 s).
	StartRetry time.Duration
}

// Run runs the clickhouse pipeline (`worker clickhouse-ingest`) until ctx is
// cancelled: the metrics and health endpoints, the AWS public lake's tip,
// the sink and one sequencer. The AWS lake is read like the explorer reads
// the lake, and is the only source: the archive bucket stalled on single
// objects for minutes. A ledger it can't serve is retried until it can. A
// halted pipeline keeps the process running and waits for an operator.
func Run(ctx context.Context, o RunOptions) error {
	cfg, log := o.Config, o.Log
	if log == nil {
		log = slog.Default()
	}
	m := metrics.New(log)
	alert := func(kind, msg string, args ...any) {
		m.Alert(kind)
		log.Error(msg, append([]any{"alert", kind}, args...)...)
	}
	aws := ingest.NewLazySource(sep54.SourceAWS, sep54.OpenAWSLake)
	tip := &sep54.StoreTip{
		Newest: func(ctx context.Context) (uint32, time.Time, error) {
			src, err := aws.Source(ctx)
			if err != nil {
				return 0, time.Time{}, err
			}
			return src.NewestObject(ctx)
		},
		// The pipeline never reads below its cursor.
		Oldest: 2,
		Poll:   cfg.ClickHouse.TipPoll,
	}
	conn, err := Open(ctx, cfg.ClickHouse, false)
	if err != nil {
		return err
	}
	defer conn.Close()
	sink := NewSink(conn, SinkOptions{Log: log})

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	m.SetCursor(Pipeline, sink.LastCursor)
	m.SetSources(metrics.Sources{LakeTip: tip})
	health := metrics.Health{LakeTip: tip, Database: sink.Ping, Cursor: sink.LastCursor, MaxLag: cfg.Metrics.ReadyMaxLag,
		TipName: "AWS lake", DatabaseName: "clickhouse", Pipeline: Pipeline}
	srv, err := metrics.Start(ctx, cfg.Metrics.Listen, cfg.Metrics.PrivateListen, metrics.Handler(m, health), log)
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	defer func() { cancel(); wg.Wait(); _ = srv.Wait() }()
	if cfg.Metrics.NetworkTip {
		// The network tip is the same listing as the pipeline's tip (one
		// LIST per tip_poll for both).
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.PollNetworkTip(ctx, func(ctx context.Context) (uint32, error) {
				t, err := tip.Get(ctx)
				if err != nil {
					return 0, err
				}
				if t.Tip == nil {
					return 0, sep54.ErrNoLedgers
				}
				return *t.Tip, nil
			}, ingest.NetworkTipInterval)
		}()
	}

	if err := waitForCursor(ctx, sink, o.StartRetry, log); err != nil || ctx.Err() != nil {
		return err
	}
	var lastLog time.Time
	seq, err := sequencer.New(sequencer.Options[*explorer.Prepared]{
		Pipeline:        Pipeline,
		Sink:            sink,
		Process:         explorer.HistoryProcessor(network.PublicNetworkPassphrase),
		Sources:         sequencer.Sources{Lake: aws, LakeTip: tip},
		MaxBatchLedgers: cfg.Ingest.MaxBatchLedgers,
		MaxBatchBytes:   int64(cfg.Ingest.MaxBatchMiB) << 20,
		LakeGrace:       cfg.Ingest.LakeGrace,
		Log:             log,
		Alert:           alert,
		OnCommit: func(c sequencer.CommitInfo) {
			m.Commit(Pipeline, c)
			if time.Since(lastLog) >= time.Minute {
				lastLog = time.Now()
				log.Info("committed", "pipeline", Pipeline, "cursor", c.Last, "ledgers", c.Ledgers)
			}
		},
	})
	if err != nil {
		return err
	}
	err = seq.Run(ctx)
	switch {
	case err == nil || ctx.Err() != nil:
		log.Info("clickhouse ingest stopped", "cursor", seq.Stats().Cursor)
		return nil
	case sequencer.IsHalt(err):
		log.Error("clickhouse pipeline halted; waiting for an operator", "err", err)
		<-ctx.Done()
		return nil
	default:
		return err
	}
}

// waitForCursor reads the cursor until ClickHouse answers. No cursor at all
// is an error: `worker clickhouse-init` seeds it.
func waitForCursor(ctx context.Context, sink *Sink, maxWait time.Duration, log *slog.Logger) error {
	if maxWait <= 0 {
		maxWait = 30 * time.Second
	}
	delay := time.Second
	for {
		seq, _, err := sink.Cursor(ctx)
		switch {
		case err == nil:
			log.Info("starting clickhouse ingest", "cursor", seq)
			return nil
		case errors.Is(err, ErrNoCursor):
			return ErrNoCursor
		case ctx.Err() != nil:
			return nil
		}
		log.Warn("clickhouse is not reachable yet; retrying", "err", err, "in", delay)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(delay):
		}
		delay = min(2*delay, maxWait)
	}
}
