package main

import (
	"context"
	"flag"
	"fmt"
	"os/signal"
	"syscall"
	"time"

	"stellar-explorer/worker/clickhouse"
	"stellar-explorer/worker/statecopy"
)

// setupStateCopy wires `worker state-copy` (psql host, daily timer):
// PostgreSQL's state into ClickHouse, the rankings computed from it, and
// with --snapshot the day's snapshots (clickhouse/README.md §10).
func setupStateCopy(fs *flag.FlagSet) runFunc {
	snapshot := fs.Bool("snapshot", false, "also append the snapshots, dated the day before (UTC)")
	return func(ctx context.Context, env runEnv) error {
		cfg, log, err := loadConfig(env.configPath, env.stderr)
		if err != nil {
			return err
		}
		if err := requireDSN(cfg); err != nil {
			return err
		}
		ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
		defer stop()
		conn, err := clickhouse.Open(ctx, cfg.ClickHouse, false)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := statecopy.Run(ctx, statecopy.Options{DSN: cfg.Postgres.DSN, CH: conn, Snapshot: *snapshot, Log: log})
		if err != nil {
			return err
		}
		fmt.Fprintf(env.stdout, "state copied as of ledger %d (closed %s) in %s: %v",
			res.Ledger, res.ClosedAt.UTC().Format(time.RFC3339), res.Duration.Round(time.Second), res.Rows)
		if !res.Snapshot.IsZero() {
			fmt.Fprintf(env.stdout, "; snapshots of %s appended", res.Snapshot.Format(time.DateOnly))
		}
		fmt.Fprintln(env.stdout)
		return nil
	}
}
