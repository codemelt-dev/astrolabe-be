package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math"
	"os/signal"
	"syscall"

	"stellar-explorer/worker/clickhouse"
	"stellar-explorer/worker/ingest"
	"stellar-explorer/worker/sep54"
)

// The commands of the clickhouse pipeline (clickhouse/README.md), run on the
// clickhouse host: clickhouse-migrate, -init, -ingest and -check.

// setupClickhouseMigrate wires `worker clickhouse-migrate`, as stellar_owner.
// With --check it lists the pending migrations and fails while there are
// any (lab clickhouse uses it to decide).
func setupClickhouseMigrate(fs *flag.FlagSet) runFunc {
	check := fs.Bool("check", false, "only list the pending migrations; exit 1 if there are any")
	return func(ctx context.Context, env runEnv) error {
		cfg, log, err := loadConfig(env.configPath, env.stderr)
		if err != nil {
			return err
		}
		conn, err := clickhouse.Open(ctx, cfg.ClickHouse, true)
		if err != nil {
			return err
		}
		defer conn.Close()
		if *check {
			pending, err := clickhouse.Pending(ctx, conn)
			if err != nil {
				return err
			}
			for _, m := range pending {
				fmt.Fprintf(env.stdout, "pending %d %s (%s)\n", m.Version, m.Name, m.File)
			}
			if len(pending) > 0 {
				return fmt.Errorf("%d pending migration(s): run `worker clickhouse-migrate`", len(pending))
			}
			fmt.Fprintln(env.stdout, "schema is up to date")
			return nil
		}
		applied, err := clickhouse.Migrate(ctx, conn, log)
		for _, m := range applied {
			fmt.Fprintf(env.stdout, "applied %d %s (%s)\n", m.Version, m.Name, m.File)
		}
		if err == nil && len(applied) == 0 {
			fmt.Fprintln(env.stdout, "schema is up to date")
		}
		return err
	}
}

// setupClickhouseInit wires `worker clickhouse-init`: it seeds the cursor
// once, before the archive's oldest ledger (or --at), with that ledger's
// previous hash, so the pipeline starts where the explorer's history does.
// With --check it only reports whether the cursor exists (exit 1 if not).
func setupClickhouseInit(fs *flag.FlagSet) runFunc {
	at := fs.Uint("at", 0, "first ledger to write (default: the archive's oldest)")
	check := fs.Bool("check", false, "only report the cursor; exit 1 if there is none")
	return func(ctx context.Context, env runEnv) error {
		if *at > math.MaxUint32 || *at == 1 {
			return fmt.Errorf("%w: --at must be a ledger ≥ 2", errUsage)
		}
		cfg, _, err := loadConfig(env.configPath, env.stderr)
		if err != nil {
			return err
		}
		conn, err := clickhouse.Open(ctx, cfg.ClickHouse, false)
		if err != nil {
			return err
		}
		defer conn.Close()
		if *check {
			seq, _, err := clickhouse.ReadCursor(ctx, conn, clickhouse.Pipeline)
			if err != nil {
				return err
			}
			fmt.Fprintf(env.stdout, "cursor %d\n", seq)
			return nil
		}
		srcs, err := ingest.OpenArchiveSources(cfg)
		if err != nil {
			return err
		}
		src, err := srcs.Archive.Source(ctx)
		if err != nil {
			return fmt.Errorf("open the archive: %w", err)
		}
		first := uint32(*at)
		if first == 0 {
			if first, err = src.Oldest(ctx); err != nil {
				return fmt.Errorf("the archive's oldest ledger: %w", err)
			}
		}
		obj, err := src.GetLedger(ctx, first)
		if err != nil {
			return fmt.Errorf("read ledger %d from the archive: %w", first, err)
		}
		h, err := sep54.DecodeHeader(first, obj.Body)
		if err != nil {
			return err
		}
		created, err := clickhouse.Init(ctx, conn, clickhouse.Pipeline, first-1, h.PreviousHash)
		if err != nil {
			return err
		}
		if created {
			fmt.Fprintf(env.stdout, "cursor seeded at %d: the pipeline starts at ledger %d\n", first-1, first)
		} else {
			fmt.Fprintf(env.stdout, "cursor already at %d\n", first-1)
		}
		return nil
	}
}

// runClickhouseIngest runs `worker clickhouse-ingest` until SIGTERM/SIGINT.
func runClickhouseIngest(ctx context.Context, env runEnv) error {
	cfg, log, err := loadConfig(env.configPath, env.stderr)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log.Info("starting clickhouse-ingest", "version", version, "clickhouse", cfg.ClickHouse.Addr,
		"source", sep54.SourceAWS, "lake_grace", cfg.Ingest.LakeGrace)
	return clickhouse.Run(ctx, clickhouse.RunOptions{Config: cfg, Log: log})
}

// setupClickhouseCheck wires `worker clickhouse-check`: the consistency of
// a range (by default the cursor's last 10,000 ledgers).
func setupClickhouseCheck(fs *flag.FlagSet) runFunc {
	from := fs.Uint("from", 0, "first ledger (default: cursor − 9,999)")
	to := fs.Uint("to", 0, "last ledger (default: the cursor)")
	return func(ctx context.Context, env runEnv) error {
		if *from > math.MaxUint32 || *to > math.MaxUint32 {
			return fmt.Errorf("%w: ledgers are 32-bit", errUsage)
		}
		cfg, _, err := loadConfig(env.configPath, env.stderr)
		if err != nil {
			return err
		}
		conn, err := clickhouse.Open(ctx, cfg.ClickHouse, false)
		if err != nil {
			return err
		}
		defer conn.Close()
		lo, hi := uint32(*from), uint32(*to)
		if hi == 0 || lo == 0 {
			cur, _, err := clickhouse.ReadCursor(ctx, conn, clickhouse.Pipeline)
			if err != nil {
				return err
			}
			if hi == 0 {
				hi = cur
			}
			if lo == 0 {
				lo = max(2, hi-min(hi, 9_999))
			}
		}
		r, err := clickhouse.Check(ctx, conn, clickhouse.Pipeline, lo, hi)
		if err != nil {
			return err
		}
		fmt.Fprintln(env.stdout, r)
		if p := r.Problems(); len(p) > 0 {
			return errors.New("inconsistent")
		}
		return nil
	}
}
