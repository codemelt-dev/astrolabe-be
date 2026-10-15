package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"os/signal"
	"strings"
	"syscall"

	"github.com/jackc/pgx/v5"

	"stellar-explorer/worker/config"
	"stellar-explorer/worker/explorer"
	"stellar-explorer/worker/ingest"
	"stellar-explorer/worker/reindex"
	"stellar-explorer/worker/sep54"
	"stellar-explorer/worker/sequencer"
)

// The commands of the explorer pipeline: ingest, migrate, bootstrap,
// reindex.

func requireDSN(cfg config.Config) error {
	if cfg.Postgres.DSN == "" {
		return errors.New("postgres.dsn is not set in the config")
	}
	return nil
}

// runIngest runs `worker ingest` until SIGTERM/SIGINT.
func runIngest(ctx context.Context, env runEnv) error {
	cfg, log, err := loadConfig(env.configPath, env.stderr)
	if err != nil {
		return err
	}
	if err := requireDSN(cfg); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log.Info("starting ingest", "version", version, "lake", cfg.Lake.S3Listen, "tip", cfg.Lake.ControlListen,
		"archive", cfg.Archive.Enabled, "lake_grace", cfg.Ingest.LakeGrace)
	return ingest.Run(ctx, ingest.Options{Config: cfg, Log: log, Version: version})
}

// setupMigrate wires `worker migrate`. With --check it changes nothing: it
// lists the pending migrations and fails while there are any (lab galexie
// uses it to decide whether to migrate).
func setupMigrate(fs *flag.FlagSet) runFunc {
	check := fs.Bool("check", false, "only list the pending migrations; exit 1 if there are any")
	return func(ctx context.Context, env runEnv) error {
		conn, log, err := connectOwner(ctx, env)
		if err != nil {
			return err
		}
		defer conn.Close(context.WithoutCancel(ctx))
		if *check {
			return checkMigrations(ctx, conn, env)
		}
		return runMigrate(ctx, conn, log, env)
	}
}

// connectOwner connects as the role that owns the schema (explorer_owner).
func connectOwner(ctx context.Context, env runEnv) (*pgx.Conn, *slog.Logger, error) {
	cfg, log, err := loadConfig(env.configPath, env.stderr)
	if err != nil {
		return nil, nil, err
	}
	dsn := cfg.Postgres.MigrationDSN()
	if dsn == "" {
		return nil, nil, errors.New("postgres.migrate_dsn (or postgres.dsn) is not set in the config")
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("connect to postgres: %w", err)
	}
	return conn, log, nil
}

func checkMigrations(ctx context.Context, conn *pgx.Conn, env runEnv) error {
	pending, err := explorer.Pending(ctx, conn)
	if err != nil {
		return err
	}
	for _, m := range pending {
		fmt.Fprintf(env.stdout, "pending %d %s (%s)\n", m.Version, m.Name, m.File)
	}
	if len(pending) > 0 {
		return fmt.Errorf("%d pending migration(s): run `worker migrate`", len(pending))
	}
	fmt.Fprintln(env.stdout, "schema is up to date")
	return nil
}

// runMigrate applies the migrations as explorer_owner.
func runMigrate(ctx context.Context, conn *pgx.Conn, log *slog.Logger, env runEnv) error {
	applied, err := explorer.Migrate(ctx, conn, log)
	for _, m := range applied {
		fmt.Fprintf(env.stdout, "applied %d %s (%s)\n", m.Version, m.Name, m.File)
	}
	if err == nil && len(applied) == 0 {
		fmt.Fprintln(env.stdout, "schema is up to date")
	}
	return err
}

// setupReindex wires `worker reindex`: it replaces the ledger rows of a
// closed range below the cursor while live ingestion continues (with
// --dry-run it only validates a source, as `worker gapcheck` does).
func setupReindex(fs *flag.FlagSet) runFunc {
	from := fs.Uint("from", 0, "first ledger")
	to := fs.Uint("to", 0, "last ledger")
	source := fs.String("source", sep54.SourceLake, "lake, archive or aws")
	dryRun := fs.Bool("dry-run", false, "only decode and validate (sequence, self-hash, chain, gaps)")
	return func(ctx context.Context, env runEnv) error {
		if *from < 2 || *to < *from || *to > math.MaxUint32 {
			return fmt.Errorf("%w: need --from A --to B with 2 ≤ A ≤ B", errUsage)
		}
		cfg, log, err := loadConfig(env.configPath, env.stderr)
		if err != nil {
			return err
		}
		if !*dryRun {
			if err := requireDSN(cfg); err != nil {
				return err
			}
		}
		srcs, err := ingest.OpenSources(cfg)
		if err != nil {
			return err
		}
		src, err := srcs.ByName(*source)
		if err != nil {
			return fmt.Errorf("%w: --source: %v", errUsage, err)
		}
		ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
		defer stop()
		res, err := reindex.Run(ctx, reindex.Options{
			DSN: cfg.Postgres.DSN, From: uint32(*from), To: uint32(*to), Source: src, DryRun: *dryRun, Log: log,
			MaxBatchLedgers: cfg.Ingest.MaxBatchLedgers, MaxBatchBytes: int64(cfg.Ingest.MaxBatchMiB) << 20,
		})
		if *dryRun {
			fmt.Fprintf(env.stdout, "validated %d ledgers of %d-%d from %s\n", res.Ledgers, *from, *to, *source)
			if len(res.Missing) > 0 {
				fmt.Fprintf(env.stdout, "missing: %s\n", compactRanges(res.Missing))
			}
			for _, inv := range res.Invalid {
				fmt.Fprintf(env.stdout, "invalid %s\n", inv)
			}
			return err
		}
		if err != nil {
			if sequencer.IsHalt(err) {
				return fmt.Errorf("reindex stopped without replacing the failing ledger: %w", err)
			}
			return err
		}
		fmt.Fprintf(env.stdout, "reindexed %d-%d from %s (%d ledgers this run, %d conflicts); the explorer cursor is unchanged\n",
			res.From, *to, *source, res.Ledgers, res.Conflicts)
		return nil
	}
}

// compactRanges formats sorted ledger numbers as "5-7, 10".
func compactRanges(seqs []uint32) string {
	var parts []string
	for i := 0; i < len(seqs); {
		j := i
		for j+1 < len(seqs) && seqs[j+1] == seqs[j]+1 {
			j++
		}
		if i == j {
			parts = append(parts, fmt.Sprint(seqs[i]))
		} else {
			parts = append(parts, fmt.Sprintf("%d-%d", seqs[i], seqs[j]))
		}
		i = j + 1
	}
	return strings.Join(parts, ", ")
}
