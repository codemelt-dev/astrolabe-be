package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"os/signal"
	"syscall"
	"time"

	"stellar-explorer/worker/archive"
	"stellar-explorer/worker/bootstrap"
	"stellar-explorer/worker/config"
	"stellar-explorer/worker/ingest"
	"stellar-explorer/worker/launcher"
	"stellar-explorer/worker/sep54"
	"stellar-explorer/worker/watchdog"
)

// The operational commands: galexie-start and watchdog (run from
// worker-lake), gapcheck and the checkpoint bootstrap.

// setupGalexieStart wires `worker galexie-start --out <file>`,
// stellar-galexie.service's ExecStartPre.
func setupGalexieStart(fs *flag.FlagSet) runFunc {
	out := fs.String("out", "", "env file to write START=… to (atomically)")
	return func(ctx context.Context, env runEnv) error {
		if *out == "" {
			return fmt.Errorf("%w: --out is required", errUsage)
		}
		cfg, log, err := loadConfig(env.configPath, env.stderr)
		if err != nil {
			return err
		}
		gw, err := config.LoadGatewayEnv(cfg.Credentials.GatewayEnv)
		if err != nil {
			return err
		}
		var arch *config.ArchiveCredentials
		if cfg.Archive.Enabled {
			c, err := config.LoadArchiveEnv(cfg.Credentials.ArchiveEnv)
			if err != nil {
				return err
			}
			arch = &c
		}
		ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
		defer stop()
		res, err := launcher.Compute(ctx, launcher.NewOptions(cfg, gw, arch, log))
		if err != nil {
			return err
		}
		if err := launcher.WriteEnv(*out, res.Start); err != nil {
			return err
		}
		archiveTip := any(res.Inputs.Archive)
		if res.Inputs.Archive == launcher.ArchiveKnown {
			archiveTip = res.Inputs.ArchiveTip
		}
		log.Info("galexie start computed", "start", res.Start, "reason", res.Reason, "lake_tip", fmtTip(res.Inputs.LakeTip),
			"archive_tip", archiveTip, "elapsed", res.Elapsed.Round(time.Millisecond))
		fmt.Fprintf(env.stdout, "START=%d (%s) written to %s\n", res.Start, res.Reason, *out)
		return nil
	}
}

func fmtTip(t *uint32) any {
	if t == nil {
		return "empty"
	}
	return *t
}

// runWatchdog is `worker watchdog`, run every 30 s by
// stellar-watchdog.timer.
func runWatchdog(ctx context.Context, env runEnv) error {
	cfg, log, err := loadConfig(env.configPath, env.stderr)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	o := watchdog.Options{
		Config:            cfg.Watchdog,
		Unit:              cfg.Galexie.Unit,
		LakeMetricsURL:    "http://" + cfg.Lake.ControlListen + "/metrics",
		GalexieMetricsURL: cfg.Galexie.MetricsURL,
		Systemd:           watchdog.Systemctl{},
		Log:               log,
	}
	if cfg.Watchdog.NetworkTip {
		o.NetworkTip = awsNewest
	}
	st, err := watchdog.Run(ctx, o)
	if err != nil {
		return err
	}
	fmt.Fprintf(env.stdout, "%s\n", st.LastDecision)
	return nil
}

// awsNewest reads the AWS public lake's newest ledger: the network tip.
func awsNewest(ctx context.Context) (uint32, error) {
	src, err := sep54.OpenAWSLake(ctx)
	if err != nil {
		return 0, err
	}
	return src.Newest(ctx)
}

// runGapcheck is `worker gapcheck`, run weekly by
// stellar-gapcheck.timer.
func runGapcheck(ctx context.Context, env runEnv) error {
	cfg, log, err := loadConfig(env.configPath, env.stderr)
	if err != nil {
		return err
	}
	if !cfg.Archive.Enabled {
		return errors.New("archiving is disabled (archive.enabled = false): there is no archive to check")
	}
	creds, err := config.LoadArchiveEnv(cfg.Credentials.ArchiveEnv)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	openArchive := func(ctx context.Context) (*sep54.Source, error) {
		return sep54.OpenArchive(ctx, cfg.Archive, cfg.Lake, creds)
	}
	rep, err := archive.Gapcheck(ctx, archive.GapcheckOptions{
		Method:  cfg.Gapcheck.Method,
		Origin:  func() (bootstrap.Origin, error) { return bootstrap.ReadOrigin(cfg.Lake.Dir) },
		Archive: openArchive,
		DetectGaps: archive.DockerDetectGaps{Docker: cfg.Galexie.Docker, Image: cfg.Galexie.Image,
			GalexieConfig: cfg.Gapcheck.GalexieConfig, EnvFile: cfg.Gapcheck.EnvFile},
		DryRun:     archive.ReindexDryRun(openArchive, log),
		ResultFile: cfg.Gapcheck.ResultFile,
		Log:        log,
	})
	switch {
	case errors.Is(err, archive.ErrGaps):
		fmt.Fprintf(env.stdout, "archive %d-%d: %d ledgers missing in %d gaps, %d invalid objects (%s)\n",
			rep.From, rep.To, rep.Missing, len(rep.Gaps), len(rep.Invalid), rep.Method)
		for _, g := range rep.Gaps {
			fmt.Fprintf(env.stdout, "gap %d-%d\n", g.Start, g.End)
		}
		for _, inv := range rep.Invalid {
			fmt.Fprintf(env.stdout, "invalid %s\n", inv)
		}
		return err
	case err != nil:
		return err
	case rep.To < rep.From:
		fmt.Fprintf(env.stdout, "nothing archived yet (archive_origin %d)\n", rep.From)
	default:
		fmt.Fprintf(env.stdout, "archive %d-%d has no gaps (%s)\n", rep.From, rep.To, rep.Method)
	}
	return nil
}

// setupBootstrap wires `worker bootstrap`: the checkpoint
// load by default, --force (state repair), --history-only --at A
// (dev/test), --recreate-origin (state/bootstrap.json lost).
func setupBootstrap(fs *flag.FlagSet) runFunc {
	force := fs.Bool("force", false, "repair the state tables from a checkpoint at or above the cursor (stop stellar-ingest first)")
	historyOnly := fs.Bool("history-only", false, "seed only the cursor (dev/test): ingest_state('explorer', A-1, PreviousLedgerHash(A))")
	at := fs.Uint("at", 0, "with --history-only: first ledger to ingest")
	recreate := fs.Bool("recreate-origin", false, "recreate a lost state/bootstrap.json from Postgres")
	return func(ctx context.Context, env runEnv) error {
		n := 0
		for _, b := range []bool{*force, *historyOnly, *recreate} {
			if b {
				n++
			}
		}
		switch {
		case n > 1:
			return fmt.Errorf("%w: --force, --history-only and --recreate-origin are exclusive", errUsage)
		case *at != 0 && !*historyOnly:
			return fmt.Errorf("%w: --at goes with --history-only", errUsage)
		case *historyOnly && (*at < 2 || *at > math.MaxUint32):
			return fmt.Errorf("%w: --history-only needs --at <ledger ≥ 2>", errUsage)
		}
		cfg, log, err := loadConfig(env.configPath, env.stderr)
		if err != nil {
			return err
		}
		if err := requireDSN(cfg); err != nil {
			return err
		}
		ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
		defer stop()
		opts := bootstrap.Options{
			DSN: cfg.Postgres.DSN, Archives: cfg.Bootstrap.HistoryArchives, LakeDir: cfg.Lake.Dir,
			CheckpointPoll: cfg.Bootstrap.CheckpointPoll, Log: log,
			MaxBatchLedgers: cfg.Ingest.MaxBatchLedgers, MaxBatchBytes: int64(cfg.Ingest.MaxBatchMiB) << 20,
		}
		switch {
		case *recreate:
			origin, written, err := bootstrap.RecreateOrigin(ctx, opts)
			if err != nil {
				return err
			}
			verb := "kept the existing"
			if written {
				verb = "recreated"
			}
			fmt.Fprintf(env.stdout, "%s %s: checkpoint %d, archive_origin %d\n", verb, bootstrap.OriginFile(cfg.Lake.Dir), origin.Checkpoint, origin.ArchiveOrigin)
			return nil
		case *historyOnly:
			return historyOnlyBootstrap(ctx, cfg, log, uint32(*at), env)
		}
		srcs, err := ingest.OpenSources(cfg)
		if err != nil {
			return err
		}
		opts.Sources = srcs.InOrder()
		run := bootstrap.Run
		if *force {
			run = bootstrap.Force
		}
		res, err := run(ctx, opts)
		if err != nil {
			return err
		}
		if *force {
			fmt.Fprintf(env.stdout, "state repaired from checkpoint %d (hash %x); the explorer cursor moved from %d to %d", res.Checkpoint, res.Hash, res.PreviousCursor, res.Checkpoint)
			if res.HistoryLedgers > 0 {
				fmt.Fprintf(env.stdout, " after writing the ledger rows of %d ledgers from %s", res.HistoryLedgers, res.HistorySource)
			}
			fmt.Fprintln(env.stdout)
		} else {
			fmt.Fprintf(env.stdout, "state loaded from checkpoint %d (hash %x); the explorer cursor is at %d, ingest starts at %d\n",
				res.Checkpoint, res.Hash, res.Checkpoint, res.Checkpoint+1)
		}
		fmt.Fprintf(env.stdout, "rows %v; load %s, total %s\n", res.Entries,
			res.LoadDuration.Round(time.Millisecond), res.Duration.Round(time.Millisecond))
		verb := "kept"
		if res.OriginWritten {
			verb = "written"
		}
		fmt.Fprintf(env.stdout, "%s %s: archive_origin %d\n", bootstrap.OriginFile(cfg.Lake.Dir), verb, res.Origin.ArchiveOrigin)
		return nil
	}
}

func historyOnlyBootstrap(ctx context.Context, cfg config.Config, log *slog.Logger, at uint32, env runEnv) error {
	srcs, err := ingest.OpenSources(cfg)
	if err != nil {
		return err
	}
	res, err := bootstrap.HistoryOnly(ctx, bootstrap.HistoryOnlyOptions{
		DSN: cfg.Postgres.DSN, At: at, Sources: srcs.InOrder(), LakeDir: cfg.Lake.Dir, Log: log,
	})
	if err != nil {
		return err
	}
	fmt.Fprintf(env.stdout, "explorer cursor seeded at %d (hash %x, read from %s); ingest starts at %d\n",
		res.Cursor, res.Hash, res.Source, res.Cursor+1)
	return nil
}
