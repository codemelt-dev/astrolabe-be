// Command worker is the Galexie-host worker. One binary is installed
// twice, as versioned builds behind two symlinks: `worker-lake` runs lake,
// galexie-start and watchdog, and `worker` runs everything else, so an
// ingest deploy never touches the lake (galexie/README.md §7.2).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/stellar/go-stellar-sdk/network"

	"stellar-explorer/worker/archive"
	"stellar-explorer/worker/config"
	"stellar-explorer/worker/devfeed"
	"stellar-explorer/worker/lake"
	"stellar-explorer/worker/sep54"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

// errUsage marks command-line errors (exit status 2).
var errUsage = errors.New("usage error")

type command struct {
	name    string
	summary string
	// setup registers the command's own flags (besides --config) and
	// returns the function that runs the command with their parsed values.
	setup func(fs *flag.FlagSet) runFunc
}

// runEnv is what every command gets besides its own flags.
type runEnv struct {
	configPath     string
	stdout, stderr io.Writer
}

type runFunc func(ctx context.Context, env runEnv) error

func commands() []command {
	return []command{
		{name: "lake", summary: "Galexie's S3 endpoint: embedded VersityGW, durability chain, commit tip (run from worker-lake)",
			setup: func(*flag.FlagSet) runFunc { return runLake }},
		{name: "ingest", summary: "sequencer(s), archiver, pruner, metrics",
			setup: func(*flag.FlagSet) runFunc { return runIngest }},
		{name: "galexie-start", summary: "systemd ExecStartPre for Galexie: compute --start and write START=… (run from worker-lake)",
			setup: setupGalexieStart},
		{name: "watchdog", summary: "restart Galexie only if its upload is hung (timer, run from worker-lake)",
			setup: func(*flag.FlagSet) runFunc { return runWatchdog }},
		{name: "gapcheck", summary: "run Galexie's detect-gaps over [archive_origin, archive_tip] (weekly timer)",
			setup: func(*flag.FlagSet) runFunc { return runGapcheck }},
		{name: "bootstrap", summary: "load current state from a history-archive checkpoint and seed the cursor",
			setup: setupBootstrap},
		{name: "migrate", summary: "apply the embedded SQL migrations (--check: only list pending ones)",
			setup: setupMigrate},
		{name: "reindex", summary: "re-read a closed range and replace its ledger rows (--dry-run: only validate a source)",
			setup: setupReindex},
		{name: "dev-feed", summary: "dev/test only: behave like Galexie against the lake, copying from the AWS public lake",
			setup: setupDevFeed},
		{name: "smoke-archive", summary: "S3 compatibility test against a scratch bucket at the archive provider",
			setup: setupSmokeArchive},
		{name: "clickhouse-migrate", summary: "clickhouse host: apply the ClickHouse schema (--check: only list pending migrations)",
			setup: setupClickhouseMigrate},
		{name: "clickhouse-init", summary: "clickhouse host: seed the cursor before the archive's oldest ledger, once (--check: show it)",
			setup: setupClickhouseInit},
		{name: "clickhouse-ingest", summary: "clickhouse host: the clickhouse pipeline, archive → ClickHouse, with metrics",
			setup: func(*flag.FlagSet) runFunc { return runClickhouseIngest }},
		{name: "clickhouse-check", summary: "clickhouse host: a range's consistency (default: the cursor's last 10,000 ledgers)",
			setup: setupClickhouseCheck},
		{name: "state-copy", summary: "psql host: copy the current state into ClickHouse, compute assets and top holders (--snapshot: and the day's snapshots)",
			setup: setupStateCopy},
	}
}

func main() {
	os.Exit(run(context.Background(), os.Args[1:], os.Stdout, os.Stderr))
}

// run executes one subcommand and returns the process exit status.
func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		usage(stderr)
		return 2
	}
	name, args := args[0], args[1:]
	switch name {
	case "help", "-h", "-help", "--help":
		usage(stdout)
		return 0
	case "version", "--version":
		fmt.Fprintln(stdout, "worker", version)
		return 0
	}
	for _, c := range commands() {
		if c.name != name {
			continue
		}
		fs := flag.NewFlagSet("worker "+name, flag.ContinueOnError)
		fs.SetOutput(stderr)
		configPath := fs.String("config", config.DefaultPath, "config file")
		runCmd := c.setup(fs)
		if err := fs.Parse(args); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return 0
			}
			return 2
		}
		if fs.NArg() > 0 {
			fmt.Fprintf(stderr, "worker %s: unexpected arguments: %v\n", name, fs.Args())
			return 2
		}
		if err := runCmd(ctx, runEnv{configPath: *configPath, stdout: stdout, stderr: stderr}); err != nil {
			fmt.Fprintf(stderr, "worker %s: %v\n", name, err)
			if errors.Is(err, errUsage) {
				return 2
			}
			return 1
		}
		return 0
	}
	fmt.Fprintf(stderr, "worker: unknown command %q\n\n", name)
	usage(stderr)
	return 2
}

func usage(w io.Writer) {
	fmt.Fprintf(w, "usage: worker <command> [--config %s] [flags]\n\ncommands:\n", config.DefaultPath)
	for _, c := range commands() {
		fmt.Fprintf(w, "  %-18s %s\n", c.name, c.summary)
	}
	fmt.Fprintf(w, "  %-18s %s\n", "version", "print the build version")
}

// loadConfig loads the config and builds the process logger.
func loadConfig(path string, stderr io.Writer) (config.Config, *slog.Logger, error) {
	cfg, err := config.Load(path)
	if err != nil {
		return config.Config{}, nil, err
	}
	level, err := cfg.SlogLevel()
	if err != nil {
		return config.Config{}, nil, err
	}
	log := slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(log)
	if vars := config.AWSEnvVars(); len(vars) > 0 {
		log.Warn("AWS_* variables are set in the environment; the worker ignores them (credentials are read from files by path)", "vars", vars)
	}
	return cfg, log, nil
}

func runLake(ctx context.Context, env runEnv) error {
	cfg, log, err := loadConfig(env.configPath, env.stderr)
	if err != nil {
		return err
	}
	creds, err := config.LoadGatewayEnv(cfg.Credentials.GatewayEnv)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log.Info("starting lake", "version", version, "dir", cfg.Lake.Dir, "bucket_path", cfg.Lake.BucketPath)
	l, err := lake.Open(cfg.Lake, creds, lake.Options{Logger: log})
	if err != nil {
		return err
	}
	return l.Serve(ctx)
}

// setupDevFeed wires `worker dev-feed`, the stand-in for Galexie in
// development and tests.
func setupDevFeed(fs *flag.FlagSet) runFunc {
	start := fs.Uint("start", 0, "first ledger to publish, like Galexie's --start (required; resumes at max(start, newest+1))")
	end := fs.Uint("end", 0, "last ledger to publish (0: follow the AWS lake tip forever)")
	speed := fs.Float64("speed", 1, "pace by the ledgers' close times: 1 = ledger cadence, 10 = ten times faster, 0 = as fast as possible")
	return func(ctx context.Context, env runEnv) error {
		if *start == 0 {
			return fmt.Errorf("%w: --start is required", errUsage)
		}
		if *start > math.MaxUint32 || *end > math.MaxUint32 {
			return fmt.Errorf("%w: --start and --end must be ledger sequences (uint32)", errUsage)
		}
		cfg, log, err := loadConfig(env.configPath, env.stderr)
		if err != nil {
			return err
		}
		if err := devfeed.CheckArchive(cfg.Archive); err != nil {
			return err
		}
		creds, err := config.LoadGatewayEnv(cfg.Credentials.GatewayEnv)
		if err != nil {
			return err
		}
		ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
		defer stop()
		lakeSrc, err := sep54.OpenLake(ctx, cfg.Lake, creds)
		if err != nil {
			return err
		}
		aws, err := sep54.OpenAWSLake(ctx)
		if err != nil {
			return err
		}
		if _, err := aws.LoadSchema(ctx, network.PublicNetworkPassphrase); err != nil {
			return fmt.Errorf("AWS lake manifest: %w", err)
		}
		log.Info("starting dev-feed", "version", version, "start", *start, "end", *end, "speed", *speed,
			"lake", cfg.Lake.S3Listen, "bucket_path", cfg.Lake.BucketPath, "upstream", sep54.AWSLakeBucketPath)
		res, err := devfeed.Run(ctx, devfeed.Options{
			Upstream:   aws,
			Lake:       lakeSrc.DataStore(),
			Passphrase: network.PublicNetworkPassphrase,
			Start:      uint32(*start),
			End:        uint32(*end),
			Speed:      *speed,
			Log:        log,
		})
		log.Info("dev-feed stopped", "from", res.From, "last", res.Last, "uploaded", res.Uploaded, "already_present", res.Existed)
		return err
	}
}

// setupSmokeArchive wires `worker smoke-archive`, the archive provider's
// compatibility test (galexie/README.md §3d).
func setupSmokeArchive(fs *flag.FlagSet) runFunc {
	bucket := fs.String("bucket", "", "scratch bucket carrying the archive policy (never the archive bucket)")
	probeOnly := fs.Bool("delete-probe-only", false, "only check that deleting a non-existent key is refused (safe on the archive bucket)")
	return func(ctx context.Context, env runEnv) error {
		if *bucket == "" {
			return fmt.Errorf("%w: --bucket is required", errUsage)
		}
		cfg, log, err := loadConfig(env.configPath, env.stderr)
		if err != nil {
			return err
		}
		if *bucket == cfg.Archive.Bucket && !*probeOnly {
			return fmt.Errorf("refusing to run against the archive bucket %q: use a scratch bucket, or --delete-probe-only", *bucket)
		}
		creds, err := config.LoadArchiveEnv(cfg.Credentials.ArchiveEnv)
		if err != nil {
			return err
		}
		ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
		defer stop()
		checks := archive.Smoke(ctx, archive.SmokeOptions{
			Creds:           creds,
			Bucket:          *bucket,
			PathStyle:       cfg.Archive.PathStyle,
			DeleteProbeOnly: *probeOnly,
			Log:             log,
		})
		for _, c := range checks {
			status := "PASS"
			if !c.Pass {
				status = "FAIL"
			}
			fmt.Fprintf(env.stdout, "%s  %-26s %s\n", status, c.Name, c.Detail)
		}
		if failed := archive.Failed(checks); len(failed) > 0 {
			return fmt.Errorf("%d of %d checks failed: %s", len(failed), len(checks), strings.Join(failed, ", "))
		}
		fmt.Fprintf(env.stdout, "all %d checks passed\n", len(checks))
		return nil
	}
}
