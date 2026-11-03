package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"

	"stellar-explorer/worker/archive"
	"stellar-explorer/worker/config"
	"stellar-explorer/worker/sep54"
)

// setupArchiveBackfill wires `worker archive-backfill`: it copies ledgers
// [--from, --to] from the AWS public lake into the archive, verified and
// chained to ledger --to+1, which the archive must already hold. It reads
// the archive's endpoint and key from archive.env, so it can run on any
// host that has them (the clickhouse host, whose IP doesn't share the
// archiver's request budget at the store).
func setupArchiveBackfill(fs *flag.FlagSet) runFunc {
	from := fs.Uint("from", 0, "first ledger to copy")
	to := fs.Uint("to", 0, "last ledger to copy: one below the archive's oldest (its archive_origin)")
	workers := fs.Int("workers", 128, "ledgers in flight")
	rps := fs.Float64("rate", 500, "archive requests per second, at most (Hetzner allows 750 per bucket and per IP)")
	state := fs.String("state", "/var/lib/stellar-archive-backfill/state.json", "progress file: finished partitions are skipped on a rerun")
	dryRun := fs.Bool("dry-run", false, "only read and check the source: no uploads")
	return func(ctx context.Context, env runEnv) error {
		if *from < 2 || *to < *from || *to >= math.MaxUint32 {
			return fmt.Errorf("%w: need --from A --to B with 2 ≤ A ≤ B", errUsage)
		}
		if *workers < 1 || *rps < 0 {
			return fmt.Errorf("%w: --workers must be ≥ 1 and --rate ≥ 0", errUsage)
		}
		cfg, log, err := loadConfig(env.configPath, env.stderr)
		if err != nil {
			return err
		}
		creds, err := config.LoadArchiveEnv(cfg.Credentials.ArchiveEnv)
		if err != nil {
			return err
		}
		ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		// One connection per worker on each side, kept open: the SDK's
		// default keeps 10 per host and would reconnect constantly.
		httpClient := func() *awshttp.BuildableClient {
			return awshttp.NewBuildableClient().WithTimeout(2 * time.Minute).WithTransportOptions(func(t *http.Transport) {
				t.MaxIdleConns, t.MaxIdleConnsPerHost = 2**workers, *workers
			})
		}
		// One attempt per request: a SlowDown must reach the backfill at
		// once to slow it down, not wait in the SDK's retries.
		opts := sep54.ArchiveClientOptions(cfg.Archive.PathStyle, creds)
		opts.HTTPClient, opts.RetryMaxAttempts = httpClient(), 1
		arch, err := sep54.Open(ctx, sep54.SourceArchive, sep54.NewS3Client(opts), cfg.Archive.BucketPath(cfg.Lake))
		if err != nil {
			return fmt.Errorf("open the archive: %w", err)
		}
		src, err := sep54.Open(ctx, sep54.SourceAWS, sep54.NewS3Client(sep54.ClientOptions{
			Region: sep54.AWSLakeRegion, Credentials: aws.AnonymousCredentials{}, HTTPClient: httpClient(),
		}), sep54.AWSLakeBucketPath)
		if err != nil {
			return fmt.Errorf("open the AWS lake: %w", err)
		}

		st, err := archive.Backfill(ctx, archive.BackfillOptions{
			Source: src, Archive: arch, From: uint32(*from), To: uint32(*to),
			Workers: *workers, Rate: *rps, State: *state, DryRun: *dryRun, Log: log,
		})
		fmt.Fprintf(env.stdout, "partitions %d (%d done before), created %d, existed %d, differed %d, checked %d, %.1f GiB, throttled %d\n",
			st.Partitions, st.Skipped, st.Created, st.Existed, st.Differed, st.Checked, float64(st.Bytes)/(1<<30), st.Throttled)
		return err
	}
}
