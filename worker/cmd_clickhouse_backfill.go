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

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/stellar/go-stellar-sdk/network"

	"stellar-explorer/worker/clickhouse"
	"stellar-explorer/worker/sep54"
)

// setupClickhouseBackfill wires `worker clickhouse-backfill`: the history
// of the ledgers below go-live (clickhouse.first_ledger), from the AWS
// public lake, into the tables the live pipeline writes
// (clickhouse.Backfill). With --trades-only it fills the trades of the
// ledgers the live pipeline wrote before it knew them.
func setupClickhouseBackfill(fs *flag.FlagSet) runFunc {
	from := fs.Uint("from", 0, "first ledger (default: 2; with --trades-only, clickhouse.first_ledger)")
	to := fs.Uint("to", 0, "last ledger (default: clickhouse.first_ledger − 1; with --trades-only, one below the live pipeline's first trade)")
	segment := fs.Uint("segment", 1_000_000, "ledgers per segment, a multiple of 1,000,000")
	parallel := fs.Int("parallel", 2, "segments running at once")
	tradesOnly := fs.Bool("trades-only", false, "write only the trades, of ledgers from clickhouse.first_ledger on")
	batchLedgers := fs.Int("batch-ledgers", 2000, "ledgers per commit of a segment, at most (the early years' ledgers are nearly empty)")
	batchMiB := fs.Int("batch-mib", 128, "MiB of rows per commit of a segment, at most")
	readWorkers := fs.Int("read-workers", 32, "ledgers a segment downloads at once")
	verifyEvery := fs.Uint("verify-every", 1000, "check the transfers and the trades of every ledger whose sequence is a multiple of this (0: none)")
	return func(ctx context.Context, env runEnv) error {
		if *from > math.MaxUint32 || *to > math.MaxUint32 || *segment > math.MaxUint32 || *verifyEvery > math.MaxUint32 ||
			*parallel < 1 || *batchLedgers < 1 || *batchMiB < 1 || *readWorkers < 1 || *readWorkers > 256 {
			return fmt.Errorf("%w: bad flags", errUsage)
		}
		cfg, log, err := loadConfig(env.configPath, env.stderr)
		if err != nil {
			return err
		}
		first := cfg.ClickHouse.FirstLedger
		if first < 2 {
			return fmt.Errorf("%w: set clickhouse.first_ledger (the explorer's go-live)", errUsage)
		}
		ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
		defer stop()
		open := func(ctx context.Context) (driver.Conn, error) { return clickhouse.Open(ctx, cfg.ClickHouse, false) }

		a, b := uint32(*from), uint32(*to)
		switch {
		case *tradesOnly:
			if a == 0 {
				a = first
			}
			if b == 0 {
				conn, err := open(ctx)
				if err != nil {
					return err
				}
				b, err = clickhouse.TradesFillEnd(ctx, conn, first)
				conn.Close()
				if err != nil {
					return err
				}
			}
		default:
			if a == 0 {
				a = 2
			}
			if b == 0 {
				b = first - 1
			}
		}

		// Each segment's catch-up reader keeps --read-workers downloads
		// going: keep the connections open instead of the SDK's 10 per host.
		conns := (*readWorkers + 8) * *parallel
		src, err := sep54.Open(ctx, sep54.SourceAWS, sep54.NewS3Client(sep54.ClientOptions{
			Region: sep54.AWSLakeRegion, Credentials: aws.AnonymousCredentials{},
			HTTPClient: awshttp.NewBuildableClient().WithTimeout(2 * time.Minute).WithTransportOptions(func(t *http.Transport) {
				t.MaxIdleConns, t.MaxIdleConnsPerHost = conns, conns
			}),
		}), sep54.AWSLakeBucketPath)
		if err != nil {
			return fmt.Errorf("open the AWS lake: %w", err)
		}

		log.Info("starting clickhouse-backfill", "version", version, "from", a, "to", b, "first_ledger", first)
		res, err := clickhouse.Backfill(ctx, clickhouse.BackfillOptions{
			Open: open, Source: src, From: a, To: b, Segment: uint32(*segment), Parallel: *parallel, FirstLedger: first,
			TradesOnly: *tradesOnly, MaxBatchLedgers: *batchLedgers, MaxBatchBytes: int64(*batchMiB) << 20, ReadWorkers: *readWorkers,
			VerifyEvery: uint32(*verifyEvery), Passphrase: network.PublicNetworkPassphrase, Log: log,
		})
		fmt.Fprintf(env.stdout, "segments %d: %d done before, %d done now; %d ledgers committed\n", res.Segments, res.DoneBefore, res.Done, res.Ledgers)
		return err
	}
}

// setupClickhouseDailyStats wires `worker clickhouse-daily-stats`: it
// computes daily_stats for a range of days once, after a backfill (the
// view only recomputes the two newest days).
func setupClickhouseDailyStats(fs *flag.FlagSet) runFunc {
	from := fs.String("from", "", "first day, YYYY-MM-DD (default: the oldest ledger's)")
	to := fs.String("to", "", "last day, YYYY-MM-DD (default: the day of clickhouse.first_ledger, whose earlier ledgers a backfill added)")
	return func(ctx context.Context, env runEnv) error {
		cfg, log, err := loadConfig(env.configPath, env.stderr)
		if err != nil {
			return err
		}
		ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
		defer stop()
		conn, err := clickhouse.Open(ctx, cfg.ClickHouse, false)
		if err != nil {
			return err
		}
		defer conn.Close()
		day := func(flagValue, query string, args ...any) (time.Time, error) {
			if flagValue != "" {
				d, err := time.Parse(time.DateOnly, flagValue)
				if err != nil {
					return d, fmt.Errorf("%w: %v", errUsage, err)
				}
				return d, nil
			}
			var d time.Time
			err := conn.QueryRow(ctx, query, args...).Scan(&d)
			return d, err
		}
		a, err := day(*from, "SELECT toDate(min(closed_at)) FROM ledgers")
		if err != nil {
			return err
		}
		b, err := day(*to, "SELECT toDate(closed_at) FROM ledgers WHERE sequence = ?", cfg.ClickHouse.FirstLedger)
		if err != nil {
			return fmt.Errorf("the day of clickhouse.first_ledger: %w", err)
		}
		if err := clickhouse.RecomputeDailyStats(ctx, conn, a, b, log); err != nil {
			return err
		}
		fmt.Fprintf(env.stdout, "daily_stats computed from %s to %s\n", a.Format(time.DateOnly), b.Format(time.DateOnly))
		return nil
	}
}
