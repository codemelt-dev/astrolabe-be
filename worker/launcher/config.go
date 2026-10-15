package launcher

import (
	"context"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"stellar-explorer/worker/bootstrap"
	"stellar-explorer/worker/config"
	"stellar-explorer/worker/sep54"
)

// NewOptions wires Compute to the configured lake and archive: the gateway
// keys (parsed from gateway.env) for the lake; the uploader key (parsed
// from archive.env, nil when archiving is disabled) for a dedicated
// archive client with a single attempt.
func NewOptions(cfg config.Config, gw config.GatewayCredentials, archive *config.ArchiveCredentials, log *slog.Logger) Options {
	lakeClient := sep54.NewS3Client(sep54.ClientOptions{
		Endpoint:         "http://" + cfg.Lake.S3Listen,
		Region:           cfg.Lake.Region,
		Credentials:      credentials.NewStaticCredentialsProvider(gw.AccessKey, gw.SecretKey, ""),
		PathStyle:        true,
		RetryMaxAttempts: 1,
		Timeout:          5 * time.Second,
	})
	tip := sep54.NewTipClient(cfg.Lake.ControlListen)
	o := Options{
		LakeS3: func(ctx context.Context) error {
			prefix := cfg.Lake.Prefix()
			if prefix != "" {
				prefix += "/"
			}
			_, err := lakeClient.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
				Bucket: aws.String(cfg.Lake.Bucket()), Prefix: aws.String(prefix), MaxKeys: aws.Int32(1)})
			return err
		},
		LakeTip: tip.Get,
		Origin:  func() (bootstrap.Origin, error) { return bootstrap.ReadOrigin(cfg.Lake.Dir) },
		Log:     log,
	}
	if archive != nil {
		opts := sep54.ArchiveClientOptions(cfg.Archive.PathStyle, *archive)
		opts.RetryMaxAttempts = 1
		opts.Timeout = ProbeTimeout
		client := sep54.NewS3Client(opts)
		o.ArchiveTip = func(ctx context.Context) (uint32, error) {
			src, err := sep54.Open(ctx, sep54.SourceArchive, client, cfg.Archive.BucketPath(cfg.Lake))
			if err != nil {
				return 0, err
			}
			return src.Newest(ctx)
		}
	}
	return o
}
