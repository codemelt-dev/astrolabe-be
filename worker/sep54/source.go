package sep54

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stellar/go-stellar-sdk/support/datastore"

	"stellar-explorer/worker/config"
)

// The AWS Open Data public lake: anonymous, us-east-2,
// current up to about 4 s behind the network. dev-feed copies from it and it
// is the sequencer's last-resort source.
const (
	AWSLakeBucketPath = "aws-public-blockchain/v1.1/stellar/ledgers/pubnet"
	AWSLakeRegion     = "us-east-2"
)

// Source names, as used in logs, metrics and ledgers.source.
const (
	SourceLake    = "lake"
	SourceArchive = "archive"
	SourceAWS     = "aws"
)

// MetadataKeys are the 9 x-amz-meta-* keys Galexie writes on every ledger
// object. They are unauthenticated and never load-bearing; they
// are copied verbatim.
var MetadataKeys = [...]string{
	"start-ledger", "end-ledger", "start-ledger-close-time", "end-ledger-close-time",
	"protocol-version", "core-version", "network-passphrase", "compression-type", "version",
}

// ClientOptions describe one explicit S3 client. Every source has its own
// client, built with s3.New from these values only: never
// config.LoadDefaultConfig, so neither the process environment (AWS_*),
// shared config files nor instance metadata can change which endpoint or key
// a source uses.
type ClientOptions struct {
	// Endpoint is the base URL; empty means AWS's regional endpoint.
	Endpoint string
	Region   string
	// Credentials are static keys or aws.AnonymousCredentials{}.
	Credentials aws.CredentialsProvider
	PathStyle   bool
	// RetryMaxAttempts is the SDK's attempts per request (0: SDK default 3).
	RetryMaxAttempts int
	// Timeout bounds one HTTP attempt, including reading the body.
	Timeout time.Duration
	// HTTPClient replaces the SDK's HTTP client (tests: a private CA).
	HTTPClient s3.HTTPClient
}

// NewS3Client builds an explicit S3 client (see ClientOptions).
func NewS3Client(o ClientOptions) *s3.Client {
	var hc s3.HTTPClient = awshttp.NewBuildableClient().WithTimeout(o.Timeout)
	if o.HTTPClient != nil {
		hc = o.HTTPClient
	}
	return s3.New(s3.Options{
		Region:           o.Region,
		Credentials:      o.Credentials,
		BaseEndpoint:     endpointPtr(o.Endpoint),
		UsePathStyle:     o.PathStyle,
		RetryMaxAttempts: o.RetryMaxAttempts,
		HTTPClient:       hc,
		// s3.New leaves these unset, which silently turns off the SDK's
		// default checksums; set what config.LoadDefaultConfig (and so
		// Galexie's client) defaults to: a checksum on every PUT (an
		// aws-chunked trailer over HTTPS) and validation on every GET.
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenSupported,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenSupported,
		// Multipart objects carry composite checksums the SDK cannot
		// validate; we verify CRC32C ourselves where it matters.
		DisableLogOutputChecksumValidationSkipped: true,
	})
}

func endpointPtr(e string) *string {
	if e == "" {
		return nil
	}
	return aws.String(e)
}

// LakeClient is the lake's client: the gateway root keys parsed from
// gateway.env, path-style, on the loopback S3 listener.
func LakeClient(cfg config.Lake, creds config.GatewayCredentials) *s3.Client {
	return NewS3Client(ClientOptions{
		Endpoint:    "http://" + cfg.S3Listen,
		Region:      cfg.Region,
		Credentials: credentials.NewStaticCredentialsProvider(creds.AccessKey, creds.SecretKey, ""),
		PathStyle:   true,
		Timeout:     30 * time.Second,
	})
}

// ArchiveClient is the archive's client: the uploader key and endpoint from
// archive.env, virtual-hosted unless archive.path_style, about 10 attempts
// per request.
func ArchiveClient(cfg config.Archive, creds config.ArchiveCredentials) *s3.Client {
	return NewS3Client(ArchiveClientOptions(cfg.PathStyle, creds))
}

// ArchiveClientOptions are the archive client's options with the given
// addressing style.
func ArchiveClientOptions(pathStyle bool, creds config.ArchiveCredentials) ClientOptions {
	return ClientOptions{
		Endpoint:         creds.Endpoint,
		Region:           creds.Region,
		Credentials:      credentials.NewStaticCredentialsProvider(creds.AccessKey, creds.SecretKey, ""),
		PathStyle:        pathStyle,
		RetryMaxAttempts: 10,
		Timeout:          2 * time.Minute,
	}
}

// AWSLakeClient is the AWS public lake's client: anonymous, us-east-2.
func AWSLakeClient() *s3.Client {
	return NewS3Client(ClientOptions{
		Region:      AWSLakeRegion,
		Credentials: aws.AnonymousCredentials{},
		Timeout:     2 * time.Minute,
	})
}

// Source is one SEP-54 store (lake, archive or AWS lake) with its own
// explicit client. DataStore wraps the same client with
// datastore.FromS3Client for the SDK helpers (BufferedStorageBackend,
// LoadSchema, PutFileIfNotExists).
type Source struct {
	name   string
	client *s3.Client
	bucket string
	prefix string
	ds     datastore.DataStore
}

// Open wraps client as a Source for bucketPath ("<bucket>/<prefix>").
// datastore.FromS3Client lists the bucket once, so an unreachable store,
// a missing bucket or refused credentials fail here.
func Open(ctx context.Context, name string, client *s3.Client, bucketPath string) (*Source, error) {
	bucket, prefix, err := config.SplitBucketPath(bucketPath)
	if err != nil {
		return nil, fmt.Errorf("%s: bucket path: %w", name, err)
	}
	ds, err := datastore.FromS3Client(ctx, client, bucketPath)
	if err != nil {
		if IsUnavailable(err) {
			return nil, &UnavailableError{Op: name, Err: err}
		}
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	return &Source{name: name, client: client, bucket: bucket, prefix: prefix, ds: ds}, nil
}

// OpenLake opens the lake as a source.
func OpenLake(ctx context.Context, cfg config.Lake, creds config.GatewayCredentials) (*Source, error) {
	return Open(ctx, SourceLake, LakeClient(cfg, creds), cfg.BucketPath)
}

// OpenArchive opens the archive bucket as a source.
func OpenArchive(ctx context.Context, cfg config.Archive, lake config.Lake, creds config.ArchiveCredentials) (*Source, error) {
	return Open(ctx, SourceArchive, ArchiveClient(cfg, creds), cfg.BucketPath(lake))
}

// OpenAWSLake opens the AWS public lake as a source.
func OpenAWSLake(ctx context.Context) (*Source, error) {
	return Open(ctx, SourceAWS, AWSLakeClient(), AWSLakeBucketPath)
}

func (s *Source) Name() string                   { return s.name }
func (s *Source) Client() *s3.Client             { return s.client }
func (s *Source) DataStore() datastore.DataStore { return s.ds }
func (s *Source) Bucket() string                 { return s.bucket }
func (s *Source) Prefix() string                 { return s.prefix }

// Key returns the bucket-relative key of a store-relative path.
func (s *Source) Key(rel string) string {
	if s.prefix == "" {
		return rel
	}
	return s.prefix + "/" + rel
}

// Object is one object read from a store, with everything the archiver
// copies 1:1.
type Object struct {
	// Key is relative to the store prefix.
	Key  string
	Body []byte
	// Metadata are the x-amz-meta-* values, keyed without the prefix.
	Metadata    map[string]string
	ContentType string
	ETag        string
	// ChecksumCRC32C is the stored base64 CRC32C; empty if the store
	// returned none.
	ChecksumCRC32C string
	LastModified   time.Time
}

// LedgerMetadata returns the 9 Galexie metadata keys of o, and an error if
// any is missing.
func (o Object) LedgerMetadata() (map[string]string, error) {
	md := make(map[string]string, len(MetadataKeys))
	var missing []string
	for _, k := range MetadataKeys {
		v, ok := o.Metadata[k]
		if !ok {
			missing = append(missing, k)
			continue
		}
		md[k] = v
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("%s: missing metadata %s", o.Key, strings.Join(missing, ", "))
	}
	return md, nil
}

// GetObject reads a whole object with ChecksumMode enabled, so the SDK
// verifies a stored full-object checksum while reading. A missing key is
// ErrNotFound; transport failures and 5xx are *UnavailableError.
func (s *Source) GetObject(ctx context.Context, rel string) (Object, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket:       aws.String(s.bucket),
		Key:          aws.String(s.Key(rel)),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		return Object{}, s.wrap(rel, err)
	}
	defer out.Body.Close()
	body, err := io.ReadAll(io.LimitReader(out.Body, MaxObjectSize+1))
	if err != nil {
		return Object{}, s.wrap(rel, err)
	}
	if len(body) > MaxObjectSize {
		return Object{}, fmt.Errorf("%s: %s is larger than %d bytes", s.name, rel, MaxObjectSize)
	}
	if out.ContentLength != nil && *out.ContentLength != int64(len(body)) {
		return Object{}, &UnavailableError{Op: s.name + " get " + rel,
			Err: fmt.Errorf("short body: %d of %d bytes", len(body), *out.ContentLength)}
	}
	return Object{
		Key:            rel,
		Body:           body,
		Metadata:       maps.Clone(out.Metadata),
		ContentType:    aws.ToString(out.ContentType),
		ETag:           aws.ToString(out.ETag),
		ChecksumCRC32C: aws.ToString(out.ChecksumCRC32C),
		LastModified:   aws.ToTime(out.LastModified),
	}, nil
}

// GetLedger reads the object of ledger seq.
func (s *Source) GetLedger(ctx context.Context, seq uint32) (Object, error) {
	return s.GetObject(ctx, ObjectPath(seq))
}

// Newest returns the newest ledger in the store, from the first canonical
// ledger key of an ascending listing (partitions and files use reverse hex,
// so the newest sorts first). ErrNoLedgers means the store has none. Like
// Galexie's resume, it does not look for holes.
func (s *Source) Newest(ctx context.Context) (uint32, error) {
	seq, _, err := s.NewestObject(ctx)
	return seq, err
}

// NewestObject is Newest with the object's LastModified, when its upload
// finished.
func (s *Source) NewestObject(ctx context.Context) (uint32, time.Time, error) {
	prefix := ""
	if s.prefix != "" {
		prefix = s.prefix + "/"
	}
	p := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket:  aws.String(s.bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int32(16),
	})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return 0, time.Time{}, s.wrap("list", err)
		}
		for _, o := range page.Contents {
			if seq, ok := ParseKey(s.prefix, aws.ToString(o.Key)); ok {
				return seq, aws.ToTime(o.LastModified), nil
			}
		}
	}
	return 0, time.Time{}, fmt.Errorf("%s: %w", s.name, ErrNoLedgers)
}

// partitionDir is a partition's directory under the store prefix.
var partitionDir = regexp.MustCompile(`^[0-9A-F]{8}--[0-9]+-[0-9]+/$`)

// Oldest returns the store's oldest ledger. Keys sort newest first, so it
// is the last key of the last partition: Oldest lists every partition and
// then that partition's keys, for one-off use (`worker clickhouse-init`).
func (s *Source) Oldest(ctx context.Context) (uint32, error) {
	prefix := ""
	if s.prefix != "" {
		prefix = s.prefix + "/"
	}
	last := ""
	p := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket), Prefix: aws.String(prefix), Delimiter: aws.String("/"),
	})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return 0, s.wrap("list", err)
		}
		for _, cp := range page.CommonPrefixes {
			if dir := aws.ToString(cp.Prefix); partitionDir.MatchString(strings.TrimPrefix(dir, prefix)) {
				last = dir
			}
		}
	}
	if last == "" {
		return 0, fmt.Errorf("%s: %w", s.name, ErrNoLedgers)
	}
	var oldest uint32
	found := false
	p = s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{Bucket: aws.String(s.bucket), Prefix: aws.String(last)})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return 0, s.wrap("list", err)
		}
		for _, o := range page.Contents {
			if seq, ok := ParseKey(s.prefix, aws.ToString(o.Key)); ok {
				oldest, found = seq, true
			}
		}
	}
	if !found {
		return 0, fmt.Errorf("%s: %w (partition %s is empty)", s.name, ErrNoLedgers, last)
	}
	return oldest, nil
}

// LoadSchema checks the store's manifest against our layout on the given
// network (see the package-level LoadSchema).
func (s *Source) LoadSchema(ctx context.Context, passphrase string) (datastore.DataStoreSchema, error) {
	schema, err := LoadSchema(ctx, s.ds, passphrase)
	if err != nil {
		return schema, s.wrap(ManifestFile, err)
	}
	return schema, nil
}

// wrap classifies an S3 error of this source. A cancelled context passes
// through unchanged.
func (s *Source) wrap(rel string, err error) error {
	op := s.name + " " + rel
	switch {
	case errors.Is(err, context.Canceled):
		return err
	case isNotFound(err):
		return fmt.Errorf("%s: %w", op, ErrNotFound)
	case IsUnavailable(err):
		return &UnavailableError{Op: op, Err: err}
	default:
		return fmt.Errorf("%s: %w", op, err)
	}
}
