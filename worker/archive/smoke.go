// Package archive copies the lake to the archive bucket (the archiver, M6)
// and checks that the archive provider is compatible (smoke.go, M2).
package archive

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"math/rand/v2"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/support/datastore"

	"stellar-explorer/worker/config"
	"stellar-explorer/worker/sep54"
)

// SmokeOptions configure `worker smoke-archive` (galexie/README.md §3d).
type SmokeOptions struct {
	// Creds are the uploader key and endpoint from archive.env.
	Creds config.ArchiveCredentials
	// Bucket is a scratch bucket carrying the archive bucket's policy.
	Bucket string
	// PathStyle is the addressing the archiver is configured with
	// (archive.path_style). Both styles are exercised either way.
	PathStyle bool
	// DeleteProbeOnly runs only the check that is safe on the real archive
	// bucket: deleting a non-existent key is refused.
	DeleteProbeOnly bool
	// Prefix is where the test objects go; default smoke-archive/<time>-<rand>.
	Prefix string
	Log    *slog.Logger
	// HTTPClient replaces the SDK's HTTP client (tests: a private CA).
	HTTPClient s3.HTTPClient
}

// Check is the outcome of one smoke check.
type Check struct {
	Name   string
	Pass   bool
	Detail string
}

// Smoke checks that the archive provider supports what the archiver,
// Galexie's SDK code and detect-gaps rely on, using the uploader key:
//
//   - list: the bucket is reachable with the uploader key (and so, when the
//     key belongs to the galexie project, cross-project access works);
//   - put-sdk-default-checksum: a PUT with the SDK's default checksum;
//   - put-crc32c: the archiver's PUT (CRC32C, trailer over HTTPS,
//     If-None-Match: *, 9 metadata keys, Content-Type);
//   - etag-md5: a single-part ETag is the MD5 of the bytes (the archiver's
//     412 check compares it);
//   - get-checksum-mode / head: ChecksumMode reads return the bytes, the
//     metadata and the Content-Type, and the stored CRC32C or, where the
//     store keeps none (Hetzner), an ETag = MD5 (sep54.VerifyStored);
//   - conditional-put-412: If-None-Match: * on an existing key is a 412 and
//     the object is unchanged, also through the SDK's PutFileIfNotExists;
//   - list-start-after: StartAfter and pagination list SEP-54 keys in order;
//   - sdk-datastore: FromS3Client, PublishConfig, LoadSchema and
//     FindLatestLedgerSequence work against the provider;
//   - delete-denied: the uploader cannot delete an object, nor a missing key;
//   - path-style / virtual-hosted: authenticated reads with both addressing
//     styles (detect-gaps uses path-style).
//
// Objects are left in the scratch bucket (the uploader cannot delete them);
// empty and delete it with the admin key afterwards.
func Smoke(ctx context.Context, o SmokeOptions) []Check {
	if o.Log == nil {
		o.Log = slog.Default()
	}
	if o.Prefix == "" {
		o.Prefix = fmt.Sprintf("smoke-archive/%s-%06d", time.Now().UTC().Format("20060102T150405Z"), rand.IntN(1_000_000))
	}
	s := &smoke{o: o, rec: &recorder{}}
	s.client = s.newClient(o.PathStyle)
	if o.DeleteProbeOnly {
		s.runCtx(ctx, "delete-missing-denied", s.deleteMissingDenied)
		return s.checks
	}
	steps := []struct {
		name string
		fn   func(context.Context) (string, error)
	}{
		{"list", s.list},
		{"put-sdk-default-checksum", s.putDefault},
		{"put-crc32c", s.putCRC32C},
		{"etag-md5", s.etagMD5},
		{"get-checksum-mode", s.getChecksumMode},
		{"head", s.head},
		{"conditional-put-412", s.conditionalPut},
		{"list-start-after", s.listStartAfter},
		{"sdk-datastore", s.sdkDatastore},
		{"delete-denied", s.deleteDenied},
		{"delete-missing-denied", s.deleteMissingDenied},
		{"path-style", s.addressing(true)},
		{"virtual-hosted", s.addressing(false)},
	}
	for _, st := range steps {
		s.runCtx(ctx, st.name, st.fn)
	}
	return s.checks
}

// Failed returns the names of the failed checks.
func Failed(checks []Check) []string {
	var names []string
	for _, c := range checks {
		if !c.Pass {
			names = append(names, c.Name)
		}
	}
	return names
}

// smokeSeqs are the ledgers written as SEP-54 keys under the test prefix.
var smokeSeqs = []uint32{64_000_001, 64_000_002, 64_000_003}

type smoke struct {
	o      SmokeOptions
	client *s3.Client
	rec    *recorder
	checks []Check
	// objects written by put-crc32c, by ledger.
	bodies map[uint32][]byte
	meta   map[uint32]map[string]string
}

// newClient builds the archiver's client with the given addressing style.
func (s *smoke) newClient(pathStyle bool) *s3.Client {
	opts := sep54.ArchiveClientOptions(pathStyle, s.o.Creds)
	opts.HTTPClient = s.o.HTTPClient
	return sep54.NewS3Client(opts)
}

func (s *smoke) runCtx(ctx context.Context, name string, fn func(context.Context) (string, error)) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	detail, err := fn(ctx)
	c := Check{Name: name, Pass: err == nil, Detail: detail}
	if err != nil {
		c.Detail = strings.TrimSpace(strings.Join([]string{detail, err.Error()}, " "))
		s.o.Log.Error("smoke check failed", "check", name, "err", err)
	} else {
		s.o.Log.Info("smoke check passed", "check", name, "detail", detail)
	}
	s.checks = append(s.checks, c)
}

func (s *smoke) key(rel string) string { return s.o.Prefix + "/" + rel }

func (s *smoke) ledgerKey(seq uint32) string { return s.key(sep54.ObjectPath(seq)) }

func (s *smoke) list(ctx context.Context) (string, error) {
	_, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(s.o.Bucket), MaxKeys: aws.Int32(1)})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("bucket %s reachable at %s with the uploader key; test prefix %s", s.o.Bucket, s.o.Creds.Endpoint, s.o.Prefix), nil
}

func (s *smoke) putDefault(ctx context.Context) (string, error) {
	body := randomBytes(4096, 1)
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.o.Bucket),
		Key:    aws.String(s.key("sdk-default-checksum.bin")),
		Body:   bytes.NewReader(body),
	}, s.rec.capture("put-default"))
	return s.rec.describe("put-default"), err
}

// putCRC32C writes the test ledgers exactly as the archiver will.
func (s *smoke) putCRC32C(ctx context.Context) (string, error) {
	s.bodies = map[uint32][]byte{}
	s.meta = map[uint32]map[string]string{}
	for i, seq := range smokeSeqs {
		body := randomBytes(200_000+i*1000, uint64(seq))
		md := smokeMetadata(seq)
		out, err := s.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:            aws.String(s.o.Bucket),
			Key:               aws.String(s.ledgerKey(seq)),
			Body:              bytes.NewReader(body),
			ContentType:       aws.String("application/octet-stream"),
			Metadata:          md,
			ChecksumAlgorithm: types.ChecksumAlgorithmCrc32c,
			IfNoneMatch:       aws.String("*"),
		}, s.rec.capture("put-crc32c"))
		if err != nil {
			return s.rec.describe("put-crc32c"), err
		}
		// A store that keeps no CRC32C (Hetzner) returns none; the reads
		// below check it falls back to ETag = MD5, as the archiver does.
		if got, want := aws.ToString(out.ChecksumCRC32C), sep54.CRC32C(body); got != "" && got != want {
			return s.rec.describe("put-crc32c"), fmt.Errorf("PUT returned CRC32C %q, want %q", got, want)
		}
		s.bodies[seq], s.meta[seq] = body, md
	}
	return s.rec.describe("put-crc32c"), nil
}

func (s *smoke) etagMD5(ctx context.Context) (string, error) {
	if len(s.bodies) == 0 {
		return "", errors.New("skipped: put-crc32c failed")
	}
	seq := smokeSeqs[0]
	out, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(s.o.Bucket), Key: aws.String(s.ledgerKey(seq))})
	if err != nil {
		return "", err
	}
	sum := md5.Sum(s.bodies[seq])
	want := `"` + hex.EncodeToString(sum[:]) + `"`
	if got := aws.ToString(out.ETag); got != want {
		return "", fmt.Errorf("ETag %s, MD5 of the bytes %s", got, want)
	}
	return "ETag = MD5 " + want, nil
}

func (s *smoke) getChecksumMode(ctx context.Context) (string, error) {
	if len(s.bodies) == 0 {
		return "", errors.New("skipped: put-crc32c failed")
	}
	stored := "CRC32C"
	for _, seq := range smokeSeqs {
		out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket:       aws.String(s.o.Bucket),
			Key:          aws.String(s.ledgerKey(seq)),
			ChecksumMode: types.ChecksumModeEnabled,
		})
		if err != nil {
			return "", err
		}
		body, err := io.ReadAll(out.Body) // the SDK validates the stored checksum at EOF
		out.Body.Close()
		if err != nil {
			return "", fmt.Errorf("read with checksum validation: %w", err)
		}
		if !bytes.Equal(body, s.bodies[seq]) {
			return "", fmt.Errorf("ledger %d: bytes differ", seq)
		}
		if err := sep54.VerifyStored(sep54.Object{Body: body, ChecksumCRC32C: aws.ToString(out.ChecksumCRC32C), ETag: aws.ToString(out.ETag)}); err != nil {
			return "", fmt.Errorf("ledger %d: GET: %w", seq, err)
		}
		if out.ChecksumCRC32C == nil {
			stored = "ETag = MD5 (no stored CRC32C)"
		}
		if err := sameMetadata(out.Metadata, s.meta[seq]); err != nil {
			return "", fmt.Errorf("ledger %d: %w", seq, err)
		}
		if got := aws.ToString(out.ContentType); got != "application/octet-stream" {
			return "", fmt.Errorf("ledger %d: Content-Type %q", seq, got)
		}
	}
	return "bytes, " + stored + ", 9 metadata keys and Content-Type round-trip", nil
}

func (s *smoke) head(ctx context.Context) (string, error) {
	if len(s.bodies) == 0 {
		return "", errors.New("skipped: put-crc32c failed")
	}
	seq := smokeSeqs[1]
	out, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket:       aws.String(s.o.Bucket),
		Key:          aws.String(s.ledgerKey(seq)),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		return "", err
	}
	if got, want := aws.ToInt64(out.ContentLength), int64(len(s.bodies[seq])); got != want {
		return "", fmt.Errorf("Content-Length %d, want %d", got, want)
	}
	stored := "CRC32C"
	if err := sep54.VerifyStored(sep54.Object{Body: s.bodies[seq], ChecksumCRC32C: aws.ToString(out.ChecksumCRC32C), ETag: aws.ToString(out.ETag)}); err != nil {
		return "", fmt.Errorf("HEAD: %w", err)
	} else if out.ChecksumCRC32C == nil {
		stored = "ETag = MD5 (no stored CRC32C)"
	}
	return "HEAD returns length, " + stored + " and metadata", sameMetadata(out.Metadata, s.meta[seq])
}

func (s *smoke) conditionalPut(ctx context.Context) (string, error) {
	if len(s.bodies) == 0 {
		return "", errors.New("skipped: put-crc32c failed")
	}
	seq := smokeSeqs[0]
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:            aws.String(s.o.Bucket),
		Key:               aws.String(s.ledgerKey(seq)),
		Body:              bytes.NewReader([]byte("must not replace the object")),
		ChecksumAlgorithm: types.ChecksumAlgorithmCrc32c,
		IfNoneMatch:       aws.String("*"),
	})
	if err == nil {
		return "", errors.New("If-None-Match: * on an existing key succeeded: the object was overwritten")
	}
	if !sep54.IsPreconditionFailed(err) {
		return "", fmt.Errorf("want 412 PreconditionFailed, got %w", err)
	}
	// Galexie's (and the SDK datastore's) path: PutFileIfNotExists → (false, nil).
	ds, err := datastore.FromS3Client(ctx, s.client, s.o.Bucket+"/"+s.o.Prefix)
	if err != nil {
		return "", err
	}
	up, err := ds.PutFileIfNotExists(ctx, sep54.ObjectPath(seq), bytes.NewReader([]byte("nor this")), nil)
	if err != nil || up {
		return "", fmt.Errorf("PutFileIfNotExists on an existing key = (%v, %v), want (false, nil)", up, err)
	}
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.o.Bucket), Key: aws.String(s.ledgerKey(seq))})
	if err != nil {
		return "", err
	}
	defer out.Body.Close()
	body, err := io.ReadAll(out.Body)
	if err != nil {
		return "", err
	}
	if !bytes.Equal(body, s.bodies[seq]) {
		return "", errors.New("the object changed after a refused conditional PUT")
	}
	return "412 PreconditionFailed; PutFileIfNotExists = (false, nil); object unchanged", nil
}

func (s *smoke) listStartAfter(ctx context.Context) (string, error) {
	if len(s.bodies) == 0 {
		return "", errors.New("skipped: put-crc32c failed")
	}
	// Reverse hex: the newest ledger sorts first.
	want := []string{s.ledgerKey(smokeSeqs[2]), s.ledgerKey(smokeSeqs[1]), s.ledgerKey(smokeSeqs[0])}
	prefix := s.key(sep54.PartitionName(smokeSeqs[0])) + "/"
	out, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:     aws.String(s.o.Bucket),
		Prefix:     aws.String(prefix),
		StartAfter: aws.String(want[0]),
	})
	if err != nil {
		return "", err
	}
	if got := keys(out.Contents); !slices.Equal(got, want[1:]) {
		return "", fmt.Errorf("StartAfter %s listed %v, want %v", want[0], got, want[1:])
	}
	var all []string
	p := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{Bucket: aws.String(s.o.Bucket), Prefix: aws.String(prefix), MaxKeys: aws.Int32(1)})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return "", err
		}
		all = append(all, keys(page.Contents)...)
	}
	if !slices.Equal(all, want) {
		return "", fmt.Errorf("paginated listing %v, want %v", all, want)
	}
	return "StartAfter and MaxKeys=1 pagination list SEP-54 keys newest first", nil
}

func (s *smoke) sdkDatastore(ctx context.Context) (string, error) {
	if len(s.bodies) == 0 {
		return "", errors.New("skipped: put-crc32c failed")
	}
	src, err := sep54.Open(ctx, sep54.SourceArchive, s.client, s.o.Bucket+"/"+s.o.Prefix)
	if err != nil {
		return "", err
	}
	cfg := sep54.GalexieConfig(network.PublicNetworkPassphrase)
	if _, created, err := datastore.PublishConfig(ctx, src.DataStore(), cfg); err != nil || !created {
		return "", fmt.Errorf("first PublishConfig = (created %v, %v), want (true, nil)", created, err)
	}
	if _, created, err := datastore.PublishConfig(ctx, src.DataStore(), cfg); err != nil || created {
		return "", fmt.Errorf("second PublishConfig = (created %v, %v), want (false, nil)", created, err)
	}
	m, err := src.GetObject(ctx, sep54.ManifestFile)
	if err != nil {
		return "", err
	}
	if !bytes.Equal(m.Body, sep54.ManifestBytes(network.PublicNetworkPassphrase)) {
		return "", fmt.Errorf("manifest bytes %q differ from Galexie's", m.Body)
	}
	if _, err := src.LoadSchema(ctx, network.PublicNetworkPassphrase); err != nil {
		return "", err
	}
	latest, err := datastore.FindLatestLedgerSequence(ctx, src.DataStore())
	if err != nil || latest != smokeSeqs[2] {
		return "", fmt.Errorf("FindLatestLedgerSequence = (%d, %v), want %d", latest, err, smokeSeqs[2])
	}
	if newest, err := src.Newest(ctx); err != nil || newest != smokeSeqs[2] {
		return "", fmt.Errorf("Newest = (%d, %v), want %d", newest, err, smokeSeqs[2])
	}
	return "FromS3Client, PublishConfig (Galexie's bytes), LoadSchema and the newest-ledger listing work", nil
}

func (s *smoke) deleteDenied(ctx context.Context) (string, error) {
	if len(s.bodies) == 0 {
		return "", errors.New("skipped: put-crc32c failed")
	}
	seq := smokeSeqs[2]
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(s.o.Bucket), Key: aws.String(s.ledgerKey(seq))})
	if err == nil {
		_, herr := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(s.o.Bucket), Key: aws.String(s.ledgerKey(seq))})
		return "", fmt.Errorf("the uploader deleted %s (object still present: %v); the bucket policy must deny s3:DeleteObject", s.ledgerKey(seq), herr == nil)
	}
	if !sep54.IsAccessDenied(err) {
		return "", fmt.Errorf("want AccessDenied, got %w", err)
	}
	if _, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(s.o.Bucket), Key: aws.String(s.ledgerKey(seq))}); err != nil {
		return "", fmt.Errorf("object gone after a denied delete: %w", err)
	}
	return "DeleteObject refused with AccessDenied; object intact", nil
}

func (s *smoke) deleteMissingDenied(ctx context.Context) (string, error) {
	key := s.key(fmt.Sprintf("does-not-exist-%d", rand.Uint64()))
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(s.o.Bucket), Key: aws.String(key)})
	if err == nil {
		return "", fmt.Errorf("deleting the missing key %s was allowed; the bucket policy must deny s3:DeleteObject", key)
	}
	if !sep54.IsAccessDenied(err) {
		return "", fmt.Errorf("want AccessDenied, got %w", err)
	}
	return "DeleteObject of a missing key refused with AccessDenied", nil
}

// addressing reads the test objects with a separate client using the given
// addressing style.
func (s *smoke) addressing(pathStyle bool) func(context.Context) (string, error) {
	return func(ctx context.Context) (string, error) {
		if len(s.bodies) == 0 {
			return "", errors.New("skipped: put-crc32c failed")
		}
		c := s.newClient(pathStyle)
		rec := &recorder{}
		seq := smokeSeqs[0]
		if _, err := c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(s.o.Bucket), Prefix: aws.String(s.o.Prefix + "/"), MaxKeys: aws.Int32(5)}, rec.capture("list")); err != nil {
			return rec.describeURL("list"), err
		}
		if _, err := c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(s.o.Bucket), Key: aws.String(s.ledgerKey(seq))}); err != nil {
			return rec.describeURL("list"), err
		}
		out, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.o.Bucket), Key: aws.String(s.ledgerKey(seq)), ChecksumMode: types.ChecksumModeEnabled})
		if err != nil {
			return rec.describeURL("list"), err
		}
		defer out.Body.Close()
		body, err := io.ReadAll(out.Body)
		if err != nil {
			return rec.describeURL("list"), err
		}
		if !bytes.Equal(body, s.bodies[seq]) {
			return rec.describeURL("list"), errors.New("bytes differ")
		}
		detail := rec.describeURL("list")
		if req := rec.last("list"); req != nil && !pathStyle && !strings.HasPrefix(req.URL.Host, s.o.Bucket+".") {
			detail += " (the SDK uses path-style for an IP endpoint)"
		}
		return detail + "; authenticated LIST, HEAD and GET work", nil
	}
}

func sameMetadata(got, want map[string]string) error {
	if !maps.Equal(got, want) {
		return fmt.Errorf("metadata %v, want %v", got, want)
	}
	return nil
}

func keys(objs []types.Object) []string {
	out := make([]string, 0, len(objs))
	for _, o := range objs {
		out = append(out, aws.ToString(o.Key))
	}
	return out
}

func randomBytes(n int, seed uint64) []byte {
	r := rand.New(rand.NewPCG(seed, seed^0x5eed))
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(r.Uint32())
	}
	return b
}

func smokeMetadata(seq uint32) map[string]string {
	s := strconv.FormatUint(uint64(seq), 10)
	return datastore.MetaData{
		StartLedger:          seq,
		EndLedger:            seq,
		StartLedgerCloseTime: 1_790_000_000 + int64(seq%1000)*5,
		EndLedgerCloseTime:   1_790_000_000 + int64(seq%1000)*5,
		ProtocolVersion:      23,
		CoreVersion:          "smoke-archive " + s,
		NetworkPassPhrase:    network.PublicNetworkPassphrase,
		CompressionType:      "zst",
		Version:              "smoke-archive",
	}.ToMap()
}

// recorder captures request headers per operation to report how the SDK
// sent checksums (header or aws-chunked trailer) and which addressing style
// reached the provider.
type recorder struct {
	mu   sync.Mutex
	reqs map[string]*http.Request
}

type recordingClient struct {
	next s3.HTTPClient
	rec  *recorder
	op   string
}

func (c recordingClient) Do(req *http.Request) (*http.Response, error) {
	c.rec.mu.Lock()
	if c.rec.reqs == nil {
		c.rec.reqs = map[string]*http.Request{}
	}
	c.rec.reqs[c.op] = req.Clone(context.Background())
	c.rec.mu.Unlock()
	return c.next.Do(req)
}

func (r *recorder) capture(op string) func(*s3.Options) {
	return func(o *s3.Options) { o.HTTPClient = recordingClient{next: o.HTTPClient, rec: r, op: op} }
}

func (r *recorder) last(op string) *http.Request {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.reqs[op]
}

// describe says how the last request of op carried its checksum.
func (r *recorder) describe(op string) string {
	req := r.last(op)
	if req == nil {
		return "no request sent"
	}
	h := req.Header
	switch trailer := h.Get("X-Amz-Trailer"); {
	case trailer != "":
		return fmt.Sprintf("checksum %s sent as an aws-chunked trailer (%s, payload %s)",
			strings.TrimPrefix(trailer, "x-amz-checksum-"), trailer, h.Get("X-Amz-Content-Sha256"))
	case req.URL.Scheme == "http":
		return fmt.Sprintf("checksum %s sent as a header over plain HTTP (the trailer form is only used over HTTPS)", headerChecksum(h))
	default:
		return fmt.Sprintf("checksum %s sent as a header", headerChecksum(h))
	}
}

func headerChecksum(h http.Header) string {
	for name := range h {
		if strings.HasPrefix(strings.ToLower(name), "x-amz-checksum-") && !strings.EqualFold(name, "x-amz-checksum-mode") {
			return strings.ToLower(strings.TrimPrefix(strings.ToLower(name), "x-amz-checksum-"))
		}
	}
	return "none"
}

func (r *recorder) describeURL(op string) string {
	req := r.last(op)
	if req == nil {
		return "no request sent"
	}
	return "request URL " + req.URL.Scheme + "://" + req.URL.Host + req.URL.Path
}
