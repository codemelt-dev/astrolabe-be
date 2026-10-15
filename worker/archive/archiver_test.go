package archive_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"maps"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/klauspost/compress/zstd"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"

	"stellar-explorer/worker/archive"
	"stellar-explorer/worker/bootstrap"
	"stellar-explorer/worker/config"
	"stellar-explorer/worker/devfeed"
	"stellar-explorer/worker/lake/laketest"
	"stellar-explorer/worker/sep54"
	"stellar-explorer/worker/sep54/sep54test"
)

// The archive stand-in is a second lake in a subprocess (only one lake can
// run per process): a real S3 implementation with conditional PUTs,
// CRC32C, MD5 ETags and metadata.
func TestMain(m *testing.M) { laketest.Main(m) }

const archiveBucket = "stellar-ledgers-pubnet"

type alertLog struct {
	mu    sync.Mutex
	kinds []string
}

func (a *alertLog) alert(kind, msg string, args ...any) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.kinds = append(a.kinds, kind)
}

func (a *alertLog) has(kind string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, k := range a.kinds {
		if k == kind {
			return true
		}
	}
	return false
}

func (a *alertLog) list() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.kinds...)
}

// env is a lake (the source) and a subprocess lake standing in for the
// archive bucket, with the same prefix.
type env struct {
	t      *testing.T
	lake   *laketest.Lake
	alerts *alertLog
	// The archive endpoint, fixed for the test (the process may start
	// later).
	archS3, archControl string
}

func newEnv(t *testing.T) *env {
	t.Helper()
	e := newEnvNoArchive(t)
	e.startArchive()
	return e
}

func newEnvNoArchive(t *testing.T) *env {
	t.Helper()
	l := laketest.Start(t, laketest.Options{})
	return &env{t: t, lake: l, alerts: &alertLog{}, archS3: laketest.FreeAddr(t), archControl: laketest.FreeAddr(t)}
}

func (e *env) archBucketPath() string { return archiveBucket + "/" + e.lake.Cfg.Prefix() }

func (e *env) startArchive() *laketest.Process {
	return laketest.StartProcess(e.t, laketest.Options{BucketPath: e.archBucketPath(), S3Listen: e.archS3, ControlListen: e.archControl})
}

func (e *env) archiveCreds() config.ArchiveCredentials {
	return config.ArchiveCredentials{Endpoint: "http://" + e.archS3, Region: e.lake.Cfg.Region,
		AccessKey: laketest.Credentials.AccessKey, SecretKey: laketest.Credentials.SecretKey}
}

func (e *env) openArchive(ctx context.Context) (*sep54.Source, error) {
	opts := sep54.ArchiveClientOptions(true, e.archiveCreds())
	opts.RetryMaxAttempts = 2
	opts.Timeout = 10 * time.Second
	return sep54.Open(ctx, sep54.SourceArchive, sep54.NewS3Client(opts), e.archBucketPath())
}

func (e *env) openLake(ctx context.Context) (*sep54.Source, error) {
	return sep54.OpenLake(ctx, e.lake.Cfg, e.lake.Creds)
}

// feed publishes objects into the lake like Galexie (manifest first).
func (e *env) feed(objs []sep54.Object) {
	e.t.Helper()
	ctx := context.Background()
	src, err := e.openLake(ctx)
	if err != nil {
		e.t.Fatal(err)
	}
	first, _ := sep54.ParseObjectPath(objs[0].Key)
	last, _ := sep54.ParseObjectPath(objs[len(objs)-1].Key)
	if _, err := devfeed.Run(ctx, devfeed.Options{
		Upstream: sep54test.NewMemSource(objs...), Lake: src.DataStore(), Passphrase: network.PublicNetworkPassphrase,
		Start: first, End: last, Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}); err != nil {
		e.t.Fatal(err)
	}
}

// putLake writes one object into the lake directly (a repair, or a bad
// object).
func (e *env) putLake(o sep54.Object) {
	e.t.Helper()
	src, err := e.openLake(context.Background())
	if err != nil {
		e.t.Fatal(err)
	}
	if _, err := src.DataStore().PutFileIfNotExists(context.Background(), o.Key, bytes.NewReader(o.Body), o.Metadata); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) deleteLake(seq uint32) {
	e.t.Helper()
	src, err := e.openLake(context.Background())
	if err != nil {
		e.t.Fatal(err)
	}
	if _, err := src.Client().DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(src.Bucket()), Key: aws.String(src.Key(sep54.ObjectPath(seq)))}); err != nil {
		e.t.Fatal(err)
	}
}

// putArchive writes an object into the archive directly, like another
// producer would have.
func (e *env) putArchive(rel string, body []byte, md map[string]string) {
	e.t.Helper()
	src, err := e.openArchive(context.Background())
	if err != nil {
		e.t.Fatal(err)
	}
	if _, err := src.Client().PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(src.Bucket()), Key: aws.String(src.Key(rel)), Body: bytes.NewReader(body), Metadata: md,
		ContentType: aws.String("application/octet-stream"), ChecksumAlgorithm: types.ChecksumAlgorithmCrc32c,
	}); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) deleteArchive(rel string) {
	e.t.Helper()
	src, err := e.openArchive(context.Background())
	if err != nil {
		e.t.Fatal(err)
	}
	if _, err := src.Client().DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(src.Bucket()), Key: aws.String(src.Key(rel))}); err != nil {
		e.t.Fatal(err)
	}
}

// writeOrigin writes state/bootstrap.json for an archive starting at the
// first object of objs.
func (e *env) writeOrigin(first sep54.Object) bootstrap.Origin {
	e.t.Helper()
	seq, _ := sep54.ParseObjectPath(first.Key)
	h, err := sep54.DecodeHeader(seq, first.Body)
	if err != nil {
		e.t.Fatal(err)
	}
	o := bootstrap.NewOrigin(network.PublicNetworkPassphrase, seq-1, h.PreviousHash)
	if _, _, err := bootstrap.WriteOriginIfAbsent(e.lake.Dir, o); err != nil {
		e.t.Fatal(err)
	}
	return o
}

type running struct {
	a      *archive.Archiver
	cancel context.CancelFunc
	done   chan error
	once   sync.Once
}

func (e *env) start(o archive.Options) *running {
	e.t.Helper()
	if o.Lake == nil {
		o.Lake = e.openLake
	}
	if o.Archive == nil {
		o.Archive = e.openArchive
	}
	if o.LakeTip == nil {
		o.LakeTip = sep54.NewTipClient(e.lake.ControlAddr())
	}
	if o.Origin == nil {
		dir := e.lake.Dir
		o.Origin = func() (bootstrap.Origin, error) { return bootstrap.ReadOrigin(dir) }
	}
	o.Alert = e.alerts.alert
	o.Log = slog.New(slog.NewTextHandler(tWriter{e.t}, nil))
	if o.MaxBackoff == 0 {
		o.MaxBackoff = 50 * time.Millisecond
	}
	if o.HaltRecheck == 0 {
		o.HaltRecheck = 50 * time.Millisecond
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &running{a: archive.New(o), cancel: cancel, done: make(chan error, 1)}
	go func() { r.done <- r.a.Run(ctx) }()
	e.t.Cleanup(r.stop)
	return r
}

func (r *running) stop() {
	r.once.Do(func() {
		r.cancel()
		<-r.done
	})
}

func (r *running) waitTip(t *testing.T, want uint32, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		if tip, ok := r.a.Tip(); ok && tip >= want {
			if tip != want {
				t.Fatalf("archive tip %d, want %d", tip, want)
			}
			return
		}
		if time.Now().After(deadline) {
			tip, ok := r.a.Tip()
			t.Fatalf("archive tip %d (known %v), want %d; stats %+v", tip, ok, want, r.a.Stats())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waitBlocked waits until the archiver reports reason.
func (r *running) waitBlocked(t *testing.T, reason string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for r.a.Stats().Blocked != reason {
		if time.Now().After(deadline) {
			t.Fatalf("archiver blocked %q, want %q", r.a.Stats().Blocked, reason)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type tWriter struct{ t *testing.T }

func (w tWriter) Write(p []byte) (int, error) {
	defer func() { _ = recover() }()
	w.t.Log(strings.TrimRight(string(p), "\n"))
	return len(p), nil
}

// sameObject requires the archive's copy of rel to be identical to the
// lake's: bytes, ETag, length, CRC32C, Content-Type and every metadata key.
func (e *env) sameObject(rel string) {
	e.t.Helper()
	ctx := context.Background()
	lake, err := e.openLake(ctx)
	if err != nil {
		e.t.Fatal(err)
	}
	arch, err := e.openArchive(ctx)
	if err != nil {
		e.t.Fatal(err)
	}
	head := func(s *sep54.Source) *s3.HeadObjectOutput {
		out, err := s.Client().HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(s.Bucket()), Key: aws.String(s.Key(rel)),
			ChecksumMode: types.ChecksumModeEnabled})
		if err != nil {
			e.t.Fatalf("%s %s: %v", s.Name(), rel, err)
		}
		return out
	}
	lh, ah := head(lake), head(arch)
	if aws.ToString(lh.ETag) != aws.ToString(ah.ETag) || aws.ToInt64(lh.ContentLength) != aws.ToInt64(ah.ContentLength) ||
		aws.ToString(lh.ContentType) != aws.ToString(ah.ContentType) || !maps.Equal(lh.Metadata, ah.Metadata) {
		e.t.Fatalf("%s differs:\nlake    etag %s len %d type %q md %v\narchive etag %s len %d type %q md %v", rel,
			aws.ToString(lh.ETag), aws.ToInt64(lh.ContentLength), aws.ToString(lh.ContentType), lh.Metadata,
			aws.ToString(ah.ETag), aws.ToInt64(ah.ContentLength), aws.ToString(ah.ContentType), ah.Metadata)
	}
	if rel != sep54.ManifestFile {
		if aws.ToString(lh.ChecksumCRC32C) == "" || aws.ToString(lh.ChecksumCRC32C) != aws.ToString(ah.ChecksumCRC32C) {
			e.t.Fatalf("%s: CRC32C lake %q archive %q", rel, aws.ToString(lh.ChecksumCRC32C), aws.ToString(ah.ChecksumCRC32C))
		}
		if len(lh.Metadata) != len(sep54.MetadataKeys) {
			e.t.Fatalf("%s: %d metadata keys", rel, len(lh.Metadata))
		}
	}
	lo, err := lake.GetObject(ctx, rel)
	if err != nil {
		e.t.Fatal(err)
	}
	ao, err := arch.GetObject(ctx, rel)
	if err != nil {
		e.t.Fatal(err)
	}
	if !bytes.Equal(lo.Body, ao.Body) {
		e.t.Fatalf("%s: bytes differ", rel)
	}
}

func seqOf(o sep54.Object) uint32 {
	s, _ := sep54.ParseObjectPath(o.Key)
	return s
}

// The archive is a 1:1 copy of the lake (keys, bytes, metadata,
// Content-Type, ETag, CRC32C and the manifest); it starts on an empty
// bucket at archive_origin, and resumes after a restart from the archive's
// own newest object.
func TestArchiverFidelityAndResume(t *testing.T) {
	e := newEnv(t)
	chain := sep54test.Rechain(5_000_000, 30)
	e.feed(chain[:20])
	e.writeOrigin(chain[0])

	r := e.start(archive.Options{})
	r.waitTip(t, seqOf(chain[19]), 60*time.Second)
	e.sameObject(sep54.ManifestFile)
	for _, o := range chain[:20] {
		e.sameObject(o.Key)
	}
	if st := r.a.Stats(); st.Created != 20 || st.Existed != 0 || st.Blocked != "" || st.Bytes == 0 {
		t.Fatalf("stats %+v", st)
	}
	r.stop()

	// Restart: the tip comes from the archive itself; the manifest PUT is a
	// byte-identical 412.
	e.feed(chain[20:])
	r = e.start(archive.Options{})
	r.waitTip(t, seqOf(chain[29]), 60*time.Second)
	for _, o := range chain[20:] {
		e.sameObject(o.Key)
	}
	if st := r.a.Stats(); st.Created != 10 {
		t.Fatalf("stats after restart %+v", st)
	}
	// Live: a ledger committed while the archiver waits is archived at once.
	more := sep54test.Rechain(5_000_000, 32)[30:]
	e.feed(more)
	r.waitTip(t, seqOf(more[1]), 30*time.Second)
	if got := e.alerts.list(); len(got) != 0 {
		t.Fatalf("alerts %v", got)
	}
}

// reencode compresses the same batch with other zstd settings: other
// producer bytes for the same ledger.
func reencode(t *testing.T, o sep54.Object) []byte {
	t.Helper()
	raw, err := sep54.Decompress(o.Body)
	if err != nil {
		t.Fatal(err)
	}
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBestCompression), zstd.WithEncoderCRC(true))
	if err != nil {
		t.Fatal(err)
	}
	b := enc.EncodeAll(raw, nil)
	if bytes.Equal(b, o.Body) {
		t.Fatal("re-encoding produced the same bytes")
	}
	return b
}

// 412 handling: identical bytes (ETag = MD5) and other producer bytes with
// the same ledger hash are accepted (the archive's copy is kept); a
// different ledger halts the archiver until an operator fixes the archive.
// The objects appear in the archive while the archiver waits at the tip
// (another archiver, or a seed from the AWS lake), so they are not its
// newest object at start.
func TestArchiverExistingObjects(t *testing.T) {
	e := newEnv(t)
	chain := sep54test.Chain(6_000_000, 6)
	e.feed(chain[:1])
	e.writeOrigin(chain[0])
	r := e.start(archive.Options{})
	r.waitTip(t, seqOf(chain[0]), 30*time.Second)

	// Ledger 1: identical bytes already archived.
	e.putArchive(chain[1].Key, chain[1].Body, chain[1].Metadata)
	// Ledger 2: the AWS lake's bytes for the same ledger (another producer).
	other := reencode(t, chain[2])
	e.putArchive(chain[2].Key, other, chain[2].Metadata)
	// Ledger 4: a different ledger 4 (another chain).
	foreign := sep54test.Chain(seqOf(chain[4]), 1)[0]
	e.putArchive(chain[4].Key, foreign.Body, foreign.Metadata)
	e.feed(chain[1:])

	r.waitTip(t, seqOf(chain[3]), 30*time.Second)
	r.waitBlocked(t, archive.BlockedHalted)
	if !e.alerts.has("archive_hash_mismatch") {
		t.Fatalf("alerts %v", e.alerts.list())
	}
	time.Sleep(300 * time.Millisecond) // several rechecks: still halted, alerted once
	if tip, _ := r.a.Tip(); tip != seqOf(chain[3]) {
		t.Fatalf("tip moved to %d past a mismatch", tip)
	}
	n := 0
	for _, k := range e.alerts.list() {
		if k == "archive_hash_mismatch" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("%d mismatch alerts", n)
	}
	// The kept object is the other producer's.
	arch, _ := e.openArchive(context.Background())
	if got, err := arch.GetObject(context.Background(), chain[2].Key); err != nil || !bytes.Equal(got.Body, other) {
		t.Fatalf("ledger 2 replaced in the archive (%v)", err)
	}
	// An operator removes the foreign object: the archiver resumes by itself.
	e.deleteArchive(chain[4].Key)
	r.waitTip(t, seqOf(chain[5]), 30*time.Second)
	if st := r.a.Stats(); st.Existed != 1 || st.Differed != 1 || st.Created != 4 {
		t.Fatalf("stats %+v", st)
	}
	e.sameObject(chain[1].Key)
	e.sameObject(chain[4].Key)
}

// A ledger missing from the lake, or one that does not chain to the
// archive tip, is alerted and retried; repairing it resumes the archiver
// without a restart.
func TestArchiverMissingAndInvalidLedgers(t *testing.T) {
	e := newEnv(t)
	chain := sep54test.Chain(7_000_000, 8)
	e.feed(chain)
	e.writeOrigin(chain[0])
	// A hole at 2 (a corrupt object deleted at startup).
	e.deleteLake(seqOf(chain[2]))

	r := e.start(archive.Options{})
	r.waitTip(t, seqOf(chain[1]), 30*time.Second)
	r.waitBlocked(t, archive.BlockedMissing)
	if !e.alerts.has("archive_missing_ledger") {
		t.Fatalf("alerts %v", e.alerts.list())
	}
	// scan-and-fill puts it back with a foreign ledger first (wrong chain):
	// still blocked, now as invalid.
	foreign := sep54test.Chain(seqOf(chain[2]), 1)[0]
	e.putLake(sep54.Object{Key: chain[2].Key, Body: foreign.Body, Metadata: chain[2].Metadata})
	r.waitBlocked(t, archive.BlockedInvalid)
	if !e.alerts.has("archive_invalid_ledger") {
		t.Fatalf("alerts %v", e.alerts.list())
	}
	// The real repair.
	e.deleteLake(seqOf(chain[2]))
	e.putLake(chain[2])
	r.waitTip(t, seqOf(chain[7]), 30*time.Second)
	for _, o := range chain {
		e.sameObject(o.Key)
	}
}

// An empty archive needs archive_origin; without bootstrap.json the
// archiver alerts and the tip stays unknown (so the pruner deletes
// nothing) until it appears.
func TestArchiverEmptyBucketNeedsOrigin(t *testing.T) {
	e := newEnv(t)
	chain := sep54test.Chain(8_000_000, 3)
	e.feed(chain)
	r := e.start(archive.Options{})
	r.waitBlocked(t, archive.BlockedOrigin)
	if _, ok := r.a.Tip(); ok {
		t.Fatal("tip known without an origin")
	}
	if !e.alerts.has("archive_origin_unknown") {
		t.Fatalf("alerts %v", e.alerts.list())
	}
	// The origin may also start mid-lake: ledgers before it are never
	// archived.
	e.writeOrigin(chain[1])
	r.waitTip(t, seqOf(chain[2]), 30*time.Second)
	arch, _ := e.openArchive(context.Background())
	if _, err := arch.GetLedger(context.Background(), seqOf(chain[0])); !errors.Is(err, sep54.ErrNotFound) {
		t.Fatalf("ledger before the origin archived: %v", err)
	}
}

// A different manifest in the archive halts archiving.
func TestArchiverManifestMismatch(t *testing.T) {
	e := newEnv(t)
	chain := sep54test.Chain(9_000_000, 2)
	e.feed(chain)
	e.writeOrigin(chain[0])
	e.putArchive(sep54.ManifestFile, sep54.ManifestBytes(network.TestNetworkPassphrase), nil)
	r := e.start(archive.Options{})
	r.waitBlocked(t, archive.BlockedManifest)
	if !e.alerts.has("archive_manifest_mismatch") {
		t.Fatalf("alerts %v", e.alerts.list())
	}
	time.Sleep(200 * time.Millisecond)
	arch, _ := e.openArchive(context.Background())
	if _, err := arch.Newest(context.Background()); !errors.Is(err, sep54.ErrNoLedgers) {
		t.Fatalf("ledgers archived under a foreign manifest: %v", err)
	}
	e.deleteArchive(sep54.ManifestFile)
	r.waitTip(t, seqOf(chain[1]), 30*time.Second)
	e.sameObject(sep54.ManifestFile)
}

// unknownVersion is a ledger object whose LedgerCloseMeta arm this build
// does not know (a future protocol): the batch prefix is valid, the rest is
// opaque.
func unknownVersion(t *testing.T, seq uint32) sep54.Object {
	t.Helper()
	var raw bytes.Buffer
	for _, v := range []any{xdr.Uint32(seq), xdr.Uint32(seq), xdr.Uint32(1), xdr.Int32(9)} {
		if _, err := xdr.Marshal(&raw, v); err != nil {
			t.Fatal(err)
		}
	}
	raw.WriteString(strings.Repeat("future", 100))
	enc, _ := zstd.NewWriter(nil, zstd.WithEncoderCRC(true))
	body := enc.EncodeAll(raw.Bytes(), nil)
	md := maps.Clone(sep54test.Chain(seq, 1)[0].Metadata)
	return sep54.Object{Key: sep54.ObjectPath(seq), Body: body, Metadata: md, ChecksumCRC32C: sep54.CRC32C(body)}
}

// After a protocol upgrade the SDK does not know, objects are archived on
// checksum validation alone (with an alert), so archiving and pruning go
// on while only the sequencer halts.
func TestArchiverUnknownVersion(t *testing.T) {
	e := newEnv(t)
	chain := sep54test.Chain(10_000_000, 2)
	e.feed(chain)
	e.writeOrigin(chain[0])
	future := []sep54.Object{unknownVersion(t, 10_000_002), unknownVersion(t, 10_000_003)}
	for _, o := range future {
		e.putLake(o)
	}
	r := e.start(archive.Options{})
	r.waitTip(t, 10_000_003, 30*time.Second)
	if !e.alerts.has("archive_unknown_version") {
		t.Fatalf("alerts %v", e.alerts.list())
	}
	e.sameObject(future[1].Key)
}

// While the archive does not answer, the tip is unknown and the archiver
// retries; it starts once the archive is back.
func TestArchiverArchiveUnavailable(t *testing.T) {
	e := newEnvNoArchive(t)
	chain := sep54test.Chain(11_000_000, 3)
	e.feed(chain)
	e.writeOrigin(chain[0])
	r := e.start(archive.Options{})
	r.waitBlocked(t, archive.BlockedUnavailable)
	time.Sleep(300 * time.Millisecond)
	if _, ok := r.a.Tip(); ok {
		t.Fatal("tip known while the archive is down")
	}
	e.startArchive()
	r.waitTip(t, seqOf(chain[2]), 60*time.Second)
	if len(e.alerts.list()) != 0 {
		t.Fatalf("alerts %v", e.alerts.list())
	}
}

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
