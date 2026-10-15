package lake

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"runtime/debug"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/backend/posix"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"

	"stellar-explorer/worker/sep54"
)

// Stage identifies an fsync in the commit path; tests inject failures per
// stage through Options.Fsync.
type Stage string

const (
	// StageStaged is the fsync of the fully staged body in the post-function,
	// before the link. On failure the object is never linked.
	StageStaged Stage = "staged"
	// StagePublished is the fsync of the linked object (data and xattrs).
	StagePublished Stage = "published"
	// StageDir is the fsync of a parent directory of the linked object.
	StageDir Stage = "dir"
)

var (
	// errFailClosed answers every PUT after an fsync error following a link
	// (rule 1). 503 is retryable, so Galexie retries, then exits and is
	// restarted by systemd; nothing unsynced is ever acknowledged.
	errFailClosed = s3err.APIError{
		Code:           "ServiceUnavailable",
		Description:    "The lake is in fail-closed mode after an fsync error; restart it once the volume is healthy.",
		HTTPStatusCode: http.StatusServiceUnavailable,
	}
	// errRetry answers a PUT whose object vanished before it could be made
	// durable (a concurrent DeleteObject); the retry writes it again.
	errRetry = s3err.APIError{
		Code:           "ServiceUnavailable",
		Description:    "The object changed while it was being made durable; retry.",
		HTTPStatusCode: http.StatusServiceUnavailable,
	}
	errInternal = s3err.GetAPIError(s3err.ErrInternalError)
	errNotImpl  = s3err.GetAPIError(s3err.ErrNotImplemented)
)

// tap wraps the POSIX backend. It overrides the calls that publish or
// remove objects so that a PUT is acknowledged only once the object, its
// xattrs and its directory entries are on disk, and so that the tip index
// only ever advertises durable ledgers. Everything else is served by the
// embedded backend unchanged. The rules the lake's comments refer to:
//
//  1. fsync chain: the staged body before the link, then the linked file,
//     its partition and prefix directories. An fsync error after the link
//     makes the lake fail closed: every later PUT gets 503.
//  2. A 412 re-runs the fsync chain on the existing object (an earlier
//     attempt may have linked it unsynced), then records the commit.
//  3. CompleteMultipartUpload runs the same chain and commit record.
//  4. A length guard on the body, keeping middlewares.ChecksumReader.
//  5. No request string outlives the handler; the index holds only uint32s.
//  6. tip = max(tip, seq), oldest is set if unset; deleting the oldest
//     advances it by one.
//  7. Handlers run under recover() and do no work besides the fsyncs.
//  8. The in-flight tracker records each ledger PUT's phase: receiving,
//     then syncing.
type tap struct {
	backend.Backend
	px       *posix.Posix
	s3root   string // absolute gateway root
	bucket   string
	prefix   string
	idx      *index
	inflight *inflight
	m        *metrics
	log      *slog.Logger
	fsyncFn  func(Stage, *os.File) error
	now      func() time.Time
	failed   atomic.Bool
}

// ledgerSeq parses a request's bucket and key. The key string is used only
// here, inside the handler: fiber reuses request buffers, so no request
// string may outlive the call (rule 5).
func (t *tap) ledgerSeq(bucket, key *string) (uint32, bool) {
	if bucket == nil || key == nil || *bucket != t.bucket {
		return 0, false
	}
	return sep54.ParseKey(t.prefix, *key)
}

// PutObject stages, fsyncs, links, then fsyncs the published object and its
// directories before acknowledging; then it records the commit.
func (t *tap) PutObject(ctx context.Context, in s3response.PutObjectInput) (out s3response.PutObjectOutput, err error) {
	defer t.recoverTo("PutObject", &err)
	if t.failed.Load() {
		t.m.puts.WithLabelValues("fail_closed").Inc()
		return out, errFailClosed
	}
	start := t.now()
	seq, isLedger := t.ledgerSeq(in.Bucket, in.Key)
	put := t.inflight.start(isLedger)
	defer put.done()

	in.Body = guardLength(in.Body, in.ContentLength)
	out, err = t.px.PutObjectWithPostFunc(ctx, in, func(f *os.File) error {
		put.enter(phaseSyncing) // body fully staged
		return t.fsync(StageStaged, f)
	})
	exists := isPreconditionFailed(err)
	if err != nil && !exists {
		// Nothing was published: a failed staged fsync returns before the link.
		t.m.puts.WithLabelValues("error").Inc()
		return out, err
	}
	// Success, or 412 because an earlier attempt already linked the object:
	// make sure it is durable either way (rule 2).
	put.enter(phaseSyncing)
	if serr := t.syncPublished(*in.Bucket, *in.Key); serr != nil {
		return out, t.publishFailed(serr)
	}
	if isLedger {
		t.idx.committed(seq, t.now())
		t.m.putDuration.Observe(t.now().Sub(start).Seconds())
	}
	if exists {
		t.m.puts.WithLabelValues("exists").Inc()
	} else {
		t.m.puts.WithLabelValues("created").Inc()
	}
	return out, err
}

// CompleteMultipartUpload runs the same fsync chain and commit record as
// PutObject (rule 3). Galexie switches to multipart only for objects of at
// least 5 MiB; ledgers are far smaller today.
func (t *tap) CompleteMultipartUpload(ctx context.Context, in *s3.CompleteMultipartUploadInput) (res s3response.CompleteMultipartUploadResult, versionID string, err error) {
	defer t.recoverTo("CompleteMultipartUpload", &err)
	if t.failed.Load() {
		t.m.puts.WithLabelValues("fail_closed").Inc()
		return res, "", errFailClosed
	}
	start := t.now()
	seq, isLedger := t.ledgerSeq(in.Bucket, in.Key)
	put := t.inflight.start(isLedger)
	defer put.done()
	put.enter(phaseSyncing) // assembling parts is disk work, not receiving

	res, versionID, err = t.Backend.CompleteMultipartUpload(ctx, in)
	exists := isPreconditionFailed(err)
	if err != nil && !exists {
		t.m.puts.WithLabelValues("error").Inc()
		return res, versionID, err
	}
	if serr := t.syncPublished(*in.Bucket, *in.Key); serr != nil {
		return res, versionID, t.publishFailed(serr)
	}
	if isLedger {
		t.idx.committed(seq, t.now())
		t.m.putDuration.Observe(t.now().Sub(start).Seconds())
	}
	if exists {
		t.m.puts.WithLabelValues("exists").Inc()
	} else {
		t.m.puts.WithLabelValues("created").Inc()
	}
	return res, versionID, err
}

// DeleteObject keeps oldest in step with pruning (rule 6).
func (t *tap) DeleteObject(ctx context.Context, in *s3.DeleteObjectInput) (out *s3.DeleteObjectOutput, err error) {
	defer t.recoverTo("DeleteObject", &err)
	seq, isLedger := t.ledgerSeq(in.Bucket, in.Key)
	out, err = t.Backend.DeleteObject(ctx, in)
	if err == nil && isLedger {
		t.idx.deleted(seq)
	}
	return out, err
}

// DeleteObjects (multi-object delete) updates oldest like DeleteObject,
// applying the deleted ledgers in ascending order.
func (t *tap) DeleteObjects(ctx context.Context, in *s3.DeleteObjectsInput) (res s3response.DeleteResult, err error) {
	defer t.recoverTo("DeleteObjects", &err)
	res, err = t.Backend.DeleteObjects(ctx, in)
	if err != nil || in.Bucket == nil || *in.Bucket != t.bucket {
		return res, err
	}
	var seqs []uint32
	for _, d := range res.Deleted {
		if d.Key != nil {
			if seq, ok := sep54.ParseKey(t.prefix, *d.Key); ok {
				seqs = append(seqs, seq)
			}
		}
	}
	slices.SortFunc(seqs, cmp.Compare)
	for _, seq := range seqs {
		t.idx.deleted(seq)
	}
	return res, nil
}

// CopyObject is refused: a server-side copy would publish an object without
// the fsync chain or a commit record. Nothing in this system uses it.
func (t *tap) CopyObject(context.Context, s3response.CopyObjectInput) (s3response.CopyObjectOutput, error) {
	return s3response.CopyObjectOutput{}, errNotImpl
}

// syncPublished fsyncs the linked object (its data and xattrs), then every
// directory from its parent up to the bucket directory: the partition and
// prefix directories hold the new entries, and the upper ones matter the
// first time a directory is created.
func (t *tap) syncPublished(bucket, key string) error {
	bucketDir := filepath.Join(t.s3root, bucket)
	path := filepath.Join(bucketDir, key)
	if rel, err := filepath.Rel(bucketDir, path); err != nil || rel == "." || strings.HasPrefix(rel, "..") {
		return fmt.Errorf("object path %q escapes bucket %q", path, bucketDir)
	}
	if err := t.fsyncPath(StagePublished, path); err != nil {
		return err
	}
	for dir := filepath.Dir(path); ; dir = filepath.Dir(dir) {
		if err := t.fsyncPath(StageDir, dir); err != nil {
			return err
		}
		if dir == bucketDir || len(dir) <= len(bucketDir) {
			return nil
		}
	}
}

func (t *tap) fsyncPath(stage Stage, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return t.fsync(stage, f)
}

func (t *tap) fsync(stage Stage, f *os.File) error {
	start := t.now()
	err := t.fsyncFn(stage, f)
	t.m.fsyncDuration.WithLabelValues(string(stage)).Observe(t.now().Sub(start).Seconds())
	if err != nil {
		t.m.fsyncErrors.WithLabelValues(string(stage)).Inc()
		fe := &fsyncError{stage: stage, path: f.Name(), err: err}
		if stage == StageStaged {
			t.log.Warn("fsync of a staged body failed; the object is not published and the client will retry", "err", fe)
		}
		return fe
	}
	return nil
}

// publishFailed handles an error while making a linked object durable.
func (t *tap) publishFailed(err error) error {
	var fe *fsyncError
	if !errors.As(err, &fe) && errors.Is(err, fs.ErrNotExist) {
		// The object (or its directory) vanished under a concurrent delete.
		// It is not acknowledged; the client's retry writes it again.
		t.log.Warn("published object vanished before it was made durable", "err", err)
		t.m.puts.WithLabelValues("error").Inc()
		return errRetry
	}
	// Any other failure after the link (rule 1): on Linux a failed fsync
	// cannot be safely retried, and on xfs it means the filesystem shut
	// down. Refuse every later PUT until an operator restarts the lake.
	t.failClosed(err)
	t.m.puts.WithLabelValues("fail_closed").Inc()
	return errFailClosed
}

func (t *tap) failClosed(err error) {
	if t.failed.CompareAndSwap(false, true) {
		t.m.fsyncFailed.Set(1)
		t.log.Error("lake is now fail-closed: fsync chain failed after the link; every PUT gets 503 until restart", "err", err)
	}
}

// recoverTo turns a handler panic into a 500 (rule 7). A panic between link
// and fsync leaves the object linked but unacknowledged and uncommitted; the
// client's retry gets a 412, which re-runs the fsync chain (rule 2).
func (t *tap) recoverTo(op string, err *error) {
	if r := recover(); r != nil {
		t.log.Error("panic in lake handler", "op", op, "panic", r, "stack", string(debug.Stack()))
		*err = errInternal
	}
}

type fsyncError struct {
	stage Stage
	path  string
	err   error
}

func (e *fsyncError) Error() string {
	return fmt.Sprintf("fsync (%s) %s: %v", e.stage, e.path, e.err)
}

func (e *fsyncError) Unwrap() error { return e.err }

func isPreconditionFailed(err error) bool {
	var se s3err.S3Error
	return errors.As(err, &se) && se.BaseError().Code == "PreconditionFailed"
}

func defaultFsync(_ Stage, f *os.File) error { return f.Sync() }
