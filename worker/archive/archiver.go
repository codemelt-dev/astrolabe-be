package archive

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stellar/go-stellar-sdk/xdr"

	"stellar-explorer/worker/bootstrap"
	"stellar-explorer/worker/sep54"
)

// The archiver copies the lake to the archive bucket, 1:1:
// same keys, bytes, metadata, Content-Type and manifest, strictly in ledger
// order, so the archive's newest object is always the archive tip.
//
//	archive_tip := newest ledger in the archive bucket
//	                 empty bucket → archive_origin − 1 (state/bootstrap.json)
//	                 any other error → unknown (retry; the pruner deletes nothing)
//	prev_hash   := hash(C) from bootstrap.json at the origin, else
//	               LedgerHash(header of archived object archive_tip)
//	at start: PUT the lake's .config.json bytes with If-None-Match: *
//	          (412: GET and require byte equality)
//	loop n := archive_tip + 1 …:
//	    lake.waitTip(after = n − 1)
//	    GET lake n (body, 9 x-amz-meta-*, Content-Type)
//	    validate: stored CRC32C + zstd frame checksums; header-only decode →
//	              sequence, self-hash, PreviousLedgerHash == prev_hash
//	    PUT archive: same key, bytes, metadata, Content-Type, If-None-Match: *
//	        412 → HEAD: ETag == local MD5 → ok
//	              else GET remote, compare header LedgerHash: equal → ok
//	              (producer bytes differ; keep remote), differ → halt + alert
//	    prev_hash = LedgerHash(n); archive_tip = n
//
// A missing or invalid ledger n is alerted and retried with backoff
// (≤ 30 s); the loop never exits, so a scan-and-fill repair resumes it
// without a restart. Validation is header-only on purpose: after a protocol
// upgrade the SDK does not know yet, archiving (and so pruning) continues
// while only the sequencer halts; an unknown LedgerCloseMeta version is
// uploaded on checksum validation alone, with an alert.

// putDrain bounds the upload in flight when ingest stops (its unit allows
// 90 s).
const putDrain = 60 * time.Second

// Tip is the lake's control endpoint (*sep54.TipClient).
type Tip interface {
	WaitAbove(ctx context.Context, after uint32, grace time.Duration) (sep54.Tip, error)
}

// Options configure an Archiver.
type Options struct {
	// Lake and Archive open the two stores (each with its own explicit
	// client); they are called again after a failure.
	Lake    func(ctx context.Context) (*sep54.Source, error)
	Archive func(ctx context.Context) (*sep54.Source, error)
	LakeTip Tip
	// Origin reads state/bootstrap.json (archive_origin and hash(C)).
	Origin func() (bootstrap.Origin, error)
	Log    *slog.Logger
	// Alert reports conditions that need an operator.
	Alert func(kind, msg string, args ...any)
	// MaxBackoff caps retry delays (at most 30 s).
	MaxBackoff time.Duration
	// HaltRecheck is how often a halted archiver (the archive holds a
	// different ledger) looks again (default 5 min).
	HaltRecheck time.Duration
}

// Blocked reasons, exported as a metric label.
const (
	BlockedNone        = ""
	BlockedUnavailable = "unavailable"    // the archive or the lake does not answer
	BlockedOrigin      = "origin_unknown" // empty archive and no bootstrap.json
	BlockedManifest    = "manifest"       // the archive's manifest differs
	BlockedMissing     = "missing"        // ledger n is not in the lake
	BlockedInvalid     = "invalid"        // ledger n failed validation
	BlockedHalted      = "halted"         // the archive holds a different ledger n
)

// Stats are the archiver's counters (metrics).
type Stats struct {
	// Created counts ledgers uploaded; Existed those already in the archive
	// with the same bytes; Differed those already there with other bytes
	// but the same ledger hash (kept).
	Created, Existed, Differed uint64
	// Bytes uploaded.
	Bytes uint64
	// LastArchived is when the tip last advanced.
	LastArchived time.Time
	// Blocked is why the archiver is not advancing ("" if it is).
	Blocked string
}

// Archiver copies the lake to the archive bucket.
type Archiver struct {
	o   Options
	log *slog.Logger

	tip atomic.Int64 // archive tip; -1 unknown

	mu      sync.Mutex
	stats   Stats
	alerted map[string]bool // alert kind + ledger, so a stuck loop alerts once
}

// New returns an archiver; Run starts it.
func New(o Options) *Archiver {
	if o.Log == nil {
		o.Log = slog.Default()
	}
	if o.Alert == nil {
		log := o.Log
		o.Alert = func(kind, msg string, args ...any) { log.Error(msg, append([]any{"alert", kind}, args...)...) }
	}
	if o.MaxBackoff <= 0 {
		o.MaxBackoff = 30 * time.Second
	}
	if o.HaltRecheck <= 0 {
		o.HaltRecheck = 5 * time.Minute
	}
	a := &Archiver{o: o, log: o.Log.With("component", "archiver"), alerted: map[string]bool{}}
	a.tip.Store(-1)
	return a
}

// Tip returns the archive tip once it is known: the newest archived ledger,
// or archive_origin − 1 for an empty bucket (nothing archived yet).
func (a *Archiver) Tip() (uint32, bool) {
	t := a.tip.Load()
	return uint32(t), t >= 0
}

// Stats returns a snapshot of the counters.
func (a *Archiver) Stats() Stats {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.stats
}

func (a *Archiver) setBlocked(reason string) {
	a.mu.Lock()
	a.stats.Blocked = reason
	a.mu.Unlock()
}

// alertOnce raises an alert once per kind and key until clear(kind).
func (a *Archiver) alertOnce(kind, key, msg string, args ...any) {
	a.mu.Lock()
	k := kind + "\x00" + key
	seen := a.alerted[k]
	a.alerted[k] = true
	a.mu.Unlock()
	if !seen {
		a.o.Alert(kind, msg, args...)
	} else {
		a.log.Warn(msg, args...)
	}
}

func (a *Archiver) clearAlerts() {
	a.mu.Lock()
	clear(a.alerted)
	a.mu.Unlock()
}

// Run archives until ctx ends. It never returns early: every failure is
// retried.
func (a *Archiver) Run(ctx context.Context) error {
	st, err := a.start(ctx)
	if err != nil {
		return err // ctx ended
	}
	a.log.Info("archiving", "archive_tip", st.tip, "next", st.tip+1)
	for {
		if err := a.archiveNext(ctx, st); err != nil {
			return err // ctx ended
		}
	}
}

// state is the loop's position.
type state struct {
	lake, archive *sep54.Source
	tip           uint32
	prev          xdr.Hash
	prevKnown     bool
}

// start opens both stores, copies the manifest and finds the archive tip
// and prev_hash, retrying until it succeeds or ctx ends.
func (a *Archiver) start(ctx context.Context) (*state, error) {
	b := newBackoff(a.o.MaxBackoff)
	for {
		st, reason, err := a.tryStart(ctx)
		if err == nil {
			a.setBlocked(BlockedNone)
			return st, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		a.setBlocked(reason)
		if reason == BlockedUnavailable {
			a.log.Warn("archive tip unknown; retrying", "err", err)
		}
		if err := b.sleep(ctx); err != nil {
			return nil, err
		}
	}
}

func (a *Archiver) tryStart(ctx context.Context) (*state, string, error) {
	lake, err := a.o.Lake(ctx)
	if err != nil {
		return nil, BlockedUnavailable, err
	}
	arch, err := a.o.Archive(ctx)
	if err != nil {
		return nil, BlockedUnavailable, err
	}
	if reason, err := a.copyManifest(ctx, lake, arch); err != nil {
		return nil, reason, err
	}
	st := &state{lake: lake, archive: arch}
	newest, err := arch.Newest(ctx)
	switch {
	case errors.Is(err, sep54.ErrNoLedgers):
		origin, err := a.o.Origin()
		if err != nil {
			a.alertOnce("archive_origin_unknown", "", "the archive bucket is empty and archive_origin is unknown: run `worker bootstrap` (or --recreate-origin)", "err", err)
			return nil, BlockedOrigin, err
		}
		h, err := origin.Hash()
		if err != nil {
			return nil, BlockedOrigin, err
		}
		st.tip, st.prev, st.prevKnown = origin.ArchiveOrigin-1, h, true
		a.log.Info("the archive bucket is empty: starting at archive_origin", "archive_origin", origin.ArchiveOrigin)
	case err != nil:
		return nil, BlockedUnavailable, err
	default:
		obj, err := arch.GetLedger(ctx, newest)
		if err != nil {
			return nil, BlockedUnavailable, fmt.Errorf("read the archive tip %d: %w", newest, err)
		}
		h, err := sep54.CheckObject(newest, obj)
		switch {
		case err == nil:
			st.prev, st.prevKnown = h.Hash, true
		case errors.Is(err, sep54.ErrUnknownVersion):
			a.alertOnce("archive_unknown_version", fmt.Sprint(newest), "the archive tip has an unknown LedgerCloseMeta version: the next ledger's chain is not checked", "ledger", newest)
		default:
			a.alertOnce("archive_invalid_ledger", fmt.Sprint(newest), "the archive tip object is invalid", "ledger", newest, "err", err)
			return nil, BlockedInvalid, err
		}
		st.tip = newest
	}
	a.tip.Store(int64(st.tip))
	return st, BlockedNone, nil
}

// copyManifest PUTs the lake's .config.json bytes verbatim with
// If-None-Match: *; on 412 the archive's must be byte-identical.
func (a *Archiver) copyManifest(ctx context.Context, lake, arch *sep54.Source) (string, error) {
	m, err := lake.GetObject(ctx, sep54.ManifestFile)
	if errors.Is(err, sep54.ErrNotFound) {
		a.log.Info("the lake has no manifest yet (Galexie writes it at its first start); waiting")
		return BlockedUnavailable, err
	}
	if err != nil {
		return BlockedUnavailable, err
	}
	err = put(ctx, arch, sep54.ManifestFile, m)
	if err == nil {
		a.log.Info("manifest copied to the archive")
		return BlockedNone, nil
	}
	if !sep54.IsPreconditionFailed(err) {
		return BlockedUnavailable, err
	}
	remote, err := arch.GetObject(ctx, sep54.ManifestFile)
	if err != nil {
		return BlockedUnavailable, err
	}
	if !bytes.Equal(remote.Body, m.Body) {
		a.alertOnce("archive_manifest_mismatch", "", "the archive's .config.json differs from the lake's: archiving is halted",
			"lake", string(m.Body), "archive", string(remote.Body))
		return BlockedManifest, errors.New("manifest mismatch")
	}
	return BlockedNone, nil
}

// archiveNext archives ledger tip+1, retrying until it is in the archive.
func (a *Archiver) archiveNext(ctx context.Context, st *state) error {
	n := st.tip + 1
	b := newBackoff(a.o.MaxBackoff)
	for {
		if _, err := a.o.LakeTip.WaitAbove(ctx, n-1, 0); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			a.setBlocked(BlockedUnavailable)
			a.log.Warn("lake tip", "err", err)
			if err := b.sleep(ctx); err != nil {
				return err
			}
			continue
		}
		reason, wait, err := a.tryArchive(ctx, st, n)
		if err == nil {
			a.setBlocked(BlockedNone)
			a.clearAlerts()
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		a.setBlocked(reason)
		if wait > 0 {
			if err := sleepCtx(ctx, wait); err != nil {
				return err
			}
			continue
		}
		if err := b.sleep(ctx); err != nil {
			return err
		}
	}
}

// tryArchive validates and uploads ledger n once. On failure it returns
// why, and how long to wait if the default backoff does not apply.
func (a *Archiver) tryArchive(ctx context.Context, st *state, n uint32) (string, time.Duration, error) {
	key := fmt.Sprint(n)
	obj, err := st.lake.GetLedger(ctx, n)
	switch {
	case errors.Is(err, sep54.ErrNotFound):
		a.alertOnce("archive_missing_ledger", key, "ledger is missing from the lake: repair it with scan-and-fill (the archiver resumes by itself)", "ledger", n)
		return BlockedMissing, 0, err
	case err != nil:
		a.log.Warn("read the lake", "ledger", n, "err", err)
		return BlockedUnavailable, 0, err
	}
	h, err := sep54.CheckObject(n, obj)
	unknown := errors.Is(err, sep54.ErrUnknownVersion)
	switch {
	case unknown:
		a.alertOnce("archive_unknown_version", key, "unknown LedgerCloseMeta version: uploading on checksum validation alone", "ledger", n, "err", err)
	case err != nil:
		a.alertOnce("archive_invalid_ledger", key, "ledger in the lake is invalid: repair it (the archiver retries)", "ledger", n, "err", err)
		return BlockedInvalid, 0, err
	case st.prevKnown && h.PreviousHash != st.prev:
		err := fmt.Errorf("previous hash %x does not match ledger %d's hash %x", h.PreviousHash, n-1, st.prev)
		a.alertOnce("archive_invalid_ledger", key, "ledger in the lake does not chain to the archive tip: repair it (the archiver retries)", "ledger", n, "err", err)
		return BlockedInvalid, 0, err
	}
	if _, err := obj.LedgerMetadata(); err != nil {
		a.alertOnce("archive_metadata", key, "ledger object lacks Galexie's metadata; copied as is", "ledger", n, "err", err)
	}

	// A shutdown lets the current PUT finish, bounded.
	pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), putDrain)
	defer cancel()
	result := "created"
	err = put(pctx, st.archive, sep54.ObjectPath(n), obj)
	if sep54.IsPreconditionFailed(err) {
		result, err = a.existing(pctx, st.archive, n, obj, h, unknown)
		if result == "halted" {
			return BlockedHalted, a.o.HaltRecheck, err
		}
	}
	if err != nil {
		a.log.Warn("upload to the archive", "ledger", n, "err", err)
		return BlockedUnavailable, 0, err
	}

	a.mu.Lock()
	switch result {
	case "created":
		a.stats.Created++
		a.stats.Bytes += uint64(len(obj.Body))
	case "exists":
		a.stats.Existed++
	case "differs":
		a.stats.Differed++
	}
	a.stats.LastArchived = time.Now()
	a.mu.Unlock()
	st.tip = n
	st.prev, st.prevKnown = h.Hash, !unknown
	a.tip.Store(int64(n))
	a.log.Debug("archived", "ledger", n, "result", result, "bytes", len(obj.Body))
	return BlockedNone, 0, nil
}

// existing handles a 412: the archive already holds ledger n. Same bytes
// (ETag = MD5) or the same ledger hash from another producer is fine;
// anything else halts the archiver.
func (a *Archiver) existing(ctx context.Context, arch *sep54.Source, n uint32, obj sep54.Object, h sep54.Header, unknown bool) (string, error) {
	head, err := arch.Client().HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(arch.Bucket()), Key: aws.String(arch.Key(sep54.ObjectPath(n)))})
	if err != nil {
		return "", fmt.Errorf("HEAD after 412: %w", err)
	}
	sum := md5.Sum(obj.Body)
	if strings.Trim(aws.ToString(head.ETag), `"`) == hex.EncodeToString(sum[:]) {
		return "exists", nil
	}
	remote, err := arch.GetLedger(ctx, n)
	if err != nil {
		return "", fmt.Errorf("GET after 412: %w", err)
	}
	rh, rerr := sep54.DecodeHeader(n, remote.Body)
	if rerr == nil && !unknown && rh.Hash == h.Hash {
		a.log.Info("producer bytes differ; keeping the archive's object (same ledger hash)", "ledger", n)
		return "differs", nil
	}
	err = fmt.Errorf("the archive holds a different ledger %d (archive header hash %x, decode error %v; lake hash %x)", n, rh.Hash, rerr, h.Hash)
	a.alertOnce("archive_hash_mismatch", fmt.Sprint(n), "the archive holds a different ledger: archiving is halted", "ledger", n, "err", err)
	return "halted", err
}

// put uploads obj under rel exactly: same bytes, metadata and Content-Type,
// with a CRC32C (an aws-chunked trailer over HTTPS) and If-None-Match: *.
// The store's own checksum of what it received must match the bytes: the
// CRC32C where it keeps one, else the ETag, a single-part upload's MD5
// (Hetzner keeps no CRC32C).
func put(ctx context.Context, dst *sep54.Source, rel string, obj sep54.Object) error {
	in := &s3.PutObjectInput{
		Bucket:            aws.String(dst.Bucket()),
		Key:               aws.String(dst.Key(rel)),
		Body:              bytes.NewReader(obj.Body),
		ContentLength:     aws.Int64(int64(len(obj.Body))),
		Metadata:          obj.Metadata,
		ChecksumAlgorithm: types.ChecksumAlgorithmCrc32c,
		IfNoneMatch:       aws.String("*"),
	}
	if obj.ContentType != "" {
		in.ContentType = aws.String(obj.ContentType)
	}
	out, err := dst.Client().PutObject(ctx, in)
	if err != nil {
		return err
	}
	if err := sep54.VerifyStored(sep54.Object{Body: obj.Body, ChecksumCRC32C: aws.ToString(out.ChecksumCRC32C), ETag: aws.ToString(out.ETag)}); err != nil {
		return fmt.Errorf("%s: the archive did not store these bytes: %w", rel, err)
	}
	if obj.ChecksumCRC32C != "" && obj.ChecksumCRC32C != sep54.CRC32C(obj.Body) {
		return fmt.Errorf("%s: the lake's stored CRC32C %s does not match the bytes", rel, obj.ChecksumCRC32C)
	}
	return nil
}

// backoff doubles from 1 s up to max.
type backoff struct {
	cur, max time.Duration
}

func newBackoff(max time.Duration) *backoff { return &backoff{max: max} }

func (b *backoff) sleep(ctx context.Context) error {
	if b.cur == 0 {
		b.cur = min(time.Second, b.max)
	} else {
		b.cur = min(2*b.cur, b.max)
	}
	return sleepCtx(ctx, b.cur)
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
