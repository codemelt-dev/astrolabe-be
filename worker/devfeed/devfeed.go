// Package devfeed implements `worker dev-feed`, a dev/test
// stand-in for Galexie: it copies ledger objects from the AWS public lake
// into the lake through the same SDK calls Galexie makes, so the lake, ingest
// and the archiver can be exercised end to end without stellar-core.
//
// It does not model captive-core rebuilds; real-Galexie runs stay mandatory
// (M4).
package devfeed

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/stellar/go-stellar-sdk/support/compressxdr"
	"github.com/stellar/go-stellar-sdk/support/datastore"

	"stellar-explorer/worker/config"
	"stellar-explorer/worker/sep54"
)

// Upstream is where ledger objects are copied from: the AWS public lake
// (*sep54.Source) in production, a fake in tests.
type Upstream interface {
	// GetLedger returns ledger seq's bytes and metadata, or an error
	// wrapping sep54.ErrNotFound.
	GetLedger(ctx context.Context, seq uint32) (sep54.Object, error)
	// Newest returns the newest ledger available, or sep54.ErrNoLedgers.
	Newest(ctx context.Context) (uint32, error)
}

// Options configure one dev-feed run.
type Options struct {
	Upstream Upstream
	// Lake is the lake as Galexie sees it: datastore.FromS3Client over an
	// explicit client with the gateway keys, path-style.
	Lake       datastore.DataStore
	Passphrase string
	// Start is Galexie's --start (required, ≥ 2). Like Galexie, dev-feed
	// resumes at max(Start, newest in the lake + 1).
	Start uint32
	// End bounds the run (inclusive); 0 follows the upstream tip forever.
	End uint32
	// Speed paces uploads by the ledgers' own close times: 1 is ledger
	// cadence, 10 is ten times faster, 0 uploads as fast as possible
	// (catch-up tests). At the upstream tip, uploads follow the network.
	Speed float64
	// Prefetch is how many upstream reads run ahead of the strictly
	// sequential uploads (default 16).
	Prefetch int
	// TipPoll is how often the upstream tip is re-read while waiting for a
	// new ledger (default 1 s).
	TipPoll time.Duration
	Log     *slog.Logger
}

// Result summarises a run.
type Result struct {
	// From is the first ledger dev-feed tried to upload (after resuming);
	// 0 if there was nothing to do.
	From uint32
	// Last is the last ledger uploaded or found already present.
	Last     uint32
	Uploaded int
	Existed  int
}

// CheckArchive refuses to feed a lake whose archiver would copy AWS-lake
// bytes into a real archive: archiving must be disabled or the
// archive bucket must be a "-dev" bucket.
func CheckArchive(a config.Archive) error {
	if a.Enabled && !strings.HasSuffix(a.Bucket, "-dev") {
		return fmt.Errorf("refusing to run: archiving is enabled for bucket %q; dev-feed needs archive.enabled = false or a bucket whose name ends in -dev", a.Bucket)
	}
	return nil
}

// Retry policy for upstream reads.
const (
	fetchAttempts   = 8
	fetchMaxBackoff = 5 * time.Second
	// drainTimeout bounds the in-flight upload after a stop signal, like
	// Galexie's 10 s drain.
	drainTimeout = 10 * time.Second
	// progressEvery is how often progress is logged.
	progressEvery = 30 * time.Second
)

// Run publishes the manifest, resumes like Galexie, then uploads ledgers one
// at a time until End (or forever). It returns nil when ctx is cancelled
// after finishing the in-flight upload; an upload or upstream error ends
// the run with an error (non-zero exit).
func Run(ctx context.Context, o Options) (Result, error) {
	if err := o.setDefaults(); err != nil {
		return Result{}, err
	}
	log := o.Log
	cfg := sep54.GalexieConfig(o.Passphrase)

	// Galexie's startup against its datastore (internal/app.go): the file
	// extension check, LoadSchema, then PublishConfig (the manifest).
	ext, err := datastore.GetLedgerFileExtension(ctx, o.Lake)
	switch {
	case errors.Is(err, datastore.ErrNoLedgerFiles):
	case err != nil:
		return Result{}, fmt.Errorf("determine ledger file extension: %w", err)
	case ext != compressxdr.DefaultCompressor.Name():
		return Result{}, fmt.Errorf("the lake holds ledger files with extension %q", ext)
	}
	if _, err := datastore.LoadSchema(ctx, o.Lake, cfg); err != nil {
		return Result{}, fmt.Errorf("load lake schema: %w", err)
	}
	manifest, created, err := datastore.PublishConfig(ctx, o.Lake, cfg)
	if err != nil {
		return Result{}, fmt.Errorf("publish manifest: %w", err)
	}
	log.Info("lake manifest", "created", created, "manifest", manifest)

	next, err := resume(ctx, o.Lake, o.Start, o.End)
	if err != nil {
		return Result{}, err
	}
	if next == 0 {
		log.Info("every requested ledger is already in the lake", "start", o.Start, "end", o.End)
		return Result{}, nil
	}
	if next != o.Start {
		log.Info("resuming after the newest ledger in the lake", "start", o.Start, "resume", next)
	}
	f := &feeder{o: o, log: log}
	return f.run(ctx, next)
}

func (o *Options) setDefaults() error {
	switch {
	case o.Upstream == nil || o.Lake == nil:
		return errors.New("dev-feed: upstream and lake are required")
	case o.Passphrase == "":
		return errors.New("dev-feed: network passphrase is required")
	case o.Start < 2:
		return errors.New("dev-feed: --start must be at least 2")
	case o.End != 0 && o.End < o.Start:
		return fmt.Errorf("dev-feed: --end %d is before --start %d", o.End, o.Start)
	case o.Speed < 0 || math.IsNaN(o.Speed) || math.IsInf(o.Speed, 0):
		return errors.New("dev-feed: --speed must be 0 (unpaced) or positive")
	}
	if o.Prefetch <= 0 {
		o.Prefetch = 16
	}
	if o.TipPoll <= 0 {
		o.TipPoll = time.Second
	}
	if o.Log == nil {
		o.Log = slog.Default()
	}
	o.Log = o.Log.With("component", "dev-feed")
	return nil
}

// resume mirrors Galexie's findResumeLedger (internal/resume.go): the first
// ledger to upload is max(start, newest+1) from one lexicographic listing,
// with no hole checks; 0 means everything up to end is already there.
func resume(ctx context.Context, lake datastore.DataStore, start, end uint32) (uint32, error) {
	var latest uint32
	var err error
	if end == 0 {
		latest, err = datastore.FindLatestLedgerSequence(ctx, lake)
	} else {
		latest, err = datastore.FindLatestLedgerUpToSequence(ctx, lake, end, sep54.Schema)
	}
	switch {
	case errors.Is(err, datastore.ErrNoValidLedgerFiles):
		return start, nil
	case err != nil:
		return 0, fmt.Errorf("find the newest ledger in the lake: %w", err)
	case latest < start:
		return start, nil
	case end != 0 && latest >= end:
		return 0, nil
	}
	return latest + 1, nil
}

type feeder struct {
	o   Options
	log *slog.Logger
}

// fetched is one upstream object, validated and ready to upload.
type fetched struct {
	seq uint32
	obj sep54.Object
	md  map[string]string
	hdr sep54.Header
	err error
}

func (f *feeder) run(ctx context.Context, next uint32) (res Result, err error) {
	res.From = next
	pctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	queue := make(chan chan fetched, f.o.Prefetch)
	wg.Add(1)
	go func() {
		defer wg.Done()
		f.produce(pctx, next, queue, &wg)
	}()
	defer func() {
		cancel()
		wg.Wait()
	}()

	prev, hasPrev := f.lakeHeader(ctx, next-1)
	var (
		paceStart time.Time
		paceClose int64
		lastLog   = time.Now()
		sinceLog  int
	)
	for slot := range queue {
		var r fetched
		select {
		case r = <-slot:
		case <-ctx.Done():
			return res, nil
		}
		if r.err != nil {
			if ctx.Err() != nil {
				return res, nil
			}
			return res, r.err
		}
		if hasPrev {
			if err := sep54.CheckChain(prev, r.hdr); err != nil {
				return res, fmt.Errorf("upstream chain: %w", err)
			}
		}

		// Pace by close times: ledger n is due (closeTime(n) -
		// closeTime(first)) / speed after the first upload.
		if f.o.Speed > 0 {
			if paceStart.IsZero() {
				paceStart, paceClose = time.Now(), r.hdr.CloseTime
			}
			due := paceStart.Add(time.Duration(float64(r.hdr.CloseTime-paceClose) * float64(time.Second) / f.o.Speed))
			if err := sleepUntil(ctx, due); err != nil {
				return res, nil
			}
		}

		// The upload itself survives a stop signal for up to drainTimeout.
		uctx, ucancel := context.WithTimeout(context.WithoutCancel(ctx), drainTimeout)
		uploaded, err := f.o.Lake.PutFileIfNotExists(uctx, sep54.ObjectPath(r.seq), bytes.NewReader(r.obj.Body), r.md)
		ucancel()
		if err != nil {
			return res, fmt.Errorf("upload ledger %d: %w", r.seq, err)
		}
		if uploaded {
			res.Uploaded++
		} else {
			res.Existed++
			f.log.Info("ledger already in the lake", "ledger", r.seq)
		}
		res.Last = r.seq
		prev, hasPrev = r.hdr, true
		sinceLog++
		f.log.Debug("uploaded ledger", "ledger", r.seq, "bytes", len(r.obj.Body), "uploaded", uploaded)
		if time.Since(lastLog) >= progressEvery {
			f.log.Info("progress", "ledger", r.seq, "uploads", sinceLog,
				"per_second", float64(sinceLog)/time.Since(lastLog).Seconds())
			lastLog, sinceLog = time.Now(), 0
		}
		if ctx.Err() != nil {
			return res, nil
		}
	}
	return res, nil
}

// lakeHeader returns the header of ledger seq if the lake already holds it,
// so the first upload of a resumed run is chain-checked too.
func (f *feeder) lakeHeader(ctx context.Context, seq uint32) (sep54.Header, bool) {
	if seq < 2 {
		return sep54.Header{}, false
	}
	rc, _, err := f.o.Lake.GetFile(ctx, sep54.ObjectPath(seq))
	if err != nil {
		return sep54.Header{}, false
	}
	defer rc.Close()
	body, err := io.ReadAll(io.LimitReader(rc, sep54.MaxObjectSize))
	if err != nil {
		return sep54.Header{}, false
	}
	h, err := sep54.DecodeHeader(seq, body)
	if err != nil {
		f.log.Warn("cannot decode the lake's newest ledger; not chain-checking the first upload", "ledger", seq, "err", err)
		return sep54.Header{}, false
	}
	return h, true
}

// produce reads ledgers next, next+1, … from upstream, at most Prefetch
// ahead of the uploads, and queues one result slot per ledger in order.
// It waits for the upstream tip to reach each ledger before reading it.
func (f *feeder) produce(ctx context.Context, next uint32, queue chan<- chan fetched, wg *sync.WaitGroup) {
	defer close(queue)
	var tip uint32
	for seq := next; f.o.End == 0 || seq <= f.o.End; seq++ {
		for seq > tip {
			t, err := f.upstreamTip(ctx)
			if err != nil {
				if ctx.Err() == nil {
					f.fail(ctx, queue, seq, err)
				}
				return
			}
			tip = t
			if seq <= tip {
				break
			}
			if err := sleepCtx(ctx, f.o.TipPoll); err != nil {
				return
			}
		}
		slot := make(chan fetched, 1)
		select {
		case queue <- slot:
		case <-ctx.Done():
			return
		}
		wg.Add(1)
		go func(seq uint32) {
			defer wg.Done()
			slot <- f.fetch(ctx, seq)
		}(seq)
		if seq == math.MaxUint32 {
			return
		}
	}
}

// fail queues an error slot (the consumer stops at it).
func (f *feeder) fail(ctx context.Context, queue chan<- chan fetched, seq uint32, err error) {
	slot := make(chan fetched, 1)
	slot <- fetched{seq: seq, err: err}
	select {
	case queue <- slot:
	case <-ctx.Done():
	}
}

// upstreamTip reads the upstream's newest ledger, retrying transient errors.
func (f *feeder) upstreamTip(ctx context.Context) (uint32, error) {
	var err error
	backoff := 250 * time.Millisecond
	for attempt := 1; attempt <= fetchAttempts; attempt++ {
		var tip uint32
		tip, err = f.o.Upstream.Newest(ctx)
		if err == nil {
			return tip, nil
		}
		if errors.Is(err, sep54.ErrNoLedgers) {
			return 0, nil
		}
		if ctx.Err() != nil || !sep54.IsUnavailable(err) {
			break
		}
		f.log.Warn("upstream tip unavailable; retrying", "attempt", attempt, "err", err)
		if serr := sleepCtx(ctx, backoff); serr != nil {
			return 0, serr
		}
		backoff = min(2*backoff, fetchMaxBackoff)
	}
	return 0, fmt.Errorf("upstream tip: %w", err)
}

// fetch reads one ledger with retries and validates it: stored CRC32C, zstd
// frame checksums, header-only decode (sequence and self-hash), and the 9
// metadata keys naming this ledger on this network.
func (f *feeder) fetch(ctx context.Context, seq uint32) fetched {
	var obj sep54.Object
	var err error
	backoff := 250 * time.Millisecond
	for attempt := 1; attempt <= fetchAttempts; attempt++ {
		obj, err = f.o.Upstream.GetLedger(ctx, seq)
		// NotFound at or below the listed tip is retried too: a listing
		// can run ahead of a read on some stores.
		if err == nil || ctx.Err() != nil || !(sep54.IsUnavailable(err) || errors.Is(err, sep54.ErrNotFound)) {
			break
		}
		f.log.Warn("upstream read failed; retrying", "ledger", seq, "attempt", attempt, "err", err)
		if serr := sleepCtx(ctx, backoff); serr != nil {
			return fetched{seq: seq, err: serr}
		}
		backoff = min(2*backoff, fetchMaxBackoff)
	}
	if err != nil {
		return fetched{seq: seq, err: fmt.Errorf("read ledger %d upstream: %w", seq, err)}
	}
	hdr, err := sep54.CheckObject(seq, obj)
	if err != nil {
		return fetched{seq: seq, err: fmt.Errorf("upstream object: %w", err)}
	}
	md, err := obj.LedgerMetadata()
	if err != nil {
		return fetched{seq: seq, err: fmt.Errorf("upstream object: %w", err)}
	}
	s := strconv.FormatUint(uint64(seq), 10)
	if md["start-ledger"] != s || md["end-ledger"] != s || md["network-passphrase"] != f.o.Passphrase {
		return fetched{seq: seq, err: fmt.Errorf("upstream ledger %d: metadata names ledgers %s-%s on %q",
			seq, md["start-ledger"], md["end-ledger"], md["network-passphrase"])}
	}
	return fetched{seq: seq, obj: obj, md: md, hdr: hdr}
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

func sleepUntil(ctx context.Context, due time.Time) error {
	d := time.Until(due)
	if d <= 0 {
		return ctx.Err()
	}
	return sleepCtx(ctx, d)
}
