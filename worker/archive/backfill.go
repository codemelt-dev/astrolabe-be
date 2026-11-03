package archive

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stellar/go-stellar-sdk/xdr"
	"golang.org/x/time/rate"

	"stellar-explorer/worker/sep54"
)

// Backfill copies older ledgers into the archive, below what the archiver
// writes: the same keys, bytes, metadata and Content-Type, with the same
// checks.
//
//	every ledger n in [From, To], in parallel:
//	    GET source n → CheckObject (stored CRC32C, zstd frame checksums,
//	                   header self-hash)
//	    PUT archive, If-None-Match: *
//	        412 → HEAD: ETag == local MD5 → exists
//	              else GET the archive's, same ledger hash → differs (kept)
//	              else stop: the archive holds a different ledger
//	the hash chain, per partition (64,000 ledgers) once it is complete:
//	    within it, and across its finished neighbours; the newest partition
//	    chains to ledger To+1, read from the archive, so every copied
//	    ledger is linked to the ones Galexie wrote
//	progress: each finished partition goes to the state file; a rerun skips
//	    it and redoes an unfinished one. A partition's listing comes first:
//	    a ledger stored with the same ETag (MD5) needs no upload.
//
// Partitions run newest first, a few at a time, so one slow upload doesn't
// hold up the others. Read and upload errors are retried with backoff; a
// ledger the source lacks, one that stays invalid, a broken chain or a
// different ledger in the archive stop the run.
//
// The archive's request rate adapts to the store: each SlowDown cuts it by
// a fifth (at most once a second), and every 10 s without one raises it by
// 2 % of Rate, up to Rate. A throttled request is retried after a second.

// LedgerSource is where a backfill reads: the AWS public lake.
type LedgerSource interface {
	GetLedger(ctx context.Context, seq uint32) (sep54.Object, error)
}

// BackfillOptions configure Backfill.
type BackfillOptions struct {
	Source  LedgerSource
	Archive *sep54.Source
	// From and To are the range, inclusive. Ledger To+1 must be in the
	// archive: it anchors the hash chain.
	From, To uint32
	// Workers is how many ledgers are in flight.
	Workers int
	// Rate caps the archive's requests per second (0: no cap). Hetzner
	// allows 750 per bucket and per source IP.
	Rate float64
	// State is the progress file. Empty: no resume.
	State string
	// DryRun reads and checks only: no uploads, no state.
	DryRun bool
	Log    *slog.Logger
}

// BackfillStats are a run's counters.
type BackfillStats struct {
	Partitions, Skipped                 int
	Created, Existed, Differed, Checked uint64
	Bytes, Throttled                    uint64
}

const (
	// backfillInFlight is how many partitions may be open at once.
	backfillInFlight = 4
	// minRate is the floor of the adaptive rate.
	minRate = 10
	// throttlePause is the wait before a throttled request is retried.
	throttlePause = time.Second
	// rampEvery is how often the rate may rise; progressEvery how often
	// progress is logged.
	rampEvery     = 10 * time.Second
	progressEvery = 30 * time.Second
)

// invalidRetries is how often an invalid ledger is read again before the
// run stops.
const invalidRetries = 3

// donePartition is a finished partition, as kept in the state file.
type donePartition struct {
	Start             uint32 `json:"start"`
	End               uint32 `json:"end"`
	FirstPreviousHash string `json:"first_previous_hash"`
	LastHash          string `json:"last_hash"`
}

type backfillState struct {
	Partitions []donePartition `json:"partitions"`
}

// openPartition is a partition in progress: the headers by offset, and the
// ETags of the ledgers the archive already holds.
type openPartition struct {
	start, end uint32
	headers    []sep54.Header
	left       int
	stored     map[uint32]string
}

type backfill struct {
	o       BackfillOptions
	log     *slog.Logger
	limiter *rate.Limiter
	anchor  xdr.Hash // ledger To+1's previous hash
	cancel  context.CancelCauseFunc
	began   time.Time
	total   int // ledgers to copy in this run

	mu      sync.Mutex
	open    map[uint32]*openPartition // by start
	byStart map[uint32]donePartition
	byEnd   map[uint32]donePartition
	done    int // ledgers finished in this run
	stats   BackfillStats
	slots   chan struct{}

	created, existed, differed, checked, bytes, throttles atomic.Uint64
	lastThrottle                                          atomic.Int64 // Unix nanoseconds
	gets, getNanos, puts, putNanos                        atomic.Int64 // read and upload latency
	cutMu                                                 sync.Mutex
	lastCut                                               time.Time
}

// fatal marks an error that stops the run instead of being retried.
type fatal struct{ error }

func (f fatal) Unwrap() error { return f.error }

// Backfill runs a backfill until it is done, ctx ends or an error stops it.
func Backfill(ctx context.Context, o BackfillOptions) (BackfillStats, error) {
	switch {
	case o.From < 2 || o.From > o.To:
		return BackfillStats{}, fmt.Errorf("backfill: bad range %d-%d", o.From, o.To)
	case o.Workers < 1:
		return BackfillStats{}, errors.New("backfill: workers must be at least 1")
	}
	b := &backfill{o: o, log: o.Log, open: map[uint32]*openPartition{},
		byStart: map[uint32]donePartition{}, byEnd: map[uint32]donePartition{}, slots: make(chan struct{}, backfillInFlight)}
	if b.log == nil {
		b.log = slog.Default()
	}
	if o.Rate > 0 {
		b.limiter = rate.NewLimiter(rate.Limit(o.Rate), 10)
	}
	if err := b.loadState(); err != nil {
		return BackfillStats{}, err
	}
	if err := b.readAnchor(ctx); err != nil {
		return BackfillStats{}, err
	}

	ctx, b.cancel = context.WithCancelCause(ctx)
	defer b.cancel(nil)
	parts := backfillPartitions(o.From, o.To)
	var todo [][2]uint32
	for _, p := range parts {
		if d, ok := b.byStart[p[0]]; ok && d.End == p[1] {
			b.stats.Skipped++
			continue
		}
		todo = append(todo, p)
		b.total += int(p[1] - p[0] + 1)
	}
	b.stats.Partitions = len(parts)
	b.began = time.Now()
	b.log.Info("backfill", "from", o.From, "to", o.To, "partitions", len(parts), "done_before", b.stats.Skipped,
		"ledgers", b.total, "workers", o.Workers, "rate", o.Rate, "dry_run", o.DryRun)

	stop, bg := make(chan struct{}), sync.WaitGroup{}
	bg.Go(func() { b.background(ctx, stop) })
	jobs := make(chan uint32, o.Workers)
	var wg sync.WaitGroup
	for range o.Workers {
		wg.Go(func() {
			for n := range jobs {
				if ctx.Err() != nil {
					continue
				}
				h, err := b.copyLedger(ctx, n)
				if err != nil {
					b.cancel(err)
					continue
				}
				b.finished(n, h)
			}
		})
	}
feed:
	for _, p := range todo {
		select {
		case b.slots <- struct{}{}:
		case <-ctx.Done():
			break feed
		}
		stored := b.listStored(ctx, p[0], p[1])
		b.mu.Lock()
		b.open[p[0]] = &openPartition{start: p[0], end: p[1], headers: make([]sep54.Header, p[1]-p[0]+1), left: int(p[1] - p[0] + 1), stored: stored}
		b.mu.Unlock()
		for n := p[1]; n >= p[0]; n-- {
			select {
			case jobs <- n:
			case <-ctx.Done():
				break feed
			}
		}
	}
	close(jobs)
	wg.Wait()
	close(stop)
	bg.Wait()

	b.mu.Lock()
	defer b.mu.Unlock()
	b.stats.Created, b.stats.Existed, b.stats.Differed = b.created.Load(), b.existed.Load(), b.differed.Load()
	b.stats.Checked, b.stats.Bytes, b.stats.Throttled = b.checked.Load(), b.bytes.Load(), b.throttles.Load()
	return b.stats, context.Cause(ctx)
}

// backfillPartitions splits [from, to] at partition boundaries, newest
// first.
func backfillPartitions(from, to uint32) [][2]uint32 {
	const size = sep54.LedgersPerFile * sep54.FilesPerPartition
	var out [][2]uint32
	for end := to; ; {
		start := max(end/size*size, from)
		out = append(out, [2]uint32{start, end})
		if start == from {
			return out
		}
		end = start - 1
	}
}

// readAnchor reads ledger To+1 from the archive: the newest copied ledger
// must chain to it.
func (b *backfill) readAnchor(ctx context.Context) error {
	n := b.o.To + 1
	obj, err := b.o.Archive.GetLedger(ctx, n)
	if err != nil {
		return fmt.Errorf("backfill: read ledger %d from the archive (the range must end just below what it holds): %w", n, err)
	}
	h, err := sep54.DecodeHeader(n, obj.Body)
	if err != nil {
		return fmt.Errorf("backfill: ledger %d in the archive: %w", n, err)
	}
	b.anchor = h.PreviousHash
	return nil
}

// copyLedger reads, checks and uploads ledger n. Only fatal errors return.
func (b *backfill) copyLedger(ctx context.Context, n uint32) (sep54.Header, error) {
	obj, h, err := b.read(ctx, n)
	if err != nil {
		return h, err
	}
	if b.o.DryRun {
		b.checked.Add(1)
		return h, nil
	}
	if etag, ok := b.storedETag(n); ok {
		if sum := md5.Sum(obj.Body); etag == hex.EncodeToString(sum[:]) {
			b.existed.Add(1)
			return h, nil
		}
		// Other bytes: the upload's 412 compares the ledgers.
	}

	wait := newBackoff(time.Minute)
	for {
		err := b.upload(ctx, n, obj, h)
		var f fatal
		switch {
		case err == nil:
			return h, nil
		case ctx.Err() != nil:
			return h, context.Cause(ctx)
		case errors.As(err, &f):
			return h, err
		case sep54.IsThrottled(err):
			b.throttle()
			if sleepCtx(ctx, throttlePause) != nil {
				return h, context.Cause(ctx)
			}
			continue
		}
		b.log.Warn("upload to the archive", "ledger", n, "err", err)
		if wait.sleep(ctx) != nil {
			return h, context.Cause(ctx)
		}
	}
}

// listStored lists what the archive holds of partition [start, end]:
// ledger → ETag. It gives up after a few errors (nil: every ledger is
// uploaded, and a 412 sorts out what exists).
func (b *backfill) listStored(ctx context.Context, start, end uint32) map[uint32]string {
	if b.o.DryRun {
		return nil
	}
	arch := b.o.Archive
	in := &s3.ListObjectsV2Input{Bucket: aws.String(arch.Bucket()), Prefix: aws.String(arch.Key(sep54.PartitionName(start)) + "/")}
	stored, failures := map[uint32]string{}, 0
	for {
		if b.wait(ctx) != nil {
			return nil
		}
		page, err := arch.Client().ListObjectsV2(ctx, in)
		switch {
		case err == nil:
		case sep54.IsThrottled(err):
			b.throttle()
			if sleepCtx(ctx, throttlePause) != nil {
				return nil
			}
			continue
		case ctx.Err() != nil:
			return nil
		default:
			if failures++; failures > 5 {
				b.log.Warn("list a partition: uploading every ledger", "start", start, "err", err)
				return nil
			}
			if sleepCtx(ctx, time.Duration(failures)*time.Second) != nil {
				return nil
			}
			continue
		}
		for _, o := range page.Contents {
			if seq, ok := sep54.ParseKey(arch.Prefix(), aws.ToString(o.Key)); ok && seq >= start && seq <= end {
				stored[seq] = strings.ToLower(strings.Trim(aws.ToString(o.ETag), `"`))
			}
		}
		if !aws.ToBool(page.IsTruncated) {
			return stored
		}
		in.ContinuationToken = page.NextContinuationToken
	}
}

// storedETag is ledger n's ETag in the archive, from its partition's
// listing.
func (b *backfill) storedETag(n uint32) (string, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, p := range b.open {
		if n >= p.start && n <= p.end {
			etag, ok := p.stored[n]
			return etag, ok
		}
	}
	return "", false
}

// read gets ledger n from the source and checks it. Unavailable reads are
// retried until ctx ends; an invalid ledger is read again a few times.
func (b *backfill) read(ctx context.Context, n uint32) (sep54.Object, sep54.Header, error) {
	wait := newBackoff(time.Minute)
	for invalids := 0; ; {
		began := time.Now()
		obj, err := b.o.Source.GetLedger(ctx, n)
		b.gets.Add(1)
		b.getNanos.Add(int64(time.Since(began)))
		if errors.Is(err, sep54.ErrNotFound) {
			return obj, sep54.Header{}, fmt.Errorf("the source lacks ledger %d: %w", n, err)
		}
		if err == nil {
			h, cerr := sep54.CheckObject(n, obj)
			if cerr == nil {
				return obj, h, nil
			}
			if errors.Is(cerr, sep54.ErrUnknownVersion) || invalids >= invalidRetries {
				return obj, h, fmt.Errorf("ledger %d from the source: %w", n, cerr)
			}
			invalids++
			err = cerr
		}
		if ctx.Err() != nil {
			return obj, sep54.Header{}, context.Cause(ctx)
		}
		b.log.Warn("read the source", "ledger", n, "err", err)
		if wait.sleep(ctx) != nil {
			return obj, sep54.Header{}, context.Cause(ctx)
		}
	}
}

// upload puts ledger n once; a 412 compares with the archive's copy.
func (b *backfill) upload(ctx context.Context, n uint32, obj sep54.Object, h sep54.Header) error {
	if err := b.wait(ctx); err != nil {
		return err
	}
	began := time.Now()
	err := put(ctx, b.o.Archive, sep54.ObjectPath(n), obj)
	b.puts.Add(1)
	b.putNanos.Add(int64(time.Since(began)))
	switch {
	case err == nil:
		b.created.Add(1)
		b.bytes.Add(uint64(len(obj.Body)))
		return nil
	case sep54.IsAccessDenied(err):
		return fatal{fmt.Errorf("upload ledger %d: %w", n, err)}
	case !sep54.IsPreconditionFailed(err):
		return err
	}

	if err := b.wait(ctx); err != nil {
		return err
	}
	arch := b.o.Archive
	head, err := arch.Client().HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(arch.Bucket()), Key: aws.String(arch.Key(sep54.ObjectPath(n)))})
	if err != nil {
		return fmt.Errorf("HEAD after 412: %w", err)
	}
	if sum := md5.Sum(obj.Body); strings.Trim(aws.ToString(head.ETag), `"`) == hex.EncodeToString(sum[:]) {
		b.existed.Add(1)
		return nil
	}
	if err := b.wait(ctx); err != nil {
		return err
	}
	remote, err := arch.GetLedger(ctx, n)
	if err != nil {
		return fmt.Errorf("GET after 412: %w", err)
	}
	if rh, err := sep54.DecodeHeader(n, remote.Body); err != nil || rh.Hash != h.Hash {
		return fatal{fmt.Errorf("the archive holds a different ledger %d (archive hash %x, decode error %v; source hash %x)", n, rh.Hash, err, h.Hash)}
	}
	b.differed.Add(1)
	return nil
}

// throttle records a SlowDown and cuts the rate, at most once a second.
func (b *backfill) throttle() {
	b.throttles.Add(1)
	now := time.Now()
	b.lastThrottle.Store(now.UnixNano())
	if b.limiter == nil {
		return
	}
	b.cutMu.Lock()
	defer b.cutMu.Unlock()
	if now.Sub(b.lastCut) < time.Second {
		return
	}
	b.lastCut = now
	b.limiter.SetLimit(max(minRate, b.limiter.Limit()*0.8))
}

// background raises the rate while the store doesn't throttle, and logs
// progress, until stop or ctx ends.
func (b *backfill) background(ctx context.Context, stop <-chan struct{}) {
	ramp, progress := time.NewTicker(rampEvery), time.NewTicker(progressEvery)
	defer ramp.Stop()
	defer progress.Stop()
	lastDone, lastThrottles, lastAt := 0, uint64(0), time.Now()
	var lastGets, lastGetNanos, lastPuts, lastPutNanos int64
	avgMs := func(nanos, n int64) string { return fmt.Sprintf("%.0f", float64(nanos)/float64(max(n, 1))/1e6) }
	for {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		case <-ramp.C:
			if b.limiter != nil && time.Since(time.Unix(0, b.lastThrottle.Load())) >= rampEvery {
				b.limiter.SetLimit(min(rate.Limit(b.o.Rate), b.limiter.Limit()+rate.Limit(b.o.Rate/50)))
			}
		case now := <-progress.C:
			b.mu.Lock()
			done := b.done
			b.mu.Unlock()
			speed := float64(done-lastDone) / now.Sub(lastAt).Seconds()
			limit := "none"
			if b.limiter != nil {
				limit = fmt.Sprintf("%.0f", float64(b.limiter.Limit()))
			}
			gets, getNanos, puts, putNanos := b.gets.Load(), b.getNanos.Load(), b.puts.Load(), b.putNanos.Load()
			b.log.Info("progress", "ledgers_done", done, "ledgers_left", b.total-done, "per_second", fmt.Sprintf("%.0f", speed),
				"rate_limit", limit, "throttled", b.throttles.Load()-lastThrottles,
				"read_ms", avgMs(getNanos-lastGetNanos, gets-lastGets), "upload_ms", avgMs(putNanos-lastPutNanos, puts-lastPuts),
				"eta", (time.Duration(float64(b.total-done)/max(speed, 1e-9)) * time.Second).Round(time.Minute))
			lastDone, lastThrottles, lastAt = done, b.throttles.Load(), now
			lastGets, lastGetNanos, lastPuts, lastPutNanos = gets, getNanos, puts, putNanos
		}
	}
}

func (b *backfill) wait(ctx context.Context) error {
	if b.limiter == nil {
		return nil
	}
	return b.limiter.Wait(ctx)
}

// finished records ledger n; the last ledger of a partition closes it.
func (b *backfill) finished(n uint32, h sep54.Header) {
	b.mu.Lock()
	var p *openPartition
	for _, q := range b.open {
		if n >= q.start && n <= q.end {
			p = q
			break
		}
	}
	p.headers[n-p.start] = h
	p.left--
	b.done++
	if p.left > 0 {
		b.mu.Unlock()
		return
	}
	delete(b.open, p.start)
	err := b.close(p)
	b.mu.Unlock()
	<-b.slots
	if err != nil {
		b.cancel(fatal{err})
	}
}

// close checks a complete partition's chain, inside and to its finished
// neighbours, and records it. b.mu is held.
func (b *backfill) close(p *openPartition) error {
	for i := 1; i < len(p.headers); i++ {
		if err := sep54.CheckChain(p.headers[i-1], p.headers[i]); err != nil {
			return err
		}
	}
	first, last := p.headers[0], p.headers[len(p.headers)-1]
	d := donePartition{Start: p.start, End: p.end, FirstPreviousHash: hex.EncodeToString(first.PreviousHash[:]), LastHash: hex.EncodeToString(last.Hash[:])}
	if p.end == b.o.To {
		if last.Hash != b.anchor {
			return fmt.Errorf("ledger %d (hash %x) does not chain to ledger %d in the archive (previous hash %x)", p.end, last.Hash, p.end+1, b.anchor)
		}
	} else if above, ok := b.byStart[p.end+1]; ok && above.FirstPreviousHash != d.LastHash {
		return fmt.Errorf("ledger %d (hash %s) does not chain to ledger %d (previous hash %s)", p.end, d.LastHash, p.end+1, above.FirstPreviousHash)
	}
	if below, ok := b.byEnd[p.start-1]; ok && below.LastHash != d.FirstPreviousHash {
		return fmt.Errorf("ledger %d (previous hash %s) does not chain to ledger %d (hash %s)", p.start, d.FirstPreviousHash, p.start-1, below.LastHash)
	}
	b.byStart[d.Start], b.byEnd[d.End] = d, d
	if err := b.saveState(); err != nil {
		return err
	}
	elapsed := time.Since(b.began)
	speed := float64(b.done) / elapsed.Seconds()
	eta := time.Duration(float64(b.total-b.done)/max(speed, 1e-9)) * time.Second
	b.log.Info("partition done", "start", p.start, "end", p.end, "ledgers_done", b.done, "ledgers_left", b.total-b.done,
		"per_second", fmt.Sprintf("%.0f", speed), "eta", eta.Round(time.Minute),
		"created", b.created.Load(), "existed", b.existed.Load(), "differed", b.differed.Load(), "checked", b.checked.Load(),
		"gib", fmt.Sprintf("%.1f", float64(b.bytes.Load())/(1<<30)))
	return nil
}

func (b *backfill) loadState() error {
	if b.o.State == "" || b.o.DryRun {
		return nil
	}
	raw, err := os.ReadFile(b.o.State)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	var st backfillState
	if err := json.Unmarshal(raw, &st); err != nil {
		return fmt.Errorf("backfill: %s: %w", b.o.State, err)
	}
	for _, d := range st.Partitions {
		b.byStart[d.Start], b.byEnd[d.End] = d, d
	}
	return nil
}

// saveState writes the finished partitions, oldest first. b.mu is held.
func (b *backfill) saveState() error {
	if b.o.State == "" || b.o.DryRun {
		return nil
	}
	st := backfillState{Partitions: make([]donePartition, 0, len(b.byStart))}
	for _, d := range b.byStart {
		st.Partitions = append(st.Partitions, d)
	}
	slices.SortFunc(st.Partitions, func(x, y donePartition) int { return int(int64(x.Start) - int64(y.Start)) })
	raw, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(b.o.State), ".backfill-*.json")
	if err != nil {
		return err
	}
	if _, err := tmp.Write(append(raw, '\n')); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return err
	}
	return os.Rename(tmp.Name(), b.o.State)
}
