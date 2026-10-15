// Package prune deletes old ledgers from the lake, oldest
// first and through the lake's DeleteObject (which keeps its oldest index),
// only if all of these hold for ledger L:
//
//  1. L ≤ archive_tip, and archive_tip is known. archive_tip =
//     archive_origin − 1 means nothing is archived, so nothing is deleted.
//  2. L ≤ lake.tip − W, W ≥ 1 the retention in ledgers: the newest object
//     is never deleted.
//  3. L ≤ min(cursor of each local reader pipeline), or the lake
//     filesystem is above the high watermark. An unknown cursor counts as
//     "not passed"; reindex:* rows are never readers. The gate is soft on
//     purpose: a lagging pipeline can read from the archive, while a full
//     disk (507, which Galexie does not retry) stops Galexie.
//  4. The key is a canonical SEP-54 ledger key under the lake prefix: the
//     pruner only ever builds keys from sequence numbers, so .config.json,
//     .sgwtmp/ and .vgwlocks/ are never touched.
//
// Every input it cannot read makes it delete nothing (fail closed).
// Galexie resumes at newest + 1, so deleting old objects never affects it.
package prune

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"stellar-explorer/worker/sep54"
)

// Cursor is a local reader's cursor, if known.
type Cursor struct {
	Pipeline string
	Seq      uint32
	Known    bool
}

// Inputs are what one pruning decision depends on.
type Inputs struct {
	// LakeTip and LakeOldest come from the lake's /v1/tip (nil: empty).
	LakeTip, LakeOldest *uint32
	// ArchiveTip is the archiver's tip when ArchiveKnown.
	ArchiveTip   uint32
	ArchiveKnown bool
	// Retention is W.
	Retention uint32
	// Cursors of the local reader pipelines.
	Cursors []Cursor
	// DiskUsed is the used fraction of the lake filesystem when DiskKnown.
	DiskUsed      float64
	DiskKnown     bool
	HighWatermark float64
}

// Decision is the range to delete, [From, To], when OK.
type Decision struct {
	From, To uint32
	OK       bool
	// Reason explains a refusal or the binding limit.
	Reason string
	// Watermark is set when the disk watermark lifted the cursor gate.
	Watermark bool
}

// Decide applies rules 1–3 to the inputs (rule 4 holds by construction).
func Decide(in Inputs) Decision {
	if in.LakeTip == nil || in.LakeOldest == nil {
		return Decision{Reason: "the lake is empty"}
	}
	if !in.ArchiveKnown {
		return Decision{Reason: "the archive tip is unknown"}
	}
	w := max(in.Retention, 1)
	tip, oldest := *in.LakeTip, *in.LakeOldest
	if tip < w {
		return Decision{Reason: "the lake holds less than the retention"}
	}
	bound, reason := in.ArchiveTip, "archive tip"
	if t := tip - w; t < bound {
		bound, reason = t, "retention"
	}
	d := Decision{}
	if in.DiskKnown && in.DiskUsed > in.HighWatermark {
		d.Watermark = true
	} else {
		for _, c := range in.Cursors {
			if !c.Known {
				return Decision{Reason: fmt.Sprintf("the cursor of %s is unknown", c.Pipeline)}
			}
			if c.Seq < bound {
				bound, reason = c.Seq, "cursor of "+c.Pipeline
			}
		}
	}
	if bound < oldest {
		return Decision{Reason: reason + " is below the lake's oldest ledger"}
	}
	d.From, d.To, d.OK, d.Reason = oldest, bound, true, reason
	return d
}

// Tip is the lake's control endpoint (*sep54.TipClient).
type Tip interface {
	Get(ctx context.Context) (sep54.Tip, error)
}

// Options configure a Pruner.
type Options struct {
	// Lake opens the lake (the gateway root keys can delete).
	Lake    func(ctx context.Context) (*sep54.Source, error)
	LakeTip Tip
	// ArchiveTip is the archiver's in-memory tip.
	ArchiveTip func() (uint32, bool)
	// Readers are the local reader pipelines; Cursor returns each one's
	// cursor if known.
	Readers []string
	Cursor  func(pipeline string) (uint32, bool)
	// Retention is W (≥ 1).
	Retention uint32
	// DiskUsage returns the used fraction of the lake filesystem.
	DiskUsage     func() (float64, error)
	HighWatermark float64
	// Interval between passes; MaxPerPass bounds the deletes of one pass
	// (default 10,000) so the inputs are re-read regularly.
	Interval   time.Duration
	MaxPerPass int
	// ArchiveStall: alert when the archive tip has not advanced for this
	// long while the lake tip has (default 15 min).
	ArchiveStall time.Duration
	Log          *slog.Logger
	Alert        func(kind, msg string, args ...any)
	Now          func() time.Time
	// Delete removes ledger seq from the lake (tests); nil deletes through
	// the lake's S3 DeleteObject.
	Delete func(ctx context.Context, seq uint32) error
}

// Stats are the pruner's counters.
type Stats struct {
	Deleted uint64
	// LastPass is when the last pass ended; LastReason why it stopped.
	LastPass   time.Time
	LastReason string
	// Bound is the highest ledger the last pass could delete (0: none).
	Bound uint32
	// Watermark is set while the disk watermark lifts the cursor gate.
	Watermark bool
	Errors    uint64
}

// Pruner runs the pruning passes of `worker ingest`.
type Pruner struct {
	o   Options
	log *slog.Logger

	mu    sync.Mutex
	stats Stats
	// archive stall tracking
	archiveTip     uint32
	archiveKnown   bool
	archiveSince   time.Time
	lakeTipAtSince uint32
	stallAlerted   bool
	watermarkNoted bool
}

// New returns a pruner.
func New(o Options) (*Pruner, error) {
	if o.LakeTip == nil || o.ArchiveTip == nil || (o.Lake == nil && o.Delete == nil) {
		return nil, errors.New("prune: the lake, its tip and the archive tip are required")
	}
	if len(o.Readers) > 0 && o.Cursor == nil {
		return nil, errors.New("prune: readers need Cursor")
	}
	if o.Retention == 0 {
		o.Retention = 1
	}
	if o.Interval <= 0 {
		o.Interval = time.Minute
	}
	if o.MaxPerPass <= 0 {
		o.MaxPerPass = 10_000
	}
	if o.ArchiveStall <= 0 {
		o.ArchiveStall = 15 * time.Minute
	}
	if o.Log == nil {
		o.Log = slog.Default()
	}
	if o.Alert == nil {
		log := o.Log
		o.Alert = func(kind, msg string, args ...any) { log.Error(msg, append([]any{"alert", kind}, args...)...) }
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	return &Pruner{o: o, log: o.Log.With("component", "pruner")}, nil
}

// Stats returns a snapshot of the counters.
func (p *Pruner) Stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stats
}

// Run prunes every Interval until ctx ends.
func (p *Pruner) Run(ctx context.Context) {
	for {
		if _, err := p.Pass(ctx); err != nil && ctx.Err() == nil {
			p.log.Warn("pruning pass", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(p.o.Interval):
		}
	}
}

// inputs gathers the decision's inputs; an error means "delete nothing".
func (p *Pruner) inputs(ctx context.Context) (Inputs, error) {
	tip, err := p.o.LakeTip.Get(ctx)
	if err != nil {
		return Inputs{}, fmt.Errorf("lake tip: %w", err)
	}
	in := Inputs{LakeTip: tip.Tip, LakeOldest: tip.Oldest, Retention: p.o.Retention, HighWatermark: p.o.HighWatermark}
	in.ArchiveTip, in.ArchiveKnown = p.o.ArchiveTip()
	for _, r := range p.o.Readers {
		seq, ok := p.o.Cursor(r)
		in.Cursors = append(in.Cursors, Cursor{Pipeline: r, Seq: seq, Known: ok})
	}
	if p.o.DiskUsage != nil {
		if used, err := p.o.DiskUsage(); err == nil {
			in.DiskUsed, in.DiskKnown = used, true
		} else {
			p.log.Warn("lake disk usage unknown; the cursor gate applies", "err", err)
		}
	}
	return in, nil
}

// Pass runs one pruning pass and returns the number of ledgers deleted.
func (p *Pruner) Pass(ctx context.Context) (int, error) {
	in, err := p.inputs(ctx)
	if err != nil {
		p.finish(Decision{Reason: err.Error()}, 0, err)
		return 0, err
	}
	p.watchArchive(in)
	d := Decide(in)
	if d.Watermark && d.OK {
		p.mu.Lock()
		noted := p.watermarkNoted
		p.watermarkNoted = true
		p.mu.Unlock()
		if !noted {
			p.o.Alert("lake_disk_watermark", "the lake is above its disk watermark: pruning archived ledgers without waiting for the readers",
				"used", fmt.Sprintf("%.3f", in.DiskUsed), "watermark", in.HighWatermark)
		}
	} else if !d.Watermark {
		p.mu.Lock()
		p.watermarkNoted = false
		p.mu.Unlock()
	}
	if !d.OK {
		p.finish(d, 0, nil)
		return 0, nil
	}
	n := 0
	for seq := d.From; seq <= d.To && n < p.o.MaxPerPass; seq++ {
		if err := ctx.Err(); err != nil {
			p.finish(d, n, err)
			return n, err
		}
		if err := p.delete(ctx, seq); err != nil {
			err = fmt.Errorf("delete ledger %d: %w", seq, err)
			p.finish(d, n, err)
			return n, err
		}
		n++
		if seq == ^uint32(0) {
			break
		}
	}
	if n > 0 {
		p.log.Info("pruned the lake", "from", d.From, "to", d.From+uint32(n)-1, "deleted", n, "limit", d.Reason, "watermark", d.Watermark)
	}
	p.finish(d, n, nil)
	return n, nil
}

func (p *Pruner) finish(d Decision, n int, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stats.Deleted += uint64(n)
	p.stats.LastPass = p.o.Now()
	p.stats.LastReason = d.Reason
	p.stats.Watermark = d.Watermark
	p.stats.Bound = 0
	if d.OK {
		p.stats.Bound = d.To
	}
	if err != nil {
		p.stats.Errors++
	}
}

func (p *Pruner) delete(ctx context.Context, seq uint32) error {
	if p.o.Delete != nil {
		return p.o.Delete(ctx, seq)
	}
	lake, err := p.o.Lake(ctx)
	if err != nil {
		return err
	}
	key := lake.Key(sep54.ObjectPath(seq))
	if s, ok := sep54.ParseKey(lake.Prefix(), key); !ok || s != seq {
		return fmt.Errorf("refusing to delete %q: not the ledger key of %d", key, seq) // rule 4, defensively
	}
	_, err = lake.Client().DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(lake.Bucket()), Key: aws.String(key)})
	return err
}

// watchArchive alerts when the archive tip has not advanced for
// ArchiveStall while the lake tip has (rule 1).
func (p *Pruner) watchArchive(in Inputs) {
	now := p.o.Now()
	var lakeTip uint32
	if in.LakeTip != nil {
		lakeTip = *in.LakeTip
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.archiveSince.IsZero() || in.ArchiveKnown != p.archiveKnown || in.ArchiveTip != p.archiveTip {
		p.archiveTip, p.archiveKnown = in.ArchiveTip, in.ArchiveKnown
		p.archiveSince, p.lakeTipAtSince = now, lakeTip
		p.stallAlerted = false
		return
	}
	if !p.stallAlerted && now.Sub(p.archiveSince) >= p.o.ArchiveStall && lakeTip > p.lakeTipAtSince {
		p.stallAlerted = true
		p.o.Alert("archive_stalled", "the archive tip has not advanced while the lake tip has: nothing more can be pruned",
			"archive_tip", in.ArchiveTip, "archive_tip_known", in.ArchiveKnown, "since", p.archiveSince, "lake_tip", lakeTip)
	}
}
