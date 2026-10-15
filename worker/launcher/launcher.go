// Package launcher implements `worker galexie-start`, the
// ExecStartPre of stellar-galexie.service. Galexie requires --start and
// resumes at max(--start, newest_local + 1); the launcher computes --start
// on every launch:
//
//	refuse (non-zero exit → systemd retries in 5 s) unless the lake answers on :9000 and :9001
//	lake_tip    := GET /v1/tip (null when the lake is empty)
//	archive_tip := probe with a dedicated client: 1 attempt, bounded (ProbeTimeout)
//	                 empty bucket → archive_origin − 1;  error or timeout → unknown
//	if lake empty and archive empty and archive_origin unknown → refuse: run `worker bootstrap` first
//	if lake has ledger objects:   START = max(lake_tip, archive_tip if known) + 1   # never waits on the archive
//	elif archive_tip known:       START = archive_tip + 1                          # lake lost: resume from the archive
//	else:                         refuse, alert                                    # archive tip unknown and no local data
//
// The Postgres cursor is not an input. The archive probe starts with the
// lake checks and is bounded, so an archive outage never blocks a Galexie
// start when the lake has data. With archiving disabled (dev), the archive
// counts as empty.
package launcher

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"stellar-explorer/worker/bootstrap"
	"stellar-explorer/worker/sep54"
	"stellar-explorer/worker/statefile"
)

// ProbeTimeout bounds the archive probe. It must stay under 10 s: a start
// from the lake tip must take at most 10 s during an archive outage;
// the probe starts with the launcher, so 9.5 s leaves room to decide and
// write START inside that bound.
const ProbeTimeout = 9500 * time.Millisecond

// Archive states.
const (
	ArchiveKnown    = "known"    // the archive's newest ledger
	ArchiveEmpty    = "empty"    // no ledger objects: archive_origin − 1
	ArchiveUnknown  = "unknown"  // error or timeout
	ArchiveDisabled = "disabled" // archive.enabled = false: counts as empty
)

// Inputs are what the start depends on.
type Inputs struct {
	// LakeTip is the lake's newest ledger; nil when it holds none.
	LakeTip *uint32
	// Archive is one of the archive states; ArchiveTip is set when known.
	Archive    string
	ArchiveTip uint32
	// Origin is state/bootstrap.json, if readable.
	Origin *bootstrap.Origin
}

// ErrBootstrapFirst means there is nothing to resume from: run `worker
// bootstrap`.
var ErrBootstrapFirst = errors.New("the lake and the archive are empty and archive_origin is unknown: run `worker bootstrap` first")

// ErrNoStart means the archive tip is unknown and the lake holds nothing.
var ErrNoStart = errors.New("the lake holds no ledger and the archive tip is unknown: cannot compute --start")

// Decide is the launcher's truth table (see the package comment).
func Decide(in Inputs) (uint32, string, error) {
	tip, known := in.ArchiveTip, in.Archive == ArchiveKnown
	if (in.Archive == ArchiveEmpty || in.Archive == ArchiveDisabled) && in.Origin != nil {
		tip, known = in.Origin.ArchiveOrigin-1, true
	}
	switch {
	case in.LakeTip != nil:
		if known && tip > *in.LakeTip {
			return tip + 1, "archive tip (ahead of the lake)", nil
		}
		return *in.LakeTip + 1, "lake tip", nil
	case known:
		if in.Archive == ArchiveKnown {
			return tip + 1, "archive tip (the lake is empty)", nil
		}
		return tip + 1, "archive_origin (the lake and the archive are empty)", nil
	case in.Archive != ArchiveUnknown:
		return 0, "", ErrBootstrapFirst
	default:
		return 0, "", ErrNoStart
	}
}

// Options configure Compute.
type Options struct {
	// LakeS3 checks that the lake's S3 endpoint (:9000) answers.
	LakeS3 func(ctx context.Context) error
	// LakeTip reads GET /v1/tip on :9001.
	LakeTip func(ctx context.Context) (sep54.Tip, error)
	// ArchiveTip probes the archive's newest ledger (sep54.ErrNoLedgers for
	// an empty bucket); nil when archiving is disabled.
	ArchiveTip func(ctx context.Context) (uint32, error)
	// Origin reads state/bootstrap.json.
	Origin func() (bootstrap.Origin, error)
	// ProbeTimeout bounds the archive probe from the start of Compute
	// (default ProbeTimeout).
	ProbeTimeout time.Duration
	// LakeTimeout bounds each lake check (default 5 s).
	LakeTimeout time.Duration
	Log         *slog.Logger
	Alert       func(kind, msg string, args ...any)
}

// Result is a computed start.
type Result struct {
	Start  uint32
	Reason string
	Inputs Inputs
	// Elapsed is how long the computation took.
	Elapsed time.Duration
}

// Compute checks the lake, probes the archive and decides --start.
func Compute(ctx context.Context, o Options) (Result, error) {
	if o.ProbeTimeout <= 0 {
		o.ProbeTimeout = ProbeTimeout
	}
	if o.LakeTimeout <= 0 {
		o.LakeTimeout = 5 * time.Second
	}
	if o.Log == nil {
		o.Log = slog.Default()
	}
	if o.Alert == nil {
		log := o.Log
		o.Alert = func(kind, msg string, args ...any) { log.Error(msg, append([]any{"alert", kind}, args...)...) }
	}
	start := time.Now()

	// The archive probe runs alongside the lake checks: 1 attempt, bounded.
	type probe struct {
		tip uint32
		err error
	}
	probed := make(chan probe, 1)
	if o.ArchiveTip != nil {
		pctx, cancel := context.WithTimeout(ctx, o.ProbeTimeout)
		defer cancel()
		go func() {
			tip, err := o.ArchiveTip(pctx)
			probed <- probe{tip, err}
		}()
	}

	lctx, cancel := context.WithTimeout(ctx, o.LakeTimeout)
	err := o.LakeS3(lctx)
	cancel()
	if err != nil {
		return Result{}, fmt.Errorf("the lake's S3 endpoint does not answer (systemd retries): %w", err)
	}
	lctx, cancel = context.WithTimeout(ctx, o.LakeTimeout)
	lt, err := o.LakeTip(lctx)
	cancel()
	if err != nil {
		return Result{}, fmt.Errorf("the lake's control endpoint does not answer (systemd retries): %w", err)
	}

	in := Inputs{LakeTip: lt.Tip, Archive: ArchiveDisabled}
	if origin, err := o.Origin(); err == nil {
		in.Origin = &origin
	} else if !errors.Is(err, bootstrap.ErrNoOrigin) {
		o.Log.Warn("state/bootstrap.json is unreadable; archive_origin unknown", "err", err)
	}
	if o.ArchiveTip != nil {
		p := <-probed
		switch {
		case p.err == nil:
			in.Archive, in.ArchiveTip = ArchiveKnown, p.tip
		case errors.Is(p.err, sep54.ErrNoLedgers):
			in.Archive = ArchiveEmpty
		default:
			in.Archive = ArchiveUnknown
			o.Log.Warn("archive tip unknown", "err", p.err)
		}
	}
	s, reason, err := Decide(in)
	res := Result{Start: s, Reason: reason, Inputs: in, Elapsed: time.Since(start)}
	if err != nil {
		kind := "galexie_start_refused"
		if errors.Is(err, ErrBootstrapFirst) {
			kind = "galexie_needs_bootstrap"
		}
		o.Alert(kind, "cannot compute Galexie's --start", "err", err, "archive", in.Archive)
		return res, err
	}
	return res, nil
}

// WriteEnv writes START=<start> to path atomically (temporary file +
// rename), for the unit's EnvironmentFile.
func WriteEnv(path string, start uint32) error {
	return statefile.Write(path, []byte("START="+strconv.FormatUint(uint64(start), 10)+"\n"), 0o644)
}
