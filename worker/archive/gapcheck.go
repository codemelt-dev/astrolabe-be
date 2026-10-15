package archive

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"time"

	"stellar-explorer/worker/bootstrap"
	"stellar-explorer/worker/config"
	"stellar-explorer/worker/reindex"
	"stellar-explorer/worker/sep54"
	"stellar-explorer/worker/statefile"
)

// `worker gapcheck`, weekly: computes [archive_origin,
// archive_tip] and runs Galexie's detect-gaps container against the archive
// over it, then alerts on any gap. detect-gaps needs both bounds, so they
// are computed per run. If detect-gaps cannot run (a provider that rejects
// its path-style addressing, or a docker failure), the check falls back to
// `worker reindex --dry-run --source archive` over the same range, which
// also validates every object.
//
// The result is written to a JSON file that ingest exports as metrics
// (gapcheck alerts fire from there).

// Gap is a missing range of ledgers, inclusive.
type Gap struct {
	Start uint32 `json:"start"`
	End   uint32 `json:"end"`
}

// GapReport is one gap check, as written to the result file.
type GapReport struct {
	// Method is detect-gaps or reindex-dry-run; FellBack is set when
	// detect-gaps failed and the dry run replaced it.
	Method   string `json:"method"`
	FellBack bool   `json:"fell_back,omitempty"`
	// From and To are the range checked: [archive_origin, archive_tip].
	// To < From means nothing is archived yet.
	From uint32 `json:"from"`
	To   uint32 `json:"to"`
	Gaps []Gap  `json:"gaps"`
	// Missing is the number of ledgers in the gaps.
	Missing uint64 `json:"missing"`
	// Invalid lists objects that failed validation (dry run only).
	Invalid    []string  `json:"invalid,omitempty"`
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
	// Error is set when the check could not run.
	Error string `json:"error,omitempty"`
}

// Clean reports whether the archive has no gap and no invalid object.
func (r GapReport) Clean() bool { return r.Error == "" && len(r.Gaps) == 0 && len(r.Invalid) == 0 }

// ErrGaps is returned when the check found gaps or invalid objects.
var ErrGaps = errors.New("the archive has gaps or invalid objects")

// DetectGaps runs Galexie's detect-gaps over [from, to].
type DetectGaps interface {
	DetectGaps(ctx context.Context, from, to uint32) ([]Gap, error)
}

// DryRun validates [from, to] in the archive and reports the missing
// ledgers and the invalid objects (`worker reindex --dry-run --source
// archive`).
type DryRun func(ctx context.Context, from, to uint32) (missing []uint32, invalid []string, err error)

// GapcheckOptions configure Gapcheck.
type GapcheckOptions struct {
	// Method is config.GapcheckDetectGaps or config.GapcheckDryRun.
	Method     string
	Origin     func() (bootstrap.Origin, error)
	Archive    func(ctx context.Context) (*sep54.Source, error)
	DetectGaps DetectGaps
	DryRun     DryRun
	// ResultFile receives the report (empty: not written).
	ResultFile string
	Log        *slog.Logger
	Alert      func(kind, msg string, args ...any)
	Now        func() time.Time
}

// Gapcheck runs one gap check. It returns ErrGaps when the archive has
// gaps or invalid objects, and another error when the check could not run.
func Gapcheck(ctx context.Context, o GapcheckOptions) (GapReport, error) {
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
	r := GapReport{Method: o.Method, StartedAt: o.Now().UTC()}
	err := gapcheck(ctx, o, &r)
	r.FinishedAt = o.Now().UTC()
	if err != nil {
		r.Error = err.Error()
	}
	if o.ResultFile != "" {
		if werr := statefile.WriteJSON(o.ResultFile, r); werr != nil {
			err = errors.Join(err, fmt.Errorf("write %s: %w", o.ResultFile, werr))
		}
	}
	switch {
	case err != nil:
		o.Alert("gapcheck_failed", "the archive gap check could not run", "err", err)
		return r, err
	case !r.Clean():
		o.Alert("gapcheck_gaps", "the archive has gaps: repair them (scan-and-fill into the lake; the archiver cannot fill them)",
			"from", r.From, "to", r.To, "gaps", r.Gaps, "missing", r.Missing, "invalid", len(r.Invalid))
		return r, ErrGaps
	}
	o.Log.Info("the archive has no gaps", "from", r.From, "to", r.To, "method", r.Method)
	return r, nil
}

func gapcheck(ctx context.Context, o GapcheckOptions, r *GapReport) error {
	origin, err := o.Origin()
	if err != nil {
		return fmt.Errorf("archive_origin: %w", err)
	}
	arch, err := o.Archive(ctx)
	if err != nil {
		return err
	}
	r.From = origin.ArchiveOrigin
	tip, err := arch.Newest(ctx)
	switch {
	case errors.Is(err, sep54.ErrNoLedgers):
		r.To = r.From - 1
		o.Log.Info("nothing is archived yet", "archive_origin", r.From)
		return nil
	case err != nil:
		return fmt.Errorf("archive tip: %w", err)
	case tip < r.From:
		return fmt.Errorf("the archive tip %d is below archive_origin %d", tip, r.From)
	}
	r.To = tip
	o.Log.Info("checking the archive for gaps", "from", r.From, "to", r.To, "method", o.Method)

	if o.Method == config.GapcheckDetectGaps {
		gaps, err := o.DetectGaps.DetectGaps(ctx, r.From, r.To)
		if err == nil {
			r.setGaps(gaps)
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		o.Alert("gapcheck_detect_gaps_failed", "detect-gaps failed; falling back to reindex --dry-run --source archive", "err", err)
		r.FellBack = true
	}
	missing, invalid, err := o.DryRun(ctx, r.From, r.To)
	if err != nil && len(missing) == 0 && len(invalid) == 0 {
		return err
	}
	r.setGaps(rangesOf(missing))
	r.Invalid = invalid
	return nil
}

func (r *GapReport) setGaps(gaps []Gap) {
	r.Gaps = gaps
	r.Missing = 0
	for _, g := range gaps {
		r.Missing += uint64(g.End-g.Start) + 1
	}
}

// rangesOf turns sorted ledger numbers into ranges.
func rangesOf(seqs []uint32) []Gap {
	seqs = slices.Clone(seqs)
	slices.Sort(seqs)
	var out []Gap
	for _, s := range seqs {
		if n := len(out); n > 0 && out[n-1].End+1 == s {
			out[n-1].End = s
			continue
		}
		out = append(out, Gap{Start: s, End: s})
	}
	return out
}

// DockerDetectGaps runs Galexie's detect-gaps in its pinned image: the
// uploader key under AWS_* names from archive-aws.env and
// galexie-archive.toml (galexie.toml pointed at the archive,
// admin_port = 0). The JSON report goes to a temporary directory.
type DockerDetectGaps struct {
	Docker        string // /usr/bin/docker
	Image         string // stellar/stellar-galexie:<tag>@sha256:<digest>
	GalexieConfig string // /etc/stellar/galexie-archive.toml
	EnvFile       string // /etc/stellar/archive-aws.env
	// Run executes a command and returns its combined output (tests);
	// nil runs it for real.
	Run func(ctx context.Context, name string, args ...string) ([]byte, error)
}

// Args returns the docker command line for [from, to], with the report
// written to outDir/report.json.
func (d DockerDetectGaps) Args(from, to uint32, outDir string) []string {
	return []string{
		"run", "--rm", "--network", "host",
		"--env-file", d.EnvFile,
		"-v", d.GalexieConfig + ":/config.toml:ro",
		"-v", outDir + ":/out",
		d.Image,
		"detect-gaps", "--start", strconv.FormatUint(uint64(from), 10), "--end", strconv.FormatUint(uint64(to), 10),
		"--config-file", "/config.toml", "--output-file", "/out/report.json",
	}
}

// detectGapsReport is Galexie's DetectGapsOutput.
type detectGapsReport struct {
	ScanFrom uint32 `json:"scan_from"`
	ScanTo   uint32 `json:"scan_to"`
	Report   struct {
		Gaps                []Gap  `json:"gaps"`
		TotalLedgersFound   uint32 `json:"total_ledgers_found"`
		TotalLedgersMissing uint32 `json:"total_ledgers_missing"`
	} `json:"report"`
}

// DetectGaps implements the DetectGaps interface.
func (d DockerDetectGaps) DetectGaps(ctx context.Context, from, to uint32) ([]Gap, error) {
	if d.Image == "" {
		return nil, errors.New("galexie.image is not set in the config")
	}
	outDir, err := os.MkdirTemp("", "stellar-gapcheck-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(outDir)
	// The container's user must be able to write the report.
	if err := os.Chmod(outDir, 0o777); err != nil {
		return nil, err
	}
	run := d.Run
	if run == nil {
		run = func(ctx context.Context, name string, args ...string) ([]byte, error) {
			return exec.CommandContext(ctx, name, args...).CombinedOutput()
		}
	}
	out, err := run(ctx, d.Docker, d.Args(from, to, outDir)...)
	if err != nil {
		return nil, fmt.Errorf("detect-gaps: %w: %s", err, tail(out, 2000))
	}
	b, err := os.ReadFile(filepath.Join(outDir, "report.json"))
	if err != nil {
		return nil, fmt.Errorf("detect-gaps wrote no report: %w: %s", err, tail(out, 2000))
	}
	var rep detectGapsReport
	if err := json.Unmarshal(b, &rep); err != nil {
		return nil, fmt.Errorf("detect-gaps report: %w", err)
	}
	if rep.ScanFrom != from || rep.ScanTo != to {
		return nil, fmt.Errorf("detect-gaps scanned %d-%d, not %d-%d", rep.ScanFrom, rep.ScanTo, from, to)
	}
	var missing uint64
	for _, g := range rep.Report.Gaps {
		if g.Start > g.End || g.Start < from || g.End > to {
			return nil, fmt.Errorf("detect-gaps reported gap %d-%d outside %d-%d", g.Start, g.End, from, to)
		}
		missing += uint64(g.End-g.Start) + 1
	}
	if missing != uint64(rep.Report.TotalLedgersMissing) {
		return nil, fmt.Errorf("detect-gaps report is inconsistent: %d missing, but its %d gaps hold %d",
			rep.Report.TotalLedgersMissing, len(rep.Report.Gaps), missing)
	}
	return rep.Report.Gaps, nil
}

func tail(b []byte, n int) string {
	b = bytes.TrimSpace(b)
	if len(b) > n {
		b = b[len(b)-n:]
	}
	return string(b)
}

// ReadGapReport reads the result file written by Gapcheck.
func ReadGapReport(path string) (GapReport, error) {
	var r GapReport
	err := statefile.ReadJSON(path, &r)
	return r, err
}

// ReindexDryRun is the fallback DryRun: `worker reindex --dry-run --source
// archive` over the range (every object read, decoded and chain-checked).
func ReindexDryRun(open func(ctx context.Context) (*sep54.Source, error), log *slog.Logger) DryRun {
	return func(ctx context.Context, from, to uint32) ([]uint32, []string, error) {
		src, err := open(ctx)
		if err != nil {
			return nil, nil, err
		}
		res, err := reindex.Run(ctx, reindex.Options{From: from, To: to, Source: src, DryRun: true, Log: log})
		return res.Missing, res.Invalid, err
	}
}
