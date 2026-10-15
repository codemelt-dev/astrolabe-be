package archive_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"

	"stellar-explorer/worker/archive"
	"stellar-explorer/worker/bootstrap"
	"stellar-explorer/worker/config"
	"stellar-explorer/worker/sep54"
	"stellar-explorer/worker/sep54/sep54test"
)

// fakeDocker stands in for `docker run … detect-gaps`: it checks the
// command line and scans the archive like Galexie's scanner, writing the
// same JSON report.
type fakeDocker struct {
	t    *testing.T
	e    *env
	fail bool
	args []string
}

func (f *fakeDocker) run(ctx context.Context, name string, args ...string) ([]byte, error) {
	f.args = append([]string{name}, args...)
	if f.fail {
		return []byte("Error: operation error S3: ListObjectsV2, api error PermanentRedirect"), errors.New("exit status 1")
	}
	var outDir string
	var from, to uint64
	for i, a := range args {
		switch a {
		case "-v":
			if dst := args[i+1]; len(dst) > 5 && dst[len(dst)-5:] == ":/out" {
				outDir = dst[:len(dst)-5]
			}
		case "--start":
			from, _ = strconv.ParseUint(args[i+1], 10, 32)
		case "--end":
			to, _ = strconv.ParseUint(args[i+1], 10, 32)
		}
	}
	src, err := f.e.openArchive(ctx)
	if err != nil {
		return nil, err
	}
	type gap struct {
		Start uint32 `json:"start"`
		End   uint32 `json:"end"`
	}
	var gaps []gap
	var found, missing uint32
	for seq := uint32(from); seq <= uint32(to); seq++ {
		_, err := src.GetLedger(ctx, seq)
		switch {
		case err == nil:
			found++
		case errors.Is(err, sep54.ErrNotFound):
			missing++
			if n := len(gaps); n > 0 && gaps[n-1].End+1 == seq {
				gaps[n-1].End = seq
			} else {
				gaps = append(gaps, gap{seq, seq})
			}
		default:
			return nil, err
		}
	}
	rep := map[string]any{"scan_from": from, "scan_to": to, "duration": "1s", "report": map[string]any{
		"gaps": gaps, "total_ledgers_found": found, "total_ledgers_missing": missing}}
	b, _ := json.Marshal(rep)
	return []byte("level=info msg=done"), os.WriteFile(filepath.Join(outDir, "report.json"), b, 0o644)
}

func (e *env) gapcheck(t *testing.T, method string, docker *fakeDocker) (archive.GapReport, error) {
	t.Helper()
	dir := e.lake.Dir
	return archive.Gapcheck(context.Background(), archive.GapcheckOptions{
		Method:  method,
		Origin:  func() (bootstrap.Origin, error) { return bootstrap.ReadOrigin(dir) },
		Archive: e.openArchive,
		DetectGaps: archive.DockerDetectGaps{Docker: "/usr/bin/docker", Image: "stellar/stellar-galexie:29.0.0@sha256:abc",
			GalexieConfig: "/etc/stellar/galexie-archive.toml", EnvFile: "/etc/stellar/archive-aws.env", Run: docker.run},
		DryRun:     archive.ReindexDryRun(e.openArchive, quietLog()),
		ResultFile: filepath.Join(t.TempDir(), "result.json"),
		Alert:      e.alerts.alert,
		Log:        quietLog(),
	})
}

// The weekly gap check scans [archive_origin, archive_tip] with
// detect-gaps (DockerDetectGaps' command line), alerts on gaps, and
// falls back to the dry run when detect-gaps cannot run.
func TestGapcheck(t *testing.T) {
	e := newEnv(t)
	chain := sep54test.Chain(12_000_000, 10)
	e.feed(chain)
	origin := e.writeOrigin(chain[2]) // the archive starts at the third ledger

	// Nothing archived yet: a clean, empty range.
	docker := &fakeDocker{t: t, e: e}
	rep, err := e.gapcheck(t, config.GapcheckDetectGaps, docker)
	if err != nil || !rep.Clean() || rep.From != origin.ArchiveOrigin || rep.To != origin.ArchiveOrigin-1 {
		t.Fatalf("empty archive: %+v %v", rep, err)
	}

	r := e.start(archive.Options{})
	r.waitTip(t, seqOf(chain[9]), 30*time.Second)
	r.stop()

	rep, err = e.gapcheck(t, config.GapcheckDetectGaps, docker)
	if err != nil || !rep.Clean() || rep.From != seqOf(chain[2]) || rep.To != seqOf(chain[9]) || rep.FellBack {
		t.Fatalf("clean archive: %+v %v", rep, err)
	}
	want := []string{"/usr/bin/docker", "run", "--rm", "--network", "host", "--env-file", "/etc/stellar/archive-aws.env",
		"-v", "/etc/stellar/galexie-archive.toml:/config.toml:ro", "-v", docker.args[10], "stellar/stellar-galexie:29.0.0@sha256:abc",
		"detect-gaps", "--start", strconv.Itoa(int(seqOf(chain[2]))), "--end", strconv.Itoa(int(seqOf(chain[9]))),
		"--config-file", "/config.toml", "--output-file", "/out/report.json"}
	if !slices.Equal(docker.args, want) {
		t.Fatalf("docker args\n got %q\nwant %q", docker.args, want)
	}

	// Two holes in the archive.
	e.deleteArchive(chain[4].Key)
	e.deleteArchive(chain[5].Key)
	e.deleteArchive(chain[7].Key)
	rep, err = e.gapcheck(t, config.GapcheckDetectGaps, docker)
	wantGaps := []archive.Gap{{Start: seqOf(chain[4]), End: seqOf(chain[5])}, {Start: seqOf(chain[7]), End: seqOf(chain[7])}}
	if !errors.Is(err, archive.ErrGaps) || !slices.Equal(rep.Gaps, wantGaps) || rep.Missing != 3 || !e.alerts.has("gapcheck_gaps") {
		t.Fatalf("gaps: %+v %v, alerts %v", rep, err, e.alerts.list())
	}

	// detect-gaps cannot run: the dry run finds the same gaps.
	docker.fail = true
	rep, err = e.gapcheck(t, config.GapcheckDetectGaps, docker)
	if !errors.Is(err, archive.ErrGaps) || !rep.FellBack || !slices.Equal(rep.Gaps, wantGaps) || !e.alerts.has("gapcheck_detect_gaps_failed") {
		t.Fatalf("fallback: %+v %v", rep, err)
	}
	// The configured dry-run method; the result file holds the report.
	dir := e.lake.Dir
	resultFile := filepath.Join(t.TempDir(), "result.json")
	rep, err = archive.Gapcheck(context.Background(), archive.GapcheckOptions{
		Method: config.GapcheckDryRun, Origin: func() (bootstrap.Origin, error) { return bootstrap.ReadOrigin(dir) },
		Archive: e.openArchive, DryRun: archive.ReindexDryRun(e.openArchive, quietLog()), ResultFile: resultFile,
		Alert: e.alerts.alert, Log: quietLog(),
	})
	if !errors.Is(err, archive.ErrGaps) || !slices.Equal(rep.Gaps, wantGaps) {
		t.Fatalf("dry run: %+v %v", rep, err)
	}
	saved, err := archive.ReadGapReport(resultFile)
	if err != nil || !slices.Equal(saved.Gaps, wantGaps) || saved.Method != config.GapcheckDryRun || saved.FinishedAt.IsZero() {
		t.Fatalf("result file %+v %v", saved, err)
	}
	if fi, err := os.Stat(resultFile); err != nil || fi.Mode().Perm() != 0o644 {
		t.Fatalf("result file mode %v %v", fi, err)
	}
}

// Without archive_origin the check cannot run, and says so.
func TestGapcheckWithoutOrigin(t *testing.T) {
	e := newEnv(t)
	rep, err := e.gapcheck(t, config.GapcheckDetectGaps, &fakeDocker{t: t, e: e})
	if err == nil || errors.Is(err, archive.ErrGaps) || !errors.Is(err, bootstrap.ErrNoOrigin) || rep.Error == "" || !e.alerts.has("gapcheck_failed") {
		t.Fatalf("%+v %v", rep, err)
	}
}

// detect-gaps reports are checked before they are trusted.
func TestDetectGapsReportChecks(t *testing.T) {
	report := func(body string) archive.DockerDetectGaps {
		return archive.DockerDetectGaps{Docker: "docker", Image: "img", GalexieConfig: "/c.toml", EnvFile: "/e.env",
			Run: func(_ context.Context, _ string, args ...string) ([]byte, error) {
				for i, a := range args {
					if a == "-v" && len(args[i+1]) > 5 && args[i+1][len(args[i+1])-5:] == ":/out" {
						return nil, os.WriteFile(filepath.Join(args[i+1][:len(args[i+1])-5], "report.json"), []byte(body), 0o644)
					}
				}
				return nil, errors.New("no output dir")
			}}
	}
	ctx := context.Background()
	gaps, err := report(`{"scan_from":10,"scan_to":20,"report":{"gaps":[{"start":12,"end":13}],"total_ledgers_found":9,"total_ledgers_missing":2}}`).DetectGaps(ctx, 10, 20)
	if err != nil || !slices.Equal(gaps, []archive.Gap{{Start: 12, End: 13}}) {
		t.Fatalf("%v %v", gaps, err)
	}
	for name, body := range map[string]string{
		"other range":   `{"scan_from":10,"scan_to":19,"report":{}}`,
		"gap outside":   `{"scan_from":10,"scan_to":20,"report":{"gaps":[{"start":5,"end":6}],"total_ledgers_missing":2}}`,
		"missing count": `{"scan_from":10,"scan_to":20,"report":{"gaps":[{"start":12,"end":13}],"total_ledgers_missing":5}}`,
		"not json":      `{`,
	} {
		if _, err := report(body).DetectGaps(ctx, 10, 20); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := (archive.DockerDetectGaps{}).DetectGaps(ctx, 1, 2); err == nil {
		t.Error("ran without an image")
	}
}
