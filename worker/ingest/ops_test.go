package ingest

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"stellar-explorer/worker/bootstrap"
	"stellar-explorer/worker/config"
	"stellar-explorer/worker/lake/laketest"
	"stellar-explorer/worker/prune"
	"stellar-explorer/worker/sep54"
	"stellar-explorer/worker/sequencer"
)

// The archive stand-in is a second lake in a subprocess.
func TestMain(m *testing.M) { laketest.Main(m) }

func httpGet(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// metricValue returns the value of an exposition line starting with
// series ("name{labels}"), or "".
func metricValue(body, series string) string {
	for _, line := range strings.Split(body, "\n") {
		if v, ok := strings.CutPrefix(line, series+" "); ok {
			return v
		}
	}
	return ""
}

// The whole of `worker ingest` against a lake and an archive: the
// sequencer commits, the archiver copies every ledger 1:1 starting at
// archive_origin, the pruner deletes what is archived, outside the
// retention and behind the cursor, and the metrics and health endpoints
// report it all.
func TestIngestArchivesAndPrunes(t *testing.T) {
	const start, n, w = 65_000_000, 40, 10
	h := newHarness(t, start, n)
	arch := laketest.StartProcess(t, laketest.Options{BucketPath: "stellar-ledgers-pubnet/" + h.lake.Cfg.Prefix()})
	archiveEnv := filepath.Join(t.TempDir(), "archive.env")
	if err := os.WriteFile(archiveEnv, []byte(fmt.Sprintf("ARCHIVE_S3_ENDPOINT=%s\nARCHIVE_S3_REGION=us-east-1\nARCHIVE_S3_ACCESS_KEY=%s\nARCHIVE_S3_SECRET_KEY=%s\n",
		arch.S3Endpoint(), arch.Creds.AccessKey, arch.Creds.SecretKey)), 0o600); err != nil {
		t.Fatal(err)
	}
	h.cfg.Archive = config.Archive{Enabled: true, Bucket: "stellar-ledgers-pubnet", PathStyle: true}
	h.cfg.Credentials.ArchiveEnv = archiveEnv
	acreds, err := config.LoadArchiveEnv(archiveEnv)
	if err != nil {
		t.Fatal(err)
	}
	cfg := h.cfg
	srcs := h.sources()
	srcs.Archive = NewLazySource(sep54.SourceArchive, func(ctx context.Context) (*sep54.Source, error) {
		return sep54.OpenArchive(ctx, cfg.Archive, cfg.Lake, acreds)
	})

	ctx := context.Background()
	res, err := bootstrap.HistoryOnly(ctx, bootstrap.HistoryOnlyOptions{
		DSN: h.db.WriterDSN, At: start, Sources: []sequencer.Source{h.aws}, LakeDir: h.lake.Dir, Log: testLog(t)})
	if err != nil || !res.OriginWritten || res.Origin.ArchiveOrigin != start {
		t.Fatalf("bootstrap %+v %v", res, err)
	}
	// The state tables count as loaded (a checkpoint bootstrap would have
	// done it), so ingest maintains them too.
	h.db.Exec(fmt.Sprintf(`INSERT INTO explorer.state_checkpoint (checkpoint, hash, load_seconds, entries)
		VALUES (%d, decode(repeat('00', 32), 'hex'), 0, '{}')`, start-1))
	if err := h.feed(ctx, start, start+n-1, 0); err != nil {
		t.Fatal(err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- Run(runCtx, Options{Config: h.cfg, Log: testLog(t), Alert: h.alerts.alert, Sources: srcs,
			PruneOptions: func(o *prune.Options) {
				o.Retention, o.Interval = w, 50*time.Millisecond
				o.DiskUsage = func() (float64, error) { return 0.5, nil } // the dev machine's disk is not the lake Volume
			}})
	}()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("ingest: %v", err)
		}
	})
	h.waitCursor(start+n-1, 2*time.Minute)

	base := "http://" + h.cfg.Metrics.Listen
	tipClient := sep54.NewTipClient(h.lake.ControlAddr())
	deadline := time.Now().Add(time.Minute)
	for {
		tip, err := tipClient.Get(ctx)
		if err == nil && tip.Oldest != nil && *tip.Oldest == start+n-w {
			break
		}
		if time.Now().After(deadline) {
			_, body := httpGet(t, base+"/metrics")
			t.Fatalf("lake %+v, %v; metrics:\n%s", tip, err, grep(body, "archive", "prune"))
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Archived 1:1: every ledger, including the pruned ones.
	asrc, err := srcs.Archive.Source(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// The archiver may still be copying the last ledgers: pruning only
	// waits for the ones it deletes.
	for {
		newest, err := asrc.Newest(ctx)
		if err == nil && newest == start+n-1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("archive newest %d %v", newest, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	for _, o := range h.chain {
		seq, _ := sep54.ParseObjectPath(o.Key)
		got, err := asrc.GetLedger(ctx, seq)
		if err != nil || string(got.Body) != string(o.Body) || got.Metadata["core-version"] != o.Metadata["core-version"] {
			t.Fatalf("archived ledger %d: %v", seq, err)
		}
	}

	code, body := httpGet(t, base+"/metrics")
	if code != 200 {
		t.Fatalf("/metrics %d", code)
	}
	for series, want := range map[string]string{
		"archive_tip":       fmt.Sprint(start + n - 1),
		"archive_tip_known": "1",
		`archiver_ledgers_total{result="created"}`:            fmt.Sprint(n),
		`ingest_cursor{pipeline="explorer"}`:                  fmt.Sprint(start + n - 1),
		`ingest_lag_ledgers{pipeline="explorer"}`:             "0",
		"ingest_lake_up":                                      "1",
		"prune_deleted_total":                                 fmt.Sprint(n - w),
		"explorer_conflict_rows_total":                        "0",
		"explorer_state_drift_total":                          "0",
		`ingest_ledgers_committed_total{pipeline="explorer"}`: fmt.Sprint(n),
		`archiver_blocked{reason="halted"}`:                   "0",
	} {
		got, err := strconv.ParseFloat(metricValue(body, series), 64)
		if w, _ := strconv.ParseFloat(want, 64); err != nil || got != w {
			t.Errorf("%s = %v (%v), want %s", series, got, err, want)
		}
	}
	if !strings.Contains(body, `stellar_disk_bytes{kind="used",mount="lake"}`) || !strings.Contains(body, "ingest_commit_duration_seconds_bucket") ||
		strings.Contains(body, "explorer_partition_headroom_ledgers") {
		t.Errorf("metrics lack disk or commit duration, or still have the partitions' headroom:\n%s", grep(body, "stellar_disk", "headroom"))
	}
	if code, body := httpGet(t, base+"/readyz"); code != 200 || body != "ready\n" {
		t.Fatalf("/readyz %d %q", code, body)
	}
	if code, _ := httpGet(t, base+"/healthz"); code != 200 {
		t.Fatalf("/healthz %d", code)
	}
	var accounts int64
	if err := h.db.Conn().QueryRow(ctx, "SELECT count(*) FROM explorer.accounts").Scan(&accounts); err != nil || accounts == 0 {
		t.Fatalf("state not maintained: %d accounts, %v", accounts, err)
	}
	if got := h.alerts.list(); len(got) != 0 {
		t.Fatalf("alerts %v", got)
	}
}

func grep(body string, words ...string) string {
	var out []string
	for _, line := range strings.Split(body, "\n") {
		for _, w := range words {
			if strings.Contains(line, w) && !strings.HasPrefix(line, "#") {
				out = append(out, line)
				break
			}
		}
	}
	return strings.Join(out, "\n")
}

// /healthz answers as soon as ingest starts; /readyz only once the
// pipeline can commit and is not lagging.
func TestIngestHealthWhileWaiting(t *testing.T) {
	h := newHarness(t, 65_100_000, 5)
	stop := h.startIngest()
	defer stop()
	base := "http://" + h.cfg.Metrics.Listen
	deadline := time.Now().Add(30 * time.Second)
	for {
		resp, err := http.Get(base + "/healthz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("/healthz never answered: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	code, body := httpGet(t, base+"/readyz")
	if code != http.StatusServiceUnavailable || !strings.Contains(body, "cursor unknown") {
		t.Fatalf("/readyz before bootstrap: %d %q", code, body)
	}
	for {
		_, body := httpGet(t, base+"/metrics")
		if metricValue(body, `stellar_alerts_total{kind="no_cursor"}`) == "1" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no_cursor alert not counted:\n%s", grep(body, "stellar_alerts_total"))
		}
		time.Sleep(50 * time.Millisecond)
	}
}
