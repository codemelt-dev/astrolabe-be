package metrics

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"stellar-explorer/worker/archive"
	"stellar-explorer/worker/sep54"
	"stellar-explorer/worker/sequencer"
	"stellar-explorer/worker/statefile"
	"stellar-explorer/worker/watchdog"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

type fakeTip struct {
	tip *uint32
	err error
}

func (f fakeTip) Get(context.Context) (sep54.Tip, error) { return sep54.Tip{Tip: f.tip}, f.err }

func u32(v uint32) *uint32 { return &v }

func TestHealth(t *testing.T) {
	ok := func(context.Context) error { return nil }
	cursor := func(c uint32, known bool) func() (uint32, bool) { return func() (uint32, bool) { return c, known } }
	for _, tc := range []struct {
		name string
		h    Health
		want []string // substrings of the failures, in order
	}{
		{"ready", Health{LakeTip: fakeTip{tip: u32(1000)}, Database: ok, Cursor: cursor(990, true), MaxLag: 120}, nil},
		{"empty lake", Health{LakeTip: fakeTip{}, Database: ok, Cursor: cursor(990, true), MaxLag: 120}, nil},
		{"lagging", Health{LakeTip: fakeTip{tip: u32(1000)}, Database: ok, Cursor: cursor(800, true), MaxLag: 120},
			[]string{"200 ledgers behind"}},
		{"lake down", Health{LakeTip: fakeTip{err: errors.New("refused")}, Database: ok, Cursor: cursor(800, true), MaxLag: 120},
			[]string{"lake: refused"}},
		{"postgres down, cursor unknown", Health{LakeTip: fakeTip{tip: u32(1)}, Database: func(context.Context) error { return errors.New("down") },
			Cursor: cursor(0, false), MaxLag: 120}, []string{"postgres: down", "cursor unknown"}},
		{"clickhouse pipeline", Health{LakeTip: fakeTip{err: errors.New("refused")}, Database: func(context.Context) error { return errors.New("down") },
			Cursor: cursor(0, false), MaxLag: 120, TipName: "archive", DatabaseName: "clickhouse", Pipeline: "clickhouse"},
			[]string{"archive: refused", "clickhouse: down", "clickhouse: cursor unknown (ingest is starting, or clickhouse is unreachable)"}},
	} {
		got := tc.h.Check(context.Background())
		if len(got) != len(tc.want) {
			t.Errorf("%s: %q", tc.name, got)
			continue
		}
		for i := range got {
			if !strings.Contains(got[i], tc.want[i]) {
				t.Errorf("%s: %q", tc.name, got)
			}
		}
	}
}

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// The endpoints, the commit metrics and the oneshot commands' files.
func TestEndpointsAndMetrics(t *testing.T) {
	m := New(quiet)
	dir := t.TempDir()
	wdState := filepath.Join(dir, "state.json")
	gapResult := filepath.Join(dir, "result.json")
	if err := statefile.WriteJSON(wdState, watchdog.State{LastRun: time.Unix(1_800_000_000, 0), RestartsTotal: 3, RestartsInARow: 1,
		GalexieActive: true, GalexieNRestarts: 7, UploaderLatest: u32(500), Alerts: []string{"lake_io_stall"}}); err != nil {
		t.Fatal(err)
	}
	if err := statefile.WriteJSON(gapResult, archive.GapReport{From: 10, To: 99, Missing: 4, Gaps: []archive.Gap{{Start: 20, End: 23}},
		FinishedAt: time.Unix(1_800_000_100, 0)}); err != nil {
		t.Fatal(err)
	}
	m.SetSources(Sources{LakeTip: fakeTip{tip: u32(1000)}, LakeDir: dir, WatchdogState: wdState, GapcheckResult: gapResult})
	m.SetCursor("explorer", func() (uint32, bool) { return 995, true })
	m.SetNetworkTip(1003, time.Unix(1_800_000_200, 0))
	m.Commit("explorer", sequencer.CommitInfo{First: 990, Last: 995, Ledgers: 6, Duration: 20 * time.Millisecond,
		Read: time.Second, Process: 2 * time.Second, Sources: map[string]int{"lake": 5, "archive": 1}, Latency: 30 * time.Millisecond})
	m.Alert("conflict_rows")

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	h := Handler(m, Health{LakeTip: fakeTip{tip: u32(1000)}, Cursor: func() (uint32, bool) { return 995, true }, MaxLag: 120})
	ctx, cancel := context.WithCancel(context.Background())
	srv, err := Start(ctx, addr, "", h, quiet)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { cancel(); _ = srv.Wait() }()
	ln := "http://" + addr

	if code, body := get(t, ln+"/healthz"); code != 200 || body != "ok\n" {
		t.Fatalf("/healthz %d %q", code, body)
	}
	if code, body := get(t, ln+"/readyz"); code != 200 || body != "ready\n" {
		t.Fatalf("/readyz %d %q", code, body)
	}
	_, body := get(t, ln+"/metrics")
	for _, want := range []string{
		`ingest_cursor{pipeline="explorer"} 995`,
		`ingest_lag_ledgers{pipeline="explorer"} 5`,
		`ingest_lake_tip 1000`,
		`stellar_network_tip 1003`,
		`ingest_ledgers_read_total{pipeline="explorer",source="archive"} 1`,
		`ingest_ledgers_read_total{pipeline="explorer",source="lake"} 5`,
		`ingest_ledger_latency_seconds_count{pipeline="explorer"} 1`,
		`ingest_read_seconds_total{pipeline="explorer"} 1`,
		`stellar_alerts_total{kind="conflict_rows"} 1`,
		`watchdog_restarts_total 3`,
		`watchdog_alert{kind="lake_io_stall"} 1`,
		`galexie_systemd_restarts 7`,
		`gapcheck_missing_ledgers 4`,
		`gapcheck_checked_to 99`,
		`stellar_disk_bytes{kind="size",mount="lake"}`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics lack %q", want)
		}
	}
	// A second bind on a taken port fails at once.
	if _, err := Start(ctx, strings.TrimPrefix(ln, "http://"), "", h, quiet); err == nil {
		t.Fatal("bound a taken port")
	}
}
