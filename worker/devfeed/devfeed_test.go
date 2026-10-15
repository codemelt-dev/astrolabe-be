package devfeed_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"maps"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stellar/go-stellar-sdk/network"

	"stellar-explorer/worker/config"
	"stellar-explorer/worker/devfeed"
	"stellar-explorer/worker/lake/laketest"
	"stellar-explorer/worker/sep54"
	"stellar-explorer/worker/sep54/sep54test"
)

// Tests share the process with one in-process lake at a time; none of them
// run in parallel.

type env struct {
	lake *laketest.Lake
	src  *sep54.Source // the lake, as Galexie and ingest see it
	tip  *sep54.TipClient
}

func startLake(t *testing.T, dir string) env {
	t.Helper()
	l := laketest.Start(t, laketest.Options{Dir: dir})
	src, err := sep54.OpenLake(context.Background(), l.Cfg, l.Creds)
	if err != nil {
		t.Fatal(err)
	}
	return env{lake: l, src: src, tip: sep54.NewTipClient(l.ControlAddr())}
}

func (e env) options(t *testing.T, up devfeed.Upstream, start, end uint32, speed float64) devfeed.Options {
	return devfeed.Options{
		Upstream:   up,
		Lake:       e.src.DataStore(),
		Passphrase: network.PublicNetworkPassphrase,
		Start:      start,
		End:        end,
		Speed:      speed,
		TipPoll:    10 * time.Millisecond,
		Log:        slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelInfo})),
	}
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(strings.TrimRight(string(p), "\n"))
	return len(p), nil
}

// requireLakeHolds checks the lake holds exactly the upstream bytes and 9
// metadata keys for [from, to], and that they decode and chain.
func (e env) requireLakeHolds(t *testing.T, up devfeed.Upstream, from, to uint32) {
	t.Helper()
	ctx := context.Background()
	var prev sep54.Header
	for seq := from; seq <= to; seq++ {
		want, err := up.GetLedger(ctx, seq)
		if err != nil {
			t.Fatal(err)
		}
		got, err := e.src.GetLedger(ctx, seq)
		if err != nil {
			t.Fatalf("lake ledger %d: %v", seq, err)
		}
		wantMD, _ := want.LedgerMetadata()
		if !bytes.Equal(got.Body, want.Body) || !maps.Equal(got.Metadata, wantMD) {
			t.Fatalf("lake ledger %d differs from upstream (metadata %v)", seq, got.Metadata)
		}
		if got.ChecksumCRC32C != sep54.CRC32C(want.Body) {
			t.Fatalf("lake ledger %d: stored CRC32C %q", seq, got.ChecksumCRC32C)
		}
		_, h, err := sep54.DecodeLedger(seq, got.Body)
		if err != nil {
			t.Fatal(err)
		}
		if seq > from {
			if err := sep54.CheckChain(prev, h); err != nil {
				t.Fatal(err)
			}
		}
		prev = h
	}
	m, err := e.src.GetObject(ctx, sep54.ManifestFile)
	if err != nil || !bytes.Equal(m.Body, sep54.ManifestBytes(network.PublicNetworkPassphrase)) {
		t.Fatalf("lake manifest %q %v", m.Body, err)
	}
}

func (e env) requireTip(t *testing.T, tip, oldest uint32) {
	t.Helper()
	got, err := e.tip.Get(context.Background())
	if err != nil || got.Tip == nil || *got.Tip != tip || *got.Oldest != oldest {
		t.Fatalf("lake tip %+v %v, want tip %d oldest %d", got, err, tip, oldest)
	}
}

func TestFeedIntoLake(t *testing.T) {
	e := startLake(t, "")
	up := sep54test.NewMemSource(sep54test.Chain(63_990, 30)...) // crosses a partition boundary
	res, err := devfeed.Run(context.Background(), e.options(t, up, 63_990, 64_019, 0))
	if err != nil {
		t.Fatal(err)
	}
	if res != (devfeed.Result{From: 63_990, Last: 64_019, Uploaded: 30}) {
		t.Fatalf("result %+v", res)
	}
	e.requireTip(t, 64_019, 63_990)
	e.requireLakeHolds(t, up, 63_990, 64_019)
}

// dev-feed resumes like Galexie at max(start, newest+1), and a bounded run
// whose range is already exported does nothing.
func TestFeedResumesLikeGalexie(t *testing.T) {
	e := startLake(t, "")
	up := sep54test.NewMemSource(sep54test.Chain(1000, 40)...)
	ctx := context.Background()
	if res, err := devfeed.Run(ctx, e.options(t, up, 1000, 1019, 0)); err != nil || res.Uploaded != 20 {
		t.Fatalf("first run %+v %v", res, err)
	}
	gets := up.Gets.Load()
	res, err := devfeed.Run(ctx, e.options(t, up, 1000, 1039, 0))
	if err != nil || res != (devfeed.Result{From: 1020, Last: 1039, Uploaded: 20}) {
		t.Fatalf("resumed run %+v %v", res, err)
	}
	if n := up.Gets.Load() - gets; n != 20 {
		t.Fatalf("resumed run read %d upstream objects, want 20", n)
	}
	if res, err := devfeed.Run(ctx, e.options(t, up, 1000, 1039, 0)); err != nil || res != (devfeed.Result{}) {
		t.Fatalf("already exported: %+v %v", res, err)
	}
	// A start above the newest ledger wins (like --start after a gap).
	up.Add(sep54test.Chain(2000, 5)...)
	if res, err := devfeed.Run(ctx, e.options(t, up, 2000, 2004, 0)); err != nil || res.From != 2000 || res.Uploaded != 5 {
		t.Fatalf("start above newest: %+v %v", res, err)
	}
	e.requireTip(t, 2004, 1000)
	e.requireLakeHolds(t, up, 1000, 1039)
}

// Speed paces uploads by the ledgers' close times (5 s apart): at 50x a
// ledger commits every 100 ms, observed through the lake's commit tip.
func TestFeedPacesAtLedgerCadence(t *testing.T) {
	e := startLake(t, "")
	up := sep54test.NewMemSource(sep54test.Chain(5000, 11)...)
	const speed = 50
	want := time.Duration(float64(5*time.Second) / speed)

	var mu sync.Mutex
	var commits []time.Time
	stop := make(chan struct{})
	watched := make(chan struct{})
	go func() {
		defer close(watched)
		after := uint32(4999)
		for after < 5010 {
			tip, err := e.tip.WaitAbove(context.Background(), after, time.Minute)
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			for s := after + 1; s <= *tip.Tip; s++ {
				commits = append(commits, time.Now())
			}
			mu.Unlock()
			after = *tip.Tip
			select {
			case <-stop:
				return
			default:
			}
		}
	}()
	began := time.Now()
	res, err := devfeed.Run(context.Background(), e.options(t, up, 5000, 5010, speed))
	elapsed := time.Since(began)
	if err != nil || res.Uploaded != 11 {
		t.Fatalf("%+v %v", res, err)
	}
	<-watched
	close(stop)
	if elapsed < 10*want-want/2 || elapsed > 10*want+3*time.Second {
		t.Fatalf("11 ledgers at %vx took %v, want about %v", speed, elapsed, 10*want)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(commits) != 11 {
		t.Fatalf("saw %d commits", len(commits))
	}
	// Pacing is a lower bound: ledger i may not commit before i*100ms after
	// the first (a slow upload may delay it, and the next catches up).
	var gaps []string
	for i := 1; i < len(commits); i++ {
		gaps = append(gaps, commits[i].Sub(commits[i-1]).Round(time.Millisecond).String())
		if since := commits[i].Sub(commits[0]); since < time.Duration(i)*want-30*time.Millisecond {
			t.Errorf("ledger %d committed %v after the first; want at least %v", 5000+i, since, time.Duration(i)*want)
		}
	}
	t.Logf("commit spacing at %vx: %v (target %v); total %v", speed, gaps, want, elapsed.Round(time.Millisecond))
}

// Following the upstream tip: ledgers are uploaded as they appear.
func TestFeedFollowsUpstreamTip(t *testing.T) {
	e := startLake(t, "")
	chain := sep54test.Chain(300, 10)
	up := sep54test.NewMemSource(chain[:3]...)
	go func() {
		for _, o := range chain[3:] {
			time.Sleep(40 * time.Millisecond)
			up.Add(o)
		}
	}()
	res, err := devfeed.Run(context.Background(), e.options(t, up, 300, 309, 0))
	if err != nil || res.Uploaded != 10 || res.Last != 309 {
		t.Fatalf("%+v %v", res, err)
	}
	e.requireLakeHolds(t, up, 300, 309)
}

// A stop signal ends an unbounded run cleanly after the in-flight upload.
func TestFeedStopsCleanly(t *testing.T) {
	e := startLake(t, "")
	up := sep54test.NewMemSource(sep54test.Chain(400, 5)...)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var res devfeed.Result
	var err error
	go func() {
		defer close(done)
		res, err = devfeed.Run(ctx, e.options(t, up, 400, 0, 0))
	}()
	if _, werr := e.tip.WaitAbove(context.Background(), 403, time.Minute); werr != nil {
		t.Fatal(werr)
	}
	time.Sleep(50 * time.Millisecond) // now waiting for ledger 405 upstream
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("dev-feed did not stop")
	}
	if err != nil || res.Last != 404 || res.Uploaded != 5 {
		t.Fatalf("%+v %v", res, err)
	}
}

// dev-feed refuses bad upstream objects and never uploads them.
func TestFeedRejectsBadUpstream(t *testing.T) {
	e := startLake(t, "")
	ctx := context.Background()
	good := sep54test.Chain(700, 3)
	corrupt := func(mutate func(o *sep54.Object)) *sep54test.MemSource {
		objs := sep54test.Chain(700, 3)
		mutate(&objs[1])
		return sep54test.NewMemSource(objs...)
	}
	cases := map[string]struct {
		up   *sep54test.MemSource
		want string
	}{
		"CRC32C mismatch": {corrupt(func(o *sep54.Object) { o.Body = bytes.Clone(o.Body); o.Body[10] ^= 1 }), "CRC32C mismatch"},
		"missing metadata": {corrupt(func(o *sep54.Object) {
			o.Metadata = maps.Clone(o.Metadata)
			delete(o.Metadata, "core-version")
		}), "missing metadata core-version"},
		"metadata names another ledger": {corrupt(func(o *sep54.Object) {
			o.Metadata = maps.Clone(o.Metadata)
			o.Metadata["start-ledger"] = "9"
		}), "metadata names ledgers 9-701"},
		"broken chain":             {corrupt(func(o *sep54.Object) { *o = sep54test.Chain(701, 1)[0] }), "upstream chain"},
		"object of another ledger": {corrupt(func(o *sep54.Object) { o.Body, o.ChecksumCRC32C = good[2].Body, good[2].ChecksumCRC32C }), "batch covers"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := devfeed.Run(ctx, e.options(t, c.up, 700, 702, 0))
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err = %v, want %q", err, c.want)
			}
			if _, err := e.src.GetLedger(ctx, 701); !errors.Is(err, sep54.ErrNotFound) {
				t.Fatalf("ledger 701 reached the lake: %v", err)
			}
			// Ledger 700 was fine and is kept; clean up for the next case.
			if _, err := e.src.Client().DeleteObject(ctx, deleteInput(e, 700)); err != nil {
				t.Fatal(err)
			}
		})
	}

	// A resumed run is chain-checked against the lake's newest ledger:
	// ledgers from another producer do not extend it.
	if res, err := devfeed.Run(ctx, e.options(t, sep54test.NewMemSource(good...), 700, 701, 0)); err != nil || res.Uploaded != 2 {
		t.Fatalf("%+v %v", res, err)
	}
	other := sep54test.NewMemSource(sep54test.Chain(702, 1)...)
	if _, err := devfeed.Run(ctx, e.options(t, other, 700, 702, 0)); err == nil || !strings.Contains(err.Error(), "previous hash") {
		t.Fatalf("mixed producers: %v", err)
	}

	// Upstream read errors are retried, then fatal.
	flaky := sep54test.NewMemSource(good...)
	failures := 0
	flaky.Fail = func(seq uint32) error {
		if seq == 702 && failures < 2 {
			failures++
			return &sep54.UnavailableError{Op: "test", Err: errors.New("connection reset")}
		}
		return nil
	}
	if res, err := devfeed.Run(ctx, e.options(t, flaky, 700, 702, 0)); err != nil || res.From != 702 || res.Uploaded != 1 {
		t.Fatalf("transient upstream errors: %+v %v", res, err)
	}
}

// dev-feed exits non-zero when the lake refuses or loses a PUT.
func TestFeedFailsOnPutError(t *testing.T) {
	e := startLake(t, "")
	chain := sep54test.Chain(900, 4)
	up := sep54test.NewMemSource(chain[:2]...)
	errc := make(chan error, 1)
	go func() {
		_, err := devfeed.Run(context.Background(), e.options(t, up, 900, 903, 0))
		errc <- err
	}()
	if _, err := e.tip.WaitAbove(context.Background(), 900, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := e.lake.Stop(); err != nil {
		t.Fatal(err)
	}
	up.Add(chain[2:]...)
	select {
	case err := <-errc:
		if err == nil || !strings.Contains(err.Error(), "upload ledger 902") {
			t.Fatalf("err = %v", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("dev-feed did not fail")
	}
}

func TestCheckArchive(t *testing.T) {
	for _, c := range []struct {
		a  config.Archive
		ok bool
	}{
		{config.Default().Archive, false},
		{config.Archive{Enabled: true, Bucket: "stellar-ledgers-pubnet-staging"}, false},
		{config.Archive{Enabled: true, Bucket: "stellar-ledgers-pubnet-dev"}, true},
		{config.Archive{Enabled: false, Bucket: "stellar-ledgers-pubnet"}, true},
	} {
		if err := devfeed.CheckArchive(c.a); (err == nil) != c.ok {
			t.Errorf("%+v: %v", c.a, err)
		}
	}
}

func TestOptionsValidation(t *testing.T) {
	e := startLake(t, "")
	up := sep54test.NewMemSource()
	for name, o := range map[string]devfeed.Options{
		"start 0":     e.options(t, up, 0, 0, 1),
		"start 1":     e.options(t, up, 1, 0, 1),
		"end < start": e.options(t, up, 10, 9, 1),
		"speed < 0":   e.options(t, up, 10, 0, -1),
	} {
		if _, err := devfeed.Run(context.Background(), o); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}

// Opt-in: feed cached real AWS-lake ledgers (see sep54's
// TestAWSLakeRecentLedgers) through the lake at accelerated cadence, then
// read every one back and validate it.
//
//	WORKER_TEST_LAKE_CACHE=/path     a complete cache (range.json)
//	WORKER_TEST_DEVFEED_LEDGERS=N    how many to feed (default: all)
//	WORKER_TEST_DEVFEED_SPEED=S      cadence multiplier for the first 100 (default 50)
func TestFeedCachedAWSLedgers(t *testing.T) {
	dir := os.Getenv("WORKER_TEST_LAKE_CACHE")
	if dir == "" {
		t.Skip("set WORKER_TEST_LAKE_CACHE to a filled ledger cache (network test)")
	}
	up, err := sep54test.OpenCache(dir)
	if err != nil {
		t.Skipf("no complete cache in %s: %v", dir, err)
	}
	n := int(up.End - up.Start + 1)
	if v := os.Getenv("WORKER_TEST_DEVFEED_LEDGERS"); v != "" {
		if n, err = strconv.Atoi(v); err != nil || n < 2 || n > int(up.End-up.Start+1) {
			t.Fatalf("WORKER_TEST_DEVFEED_LEDGERS=%q", v)
		}
	}
	speed := 50.0
	if v := os.Getenv("WORKER_TEST_DEVFEED_SPEED"); v != "" {
		if speed, err = strconv.ParseFloat(v, 64); err != nil {
			t.Fatal(err)
		}
	}
	e := startLake(t, "")
	ctx := context.Background()
	start := up.Start
	paced := min(100, n)

	// Phase 1: accelerated ledger cadence, watching commits on the tip.
	var commits []time.Time
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		for after := start - 1; after < start+uint32(paced)-1; {
			tip, err := e.tip.WaitAbove(ctx, after, time.Minute)
			if err != nil {
				t.Error(err)
				return
			}
			for s := after + 1; s <= *tip.Tip; s++ {
				commits = append(commits, time.Now())
			}
			after = *tip.Tip
		}
	}()
	began := time.Now()
	res, err := devfeed.Run(ctx, e.options(t, up, start, start+uint32(paced)-1, speed))
	if err != nil || res.Uploaded != paced {
		t.Fatalf("paced phase %+v %v", res, err)
	}
	pacedDur := time.Since(began)
	<-watchDone
	first, _ := up.GetLedger(ctx, start)
	lastPaced, _ := up.GetLedger(ctx, start+uint32(paced)-1)
	h0, _ := sep54.DecodeHeader(start, first.Body)
	h1, _ := sep54.DecodeHeader(start+uint32(paced)-1, lastPaced.Body)
	real := time.Duration(h1.CloseTime-h0.CloseTime) * time.Second
	t.Logf("paced: %d real ledgers (%v of network time) fed at %vx in %v (target %v)",
		paced, real, speed, pacedDur.Round(time.Millisecond), time.Duration(float64(real)/speed).Round(time.Millisecond))
	if want := time.Duration(float64(real) / speed); pacedDur < want-want/10 || pacedDur > want+10*time.Second {
		t.Fatalf("paced phase took %v, want about %v", pacedDur, want)
	}

	// Phase 2: the rest as fast as possible (catch-up).
	if n > paced {
		began = time.Now()
		res, err = devfeed.Run(ctx, e.options(t, up, start, start+uint32(n)-1, 0))
		if err != nil || res.From != start+uint32(paced) || res.Uploaded != n-paced {
			t.Fatalf("catch-up phase %+v %v", res, err)
		}
		d := time.Since(began)
		t.Logf("catch-up: %d ledgers in %v (%.0f ledgers/s)", n-paced, d.Round(time.Millisecond), float64(n-paced)/d.Seconds())
	}
	e.requireTip(t, start+uint32(n)-1, start)
	e.requireLakeHolds(t, up, start, start+uint32(n)-1)
	t.Logf("lake holds ledgers %d-%d byte- and metadata-identical to the AWS lake; all decode (V2) and chain", start, start+uint32(n)-1)
}

func deleteInput(e env, seq uint32) *s3.DeleteObjectInput {
	return &s3.DeleteObjectInput{Bucket: aws.String(e.src.Bucket()), Key: aws.String(e.src.Key(sep54.ObjectPath(seq)))}
}
