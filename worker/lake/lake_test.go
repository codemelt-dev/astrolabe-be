package lake

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"stellar-explorer/worker/sep54"
)

func fileSHA(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func requireAbsent(t *testing.T, tl *testLake, seq uint32) {
	t.Helper()
	if _, err := os.Stat(tl.objectPath(seq)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ledger %d is on disk (err %v), want absent", seq, err)
	}
	if h := headObject(t, tl.s3URL, sep54.ObjectPath(seq)); !h.NotFound {
		t.Fatalf("HEAD ledger %d: not_found=%v err=%q, want NotFound", seq, h.NotFound, h.Err)
	}
}

// requireNoTempFiles checks that no staged body was left behind.
func requireNoTempFiles(t *testing.T, tl *testLake) {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(tl.dir, "s3", testBucket, ".sgwtmp"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	for _, e := range entries {
		if !e.IsDir() {
			t.Fatalf("staged file left behind: %s", e.Name())
		}
	}
}

// ------------------------------------------------------------------ bullets

// Object and its 9 metadata keys round-trip through Galexie's client stack;
// every ledger is fsynced before the link, then as a linked file, then its
// directories up to the bucket.
func TestObjectAndMetadataRoundTrip(t *testing.T) {
	tl := startLake(t, lakeOpts{})
	requireEmptyTip(t, tl.ctrlURL)
	res := mustPut(t, tl.s3URL, 1000, 1002)

	for _, r := range res {
		if len(r.Meta) != 9 {
			t.Fatalf("client sent %d metadata keys: %v", len(r.Meta), r.Meta)
		}
		h := headObject(t, tl.s3URL, r.Key)
		if h.Err != "" || fmt.Sprint(h.Meta) != fmt.Sprint(r.Meta) {
			t.Fatalf("ledger %d metadata: got %v (err %q), sent %v", r.Seq, h.Meta, h.Err, r.Meta)
		}
		g := getObject(t, tl.s3URL, r.Key) // ChecksumMode=ENABLED validates the stored CRC32C
		if g.Err != "" || g.SHA256 != r.SHA256 || g.Size != r.Size {
			t.Fatalf("ledger %d body: got sha %s size %d err %q, sent sha %s size %d", r.Seq, g.SHA256, g.Size, g.Err, r.SHA256, r.Size)
		}
		if got := fileSHA(t, tl.objectPath(r.Seq)); got != r.SHA256 {
			t.Fatalf("ledger %d on disk: sha %s, sent %s", r.Seq, got, r.SHA256)
		}

		// The fsync chain: staged body, then the linked file, then its
		// partition, prefix and bucket directories.
		path := tl.objectPath(r.Seq)
		if n := tl.fsync.count(StagePublished, path); n != 1 {
			t.Fatalf("ledger %d: %d published fsyncs, want 1", r.Seq, n)
		}
		bucketDir := filepath.Join(tl.dir, "s3", testBucket)
		for _, d := range []string{filepath.Dir(path), filepath.Join(bucketDir, "ledgers", "pubnet"), filepath.Join(bucketDir, "ledgers"), bucketDir} {
			if tl.fsync.count(StageDir, d) == 0 {
				t.Fatalf("ledger %d: directory %s never fsynced", r.Seq, d)
			}
		}
	}
	// One staged fsync per PUT with a body (manifest + 3 ledgers), each before its link.
	staged := 0
	for _, e := range tl.fsync.snapshot() {
		if e.stage == StageStaged {
			staged++
		}
	}
	if staged != 4 {
		t.Fatalf("%d staged fsyncs, want 4 (manifest + 3 ledgers)", staged)
	}
	requireStagedBeforePublished(t, tl)

	requireTip(t, tl.ctrlURL, 1002, 1000)
	requireMetric(t, tl.ctrlURL, `lake_tip`, 1002)
	requireMetric(t, tl.ctrlURL, `lake_oldest`, 1000)
	requireMetric(t, tl.ctrlURL, `lake_puts_total{result="created"}`, 4)
	requireMetric(t, tl.ctrlURL, `lake_fsync_failed`, 0)

	// Galexie's manifest landed too (and is not a ledger).
	m, err := os.ReadFile(filepath.Join(tl.dir, "s3", testBucket, testPrefix, ".config.json"))
	if err != nil || !bytes.Contains(m, []byte(`"networkPassphrase":"Public Global Stellar Network ; September 2015"`)) {
		t.Fatalf("manifest: %s (err %v)", m, err)
	}
	requireNoTempFiles(t, tl)
}

// requireStagedBeforePublished checks each published fsync is preceded by a
// staged one in the same request (PUTs here are sequential).
func requireStagedBeforePublished(t *testing.T, tl *testLake) {
	t.Helper()
	pendingStaged := 0
	for _, e := range tl.fsync.snapshot() {
		switch e.stage {
		case StageStaged:
			pendingStaged++
		case StagePublished:
			if pendingStaged == 0 {
				t.Fatalf("published fsync of %s without a staged fsync before its link", e.path)
			}
			pendingStaged--
		}
	}
}

// A duplicate PUT returns (false, nil) to Galexie and re-runs the fsync
// chain on the existing object (rule 2).
func TestDuplicateIsResynced(t *testing.T) {
	tl := startLake(t, lakeOpts{})
	first := mustPut(t, tl.s3URL, 2000, 2000)[0]
	path := tl.objectPath(2000)
	before := tl.fsync.snapshot()

	dup := putOne(t, tl.s3URL, 2000)
	if dup.Uploaded || dup.Err != "" {
		t.Fatalf("duplicate: uploaded=%v err=%q, want (false, nil)", dup.Uploaded, dup.Err)
	}
	after := tl.fsync.snapshot()[len(before):]
	var published, dirs, staged int
	for _, e := range after {
		switch {
		case e.stage == StagePublished && e.path == path:
			published++
		case e.stage == StageDir && e.path == filepath.Dir(path):
			dirs++
		case e.stage == StageStaged:
			staged++
		}
	}
	if published != 1 || dirs != 1 {
		t.Fatalf("duplicate re-fsync: %d file, %d partition-dir fsyncs; want 1 and 1 (events %v)", published, dirs, after)
	}
	if staged != 0 {
		t.Fatalf("the 412 should fail before the body is read, but %d bodies were staged", staged)
	}
	if got := fileSHA(t, path); got != first.SHA256 {
		t.Fatal("the duplicate overwrote the object")
	}
	requireMetric(t, tl.ctrlURL, `lake_puts_total{result="exists"}`, 1)
	requireTip(t, tl.ctrlURL, 2000, 2000)
}

// Eight concurrent If-None-Match PUTs of one ledger: exactly one wins.
func TestConcurrentPutsOneWins(t *testing.T) {
	tl := startLake(t, lakeOpts{})
	lines, code := runClient(t, tl.s3URL, "race", "--seq", "3000", "--n", "8")
	if code != 0 || len(lines) != 1 {
		t.Fatalf("race: exit %d, %d lines", code, len(lines))
	}
	res := decode[[]putResult](t, lines[0])
	var winners []putResult
	for _, r := range res {
		if r.Err != "" {
			t.Fatalf("race upload error: %s", r.Err)
		}
		if r.Uploaded {
			winners = append(winners, r)
		}
	}
	if len(res) != 8 || len(winners) != 1 {
		t.Fatalf("%d results, %d winners; want 8 and 1", len(res), len(winners))
	}
	if got := fileSHA(t, tl.objectPath(3000)); got != winners[0].SHA256 {
		t.Fatalf("on-disk object %s is not the winner's %s", got, winners[0].SHA256)
	}
	if g := getObject(t, tl.s3URL, winners[0].Key); g.SHA256 != winners[0].SHA256 {
		t.Fatalf("GET returns %s, winner %s", g.SHA256, winners[0].SHA256)
	}
	requireTip(t, tl.ctrlURL, 3000, 3000)
	requireMetric(t, tl.ctrlURL, `lake_puts_total{result="exists"}`, 7)
	requireNoTempFiles(t, tl)
}

// A PUT whose connection dies mid-body is never visible. Galexie's client
// (signed payload) is cut by a proxy; an unsigned-payload PUT, which
// VersityGW alone would commit truncated, is caught by the length guard.
func TestAbortedPutInvisible(t *testing.T) {
	tl := startLake(t, lakeOpts{})
	mustPut(t, tl.s3URL, 4100, 4100)

	t.Run("galexie client, connection cut mid-body", func(t *testing.T) {
		proxy := newCutProxy(t, tl.cfg.S3Listen)
		res := put(t, proxy.url, 4101, 4101)
		if len(res) != 1 || res[0].Err == "" {
			t.Fatalf("put through the cutting proxy succeeded: %+v", res)
		}
		if proxy.cuts() == 0 {
			t.Fatal("the proxy never cut a PUT")
		}
		requireAbsent(t, tl, 4101)
		requireTip(t, tl.ctrlURL, 4100, 4100)
	})

	t.Run("unsigned payload, client disconnects", func(t *testing.T) {
		key := sep54.Key(testPrefix, 4102)
		conn := startRawPut(t, tl.cfg.S3Listen, key, 200_000, 50_000)
		// While the sender stalls the PUT is in phase "receiving".
		waitFor(t, 5*time.Second, func() bool {
			v, _ := metricValue(t, tl.ctrlURL, `lake_put_phase_age_seconds{phase="receiving"}`)
			return v > 0
		})
		conn.Close()
		waitFor(t, 5*time.Second, func() bool {
			v, _ := metricValue(t, tl.ctrlURL, `lake_put_phase_age_seconds{phase="receiving"}`)
			return v == 0
		})
		requireAbsent(t, tl, 4102)
		requireTip(t, tl.ctrlURL, 4100, 4100)
	})

	requireNoTempFiles(t, tl)
	mustPut(t, tl.s3URL, 4101, 4102) // the lake still works
	requireTip(t, tl.ctrlURL, 4102, 4100)
}

// An fsync error in the post-function (before the link) leaves the object
// invisible and the tip unchanged; the lake stays healthy.
func TestStagedFsyncErrorInvisible(t *testing.T) {
	tl := startLake(t, lakeOpts{})
	mustPut(t, tl.s3URL, 4199, 4199)
	injected := errors.New("injected EIO")

	t.Run("persistent", func(t *testing.T) {
		tl.fsync.setInject(func(s Stage, _ string) error {
			if s == StageStaged {
				return injected
			}
			return nil
		})
		r := putOne(t, tl.s3URL, 4200)
		tl.fsync.setInject(nil)
		if r.Err == "" || r.Uploaded {
			t.Fatalf("put succeeded despite failing staged fsyncs: %+v", r)
		}
		requireAbsent(t, tl, 4200)
		requireTip(t, tl.ctrlURL, 4199, 4199)
		if tl.fsync.count(StagePublished, tl.objectPath(4200)) != 0 {
			t.Fatal("a staged-fsync failure reached the link")
		}
		requireMetric(t, tl.ctrlURL, `lake_fsync_failed`, 0)
		if code, _ := httpGet(t, tl.ctrlURL+"/healthz"); code != http.StatusOK {
			t.Fatalf("healthz %d after a pre-link fsync error", code)
		}
		requireNoTempFiles(t, tl)
		mustPut(t, tl.s3URL, 4200, 4200) // Galexie's retry after a restart works
		requireTip(t, tl.ctrlURL, 4200, 4199)
	})

	t.Run("transient: the SDK retry commits it once", func(t *testing.T) {
		var once sync.Once
		tl.fsync.setInject(func(s Stage, _ string) error {
			var err error
			if s == StageStaged {
				once.Do(func() { err = injected })
			}
			return err
		})
		r := putOne(t, tl.s3URL, 4201)
		tl.fsync.setInject(nil)
		if r.Err != "" || !r.Uploaded {
			t.Fatalf("put 4201: %+v", r)
		}
		requireTip(t, tl.ctrlURL, 4201, 4199)
	})
}

// An fsync error after the link puts the lake in fail-closed mode: every
// later PUT gets 503 and lake_fsync_failed=1 (rule 1).
func TestFsyncErrorAfterLinkFailsClosed(t *testing.T) {
	for _, stage := range []Stage{StagePublished, StageDir} {
		t.Run(string(stage), func(t *testing.T) {
			tl := startLake(t, lakeOpts{})
			mustPut(t, tl.s3URL, 4300, 4300)
			tl.fsync.setInject(func(s Stage, _ string) error {
				if s == stage {
					return errors.New("injected EIO")
				}
				return nil
			})
			r := putOne(t, tl.s3URL, 4301)
			tl.fsync.setInject(nil)
			if r.Err == "" || !strings.Contains(r.Err, "503") {
				t.Fatalf("put 4301: err %q, want a 503", r.Err)
			}
			requireMetric(t, tl.ctrlURL, `lake_fsync_failed`, 1)
			if code, body := httpGet(t, tl.ctrlURL+"/healthz"); code != http.StatusServiceUnavailable || !strings.Contains(body, "fail-closed") {
				t.Fatalf("healthz %d %q, want 503 fail-closed", code, body)
			}
			// Nothing unsynced is acknowledged or advertised.
			requireTip(t, tl.ctrlURL, 4300, 4300)
			// Every later PUT is refused, even with a healthy disk.
			r = putOne(t, tl.s3URL, 4302)
			if r.Err == "" || !strings.Contains(r.Err, "503") || strings.Contains(r.Err, "Precondition") {
				t.Fatalf("put 4302 after fail-closed: err %q, want 503", r.Err)
			}
			if _, err := os.Stat(tl.objectPath(4302)); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("a PUT was accepted in fail-closed mode")
			}
			requireTip(t, tl.ctrlURL, 4300, 4300)
			if v, _ := metricValue(t, tl.ctrlURL, `lake_puts_total{result="fail_closed"}`); v < 2 {
				t.Fatalf("lake_puts_total{fail_closed} = %v", v)
			}
		})
	}
}

// A handler that dies between link and fsync (no error observed) leaves the
// object linked but unacknowledged; the client's retry gets a 412, which
// re-runs the fsync chain and advances the tip (rule 2). The out-of-process
// variant (SIGKILL + restart + syncfs) is TestLakeKilledBetweenLinkAndSync.
func TestPanicBetweenLinkAndSyncRecoversVia412(t *testing.T) {
	tl := startLake(t, lakeOpts{})
	mustPut(t, tl.s3URL, 4400, 4400)
	path := tl.objectPath(4401)
	var once sync.Once
	tl.fsync.setInject(func(s Stage, p string) error {
		if s == StagePublished && p == path {
			once.Do(func() { panic("simulated crash between link and fsync") })
		}
		return nil
	})
	r := putOne(t, tl.s3URL, 4401)
	tl.fsync.setInject(nil)
	if r.Err != "" || r.Uploaded {
		t.Fatalf("put 4401: uploaded=%v err=%q; want (false, nil): the retry must see a 412", r.Uploaded, r.Err)
	}
	if n := tl.fsync.count(StagePublished, path); n != 2 {
		t.Fatalf("%d published fsyncs of 4401, want 2 (the crashed one and the 412 re-sync)", n)
	}
	requireTip(t, tl.ctrlURL, 4401, 4400)
	requireMetric(t, tl.ctrlURL, `lake_puts_total{result="exists"}`, 1)
	requireMetric(t, tl.ctrlURL, `lake_fsync_failed`, 0)
}

// The tip never moves backwards: neither a PUT nor a 412 of a ledger below
// the tip lowers it. The first PUT into an empty lake sets oldest; deleting
// a non-oldest key leaves it unchanged.
func TestTipMonotonicAndOldest(t *testing.T) {
	tl := startLake(t, lakeOpts{})
	requireEmptyTip(t, tl.ctrlURL)
	mustPut(t, tl.s3URL, 6000, 6000)
	requireTip(t, tl.ctrlURL, 6000, 6000) // first PUT sets oldest
	mustPut(t, tl.s3URL, 6001, 6005)
	requireTip(t, tl.ctrlURL, 6005, 6000)
	at := *getTip(t, tl.ctrlURL).TipCommittedAt

	mustPut(t, tl.s3URL, 5990, 5990) // a new ledger below the tip (and below oldest)
	requireTip(t, tl.ctrlURL, 6005, 6000)
	if r := putOne(t, tl.s3URL, 6002); r.Uploaded || r.Err != "" { // a 412 below the tip
		t.Fatalf("re-put 6002: %+v", r)
	}
	requireTip(t, tl.ctrlURL, 6005, 6000)
	if got := *getTip(t, tl.ctrlURL).TipCommittedAt; got != at {
		t.Fatal("tip_committed_at changed without the tip growing")
	}

	c := s3Client(tl.s3URL)
	ctx := context.Background()
	del := func(seq uint32) {
		t.Helper()
		if _, err := c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(testBucket), Key: aws.String(sep54.Key(testPrefix, seq))}); err != nil {
			t.Fatalf("delete %d: %v", seq, err)
		}
	}
	del(6003) // not the oldest
	requireTip(t, tl.ctrlURL, 6005, 6000)
	del(6000)
	requireTip(t, tl.ctrlURL, 6005, 6001)
	del(6001)
	del(6002)
	requireTip(t, tl.ctrlURL, 6005, 6003) // oldest now points at a hole (6003 was deleted)

	// Multi-object delete applies the ledgers in ascending order.
	out, err := c.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String(testBucket), Delete: &types.Delete{Objects: []types.ObjectIdentifier{
		{Key: aws.String(sep54.Key(testPrefix, 6004))},
		{Key: aws.String(sep54.Key(testPrefix, 6003))},
	}}})
	if err != nil || len(out.Errors) > 0 {
		t.Fatalf("DeleteObjects: %v %+v", err, out)
	}
	requireTip(t, tl.ctrlURL, 6005, 6005)
	if _, err := os.Stat(tl.objectPath(6005)); err != nil {
		t.Fatalf("newest object: %v", err)
	}

	// Server-side copies would bypass the commit path and are refused.
	_, err = c.CopyObject(ctx, &s3.CopyObjectInput{Bucket: aws.String(testBucket), Key: aws.String(sep54.Key(testPrefix, 6006)),
		CopySource: aws.String(testBucket + "/" + sep54.Key(testPrefix, 6005))})
	var ae smithy.APIError
	if !errors.As(err, &ae) || ae.ErrorCode() != "NotImplemented" {
		t.Fatalf("CopyObject: %v, want NotImplemented", err)
	}
	requireTip(t, tl.ctrlURL, 6005, 6005)
}

// Multipart uploads (Galexie's client switches at 5 MiB) run the same fsync
// chain and commit record, including the 412 path (rule 3).
func TestMultipartCompleteCommits(t *testing.T) {
	tl := startLake(t, lakeOpts{})
	key := sep54.ObjectPath(8000)
	lines, _ := runClient(t, tl.s3URL, "put-bytes", "--key", key, "--size", strconv.Itoa(6<<20), "--seed", "7")
	r := decode[putResult](t, lines[0])
	if r.Err != "" || !r.Uploaded {
		t.Fatalf("multipart put: %+v", r)
	}
	path := tl.objectPath(8000)
	if tl.fsync.count(StagePublished, path) != 1 || tl.fsync.count(StageDir, filepath.Dir(path)) == 0 {
		t.Fatalf("multipart object not fsynced: %v", tl.fsync.snapshot())
	}
	if got := fileSHA(t, path); got != r.SHA256 {
		t.Fatalf("on-disk %s, sent %s", got, r.SHA256)
	}
	requireTip(t, tl.ctrlURL, 8000, 8000)

	lines, _ = runClient(t, tl.s3URL, "put-bytes", "--key", key, "--size", strconv.Itoa(6<<20), "--seed", "8")
	dup := decode[putResult](t, lines[0])
	if dup.Err != "" || dup.Uploaded {
		t.Fatalf("duplicate multipart put: %+v, want (false, nil)", dup)
	}
	if got := fileSHA(t, path); got != r.SHA256 {
		t.Fatal("the duplicate multipart upload overwrote the object")
	}
	if tl.fsync.count(StagePublished, path) != 2 {
		t.Fatal("the multipart 412 did not re-run the fsync chain")
	}
	requireMetric(t, tl.ctrlURL, `lake_puts_total{result="exists"}`, 1)
}

// A long-poll wakes within 5 ms of a commit.
func TestLongPollWakesWithin5ms(t *testing.T) {
	tl := startLake(t, lakeOpts{})
	mustPut(t, tl.s3URL, 9500, 9500)
	for seq := uint32(9501); seq <= 9505; seq++ {
		type result struct {
			tip  sep54.Tip
			recv time.Time
			err  error
		}
		got := make(chan result, 1)
		go func() {
			resp, err := http.Get(fmt.Sprintf("%s/v1/tip?after=%d", tl.ctrlURL, seq-1))
			if err != nil {
				got <- result{err: err}
				return
			}
			defer resp.Body.Close()
			var tip sep54.Tip
			err = json.NewDecoder(resp.Body).Decode(&tip)
			got <- result{tip, time.Now(), err}
		}()
		time.Sleep(50 * time.Millisecond) // let the long-poll park
		select {
		case r := <-got:
			t.Fatalf("long-poll returned before the commit: %+v", r)
		default:
		}
		mustPut(t, tl.s3URL, seq, seq)
		r := <-got
		if r.err != nil || r.tip.Tip == nil || *r.tip.Tip != seq {
			t.Fatalf("long-poll after=%d: %s err %v", seq-1, fmtTip(r.tip), r.err)
		}
		lag := r.recv.Sub(time.Unix(0, *r.tip.TipCommittedAt))
		t.Logf("ledger %d: long-poll answered %v after the commit", seq, lag)
		if lag > 5*time.Millisecond {
			t.Fatalf("long-poll answered %v after the commit, want <= 5ms", lag)
		}
	}
}

func TestTipEndpoint(t *testing.T) {
	tl := startLake(t, lakeOpts{longPoll: 200 * time.Millisecond})
	// GET /v1/tip on an empty lake returns at once, all null.
	start := time.Now()
	requireEmptyTip(t, tl.ctrlURL)
	if time.Since(start) > time.Second {
		t.Fatal("GET /v1/tip without after did not return at once")
	}
	// An empty lake long-polls until the timeout, then returns nulls.
	start = time.Now()
	code, body := httpGet(t, tl.ctrlURL+"/v1/tip?after=0")
	if code != http.StatusOK || !strings.Contains(body, `"tip":null`) || !strings.Contains(body, `"tip_committed_at":null`) {
		t.Fatalf("long-poll on an empty lake: %d %s", code, body)
	}
	if d := time.Since(start); d < 200*time.Millisecond {
		t.Fatalf("long-poll returned after %v, before the timeout", d)
	}
	mustPut(t, tl.s3URL, 100, 100)
	// after >= tip: the long-poll times out with the current values.
	code, body = httpGet(t, tl.ctrlURL+"/v1/tip?after=100")
	if code != http.StatusOK || !strings.Contains(body, `"tip":100`) || !strings.Contains(body, `"oldest":100`) {
		t.Fatalf("timed-out long-poll: %d %s", code, body)
	}
	for _, bad := range []string{"-1", "x", "4294967296", ""} {
		if code, _ := httpGet(t, tl.ctrlURL+"/v1/tip?after="+bad); code != http.StatusBadRequest {
			t.Fatalf("after=%q: %d, want 400", bad, code)
		}
	}
	if code, _ := httpGet(t, tl.ctrlURL+"/healthz"); code != http.StatusOK {
		t.Fatalf("healthz %d", code)
	}
}

// Shutdown releases every long-poll at once with the current values, then
// stops; READY=1 is sent once both listeners accept connections (checked in
// the Notify hook) and STOPPING=1 at shutdown.
func TestShutdownReleasesLongPolls(t *testing.T) {
	tl := startLake(t, lakeOpts{})
	mustPut(t, tl.s3URL, 300, 301)
	const n = 3
	type result struct {
		code int
		body string
		at   time.Time
	}
	got := make(chan result, n)
	for range n {
		go func() {
			resp, err := http.Get(tl.ctrlURL + "/v1/tip?after=301")
			if err != nil {
				got <- result{code: -1, body: err.Error(), at: time.Now()}
				return
			}
			defer resp.Body.Close()
			b, _ := io.ReadAll(resp.Body)
			got <- result{resp.StatusCode, string(b), time.Now()}
		}()
	}
	time.Sleep(100 * time.Millisecond)
	stopAt := time.Now()
	stopErr := make(chan error, 1)
	go func() { stopErr <- tl.stop() }()
	for range n {
		select {
		case r := <-got:
			if r.code != http.StatusOK || !strings.Contains(r.body, `"tip":301`) {
				t.Fatalf("released long-poll: %d %s", r.code, r.body)
			}
			if d := r.at.Sub(stopAt); d > 500*time.Millisecond {
				t.Fatalf("long-poll released %v after shutdown began", d)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("shutdown did not release a long-poll")
		}
	}
	if err := <-stopErr; err != nil {
		t.Fatalf("Serve: %v", err)
	}
	notes := tl.notifications()
	if len(notes) != 2 || notes[0] != "READY=1" || notes[1] != "STOPPING=1" {
		t.Fatalf("sd_notify states %v, want [READY=1 STOPPING=1]", notes)
	}
	if _, err := net.DialTimeout("tcp", tl.cfg.ControlListen, time.Second); err == nil {
		t.Fatal("control listener still accepting after shutdown")
	}
}

// The private metrics listener is best-effort: READY does not wait for it,
// it binds once the address frees up, and it never serves /v1/tip.
func TestPrivateMetricsListener(t *testing.T) {
	dir := newLakeDir(t)
	cfg := testLakeConfig(t, dir)
	blocker, err := net.Listen("tcp", "127.0.0.1:0") // the "interface" is not up yet
	if err != nil {
		t.Fatal(err)
	}
	cfg.MetricsListen = blocker.Addr().String()
	tl := startLake(t, lakeOpts{dir: dir, cfg: &cfg})
	if !strings.Contains(tl.logs.String(), "cannot bind the private metrics listener yet") {
		t.Fatal("expected a warning about the unbound private listener")
	}
	blocker.Close()
	priv := "http://" + cfg.MetricsListen
	waitFor(t, 10*time.Second, func() bool {
		resp, err := http.Get(priv + "/healthz")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	})
	if _, ok := metricValue(t, priv, `lake_fsync_failed`); !ok {
		t.Fatal("private listener does not serve /metrics")
	}
	if code, _ := httpGet(t, priv+"/v1/tip"); code != http.StatusNotFound {
		t.Fatalf("private listener serves /v1/tip (%d); it must stay loopback-only", code)
	}
}

// ------------------------------------------------------------------ helpers

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("condition not met within %v", timeout)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// startRawPut sends a SigV4-signed UNSIGNED-PAYLOAD PUT declaring total
// bytes but sends only send bytes, leaving the connection open.
func startRawPut(t *testing.T, addr, key string, total, send int) net.Conn {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, "http://"+addr+"/"+testBucket+"/"+key, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = int64(total)
	req.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	req.Header.Set("If-None-Match", "*")
	signer := v4.NewSigner()
	if err := signer.SignHTTP(context.Background(), aws.Credentials{AccessKeyID: testAccessKey, SecretAccessKey: testSecretKey},
		req, "UNSIGNED-PAYLOAD", "s3", "us-east-1", time.Now()); err != nil {
		t.Fatal(err)
	}
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	var hdr bytes.Buffer
	fmt.Fprintf(&hdr, "PUT %s HTTP/1.1\r\nHost: %s\r\nContent-Length: %d\r\n", req.URL.RequestURI(), addr, total)
	for k, vs := range req.Header {
		for _, v := range vs {
			fmt.Fprintf(&hdr, "%s: %s\r\n", k, v)
		}
	}
	hdr.WriteString("\r\n")
	if _, err := conn.Write(hdr.Bytes()); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(bytes.Repeat([]byte{0x42}, send)); err != nil {
		t.Fatal(err)
	}
	return conn
}

// cutProxy forwards HTTP/1.1 traffic to the lake but cuts both connections
// halfway through the body of every ledger PUT.
type cutProxy struct {
	url string
	mu  sync.Mutex
	n   int
}

func (p *cutProxy) cuts() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.n
}

func newCutProxy(t *testing.T, target string) *cutProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	p := &cutProxy{url: "http://" + ln.Addr().String()}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go p.handle(c, target)
		}
	}()
	return p
}

func (p *cutProxy) handle(client net.Conn, target string) {
	defer client.Close()
	server, err := net.Dial("tcp", target)
	if err != nil {
		return
	}
	defer server.Close()
	go func() { _, _ = io.Copy(client, server) }()
	br := bufio.NewReader(client)
	for {
		var head bytes.Buffer
		contentLength := 0
		reqLine := ""
		for {
			line, err := br.ReadString('\n')
			if err != nil {
				return
			}
			head.WriteString(line)
			if reqLine == "" {
				reqLine = line
			}
			if k, v, ok := strings.Cut(strings.TrimSpace(line), ":"); ok && strings.EqualFold(k, "Content-Length") {
				contentLength, _ = strconv.Atoi(strings.TrimSpace(v))
			}
			if line == "\r\n" {
				break
			}
		}
		if _, err := server.Write(head.Bytes()); err != nil {
			return
		}
		cut := strings.HasPrefix(reqLine, "PUT ") && strings.Contains(reqLine, ".xdr.zst")
		n := contentLength
		if cut {
			n = contentLength / 2
		}
		if _, err := io.CopyN(server, br, int64(n)); err != nil {
			return
		}
		if cut {
			p.mu.Lock()
			p.n++
			p.mu.Unlock()
			time.Sleep(50 * time.Millisecond) // the lake has read half the body
			return                            // closes both connections
		}
	}
}
