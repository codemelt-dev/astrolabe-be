package lake

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"

	"stellar-explorer/worker/sep54"
)

func TestOpenRefusesWithoutVolume(t *testing.T) {
	t.Run("missing marker", func(t *testing.T) {
		dir := t.TempDir()
		_, err := openLake(t, lakeOpts{dir: dir})
		if err == nil || !strings.Contains(err.Error(), "marker") {
			t.Fatalf("Open without a marker: %v", err)
		}
		if _, err := os.Stat(filepath.Join(dir, "s3")); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("Open wrote to an unmounted lake dir")
		}
	})
	t.Run("empty marker", func(t *testing.T) {
		dir := newLakeDir(t)
		if err := os.WriteFile(filepath.Join(dir, "state", markerFile), nil, 0o640); err != nil {
			t.Fatal(err)
		}
		if _, err := openLake(t, lakeOpts{dir: dir}); err == nil || !strings.Contains(err.Error(), "non-empty") {
			t.Fatalf("Open with an empty marker: %v", err)
		}
	})
	t.Run("not a mountpoint", func(t *testing.T) {
		dir := newLakeDir(t)
		cfg := testLakeConfig(t, dir)
		cfg.AllowUnmounted = false
		if _, err := openLake(t, lakeOpts{dir: dir, cfg: &cfg}); err == nil || !strings.Contains(err.Error(), "not a mountpoint") {
			t.Fatalf("Open on a plain directory: %v", err)
		}
	})
}

func TestIsMountpoint(t *testing.T) {
	if ok, err := isMountpoint("/"); err != nil || !ok {
		t.Fatalf("isMountpoint(/) = %v, %v", ok, err)
	}
	if ok, err := isMountpoint(t.TempDir()); err != nil || ok {
		t.Fatalf("isMountpoint(tempdir) = %v, %v", ok, err)
	}
}

// The startup syncfs always runs, and an error is fatal.
func TestStartupSyncfs(t *testing.T) {
	var calls []string
	tl := startLake(t, lakeOpts{syncfs: func(dir string) error { calls = append(calls, dir); return syncFilesystem(dir) }})
	if len(calls) != 1 || calls[0] != tl.dir {
		t.Fatalf("syncfs calls %v, want one on %s", calls, tl.dir)
	}
	if _, ok := metricValue(t, tl.ctrlURL, "lake_startup_syncfs_seconds"); !ok {
		t.Fatal("lake_startup_syncfs_seconds missing")
	}
	mustPut(t, tl.s3URL, 10, 12)
	if err := tl.stop(); err != nil {
		t.Fatal(err)
	}

	// Same boot: syncfs again, no scan.
	calls = nil
	tl = startLake(t, lakeOpts{dir: tl.dir, syncfs: func(dir string) error { calls = append(calls, dir); return nil }})
	if len(calls) != 1 {
		t.Fatalf("restart ran syncfs %d times", len(calls))
	}
	requireMetric(t, tl.ctrlURL, "lake_startup_scan_checked_objects", 0)
	requireTip(t, tl.ctrlURL, 12, 10)
	if err := tl.stop(); err != nil {
		t.Fatal(err)
	}

	_, err := openLake(t, lakeOpts{dir: tl.dir, syncfs: func(string) error { return syscall.EIO }})
	if err == nil || !errors.Is(err, syscall.EIO) || !strings.Contains(err.Error(), "syncfs") {
		t.Fatalf("Open with a failing syncfs: %v", err)
	}
}

func TestSyncFilesystem(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "a", "b"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "a", "b", "f"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := syncFilesystem(dir); err != nil {
		t.Fatal(err)
	}
	if err := syncFilesystem(filepath.Join(dir, "missing")); err == nil {
		t.Fatal("syncFilesystem of a missing dir succeeded")
	}
}

// After a reboot (new boot id) the newest objects are checksum-verified and
// every corrupt one is deleted; a corrupt one below a good one is a hole.
// On the same boot the scan is skipped.
func TestRebootChecksumScan(t *testing.T) {
	tl := startLake(t, lakeOpts{bootID: "boot-1"})
	requireMetric(t, tl.ctrlURL, "lake_startup_scan_checked_objects", 0) // first start: empty lake
	mustPut(t, tl.s3URL, 9000, 9005)
	if err := tl.stop(); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(filepath.Join(tl.dir, "state", "boot-id")); string(b) != "boot-1\n" {
		t.Fatalf("state/boot-id = %q", b)
	}

	// Corrupt a suffix object (truncated), and two objects below good ones:
	// one with a flipped byte, one emptied (xattrs intact, data lost).
	truncate(t, tl.objectPath(9005))
	flipByte(t, tl.objectPath(9003))
	if err := os.Truncate(tl.objectPath(9001), 0); err != nil {
		t.Fatal(err)
	}

	// Same boot: no scan, nothing deleted.
	tl = startLake(t, lakeOpts{dir: tl.dir, bootID: "boot-1"})
	requireMetric(t, tl.ctrlURL, "lake_startup_scan_checked_objects", 0)
	requireTip(t, tl.ctrlURL, 9005, 9000)
	if err := tl.stop(); err != nil {
		t.Fatal(err)
	}

	// New boot: scan.
	tl = startLake(t, lakeOpts{dir: tl.dir, bootID: "boot-2"})
	requireMetric(t, tl.ctrlURL, "lake_startup_scan_checked_objects", 6)
	requireMetric(t, tl.ctrlURL, "lake_startup_scan_deleted_objects", 3)
	requireMetric(t, tl.ctrlURL, "lake_startup_scan_holes", 2)
	for _, seq := range []uint32{9005, 9003, 9001} {
		requireAbsent(t, tl, seq)
	}
	for _, seq := range []uint32{9004, 9002, 9000} {
		if g := getObject(t, tl.s3URL, sep54.ObjectPath(seq)); g.Err != "" {
			t.Fatalf("good ledger %d: %s", seq, g.Err)
		}
	}
	requireTip(t, tl.ctrlURL, 9004, 9000) // the corrupt suffix is gone
	logs := tl.logs.String()
	for _, want := range []string{"ledger=9005", "ledger=9003", "ledger=9001", "this is a hole"} {
		if !strings.Contains(logs, want) {
			t.Fatalf("scan log lacks %q", want)
		}
	}
	if b, _ := os.ReadFile(filepath.Join(tl.dir, "state", "boot-id")); string(b) != "boot-2\n" {
		t.Fatalf("state/boot-id = %q after the scan", b)
	}
	// Galexie resumes at newest+1 and re-exports the deleted suffix.
	mustPut(t, tl.s3URL, 9005, 9005)
	requireTip(t, tl.ctrlURL, 9005, 9000)
}

// The scan only looks at the newest K objects.
func TestRebootScanIsBoundedToNewest(t *testing.T) {
	tl := startLake(t, lakeOpts{bootID: "a"})
	mustPut(t, tl.s3URL, 20000, 20000+scanObjects+4) // K+5 objects
	if err := tl.stop(); err != nil {
		t.Fatal(err)
	}
	flipByte(t, tl.objectPath(20000)) // older than the newest K: not scanned
	tl = startLake(t, lakeOpts{dir: tl.dir, bootID: "b"})
	requireMetric(t, tl.ctrlURL, "lake_startup_scan_checked_objects", scanObjects)
	requireMetric(t, tl.ctrlURL, "lake_startup_scan_deleted_objects", 0)
	requireTip(t, tl.ctrlURL, 20000+scanObjects+4, 20000)
}

func TestVerifyObject(t *testing.T) {
	var buf bytes.Buffer
	enc, err := zstd.NewWriter(&buf) // content checksums on, like compressxdr
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("ledger close meta "), 5000)
	if _, err := enc.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	good := buf.Bytes()
	dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	defer dec.Close()

	crc := func(b []byte) string {
		var s [4]byte
		binary.BigEndian.PutUint32(s[:], crc32.Checksum(b, castagnoli))
		return base64.StdEncoding.EncodeToString(s[:])
	}
	badChecksum := append([]byte(nil), good...)
	badChecksum[len(badChecksum)-1] ^= 0xff // the frame's content checksum
	truncated := good[:len(good)/2]
	trailing := append(append([]byte(nil), good...), 0, 0, 0, 0)

	cases := []struct {
		name, stored string
		data         []byte
		want         string
	}{
		{"good", crc(good), good, ""},
		{"no stored checksum", "", good, "no stored CRC32C"},
		{"CRC32C mismatch", crc(good[1:]), good, "CRC32C mismatch"},
		{"zstd frame checksum (CRC32C matches)", crc(badChecksum), badChecksum, "zstd"},
		{"truncated (CRC32C matches)", crc(truncated), truncated, "zstd"},
		{"trailing garbage (CRC32C matches)", crc(trailing), trailing, "zstd"},
		{"empty", crc(nil), nil, "empty"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := verifyObject(bytes.NewReader(c.data), c.stored, dec)
			if (c.want == "") != (got == "") || !strings.Contains(got, c.want) {
				t.Fatalf("verifyObject = %q, want %q", got, c.want)
			}
		})
	}
}

// The lake is SIGKILLed after linking a ledger but before any fsync of it.
// The restart runs syncfs; Galexie's retried PUT gets a 412 and the tip
// covers the ledger; the next ledger goes through normally.
func TestLakeKilledBetweenLinkAndSync(t *testing.T) {
	dir := newLakeDir(t)
	cfgPath, cfg := writeLakeConfig(t, dir)
	s3URL, ctrlURL := "http://"+cfg.S3Listen, "http://"+cfg.ControlListen
	objPath := filepath.Join(dir, "s3", testBucket, testPrefix, sep54.PartitionName(5002), sep54.FileName(5002))

	p := startLakeProcess(t, cfgPath, "boot-x", envHelperKillAt+"=5002")
	mustPut(t, s3URL, 5000, 5001)
	r := putOne(t, s3URL, 5002)
	if r.Err == "" {
		t.Fatalf("put 5002 succeeded although the lake was killed: %+v", r)
	}
	err := p.wait(t, 30*time.Second)
	var ee interface{ Sys() any }
	if err == nil || !errors.As(err, &ee) || !strings.Contains(err.Error(), "killed") {
		t.Fatalf("lake process exit: %v, want killed", err)
	}
	if _, err := os.Stat(objPath); err != nil {
		t.Fatalf("ledger 5002 was not linked before the kill: %v", err)
	}

	p = startLakeProcess(t, cfgPath, "boot-x")
	if !strings.Contains(p.stderr.String(), "startup syncfs complete") {
		t.Fatal("the restarted lake did not run the startup syncfs")
	}
	if _, ok := metricValue(t, ctrlURL, "lake_startup_syncfs_seconds"); !ok {
		t.Fatal("lake_startup_syncfs_seconds missing after restart")
	}
	requireMetric(t, ctrlURL, "lake_startup_scan_checked_objects", 0) // same boot: no scan
	r = putOne(t, s3URL, 5002)
	if r.Err != "" || r.Uploaded {
		t.Fatalf("retried put 5002: uploaded=%v err=%q; want (false, nil) from a 412", r.Uploaded, r.Err)
	}
	requireMetric(t, ctrlURL, `lake_puts_total{result="exists"}`, 1)
	requireTip(t, ctrlURL, 5002, 5000)
	mustPut(t, s3URL, 5003, 5003)
	requireTip(t, ctrlURL, 5003, 5000)

	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	p.notify.await(t, "STOPPING=1", 10*time.Second, p.done)
	if err := p.wait(t, 30*time.Second); err != nil {
		t.Fatalf("lake process after SIGTERM: %v", err)
	}
}

func truncate(t *testing.T, path string) {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, fi.Size()/2); err != nil {
		t.Fatal(err)
	}
}

// flipByte corrupts one byte in the middle of path, keeping its inode (and
// so its xattrs).
func flipByte(t *testing.T, path string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 1)
	off := fi.Size() / 2
	if _, err := f.ReadAt(b, off); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0xff
	if _, err := f.WriteAt(b, off); err != nil {
		t.Fatal(err)
	}
}

// A lake that cannot bind its ports fails Serve without sending READY, so
// systemd restarts it (and Galexie's launcher keeps refusing).
func TestServeFailsWhenPortsAreTaken(t *testing.T) {
	for _, which := range []string{"s3", "control"} {
		t.Run(which, func(t *testing.T) {
			dir := newLakeDir(t)
			cfg := testLakeConfig(t, dir)
			addr := cfg.S3Listen
			if which == "control" {
				addr = cfg.ControlListen
			}
			busy, err := net.Listen("tcp", addr)
			if err != nil {
				t.Fatal(err)
			}
			defer busy.Close()
			tl, err := openLake(t, lakeOpts{dir: dir, cfg: &cfg})
			if err != nil {
				t.Fatal(err)
			}
			err = tl.l.Serve(context.Background())
			_ = os.Chdir(testWD)
			if err == nil || !strings.Contains(err.Error(), addr) {
				t.Fatalf("Serve with %s taken: %v", addr, err)
			}
			for _, n := range tl.notifications() {
				if n == "READY=1" {
					t.Fatal("READY sent although a port could not be bound")
				}
			}
		})
	}
}
