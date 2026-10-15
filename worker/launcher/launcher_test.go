package launcher_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"

	"stellar-explorer/worker/bootstrap"
	"stellar-explorer/worker/config"
	"stellar-explorer/worker/devfeed"
	"stellar-explorer/worker/lake/laketest"
	"stellar-explorer/worker/launcher"
	"stellar-explorer/worker/sep54"
	"stellar-explorer/worker/sep54/sep54test"
)

func TestMain(m *testing.M) { laketest.Main(m) }

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func u32(v uint32) *uint32 { return &v }

// Decide's truth table (the package comment).
func TestDecide(t *testing.T) {
	origin := bootstrap.NewOrigin(network.PublicNetworkPassphrase, 999, xdr.Hash{1}) // archive_origin 1000
	for _, tc := range []struct {
		name    string
		in      launcher.Inputs
		want    uint32
		wantErr error
	}{
		{"lake data, archive behind", launcher.Inputs{LakeTip: u32(5000), Archive: launcher.ArchiveKnown, ArchiveTip: 4000}, 5001, nil},
		{"lake data, archive ahead (lake lost its newest)", launcher.Inputs{LakeTip: u32(5000), Archive: launcher.ArchiveKnown, ArchiveTip: 6000}, 6001, nil},
		{"lake data, archive unknown: never waits on the archive", launcher.Inputs{LakeTip: u32(5000), Archive: launcher.ArchiveUnknown}, 5001, nil},
		{"lake data, archive empty", launcher.Inputs{LakeTip: u32(5000), Archive: launcher.ArchiveEmpty, Origin: &origin}, 5001, nil},
		{"lake data, archive empty, origin ahead", launcher.Inputs{LakeTip: u32(500), Archive: launcher.ArchiveEmpty, Origin: &origin}, 1000, nil},
		{"lake data, archive disabled", launcher.Inputs{LakeTip: u32(5000), Archive: launcher.ArchiveDisabled}, 5001, nil},
		{"lake lost: resume from the archive", launcher.Inputs{Archive: launcher.ArchiveKnown, ArchiveTip: 4000}, 4001, nil},
		{"lake and archive empty: archive_origin", launcher.Inputs{Archive: launcher.ArchiveEmpty, Origin: &origin}, 1000, nil},
		{"archive disabled, empty lake: archive_origin", launcher.Inputs{Archive: launcher.ArchiveDisabled, Origin: &origin}, 1000, nil},
		{"lake and archive empty, no origin: bootstrap first", launcher.Inputs{Archive: launcher.ArchiveEmpty}, 0, launcher.ErrBootstrapFirst},
		{"archive disabled, empty lake, no origin", launcher.Inputs{Archive: launcher.ArchiveDisabled}, 0, launcher.ErrBootstrapFirst},
		{"archive unknown, empty lake: refuse", launcher.Inputs{Archive: launcher.ArchiveUnknown, Origin: &origin}, 0, launcher.ErrNoStart},
		{"archive unknown, empty lake, no origin", launcher.Inputs{Archive: launcher.ArchiveUnknown}, 0, launcher.ErrNoStart},
	} {
		got, reason, err := launcher.Decide(tc.in)
		if got != tc.want || !errors.Is(err, tc.wantErr) || (err == nil) == (reason == "") {
			t.Errorf("%s: start %d (%s), err %v; want %d, %v", tc.name, got, reason, err, tc.want, tc.wantErr)
		}
	}
}

// blackHole accepts connections and never answers: an archive outage that
// hangs rather than refuses.
func blackHole(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var conns []net.Conn
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		mu.Lock()
		for _, c := range conns {
			c.Close()
		}
		mu.Unlock()
	})
	return "http://" + ln.Addr().String()
}

const archiveBucket = "stellar-ledgers-pubnet"

// setup is a lake (with ledgers from..to, none if from = 0) under its own
// prefix, and the configuration pointing at it and at the archive bucket.
type setup struct {
	lake *laketest.Lake
	cfg  config.Config
}

func newSetup(t *testing.T, prefix string, from, to uint32) *setup {
	t.Helper()
	l := laketest.Start(t, laketest.Options{BucketPath: "stellar/" + prefix})
	if from > 0 {
		feed(t, l, from, to)
	}
	cfg := config.Default()
	cfg.Lake = l.Cfg
	cfg.Archive.Bucket = archiveBucket
	cfg.Archive.PathStyle = true
	return &setup{lake: l, cfg: cfg}
}

func feed(t *testing.T, l *laketest.Lake, from, to uint32) {
	t.Helper()
	ctx := context.Background()
	src, err := sep54.OpenLake(ctx, l.Cfg, l.Creds)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := devfeed.Run(ctx, devfeed.Options{Upstream: sep54test.NewMemSource(sep54test.Chain(from, int(to-from+1))...),
		Lake: src.DataStore(), Passphrase: network.PublicNetworkPassphrase, Start: from, End: to, Log: quiet}); err != nil {
		t.Fatal(err)
	}
}

func archiveCreds(endpoint string) *config.ArchiveCredentials {
	return &config.ArchiveCredentials{Endpoint: endpoint, Region: "us-east-1",
		AccessKey: laketest.Credentials.AccessKey, SecretKey: laketest.Credentials.SecretKey}
}

// putArchive stores ledgers from..to in the archive stand-in under prefix.
func putArchive(t *testing.T, p *laketest.Process, prefix string, from, to uint32) {
	t.Helper()
	client := sep54.NewS3Client(sep54.ArchiveClientOptions(true, *archiveCreds(p.S3Endpoint())))
	for _, o := range sep54test.Chain(from, int(to-from+1)) {
		if _, err := client.PutObject(context.Background(), &s3.PutObjectInput{
			Bucket: aws.String(archiveBucket), Key: aws.String(prefix + "/" + o.Key), Body: bytes.NewReader(o.Body),
			Metadata: o.Metadata, ChecksumAlgorithm: types.ChecksumAlgorithmCrc32c,
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func (s *setup) compute(t *testing.T, archive *config.ArchiveCredentials, probe time.Duration) (launcher.Result, error) {
	t.Helper()
	o := launcher.NewOptions(s.cfg, s.lake.Creds, archive, quiet)
	o.ProbeTimeout = probe
	return launcher.Compute(context.Background(), o)
}

// Compute against a real lake and an archive stand-in (a subprocess lake),
// through the production clients.
func TestComputeAgainstLake(t *testing.T) {
	arch := laketest.StartProcess(t, laketest.Options{BucketPath: archiveBucket + "/ledgers/pubnet"})
	creds := archiveCreds(arch.S3Endpoint())

	t.Run("lake ahead of the archive", func(t *testing.T) {
		s := newSetup(t, "case1", 20_000_000, 20_000_009)
		putArchive(t, arch, "case1", 20_000_000, 20_000_004)
		res, err := s.compute(t, creds, 0)
		if err != nil || res.Start != 20_000_010 || res.Inputs.Archive != launcher.ArchiveKnown {
			t.Fatalf("%+v %v", res, err)
		}
	})
	t.Run("archive ahead of the lake", func(t *testing.T) {
		s := newSetup(t, "case2", 21_000_000, 21_000_001)
		putArchive(t, arch, "case2", 21_000_000, 21_000_007)
		res, err := s.compute(t, creds, 0)
		if err != nil || res.Start != 21_000_008 {
			t.Fatalf("%+v %v", res, err)
		}
	})
	t.Run("lake lost: resume from the archive", func(t *testing.T) {
		s := newSetup(t, "case3", 0, 0)
		putArchive(t, arch, "case3", 22_000_000, 22_000_003)
		res, err := s.compute(t, creds, 0)
		if err != nil || res.Start != 22_000_004 {
			t.Fatalf("%+v %v", res, err)
		}
	})
	t.Run("first start: archive_origin", func(t *testing.T) {
		s := newSetup(t, "case4", 0, 0)
		if _, _, err := bootstrap.WriteOriginIfAbsent(s.lake.Dir, bootstrap.NewOrigin(network.PublicNetworkPassphrase, 23_000_063, xdr.Hash{1})); err != nil {
			t.Fatal(err)
		}
		res, err := s.compute(t, creds, 0)
		if err != nil || res.Start != 23_000_064 || res.Inputs.Archive != launcher.ArchiveEmpty {
			t.Fatalf("%+v %v", res, err)
		}
		res, err = s.compute(t, nil, 0) // archiving disabled
		if err != nil || res.Start != 23_000_064 || res.Inputs.Archive != launcher.ArchiveDisabled {
			t.Fatalf("archive disabled: %+v %v", res, err)
		}
	})
	t.Run("nothing to resume from: bootstrap first", func(t *testing.T) {
		s := newSetup(t, "case5", 0, 0)
		if _, err := s.compute(t, creds, 0); !errors.Is(err, launcher.ErrBootstrapFirst) {
			t.Fatalf("%v", err)
		}
	})
	t.Run("lake empty, archive hangs: refuse", func(t *testing.T) {
		s := newSetup(t, "case6", 0, 0)
		if _, err := s.compute(t, archiveCreds(blackHole(t)), 500*time.Millisecond); !errors.Is(err, launcher.ErrNoStart) {
			t.Fatalf("%v", err)
		}
	})
	t.Run("lake down: refuse", func(t *testing.T) {
		s := newSetup(t, "case7", 24_000_000, 24_000_001)
		if err := s.lake.Stop(); err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		_, err := s.compute(t, creds, 0)
		if err == nil || !strings.Contains(err.Error(), "does not answer") || time.Since(start) > 10*time.Second {
			t.Fatalf("%v after %s", err, time.Since(start))
		}
	})
}

// An archive outage (here: an endpoint that accepts and never
// answers) never blocks a start when the lake has data: START comes from
// the lake tip within 10 s.
func TestArchiveOutageStartsFromLakeTip(t *testing.T) {
	s := newSetup(t, "outage", 25_000_000, 25_000_004)
	start := time.Now()
	res, err := s.compute(t, archiveCreds(blackHole(t)), 0) // the production deadline
	elapsed := time.Since(start)
	if err != nil || res.Start != 25_000_005 || res.Inputs.Archive != launcher.ArchiveUnknown {
		t.Fatalf("%+v %v", res, err)
	}
	t.Logf("START=%d after %s (probe deadline %s)", res.Start, elapsed.Round(time.Millisecond), launcher.ProbeTimeout)
	if elapsed >= 10*time.Second {
		t.Fatalf("took %s, want less than 10 s", elapsed)
	}
}

func TestWriteEnv(t *testing.T) {
	path := filepath.Join(t.TempDir(), "galexie.env")
	for _, start := range []uint32{123, 64_000_001} {
		if err := launcher.WriteEnv(path, start); err != nil {
			t.Fatal(err)
		}
		b, err := os.ReadFile(path)
		if err != nil || string(b) != "START="+strconv.FormatUint(uint64(start), 10)+"\n" {
			t.Fatalf("%q %v", b, err)
		}
	}
	entries, _ := os.ReadDir(filepath.Dir(path))
	if len(entries) != 1 {
		t.Fatalf("temporary files left: %v", entries)
	}
}
