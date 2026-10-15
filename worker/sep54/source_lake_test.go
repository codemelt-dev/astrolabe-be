package sep54_test

import (
	"bytes"
	"context"
	"errors"
	"maps"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/support/datastore"

	"stellar-explorer/worker/lake/laketest"
	"stellar-explorer/worker/sep54"
	"stellar-explorer/worker/sep54/sep54test"
)

// The lake source and the tip client against a real lake: Galexie's upload
// path in, our reads, NotFound and unavailability classification out.
func TestLakeSourceAndTipClient(t *testing.T) {
	ctx := context.Background()
	l := laketest.Start(t, laketest.Options{LongPoll: 2 * time.Second})
	src, err := sep54.OpenLake(ctx, l.Cfg, l.Creds)
	if err != nil {
		t.Fatal(err)
	}
	if src.Name() != sep54.SourceLake || src.Bucket() != "stellar" || src.Prefix() != "ledgers/pubnet" {
		t.Fatalf("source %s %s/%s", src.Name(), src.Bucket(), src.Prefix())
	}
	if _, err := src.Newest(ctx); !errors.Is(err, sep54.ErrNoLedgers) {
		t.Fatalf("empty lake Newest: %v", err)
	}
	if _, err := src.Oldest(ctx); !errors.Is(err, sep54.ErrNoLedgers) {
		t.Fatalf("empty lake Oldest: %v", err)
	}
	if _, err := src.LoadSchema(ctx, network.PublicNetworkPassphrase); err != nil {
		t.Fatalf("LoadSchema before Galexie's manifest: %v", err)
	}
	tc := sep54.NewTipClient(l.ControlAddr())
	if tip, err := tc.Get(ctx); err != nil || tip.Tip != nil {
		t.Fatalf("empty tip: %+v %v", tip, err)
	}

	// Galexie's path: manifest, then PutFileIfNotExists per ledger, across a
	// partition boundary.
	ds := src.DataStore()
	if _, _, err := datastore.PublishConfig(ctx, ds, sep54.GalexieConfig(network.PublicNetworkPassphrase)); err != nil {
		t.Fatal(err)
	}
	objs := sep54test.Chain(63_998, 6) // 63998 … 64003
	for _, o := range objs[:5] {
		if up, err := ds.PutFileIfNotExists(ctx, o.Key, bytes.NewReader(o.Body), o.Metadata); err != nil || !up {
			t.Fatalf("put %s: %v %v", o.Key, up, err)
		}
	}
	if _, err := src.LoadSchema(ctx, network.PublicNetworkPassphrase); err != nil {
		t.Fatalf("LoadSchema: %v", err)
	}
	if _, err := src.LoadSchema(ctx, network.TestNetworkPassphrase); err == nil {
		t.Fatal("LoadSchema accepted the wrong network")
	}
	if newest, err := src.Newest(ctx); err != nil || newest != 64_002 {
		t.Fatalf("Newest = %d, %v", newest, err)
	}
	if newest, at, err := src.NewestObject(ctx); err != nil || newest != 64_002 || time.Since(at) > time.Minute {
		t.Fatalf("NewestObject = %d, %s, %v", newest, at, err)
	}
	// The oldest ledger sits in the other partition (63998 < 64000).
	if oldest, err := src.Oldest(ctx); err != nil || oldest != 63_998 {
		t.Fatalf("Oldest = %d, %v", oldest, err)
	}
	m, err := src.GetObject(ctx, sep54.ManifestFile)
	if err != nil || !bytes.Equal(m.Body, sep54.ManifestBytes(network.PublicNetworkPassphrase)) {
		t.Fatalf("manifest %q %v", m.Body, err)
	}

	var prev sep54.Header
	for i, want := range objs[:5] {
		seq := uint32(63_998 + i)
		got, err := src.GetLedger(ctx, seq)
		if err != nil {
			t.Fatalf("GetLedger(%d): %v", seq, err)
		}
		if !bytes.Equal(got.Body, want.Body) || !maps.Equal(got.Metadata, want.Metadata) || got.ETag == "" || got.Key != want.Key {
			t.Fatalf("ledger %d: bytes, metadata or ETag differ: %+v", seq, got.Metadata)
		}
		h, err := sep54.CheckObject(seq, got)
		if err != nil {
			t.Fatalf("CheckObject(%d): %v (stored CRC32C %q)", seq, err, got.ChecksumCRC32C)
		}
		if _, full, err := sep54.DecodeLedger(seq, got.Body); err != nil || full != h {
			t.Fatalf("DecodeLedger(%d) = %+v, %v", seq, full, err)
		}
		if i > 0 {
			if err := sep54.CheckChain(prev, h); err != nil {
				t.Fatal(err)
			}
		}
		prev = h
	}
	_, err = src.GetLedger(ctx, 99)
	if !errors.Is(err, sep54.ErrNotFound) || !errors.Is(err, os.ErrNotExist) || sep54.IsUnavailable(err) {
		t.Fatalf("missing ledger: %v", err)
	}

	// The tip client sees the commit tip and wakes on the next commit.
	tip, err := tc.Get(ctx)
	if err != nil || *tip.Tip != 64_002 || *tip.Oldest != 63_998 || tip.TipCommittedAt == nil {
		t.Fatalf("tip %+v %v", tip, err)
	}
	woke := make(chan sep54.Tip, 1)
	go func() {
		tip, err := tc.WaitAbove(ctx, 64_002, time.Minute)
		if err != nil {
			t.Error(err)
		}
		woke <- tip
	}()
	time.Sleep(50 * time.Millisecond)
	last := objs[5]
	if _, err := ds.PutFileIfNotExists(ctx, last.Key, bytes.NewReader(last.Body), last.Metadata); err != nil {
		t.Fatal(err)
	}
	select {
	case tip := <-woke:
		if *tip.Tip != 64_003 {
			t.Fatalf("woke with %+v", tip)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("WaitAbove did not wake on the commit")
	}

	// Wrong keys: an answer, not unavailability.
	bad := l.Creds
	bad.SecretKey = "wrong-secret-0123456789"
	if _, err := sep54.OpenLake(ctx, l.Cfg, bad); err == nil || sep54.IsUnavailable(err) {
		t.Fatalf("wrong secret: %v", err)
	}

	// A stopped lake is unavailable, never NotFound. (One SDK attempt keeps
	// the test fast; the production lake client makes the default 3.)
	fast, err := sep54.Open(ctx, sep54.SourceLake, sep54.NewS3Client(sep54.ClientOptions{
		Endpoint:         l.S3Endpoint(),
		Region:           l.Cfg.Region,
		Credentials:      credentials.NewStaticCredentialsProvider(l.Creds.AccessKey, l.Creds.SecretKey, ""),
		PathStyle:        true,
		RetryMaxAttempts: 1,
	}), l.Cfg.BucketPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Stop(); err != nil {
		t.Fatal(err)
	}
	_, err = fast.GetLedger(ctx, 64_000)
	if !sep54.IsUnavailable(err) || errors.Is(err, sep54.ErrNotFound) {
		t.Fatalf("GetLedger on a stopped lake: %v", err)
	}
	if _, err := fast.Newest(ctx); !sep54.IsUnavailable(err) {
		t.Fatalf("Newest on a stopped lake: %v", err)
	}
	if _, err := tc.Get(ctx); !sep54.IsUnavailable(err) {
		t.Fatalf("tip on a stopped lake: %v", err)
	}
	if _, err := sep54.OpenLake(ctx, l.Cfg, l.Creds); !sep54.IsUnavailable(err) {
		t.Fatalf("OpenLake on a stopped lake: %v", err)
	}
}
