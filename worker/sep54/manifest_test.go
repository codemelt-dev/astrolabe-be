package sep54_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/support/datastore"

	"stellar-explorer/worker/sep54"
)

// The manifest Galexie writes for our layout, identical to the AWS
// Open Data lake's .config.json.
const pubnetManifest = `{"networkPassphrase":"Public Global Stellar Network ; September 2015","version":"1.0","compression":"zstd","ledgersPerBatch":1,"batchesPerPartition":64000}`

func TestManifestBytesAreGalexies(t *testing.T) {
	if got := string(sep54.ManifestBytes(network.PublicNetworkPassphrase)); got != pubnetManifest {
		t.Fatalf("ManifestBytes = %s", got)
	}
	// PublishConfig with Galexie's config writes exactly these bytes.
	dir := t.TempDir()
	ds, err := datastore.NewFilesystemDataStoreWithPath(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, created, err := datastore.PublishConfig(context.Background(), ds, sep54.GalexieConfig(network.PublicNetworkPassphrase)); err != nil || !created {
		t.Fatalf("PublishConfig = %v, %v", created, err)
	}
	b, err := os.ReadFile(filepath.Join(dir, sep54.ManifestFile))
	if err != nil || string(b) != pubnetManifest {
		t.Fatalf("published %q, %v", b, err)
	}
}

func writeStoreFile(t *testing.T, dir, rel string, data []byte) {
	t.Helper()
	p := filepath.Join(dir, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestLoadSchema(t *testing.T) {
	ctx := context.Background()
	pub := network.PublicNetworkPassphrase
	store := func(t *testing.T, manifest string, files ...string) datastore.DataStore {
		dir := t.TempDir()
		if manifest != "" {
			writeStoreFile(t, dir, sep54.ManifestFile, []byte(manifest))
		}
		for _, f := range files {
			writeStoreFile(t, dir, f, []byte("x"))
		}
		ds, err := datastore.NewFilesystemDataStoreWithPath(dir)
		if err != nil {
			t.Fatal(err)
		}
		return ds
	}

	t.Run("no manifest yet", func(t *testing.T) {
		schema, err := sep54.LoadSchema(ctx, store(t, ""), pub)
		if err != nil || schema.LedgersPerFile != 1 || schema.FilesPerPartition != 64000 || schema.FileExtension != "" {
			t.Fatalf("LoadSchema = %+v, %v", schema, err)
		}
	})
	t.Run("Galexie's manifest and ledger files", func(t *testing.T) {
		schema, err := sep54.LoadSchema(ctx, store(t, pubnetManifest, sep54.ObjectPath(123456)), pub)
		if err != nil || schema != (datastore.DataStoreSchema{LedgersPerFile: 1, FilesPerPartition: 64000, FileExtension: "zst"}) {
			t.Fatalf("LoadSchema = %+v, %v", schema, err)
		}
	})
	t.Run("why Compression stays empty", func(t *testing.T) {
		// The SDK compressor is "zst" but the manifest says "zstd": a
		// non-empty Compression is a ConfigMismatchError.
		_, err := datastore.LoadSchema(ctx, store(t, pubnetManifest), datastore.DataStoreConfig{
			Schema: sep54.Schema, NetworkPassphrase: pub, Compression: "zst"})
		var mismatch *datastore.ConfigMismatchError
		if !errors.As(err, &mismatch) {
			t.Fatalf("Compression %q: %v", "zst", err)
		}
	})
	t.Run("wrong network", func(t *testing.T) {
		testnet := strings.Replace(pubnetManifest, pub, network.TestNetworkPassphrase, 1)
		_, err := sep54.LoadSchema(ctx, store(t, testnet), pub)
		var mismatch *datastore.ConfigMismatchError
		if !errors.As(err, &mismatch) || !strings.Contains(err.Error(), "networkPassphrase") {
			t.Fatalf("testnet manifest: %v", err)
		}
	})
	t.Run("wrong layout", func(t *testing.T) {
		m := strings.Replace(pubnetManifest, `"ledgersPerBatch":1`, `"ledgersPerBatch":64`, 1)
		if _, err := sep54.LoadSchema(ctx, store(t, m), pub); err == nil {
			t.Fatal("a 64-ledgers-per-file manifest was accepted")
		}
	})
	t.Run("old extension", func(t *testing.T) {
		old := strings.TrimSuffix(sep54.ObjectPath(123456), ".zst") + ".zstd"
		if _, err := sep54.LoadSchema(ctx, store(t, pubnetManifest, old), pub); err == nil || !strings.Contains(err.Error(), "zstd") {
			t.Fatalf("old .zstd files: %v", err)
		}
	})
	t.Run("garbage manifest", func(t *testing.T) {
		if _, err := sep54.LoadSchema(ctx, store(t, "{not json"), pub); err == nil {
			t.Fatal("a corrupt manifest was accepted")
		}
	})
}

func TestMetadataKeysAreGalexies(t *testing.T) {
	md := datastore.MetaData{}.ToMap()
	if len(md) != len(sep54.MetadataKeys) {
		t.Fatalf("SDK writes %d keys, we copy %d", len(md), len(sep54.MetadataKeys))
	}
	for _, k := range sep54.MetadataKeys {
		if _, ok := md[k]; !ok {
			t.Errorf("SDK metadata has no %q", k)
		}
	}
	o := sep54.Object{Key: "k", Metadata: map[string]string{"start-ledger": "1"}}
	if _, err := o.LedgerMetadata(); err == nil || !strings.Contains(err.Error(), "end-ledger") {
		t.Fatalf("LedgerMetadata with missing keys: %v", err)
	}
	full := datastore.MetaData{StartLedger: 7}.ToMap()
	full["extra"] = "dropped"
	o.Metadata = full
	got, err := o.LedgerMetadata()
	if err != nil || len(got) != 9 || got["start-ledger"] != "7" || got["extra"] != "" {
		t.Fatalf("LedgerMetadata = %v, %v", got, err)
	}
}
