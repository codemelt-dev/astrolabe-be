package sep54

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/stellar/go-stellar-sdk/support/datastore"
)

const (
	// ManifestFile is the store manifest, at <prefix>/.config.json.
	ManifestFile = ".config.json"
	// ManifestCompression is what Galexie records in the manifest. The SDK
	// compressor (and the key extension) is called "zst" instead, which is
	// why readers must leave DataStoreConfig.Compression empty.
	ManifestCompression = "zstd"
)

// Schema is our fixed layout, the same as the AWS public lake.
var Schema = datastore.DataStoreSchema{LedgersPerFile: LedgersPerFile, FilesPerPartition: FilesPerPartition}

// GalexieConfig is the datastore config Galexie itself runs with for our
// layout, minus the S3 params. PublishConfig with it writes Galexie's exact
// manifest bytes; dev-feed uses it to behave like Galexie.
func GalexieConfig(passphrase string) datastore.DataStoreConfig {
	return datastore.DataStoreConfig{
		Type:              "S3",
		Schema:            Schema,
		NetworkPassphrase: passphrase,
		Compression:       ManifestCompression,
	}
}

// ManifestBytes returns the manifest Galexie publishes for our layout on the
// given network, byte for byte. For pubnet it equals the AWS
// public lake's .config.json.
func ManifestBytes(passphrase string) []byte {
	b, err := json.Marshal(datastore.DatastoreManifest{
		NetworkPassphrase: passphrase,
		Version:           datastore.Version,
		Compression:       ManifestCompression,
		LedgersPerFile:    LedgersPerFile,
		FilesPerPartition: FilesPerPartition,
	})
	if err != nil {
		panic(err) // a struct of strings and integers always marshals
	}
	return b
}

// LoadSchema reads a store's manifest with the SDK (datastore.LoadSchema)
// and checks it describes our layout on the given network:
// Compression is left empty (the manifest says "zstd", the SDK says "zst";
// a non-empty mismatch is a ConfigMismatchError) and NetworkPassphrase is
// set, which gives a free network check. A store without a manifest (a lake
// before Galexie's first run) yields our schema. Existing ledger files must
// use our extension.
func LoadSchema(ctx context.Context, ds datastore.DataStore, passphrase string) (datastore.DataStoreSchema, error) {
	schema, err := datastore.LoadSchema(ctx, ds, datastore.DataStoreConfig{
		Schema:            Schema,
		NetworkPassphrase: passphrase,
	})
	if err != nil {
		return datastore.DataStoreSchema{}, err
	}
	if schema.LedgersPerFile != LedgersPerFile || schema.FilesPerPartition != FilesPerPartition {
		return datastore.DataStoreSchema{}, fmt.Errorf("store layout is %d ledgers per file, %d files per partition; want %d and %d",
			schema.LedgersPerFile, schema.FilesPerPartition, LedgersPerFile, FilesPerPartition)
	}
	if schema.FileExtension != "" && schema.FileExtension != FileExtension {
		return datastore.DataStoreSchema{}, fmt.Errorf("store holds ledger files with extension %q; want %q", schema.FileExtension, FileExtension)
	}
	return schema, nil
}
