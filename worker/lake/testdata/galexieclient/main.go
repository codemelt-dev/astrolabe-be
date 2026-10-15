// Command galexieclient uploads to the lake exactly the way Galexie does:
// this module's go.mod/go.sum are Galexie's own (stellar-galexie c927ffc:
// go-stellar-sdk v0.7.2, aws-sdk-go-v2 v1.41.5, s3 v1.97.3), and the calls
// mirror internal/app.go (initDataStore, PublishConfig) and
// internal/uploader.go (PutFileIfNotExists of a compressxdr encoder with
// datastore.MetaData). Credentials come from AWS_* variables, like Galexie's
// container gets them from `docker run --env-file gateway.env`.
//
// The lake tests build and run it as a subprocess. Every command prints JSON
// on stdout.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"sync"

	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/support/compressxdr"
	"github.com/stellar/go-stellar-sdk/support/datastore"
	"github.com/stellar/go-stellar-sdk/xdr"
)

const (
	compressionType = "zstd" // Galexie internal/config.go
	coreVersion     = "stellar-core 29.0.0 (galexieclient test)"
	galexieVersion  = "galexieclient"
	baseCloseTime   = 1_759_190_400
)

type common struct {
	endpoint, bucketPath, region, passphrase string
}

func main() {
	if len(os.Args) < 2 {
		fail(errors.New("usage: galexieclient <init|put|race|put-bytes|head|get> [flags]"))
	}
	cmd, args := os.Args[1], os.Args[2:]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	var c common
	fs.StringVar(&c.endpoint, "endpoint", "http://127.0.0.1:9000", "lake S3 endpoint")
	fs.StringVar(&c.bucketPath, "bucket-path", "stellar/ledgers/pubnet", "destination_bucket_path")
	fs.StringVar(&c.region, "region", "us-east-1", "datastore region")
	fs.StringVar(&c.passphrase, "passphrase", network.PublicNetworkPassphrase, "network passphrase")
	from := fs.Uint("from", 0, "put: first ledger")
	to := fs.Uint("to", 0, "put: last ledger")
	seq := fs.Uint("seq", 0, "race: ledger to race on")
	n := fs.Int("n", 8, "race: concurrent uploads")
	key := fs.String("key", "", "put-bytes/head/get: object key relative to the prefix")
	size := fs.Int("size", 0, "put-bytes: body size")
	seed := fs.Uint64("seed", 1, "put-bytes: random seed")
	_ = fs.Parse(args)

	ctx := context.Background()
	switch cmd {
	case "init":
		ds, cfg := openLikeGalexie(ctx, c)
		manifest, created, err := datastore.PublishConfig(ctx, ds, cfg)
		emit(map[string]any{"created": created, "manifest": manifest, "err": errString(err)})
	case "put":
		ds, cfg := openLikeGalexie(ctx, c)
		if _, _, err := datastore.PublishConfig(ctx, ds, cfg); err != nil {
			fail(fmt.Errorf("PublishConfig: %w", err))
		}
		putRange(ctx, ds, cfg, uint32(*from), uint32(*to))
	case "race":
		ds, cfg := openLikeGalexie(ctx, c)
		race(ctx, ds, cfg, uint32(*seq), *n)
	case "put-bytes":
		ds, _ := openDataStore(ctx, c)
		body := make([]byte, *size)
		r := rand.New(rand.NewPCG(*seed, *seed))
		for i := 0; i+8 <= len(body); i += 8 {
			binary.LittleEndian.PutUint64(body[i:], r.Uint64())
		}
		up, err := ds.PutFileIfNotExists(ctx, *key, bytes.NewBuffer(body), map[string]string{"test": "put-bytes"})
		emit(map[string]any{"key": *key, "uploaded": up, "err": errString(err), "sha256": sum(body), "size": len(body)})
	case "head":
		ds, _ := openDataStore(ctx, c)
		meta, err := ds.GetFileMetadata(ctx, *key)
		emit(map[string]any{"key": *key, "meta": meta, "err": errString(err), "not_found": errors.Is(err, os.ErrNotExist)})
	case "get":
		ds, _ := openDataStore(ctx, c)
		rc, _, err := ds.GetFile(ctx, *key)
		var body []byte
		if err == nil {
			body, err = io.ReadAll(rc) // ChecksumMode=ENABLED: the SDK validates the stored CRC32C
			rc.Close()
		}
		emit(map[string]any{"key": *key, "err": errString(err), "not_found": errors.Is(err, os.ErrNotExist),
			"sha256": sum(body), "size": len(body)})
	default:
		fail(fmt.Errorf("unknown command %q", cmd))
	}
}

func dataStoreConfig(c common) datastore.DataStoreConfig {
	return datastore.DataStoreConfig{
		Type: "S3",
		Params: map[string]string{
			"destination_bucket_path": c.bucketPath,
			"region":                  c.region,
			"endpoint_url":            c.endpoint,
		},
		Schema:            datastore.DataStoreSchema{LedgersPerFile: 1, FilesPerPartition: 64000},
		NetworkPassphrase: c.passphrase,
		Compression:       compressionType,
	}
}

func openDataStore(ctx context.Context, c common) (datastore.DataStore, datastore.DataStoreConfig) {
	cfg := dataStoreConfig(c)
	ds, err := datastore.NewDataStore(ctx, cfg)
	if err != nil {
		fail(fmt.Errorf("NewDataStore: %w", err))
	}
	return ds, cfg
}

// openLikeGalexie replicates Galexie's initDataStore: connect (ListObjectsV2
// reachability check), validate the existing file extension, load the schema.
func openLikeGalexie(ctx context.Context, c common) (datastore.DataStore, datastore.DataStoreConfig) {
	ds, cfg := openDataStore(ctx, c)
	ext, err := datastore.GetLedgerFileExtension(ctx, ds)
	if err != nil && !errors.Is(err, datastore.ErrNoLedgerFiles) {
		fail(fmt.Errorf("GetLedgerFileExtension: %w", err))
	}
	if err == nil && ext != compressxdr.DefaultCompressor.Name() {
		fail(fmt.Errorf("incompatible extension %q", ext))
	}
	schema, err := datastore.LoadSchema(ctx, ds, cfg)
	if err != nil {
		fail(fmt.Errorf("LoadSchema: %w", err))
	}
	cfg.Schema = schema
	return ds, cfg
}

type putResult struct {
	Seq      uint32            `json:"seq"`
	Key      string            `json:"key"`
	Uploaded bool              `json:"uploaded"`
	Err      string            `json:"err"`
	SHA256   string            `json:"sha256"`
	Size     int               `json:"size"`
	Hash     string            `json:"hash"`
	Meta     map[string]string `json:"meta"`
}

// putRange uploads [from, to] strictly sequentially, like Galexie's single
// uploader goroutine, and stops at the first error (Galexie exits).
func putRange(ctx context.Context, ds datastore.DataStore, cfg datastore.DataStoreConfig, from, to uint32) {
	prev := syntheticPrev(from)
	for s := from; s >= from && s <= to; s++ {
		lcm := syntheticLCM(s, prev, baseCloseTime+5*int64(s))
		res := upload(ctx, ds, cfg, lcm)
		emit(res)
		if res.Err != "" {
			os.Exit(3)
		}
		prev = lcm.LedgerHash()
	}
}

// race uploads n different versions of the same ledger concurrently.
func race(ctx context.Context, ds datastore.DataStore, cfg datastore.DataStoreConfig, seq uint32, n int) {
	results := make([]putResult, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lcm := syntheticLCM(seq, syntheticPrev(seq), baseCloseTime+5*int64(seq)+int64(i))
			<-start
			results[i] = upload(ctx, ds, cfg, lcm)
		}()
	}
	close(start)
	wg.Wait()
	emit(results)
}

func upload(ctx context.Context, ds datastore.DataStore, cfg datastore.DataStoreConfig, lcm xdr.LedgerCloseMeta) putResult {
	seq := lcm.LedgerSequence()
	batch := xdr.LedgerCloseMetaBatch{
		StartSequence:    xdr.Uint32(seq),
		EndSequence:      xdr.Uint32(seq),
		LedgerCloseMetas: []xdr.LedgerCloseMeta{lcm},
	}
	md := datastore.MetaData{
		StartLedger:          seq,
		EndLedger:            seq,
		StartLedgerCloseTime: lcm.LedgerCloseTime(),
		EndLedgerCloseTime:   lcm.LedgerCloseTime(),
		NetworkPassPhrase:    cfg.NetworkPassphrase,
		CompressionType:      compressxdr.DefaultCompressor.Name(),
		ProtocolVersion:      lcm.ProtocolVersion(),
		CoreVersion:          coreVersion,
		Version:              galexieVersion,
	}
	var encoded bytes.Buffer
	if _, err := compressxdr.NewXDREncoder(compressxdr.DefaultCompressor, &batch).WriteTo(&encoded); err != nil {
		fail(err)
	}
	key := cfg.Schema.GetObjectKeyFromSequenceNumber(seq)
	up, err := ds.PutFileIfNotExists(ctx, key, compressxdr.NewXDREncoder(compressxdr.DefaultCompressor, &batch), md.ToMap())
	h := lcm.LedgerHash()
	return putResult{Seq: seq, Key: key, Uploaded: up, Err: errString(err), SHA256: sum(encoded.Bytes()),
		Size: encoded.Len(), Hash: hex.EncodeToString(h[:]), Meta: md.ToMap()}
}

func syntheticPrev(seq uint32) xdr.Hash {
	return xdr.Hash(sha256.Sum256(fmt.Appendf(nil, "synthetic-prev-%d", seq)))
}

// syntheticLCM builds a minimal, structurally valid LedgerCloseMeta V2 whose
// header hash is computed like stellar-core's: SHA-256 of the XDR header.
func syntheticLCM(seq uint32, prev xdr.Hash, closeTime int64) xdr.LedgerCloseMeta {
	header := xdr.LedgerHeader{
		LedgerVersion:      23,
		PreviousLedgerHash: prev,
		ScpValue: xdr.StellarValue{
			CloseTime: xdr.TimePoint(closeTime),
			Ext:       xdr.StellarValueExt{V: xdr.StellarValueTypeStellarValueBasic},
		},
		LedgerSeq:    xdr.Uint32(seq),
		TotalCoins:   1_000_000_000_000_000_000,
		BaseFee:      100,
		BaseReserve:  5_000_000,
		MaxTxSetSize: 1000,
	}
	hb, err := header.MarshalBinary()
	if err != nil {
		fail(err)
	}
	components := []xdr.TxSetComponent{}
	return xdr.LedgerCloseMeta{
		V: 2,
		V2: &xdr.LedgerCloseMetaV2{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{Hash: xdr.Hash(sha256.Sum256(hb)), Header: header},
			TxSet: xdr.GeneralizedTransactionSet{
				V: 1,
				V1TxSet: &xdr.TransactionSetV1{
					PreviousLedgerHash: prev,
					Phases:             []xdr.TransactionPhase{{V: 0, V0Components: &components}},
				},
			},
		},
	}
}

func sum(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func emit(v any) {
	if err := json.NewEncoder(os.Stdout).Encode(v); err != nil {
		fail(err)
	}
}

func fail(err error) {
	_ = json.NewEncoder(os.Stdout).Encode(map[string]string{"fatal": err.Error()})
	os.Exit(1)
}
