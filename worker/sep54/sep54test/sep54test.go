// Package sep54test provides ledger objects for tests: synthetic hash-chained
// ledgers encoded exactly like Galexie's, an in-memory upstream, and an
// on-disk cache of real AWS-lake ledgers shared by the opt-in network tests.
package sep54test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"embed"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/support/compressxdr"
	"github.com/stellar/go-stellar-sdk/support/datastore"
	"github.com/stellar/go-stellar-sdk/xdr"

	"stellar-explorer/worker/sep54"
)

// BaseCloseTime is the close time of synthetic ledger 0; ledgers close every
// 5 s after it.
const BaseCloseTime = 1_759_190_400

// SyntheticLCM builds a minimal, structurally valid LedgerCloseMeta V2 whose
// header hash is computed like stellar-core's: SHA-256 of the XDR header.
func SyntheticLCM(seq uint32, prev xdr.Hash, closeTime int64) xdr.LedgerCloseMeta {
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
		panic(err)
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

// HeaderLCM builds a LedgerCloseMeta V2 with no transactions around a real
// ledger header (e.g. one read from a history archive), so its hash and
// chain are the network's.
func HeaderLCM(hhe xdr.LedgerHeaderHistoryEntry) xdr.LedgerCloseMeta {
	components := []xdr.TxSetComponent{}
	return xdr.LedgerCloseMeta{
		V: 2,
		V2: &xdr.LedgerCloseMetaV2{
			LedgerHeader: hhe,
			TxSet: xdr.GeneralizedTransactionSet{
				V: 1,
				V1TxSet: &xdr.TransactionSetV1{
					PreviousLedgerHash: hhe.Header.PreviousLedgerHash,
					Phases:             []xdr.TransactionPhase{{V: 0, V0Components: &components}},
				},
			},
		},
	}
}

// Compress encodes a batch the way Galexie does (compressxdr, zstd).
func Compress(batch *xdr.LedgerCloseMetaBatch) []byte {
	var buf bytes.Buffer
	if _, err := compressxdr.NewXDREncoder(compressxdr.DefaultCompressor, batch).WriteTo(&buf); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

// Encode turns one LedgerCloseMeta into the object Galexie would upload:
// the compressed single-ledger batch, its CRC32C and the 9 metadata keys.
func Encode(lcm xdr.LedgerCloseMeta) sep54.Object {
	seq := lcm.LedgerSequence()
	body := Compress(&xdr.LedgerCloseMetaBatch{
		StartSequence:    xdr.Uint32(seq),
		EndSequence:      xdr.Uint32(seq),
		LedgerCloseMetas: []xdr.LedgerCloseMeta{lcm},
	})
	return sep54.Object{
		Key:            sep54.ObjectPath(seq),
		Body:           body,
		Metadata:       metadataFor(lcm),
		ContentType:    "application/octet-stream",
		ChecksumCRC32C: sep54.CRC32C(body),
	}
}

// Chain returns n synthetic, hash-chained ledger objects starting at start.
// The first ledger's previous hash is derived from start, so two chains with
// the same start are identical.
func Chain(start uint32, n int) []sep54.Object {
	prev := xdr.Hash(sha256.Sum256(fmt.Appendf(nil, "sep54test-prev-%d", start)))
	objs := make([]sep54.Object, 0, n)
	for i := range n {
		seq := start + uint32(i)
		lcm := SyntheticLCM(seq, prev, BaseCloseTime+5*int64(seq))
		objs = append(objs, Encode(lcm))
		prev = lcm.LedgerHash()
	}
	return objs
}

// MemSource is an in-memory upstream store of ledger objects.
type MemSource struct {
	mu   sync.Mutex
	objs map[uint32]sep54.Object
	// Gets counts GetLedger calls.
	Gets atomic.Int64
	// Fail, if set, is consulted before every GetLedger.
	Fail func(seq uint32) error
}

// NewMemSource returns a MemSource holding objs.
func NewMemSource(objs ...sep54.Object) *MemSource {
	m := &MemSource{objs: map[uint32]sep54.Object{}}
	m.Add(objs...)
	return m
}

// Add stores objects (keyed by the ledger in their key).
func (m *MemSource) Add(objs ...sep54.Object) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, o := range objs {
		seq, ok := sep54.ParseObjectPath(o.Key)
		if !ok {
			panic("not a ledger key: " + o.Key)
		}
		m.objs[seq] = o
	}
}

// Remove deletes ledger seq (a hole).
func (m *MemSource) Remove(seq uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objs, seq)
}

// GetLedger returns a copy of ledger seq, or sep54.ErrNotFound.
func (m *MemSource) GetLedger(ctx context.Context, seq uint32) (sep54.Object, error) {
	m.Gets.Add(1)
	if err := ctx.Err(); err != nil {
		return sep54.Object{}, err
	}
	m.mu.Lock()
	fail := m.Fail
	o, ok := m.objs[seq]
	m.mu.Unlock()
	if fail != nil {
		if err := fail(seq); err != nil {
			return sep54.Object{}, err
		}
	}
	if !ok {
		return sep54.Object{}, fmt.Errorf("mem ledger %d: %w", seq, sep54.ErrNotFound)
	}
	o.Body = bytes.Clone(o.Body)
	o.Metadata = maps.Clone(o.Metadata)
	return o, nil
}

// Newest returns the highest ledger held, or sep54.ErrNoLedgers.
func (m *MemSource) Newest(ctx context.Context) (uint32, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var newest uint32
	for seq := range m.objs {
		newest = max(newest, seq)
	}
	if len(m.objs) == 0 {
		return 0, sep54.ErrNoLedgers
	}
	return newest, nil
}

// ------------------------------------------------------------ on-disk cache

// The cache holds a contiguous range of real ledger objects:
// <seq>.xdr.zst, <seq>.json (metadata, content type, CRC32C) and range.json.

type cacheMeta struct {
	Metadata       map[string]string `json:"metadata"`
	ContentType    string            `json:"content_type"`
	ChecksumCRC32C string            `json:"checksum_crc32c"`
	ETag           string            `json:"etag"`
}

type cacheRange struct {
	Start uint32 `json:"start"`
	End   uint32 `json:"end"`
}

// CacheRange returns the complete range held by the cache at dir.
func CacheRange(dir string) (start, end uint32, err error) {
	b, err := os.ReadFile(filepath.Join(dir, "range.json"))
	if err != nil {
		return 0, 0, err
	}
	var r cacheRange
	if err := json.Unmarshal(b, &r); err != nil {
		return 0, 0, err
	}
	if r.Start == 0 || r.End < r.Start {
		return 0, 0, fmt.Errorf("bad cache range %+v", r)
	}
	return r.Start, r.End, nil
}

// CacheObject reads ledger seq from the cache at dir.
func CacheObject(dir string, seq uint32) (sep54.Object, error) {
	name := strconv.FormatUint(uint64(seq), 10)
	body, err := os.ReadFile(filepath.Join(dir, name+".xdr.zst"))
	if err != nil {
		return sep54.Object{}, err
	}
	b, err := os.ReadFile(filepath.Join(dir, name+".json"))
	if err != nil {
		return sep54.Object{}, err
	}
	var cm cacheMeta
	if err := json.Unmarshal(b, &cm); err != nil {
		return sep54.Object{}, err
	}
	return sep54.Object{Key: sep54.ObjectPath(seq), Body: body, Metadata: cm.Metadata, ContentType: cm.ContentType,
		ChecksumCRC32C: cm.ChecksumCRC32C, ETag: cm.ETag}, nil
}

// Upstream is what FillCache reads from (a *sep54.Source).
type Upstream interface {
	GetLedger(ctx context.Context, seq uint32) (sep54.Object, error)
}

// FillCache downloads [start, end] from src into dir with the given
// concurrency, verifying each object's stored CRC32C, then records the
// range. Objects already cached are kept.
func FillCache(ctx context.Context, src Upstream, dir string, start, end uint32, workers int) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	_ = os.Remove(filepath.Join(dir, "range.json"))
	seqs := make(chan uint32)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for seq := range seqs {
				if err := cacheOne(ctx, src, dir, seq); err != nil {
					errs <- err
					cancel()
					return
				}
			}
		}()
	}
feed:
	for seq := start; seq <= end; seq++ {
		select {
		case seqs <- seq:
		case <-ctx.Done():
			break feed
		}
	}
	close(seqs)
	wg.Wait()
	select {
	case err := <-errs:
		return err
	default:
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	b, _ := json.Marshal(cacheRange{Start: start, End: end})
	return os.WriteFile(filepath.Join(dir, "range.json"), b, 0o644)
}

func cacheOne(ctx context.Context, src Upstream, dir string, seq uint32) error {
	name := strconv.FormatUint(uint64(seq), 10)
	if _, err := CacheObject(dir, seq); err == nil {
		return nil
	}
	var o sep54.Object
	var err error
	for attempt := 1; attempt <= 5; attempt++ {
		o, err = src.GetLedger(ctx, seq)
		if err == nil || !sep54.IsUnavailable(err) || ctx.Err() != nil {
			break
		}
		time.Sleep(time.Duration(attempt) * time.Second)
	}
	if err != nil {
		return err
	}
	if err := sep54.VerifyCRC32C(o.Body, o.ChecksumCRC32C); err != nil {
		return fmt.Errorf("ledger %d: %w", seq, err)
	}
	b, _ := json.Marshal(cacheMeta{Metadata: o.Metadata, ContentType: o.ContentType, ChecksumCRC32C: o.ChecksumCRC32C, ETag: o.ETag})
	if err := os.WriteFile(filepath.Join(dir, name+".json"), b, 0o644); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, name+".xdr.zst"), o.Body, 0o644)
}

// DirSource serves the ledgers of a cache directory, read on demand.
type DirSource struct {
	Dir        string
	Start, End uint32
}

// OpenCache returns a DirSource for the complete range of the cache at dir.
func OpenCache(dir string) (*DirSource, error) {
	start, end, err := CacheRange(dir)
	if err != nil {
		return nil, err
	}
	return &DirSource{Dir: dir, Start: start, End: end}, nil
}

// GetLedger reads ledger seq from the cache, or returns sep54.ErrNotFound.
func (d *DirSource) GetLedger(ctx context.Context, seq uint32) (sep54.Object, error) {
	if err := ctx.Err(); err != nil {
		return sep54.Object{}, err
	}
	if seq < d.Start || seq > d.End {
		return sep54.Object{}, fmt.Errorf("cache ledger %d: %w", seq, sep54.ErrNotFound)
	}
	return CacheObject(d.Dir, seq)
}

// Newest returns the end of the cached range.
func (d *DirSource) Newest(context.Context) (uint32, error) { return d.End, nil }

// ------------------------------------------------------------ real ledgers

// The fixtures are consecutive, hash-chained pubnet ledgers (protocol 28,
// LedgerCloseMeta V2) copied from the AWS public lake: about 450
// transactions each, fee bumps, Soroban invocations, offers, path payments,
// claimable balances and sponsorships.
//
//go:embed testdata/*.xdr.zst
var realFS embed.FS

// RealLedger is one checked-in pubnet ledger.
type RealLedger struct {
	Seq  uint32
	Body []byte // the object exactly as the AWS lake serves it
	LCM  xdr.LedgerCloseMeta
}

var realLedgers = sync.OnceValues(func() ([]RealLedger, error) {
	entries, err := fs.ReadDir(realFS, "testdata")
	if err != nil {
		return nil, err
	}
	var out []RealLedger
	for _, e := range entries {
		seq, err := strconv.ParseUint(strings.TrimSuffix(e.Name(), ".xdr.zst"), 10, 32)
		if err != nil {
			return nil, fmt.Errorf("fixture %s: %w", e.Name(), err)
		}
		body, err := realFS.ReadFile("testdata/" + e.Name())
		if err != nil {
			return nil, err
		}
		lcm, _, err := sep54.DecodeLedger(uint32(seq), body)
		if err != nil {
			return nil, err
		}
		out = append(out, RealLedger{Seq: uint32(seq), Body: body, LCM: lcm})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out, nil
})

// RealLedgers returns the checked-in pubnet ledgers in ascending order. The
// LedgerCloseMetas are shared; callers must not modify them.
func RealLedgers() []RealLedger {
	ls, err := realLedgers()
	if err != nil {
		panic(err)
	}
	return ls
}

// RealObject returns the checked-in ledger seq as a lake object, with the
// metadata Galexie would have written for it.
func RealObject(seq uint32) sep54.Object {
	for _, l := range RealLedgers() {
		if l.Seq == seq {
			return sep54.Object{
				Key:            sep54.ObjectPath(seq),
				Body:           bytes.Clone(l.Body),
				Metadata:       metadataFor(l.LCM),
				ContentType:    "application/octet-stream",
				ChecksumCRC32C: sep54.CRC32C(l.Body),
			}
		}
	}
	panic(fmt.Sprintf("no fixture for ledger %d", seq))
}

func metadataFor(lcm xdr.LedgerCloseMeta) map[string]string {
	seq := lcm.LedgerSequence()
	md := datastore.MetaData{
		StartLedger:          seq,
		EndLedger:            seq,
		StartLedgerCloseTime: lcm.LedgerCloseTime(),
		EndLedgerCloseTime:   lcm.LedgerCloseTime(),
		ProtocolVersion:      lcm.ProtocolVersion(),
		CoreVersion:          "stellar-core 29.0.0 (sep54test)",
		NetworkPassPhrase:    network.PublicNetworkPassphrase,
		CompressionType:      compressxdr.DefaultCompressor.Name(),
		Version:              "sep54test",
	}
	return md.ToMap()
}

// rechainTemplate is one real ledger encoded as a single-ledger batch, with
// the offset of its LedgerHeaderHistoryEntry.
type rechainTemplate struct {
	raw       []byte
	hdrOffset int
	hdr       xdr.LedgerHeaderHistoryEntry
	lcm       xdr.LedgerCloseMeta
}

var rechainTemplates = sync.OnceValues(func() ([]rechainTemplate, error) {
	var out []rechainTemplate
	for _, l := range RealLedgers() {
		raw, err := sep54.Decompress(l.Body)
		if err != nil {
			return nil, err
		}
		hhe := l.LCM.LedgerHeaderHistoryEntry()
		hb, err := hhe.MarshalBinary()
		if err != nil {
			return nil, err
		}
		off := bytes.Index(raw, hb)
		if off < 0 {
			return nil, fmt.Errorf("ledger %d: header not found in its own encoding", l.Seq)
		}
		out = append(out, rechainTemplate{raw: raw, hdrOffset: off, hdr: hhe, lcm: l.LCM})
	}
	return out, nil
})

// rechainEncoder is a fast zstd encoder: re-chained ledgers are test input,
// so speed matters more than ratio.
var rechainEncoder = sync.OnceValues(func() (*zstd.Encoder, error) {
	return zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderCRC(true))
})

// Rechain returns n hash-chained ledger objects starting at start whose
// content (transaction set, results, meta, events) is copied in turn from
// the real fixtures, under new headers: sequence, previous hash and close
// time change and the hash is recomputed, so every object passes the full
// decode, the self-hash and the chain checks, and processes like a real
// pubnet ledger. Like Chain, the first previous hash is derived from start,
// so two calls with the same start return the same chain.
func Rechain(start uint32, n int) []sep54.Object {
	tmpls, err := rechainTemplates()
	if err != nil {
		panic(err)
	}
	enc, err := rechainEncoder()
	if err != nil {
		panic(err)
	}
	prev := xdr.Hash(sha256.Sum256(fmt.Appendf(nil, "sep54test-prev-%d", start)))
	objs := make([]sep54.Object, 0, n)
	for i := range n {
		seq := start + uint32(i)
		t := tmpls[i%len(tmpls)]
		header := t.hdr.Header
		header.LedgerSeq = xdr.Uint32(seq)
		header.PreviousLedgerHash = prev
		header.ScpValue.CloseTime = xdr.TimePoint(BaseCloseTime + 5*int64(seq))
		hb, err := header.MarshalBinary()
		if err != nil {
			panic(err)
		}
		hhe := xdr.LedgerHeaderHistoryEntry{Hash: xdr.Hash(sha256.Sum256(hb)), Header: header, Ext: t.hdr.Ext}
		hhb, err := hhe.MarshalBinary()
		if err != nil {
			panic(err)
		}
		raw := bytes.Clone(t.raw)
		if len(hhb) != len(t.raw[t.hdrOffset:t.hdrOffset+len(hhb)]) {
			panic("header size changed")
		}
		copy(raw[t.hdrOffset:], hhb)
		binary.BigEndian.PutUint32(raw[0:], seq) // batch StartSequence
		binary.BigEndian.PutUint32(raw[4:], seq) // batch EndSequence
		body := enc.EncodeAll(raw, nil)
		lcm := t.lcm
		v2 := *lcm.V2
		v2.LedgerHeader = hhe
		lcm.V2 = &v2
		objs = append(objs, sep54.Object{
			Key:            sep54.ObjectPath(seq),
			Body:           body,
			Metadata:       metadataFor(lcm),
			ContentType:    "application/octet-stream",
			ChecksumCRC32C: sep54.CRC32C(body),
		})
		prev = hhe.Hash
	}
	return objs
}
