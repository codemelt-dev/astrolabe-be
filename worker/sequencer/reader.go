package sequencer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/datastore"

	"stellar-explorer/worker/sep54"
)

// Source is one SEP-54 store: the lake, the archive or the AWS public lake
// (*sep54.Source), or a fake in tests.
type Source interface {
	// Name is sep54.SourceLake, SourceArchive or SourceAWS.
	Name() string
	// GetLedger returns the object of ledger seq. A missing object is an
	// error wrapping sep54.ErrNotFound; an unreachable store is
	// sep54.IsUnavailable.
	GetLedger(ctx context.Context, seq uint32) (sep54.Object, error)
}

// readOne reads ledger seq with a single GET and fully decodes it. This is
// the path at the tip and the precise fallback after a
// catch-up reader fails: its errors say exactly what went wrong.
func readOne(ctx context.Context, src Source, seq uint32) (Ledger, error) {
	obj, err := src.GetLedger(ctx, seq)
	if err != nil {
		return Ledger{}, err
	}
	lcm, h, err := sep54.DecodeLedger(seq, obj.Body)
	if err != nil {
		return Ledger{}, err
	}
	return Ledger{Header: h, LCM: lcm, Source: src.Name(), Metadata: obj.Metadata, Size: len(obj.Body)}, nil
}

// rangeReader is the catch-up path: the SDK's
// BufferedStorageBackend over a bounded range of one source. A backend is
// never reused after an error (a cancelled buffer cannot be re-prepared);
// the sequencer closes it and opens a new one.
type rangeReader struct {
	src      Source
	from, to uint32
	next     uint32
	bsb      *ledgerbackend.BufferedStorageBackend
	store    *captureStore
}

func openRange(ctx context.Context, src Source, from, to uint32, cfg ledgerbackend.BufferedStorageBackendConfig) (*rangeReader, error) {
	store := &captureStore{src: src, info: map[uint32]objectInfo{}}
	bsb, err := ledgerbackend.NewBufferedStorageBackend(cfg, store, sep54.Schema)
	if err != nil {
		return nil, err
	}
	if err := bsb.PrepareRange(ctx, ledgerbackend.BoundedRange(from, to)); err != nil {
		_ = bsb.Close()
		return nil, err
	}
	return &rangeReader{src: src, from: from, to: to, next: from, bsb: bsb, store: store}, nil
}

// read returns ledger seq, which must be the reader's next ledger. The
// backend already SafeUnmarshals the LedgerCloseMeta; CheckLCM adds the V2,
// sequence and self-hash checks of the full decode.
func (r *rangeReader) read(ctx context.Context, seq uint32) (Ledger, error) {
	if seq != r.next {
		return Ledger{}, fmt.Errorf("catch-up reader is at %d, not %d", r.next, seq)
	}
	lcm, err := r.bsb.GetLedger(ctx, seq)
	if err != nil {
		return Ledger{}, err
	}
	r.next++
	info := r.store.take(seq)
	h, err := sep54.CheckLCM(seq, lcm)
	if err != nil {
		return Ledger{}, err
	}
	return Ledger{Header: h, LCM: lcm, Source: r.src.Name(), Metadata: info.metadata, Size: info.size}, nil
}

func (r *rangeReader) close() {
	_ = r.bsb.Close()
}

// objectInfo is what BufferedStorageBackend drops but the pipeline keeps:
// the object's metadata (core version) and size.
type objectInfo struct {
	metadata map[string]string
	size     int
}

// captureStore is the DataStore the BufferedStorageBackend reads through:
// a thin adapter over the source's own explicit client (Source.GetLedger,
// with ChecksumMode). It records each object's metadata and the lowest
// ledger found missing, so a hole ahead of the reader is known exactly.
// BufferedStorageBackend only calls GetFile; the embedded interface is nil.
type captureStore struct {
	datastore.DataStore
	src Source

	mu      sync.Mutex
	info    map[uint32]objectInfo
	missing uint32 // lowest ledger found missing; 0 = none
}

func (c *captureStore) GetFile(ctx context.Context, path string) (io.ReadCloser, int64, error) {
	seq, ok := sep54.ParseObjectPath(path)
	if !ok {
		return nil, 0, fmt.Errorf("not a ledger object key: %q", path)
	}
	obj, err := c.src.GetLedger(ctx, seq)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			c.mu.Lock()
			if c.missing == 0 || seq < c.missing {
				c.missing = seq
			}
			c.mu.Unlock()
		}
		return nil, 0, err
	}
	c.mu.Lock()
	c.info[seq] = objectInfo{metadata: obj.Metadata, size: len(obj.Body)}
	c.mu.Unlock()
	return io.NopCloser(bytes.NewReader(obj.Body)), int64(len(obj.Body)), nil
}

func (c *captureStore) take(seq uint32) objectInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	info := c.info[seq]
	delete(c.info, seq)
	return info
}

// firstMissing returns the lowest ledger the backend found missing.
func (c *captureStore) firstMissing() (uint32, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.missing, c.missing != 0
}
