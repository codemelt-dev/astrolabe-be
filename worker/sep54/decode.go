package sep54

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"regexp"
	"strings"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/stellar/go-stellar-sdk/xdr"
)

const (
	// MaxObjectSize bounds a compressed ledger object read into memory
	// (today's max is ~480 KB).
	MaxObjectSize = 64 << 20
	// MaxDecodedSize bounds a decompressed ledger object (today ~2.4 MB); it
	// is the zstd decoder's memory limit for hostile or corrupt input.
	MaxDecodedSize = 256 << 20
)

// zstdDecoder is shared: DecodeAll is safe for concurrent use, up to
// GOMAXPROCS decodes at a time. It verifies each frame's content checksum.
var zstdDecoder = sync.OnceValues(func() (*zstd.Decoder, error) {
	return zstd.NewReader(nil, zstd.WithDecoderMaxMemory(MaxDecodedSize), zstd.WithDecoderConcurrency(0))
})

// Header is what the pipeline needs from a ledger header: identity, chain
// link and a few facts, taken from a header whose hash was verified.
type Header struct {
	Sequence        uint32
	Hash            xdr.Hash
	PreviousHash    xdr.Hash
	CloseTime       int64 // Unix seconds
	ProtocolVersion uint32
	// LCMVersion is the LedgerCloseMeta union arm (2 for everything since
	// Protocol 23).
	LCMVersion int32
}

// Decompress decompresses a ledger object, verifying every zstd frame
// checksum. The output is bounded by MaxDecodedSize.
func Decompress(body []byte) ([]byte, error) {
	dec, err := zstdDecoder()
	if err != nil {
		return nil, err
	}
	if len(body) == 0 {
		return nil, errors.New("empty object")
	}
	out, err := dec.DecodeAll(body, nil)
	if err != nil {
		return nil, fmt.Errorf("zstd: %w", err)
	}
	if len(out) == 0 {
		return nil, errors.New("zstd: no decompressed data")
	}
	return out, nil
}

// DecodeLedger fully decodes the object of ledger seq:
// decompress (zstd with a memory limit), xdr.SafeUnmarshal of the batch (no
// trailing bytes), then assert StartSequence == EndSequence == seq, exactly
// one LedgerCloseMeta, lcm.LedgerSequence() == seq, LedgerCloseMeta V2 only,
// and sha256(XDR(header)) == its hash. Every failure is an *InvalidError.
func DecodeLedger(seq uint32, body []byte) (xdr.LedgerCloseMeta, Header, error) {
	raw, err := Decompress(body)
	if err != nil {
		return xdr.LedgerCloseMeta{}, Header{}, &InvalidError{Seq: seq, Err: err}
	}
	var batch xdr.LedgerCloseMetaBatch
	if err := xdr.SafeUnmarshal(raw, &batch); err != nil {
		return xdr.LedgerCloseMeta{}, Header{}, invalid(seq, "decode LedgerCloseMetaBatch: %w", err)
	}
	if uint32(batch.StartSequence) != seq || uint32(batch.EndSequence) != seq {
		return xdr.LedgerCloseMeta{}, Header{}, invalid(seq, "batch covers %d-%d", batch.StartSequence, batch.EndSequence)
	}
	if n := len(batch.LedgerCloseMetas); n != 1 {
		return xdr.LedgerCloseMeta{}, Header{}, invalid(seq, "batch holds %d LedgerCloseMetas, want 1", n)
	}
	lcm := batch.LedgerCloseMetas[0]
	h, err := CheckLCM(seq, lcm)
	if err != nil {
		return xdr.LedgerCloseMeta{}, Header{}, err
	}
	return lcm, h, nil
}

// CheckLCM validates a LedgerCloseMeta that was decoded elsewhere (by the
// SDK's BufferedStorageBackend, which already SafeUnmarshals it): V2 only,
// lcm.LedgerSequence() == seq, and sha256(XDR(header)) == its hash. Every
// failure is an *InvalidError.
func CheckLCM(seq uint32, lcm xdr.LedgerCloseMeta) (Header, error) {
	if lcm.V != 2 || lcm.V2 == nil {
		// Everything we ingest is V2 (P23+); anything else is a validation
		// failure, never silently processed.
		return Header{}, invalid(seq, "LedgerCloseMeta V%d, only V2 is supported", lcm.V)
	}
	if got := lcm.LedgerSequence(); got != seq {
		return Header{}, invalid(seq, "LedgerCloseMeta is for ledger %d", got)
	}
	return headerOf(seq, lcm.V, lcm.LedgerHeaderHistoryEntry())
}

// DecodeHeader decompresses the object of ledger seq (verifying every zstd
// frame checksum) but XDR-decodes only the batch prefix and the ledger
// header, so a protocol that changes transaction meta does not stop the
// archiver. It checks the batch covers exactly seq, the header
// sequence and the header self-hash. LedgerCloseMeta V0, V1 and V2 are
// understood; any other layout yields an error wrapping ErrUnknownVersion.
func DecodeHeader(seq uint32, body []byte) (Header, error) {
	raw, err := Decompress(body)
	if err != nil {
		return Header{}, &InvalidError{Seq: seq, Err: err}
	}
	return parseHeader(seq, raw)
}

// CheckObject is the archiver's validation of one object: the checksum
// its store keeps (VerifyStored), the zstd frame checksums, and a
// header-only decode. An error wrapping ErrUnknownVersion means both
// checksums passed but the header could not be located.
func CheckObject(seq uint32, obj Object) (Header, error) {
	if err := VerifyStored(obj); err != nil {
		return Header{}, &InvalidError{Seq: seq, Err: err}
	}
	return DecodeHeader(seq, obj.Body)
}

// VerifyStored checks an object's bytes against the checksum its store
// keeps: the CRC32C if the store returns one (the lake, AWS), else the
// ETag of a single-part upload, which is the MD5 of the bytes. Hetzner
// Object Storage accepts the archiver's CRC32C but returns none.
func VerifyStored(obj Object) error {
	if obj.ChecksumCRC32C != "" {
		return VerifyCRC32C(obj.Body, obj.ChecksumCRC32C)
	}
	etag := strings.ToLower(strings.Trim(obj.ETag, `"`))
	if !md5ETag.MatchString(etag) {
		return fmt.Errorf("no stored CRC32C checksum, and the ETag %q is not an MD5", obj.ETag)
	}
	if sum := md5.Sum(obj.Body); hex.EncodeToString(sum[:]) != etag {
		return fmt.Errorf("MD5 mismatch: ETag %s, computed %x", etag, sum)
	}
	return nil
}

// md5ETag is a single-part upload's ETag; a multipart one ends in "-<parts>".
var md5ETag = regexp.MustCompile(`^[0-9a-f]{32}$`)

// VerifyCRC32C checks body against a base64 CRC32C (Castagnoli) checksum as
// stored by S3 in x-amz-checksum-crc32c. A missing checksum is an error.
func VerifyCRC32C(body []byte, stored string) error {
	if stored == "" {
		return errors.New("no stored CRC32C checksum")
	}
	if got := CRC32C(body); got != stored {
		return fmt.Errorf("CRC32C mismatch: stored %s, computed %s", stored, got)
	}
	return nil
}

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// CRC32C returns the base64 CRC32C of b, formatted like S3's
// x-amz-checksum-crc32c.
func CRC32C(b []byte) string {
	var sum [4]byte
	binary.BigEndian.PutUint32(sum[:], crc32.Checksum(b, castagnoli))
	return base64.StdEncoding.EncodeToString(sum[:])
}

// parseHeader decodes the batch prefix of a decompressed object up to and
// including the first LedgerHeaderHistoryEntry.
func parseHeader(seq uint32, raw []byte) (Header, error) {
	r := bytes.NewReader(raw)
	var start, end, count xdr.Uint32
	for _, f := range []*xdr.Uint32{&start, &end, &count} {
		if _, err := xdr.Unmarshal(r, f); err != nil {
			return Header{}, invalid(seq, "decode batch prefix: %w", err)
		}
	}
	if uint32(start) != seq || uint32(end) != seq {
		return Header{}, invalid(seq, "batch covers %d-%d", start, end)
	}
	if count != 1 {
		return Header{}, invalid(seq, "batch holds %d LedgerCloseMetas, want 1", count)
	}
	var v xdr.Int32
	if _, err := xdr.Unmarshal(r, &v); err != nil {
		return Header{}, invalid(seq, "decode LedgerCloseMeta version: %w", err)
	}
	switch v {
	case 0:
		// LedgerCloseMetaV0 starts with the header.
	case 1, 2:
		// V1 and V2 start with LedgerCloseMetaExt. An unknown arm there
		// changes the layout before the header just like a new version.
		if r.Len() >= 4 {
			if ext := int32(binary.BigEndian.Uint32(raw[len(raw)-r.Len():])); ext != 0 && ext != 1 {
				return Header{}, &InvalidError{Seq: seq, Err: fmt.Errorf("%w: V%d with LedgerCloseMetaExt arm %d", ErrUnknownVersion, v, ext)}
			}
		}
		var ext xdr.LedgerCloseMetaExt
		if _, err := xdr.Unmarshal(r, &ext); err != nil {
			return Header{}, invalid(seq, "decode LedgerCloseMetaExt: %w", err)
		}
	default:
		return Header{}, &InvalidError{Seq: seq, Err: fmt.Errorf("%w: V%d", ErrUnknownVersion, v)}
	}
	var hhe xdr.LedgerHeaderHistoryEntry
	if _, err := xdr.Unmarshal(r, &hhe); err != nil {
		return Header{}, invalid(seq, "decode ledger header: %w", err)
	}
	return headerOf(seq, int32(v), hhe)
}

// headerOf checks the header's sequence and self-hash
// (sha256(XDR(header)) == hash) and extracts the Header.
func headerOf(seq uint32, lcmVersion int32, hhe xdr.LedgerHeaderHistoryEntry) (Header, error) {
	if got := uint32(hhe.Header.LedgerSeq); got != seq {
		return Header{}, invalid(seq, "header is for ledger %d", got)
	}
	b, err := hhe.Header.MarshalBinary()
	if err != nil {
		return Header{}, invalid(seq, "encode header: %w", err)
	}
	if sum := sha256.Sum256(b); xdr.Hash(sum) != hhe.Hash {
		return Header{}, invalid(seq, "header self-hash mismatch: sha256(header) %x, stored hash %x", sum, hhe.Hash)
	}
	return Header{
		Sequence:        seq,
		Hash:            hhe.Hash,
		PreviousHash:    hhe.Header.PreviousLedgerHash,
		CloseTime:       int64(hhe.Header.ScpValue.CloseTime),
		ProtocolVersion: uint32(hhe.Header.LedgerVersion),
		LCMVersion:      lcmVersion,
	}, nil
}

// CheckChain checks that next directly follows prev: consecutive sequences
// and next.PreviousHash == prev.Hash. Stellar has no reorgs, so
// a mismatch means corruption, the wrong network, or mixed sources.
func CheckChain(prev, next Header) error {
	if next.Sequence != prev.Sequence+1 {
		return invalid(next.Sequence, "does not follow ledger %d", prev.Sequence)
	}
	if next.PreviousHash != prev.Hash {
		return invalid(next.Sequence, "previous hash %x does not match ledger %d hash %x", next.PreviousHash, prev.Sequence, prev.Hash)
	}
	return nil
}
