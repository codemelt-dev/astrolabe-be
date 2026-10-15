package sep54_test

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stellar/go-stellar-sdk/xdr"

	"stellar-explorer/worker/sep54"
	"stellar-explorer/worker/sep54/sep54test"
)

func TestDecodeSyntheticChain(t *testing.T) {
	objs := sep54test.Chain(1000, 5)
	var prev sep54.Header
	for i, o := range objs {
		seq := uint32(1000 + i)
		lcm, full, err := sep54.DecodeLedger(seq, o.Body)
		if err != nil {
			t.Fatalf("DecodeLedger(%d): %v", seq, err)
		}
		if lcm.LedgerSequence() != seq || full.Hash != lcm.LedgerHash() || full.PreviousHash != lcm.PreviousLedgerHash() ||
			full.LCMVersion != 2 || full.ProtocolVersion != 23 || full.CloseTime != lcm.LedgerCloseTime() {
			t.Fatalf("ledger %d: header %+v does not match the LCM", seq, full)
		}
		hdr, err := sep54.DecodeHeader(seq, o.Body)
		if err != nil {
			t.Fatalf("DecodeHeader(%d): %v", seq, err)
		}
		if hdr != full {
			t.Fatalf("header-only %+v != full %+v", hdr, full)
		}
		if got, err := sep54.CheckObject(seq, o); err != nil || got != full {
			t.Fatalf("CheckObject(%d) = %+v, %v", seq, got, err)
		}
		if i > 0 {
			if err := sep54.CheckChain(prev, full); err != nil {
				t.Fatalf("chain: %v", err)
			}
		}
		prev = full
	}
	// Out of order or broken links.
	h := func(i int) sep54.Header { _, h, _ := sep54.DecodeLedger(uint32(1000+i), objs[i].Body); return h }
	if err := sep54.CheckChain(h(0), h(2)); !sep54.IsInvalid(err) {
		t.Fatalf("skipped ledger: %v", err)
	}
	other := sep54test.Chain(1001, 1)[0] // same sequence, different previous hash
	_, h1b, err := sep54.DecodeLedger(1001, other.Body)
	if err != nil {
		t.Fatal(err)
	}
	if err := sep54.CheckChain(h(0), h1b); !sep54.IsInvalid(err) || !strings.Contains(err.Error(), "previous hash") {
		t.Fatalf("hash mismatch: %v", err)
	}
}

// encodeBatch compresses an arbitrary batch like Galexie.
func encodeBatch(start, end uint32, lcms ...xdr.LedgerCloseMeta) []byte {
	return sep54test.Compress(&xdr.LedgerCloseMetaBatch{StartSequence: xdr.Uint32(start), EndSequence: xdr.Uint32(end), LedgerCloseMetas: lcms})
}

func zstdBytes(t *testing.T, raw []byte) []byte {
	t.Helper()
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer enc.Close()
	return enc.EncodeAll(raw, nil)
}

func rawBatch(t *testing.T, o sep54.Object) []byte {
	t.Helper()
	raw, err := sep54.Decompress(o.Body)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestDecodeRejects(t *testing.T) {
	const seq = 5000
	good := sep54test.Chain(seq, 1)[0]
	lcm := sep54test.SyntheticLCM(seq, xdr.Hash{1}, 1)
	next := sep54test.SyntheticLCM(seq+1, lcm.LedgerHash(), 6)

	flipped := bytes.Clone(good.Body)
	flipped[len(flipped)-2] ^= 0xff // inside the frame's content checksum

	badHash := sep54test.SyntheticLCM(seq, xdr.Hash{1}, 1)
	badHash.V2.LedgerHeader.Header.TotalCoins++ // hash no longer matches
	wrongSeqHeader := sep54test.SyntheticLCM(seq+1, xdr.Hash{1}, 1)

	trailing := append(rawBatch(t, good), 0, 0, 0, 0)

	cases := []struct {
		name      string
		body      []byte
		ledgerErr string // expected in DecodeLedger's error
		headerErr string // expected in DecodeHeader's error; "" means it accepts the object
	}{
		{"empty", nil, "empty object", "empty object"},
		{"not zstd", []byte("definitely not a zstd frame"), "zstd", "zstd"},
		{"truncated frame", good.Body[:len(good.Body)/2], "zstd", "zstd"},
		{"content checksum mismatch", flipped, "zstd", "zstd"},
		{"trailing garbage after the frame", append(bytes.Clone(good.Body), "garbage"...), "zstd", "zstd"},
		{"other ledger's object", sep54test.Chain(seq+1, 1)[0].Body, "batch covers", "batch covers"},
		{"two-ledger batch", encodeBatch(seq, seq+1, lcm, next), "batch covers", "batch covers"},
		{"empty batch", encodeBatch(seq, seq), "0 LedgerCloseMetas", "0 LedgerCloseMetas"},
		{"two LCMs for one ledger", encodeBatch(seq, seq, lcm, lcm), "2 LedgerCloseMetas", "2 LedgerCloseMetas"},
		{"header for another ledger", encodeBatch(seq, seq, wrongSeqHeader), "LedgerCloseMeta is for ledger", "header is for ledger"},
		{"header self-hash mismatch", encodeBatch(seq, seq, badHash), "self-hash", "self-hash"},
		// SafeUnmarshal refuses trailing bytes; the header-only decode
		// deliberately never looks past the header.
		{"trailing XDR bytes", zstdBytes(t, trailing), "input not fully consumed", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, _, err := sep54.DecodeLedger(seq, c.body)
			if !sep54.IsInvalid(err) || !strings.Contains(err.Error(), c.ledgerErr) {
				t.Fatalf("DecodeLedger = %v, want an *InvalidError mentioning %q", err, c.ledgerErr)
			}
			_, herr := sep54.DecodeHeader(seq, c.body)
			if c.headerErr == "" {
				if herr != nil {
					t.Fatalf("DecodeHeader: %v", herr)
				}
				return
			}
			if !sep54.IsInvalid(herr) || errors.Is(herr, sep54.ErrUnknownVersion) || !strings.Contains(herr.Error(), c.headerErr) {
				t.Fatalf("DecodeHeader = %v, want a plain *InvalidError mentioning %q", herr, c.headerErr)
			}
		})
	}
}

// Everything we ingest is V2; V0 and V1 are known layouts, so the archiver's
// header-only decode still reads them, but the full decode refuses them.
func TestDecodeV2Only(t *testing.T) {
	const seq = 7000
	v2 := sep54test.SyntheticLCM(seq, xdr.Hash{9}, 1)
	hhe := v2.V2.LedgerHeader
	v1 := xdr.LedgerCloseMeta{V: 1, V1: &xdr.LedgerCloseMetaV1{LedgerHeader: hhe, TxSet: v2.V2.TxSet}}
	v0 := xdr.LedgerCloseMeta{V: 0, V0: &xdr.LedgerCloseMetaV0{LedgerHeader: hhe}}
	for _, lcm := range []xdr.LedgerCloseMeta{v0, v1} {
		body := encodeBatch(seq, seq, lcm)
		if _, _, err := sep54.DecodeLedger(seq, body); !sep54.IsInvalid(err) || !strings.Contains(err.Error(), "only V2") {
			t.Fatalf("V%d: DecodeLedger = %v, want the V2-only rule", lcm.V, err)
		}
		h, err := sep54.DecodeHeader(seq, body)
		if err != nil || h.Hash != hhe.Hash || h.LCMVersion != lcm.V {
			t.Fatalf("V%d: DecodeHeader = %+v, %v", lcm.V, h, err)
		}
	}
}

// An unknown LedgerCloseMeta layout passes CheckObject's checksums and
// yields ErrUnknownVersion, so the archiver can fall back to checksum-only
// validation. The full decode rejects it outright.
func TestUnknownVersionFallsBackToChecksums(t *testing.T) {
	const seq = 8000
	good := rawBatch(t, sep54test.Chain(seq, 1)[0])
	withArm := func(v, ext int32) []byte {
		raw := bytes.Clone(good)
		binary.BigEndian.PutUint32(raw[12:], uint32(v)) // LedgerCloseMeta arm after start, end, count
		if ext >= 0 {
			binary.BigEndian.PutUint32(raw[16:], uint32(ext)) // LedgerCloseMetaExt arm
		}
		return zstdBytes(t, raw)
	}
	for name, body := range map[string][]byte{
		"LedgerCloseMeta V3":       withArm(3, -1),
		"LedgerCloseMetaExt arm 2": withArm(2, 2),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := sep54.CheckObject(seq, sep54.Object{Body: body, ChecksumCRC32C: sep54.CRC32C(body)})
			if !errors.Is(err, sep54.ErrUnknownVersion) || !sep54.IsInvalid(err) {
				t.Fatalf("CheckObject = %v, want ErrUnknownVersion", err)
			}
			if _, _, err := sep54.DecodeLedger(seq, body); err == nil || errors.Is(err, sep54.ErrUnknownVersion) {
				t.Fatalf("DecodeLedger = %v, want a decode failure", err)
			}
			// A bad checksum is caught before the version question.
			if _, err := sep54.CheckObject(seq, sep54.Object{Body: body, ChecksumCRC32C: sep54.CRC32C([]byte("x"))}); errors.Is(err, sep54.ErrUnknownVersion) || !strings.Contains(err.Error(), "CRC32C mismatch") {
				t.Fatalf("CheckObject with a bad CRC = %v", err)
			}
		})
	}
}

// Stores that keep no CRC32C (Hetzner) are checked by a single-part
// upload's ETag, the MD5 of the bytes; anything else is refused.
func TestVerifyStored(t *testing.T) {
	body := []byte("hello, ledger")
	sum := md5.Sum(body)
	md5hex := hex.EncodeToString(sum[:])
	for _, c := range []struct {
		name string
		obj  sep54.Object
		want string // "" for no error, else part of the error
	}{
		{"crc32c", sep54.Object{Body: body, ChecksumCRC32C: sep54.CRC32C(body)}, ""},
		{"crc32c wins over a wrong etag", sep54.Object{Body: body, ChecksumCRC32C: sep54.CRC32C(body), ETag: `"00"`}, ""},
		{"crc32c mismatch", sep54.Object{Body: body, ChecksumCRC32C: sep54.CRC32C([]byte("x")), ETag: `"` + md5hex + `"`}, "CRC32C mismatch"},
		{"md5 etag", sep54.Object{Body: body, ETag: `"` + md5hex + `"`}, ""},
		{"md5 etag, upper case", sep54.Object{Body: body, ETag: strings.ToUpper(md5hex)}, ""},
		{"md5 mismatch", sep54.Object{Body: append(bytes.Clone(body), '!'), ETag: `"` + md5hex + `"`}, "MD5 mismatch"},
		{"multipart etag", sep54.Object{Body: body, ETag: `"` + md5hex + `-2"`}, "not an MD5"},
		{"nothing stored", sep54.Object{Body: body}, "no stored CRC32C"},
	} {
		err := sep54.VerifyStored(c.obj)
		if (c.want == "") != (err == nil) || err != nil && !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: VerifyStored = %v, want %q", c.name, err, c.want)
		}
	}
}

func TestVerifyCRC32C(t *testing.T) {
	body := []byte("hello, ledger")
	sum := sep54.CRC32C(body)
	if err := sep54.VerifyCRC32C(body, sum); err != nil {
		t.Fatal(err)
	}
	if err := sep54.VerifyCRC32C(body, ""); err == nil || !strings.Contains(err.Error(), "no stored") {
		t.Fatalf("missing checksum: %v", err)
	}
	if err := sep54.VerifyCRC32C(append(body, '!'), sum); err == nil {
		t.Fatal("mismatch accepted")
	}
	// S3 formats CRC32C as base64 of the big-endian value; "" hashes to 0.
	if got := sep54.CRC32C(nil); got != "AAAAAA==" {
		t.Fatalf("CRC32C(nil) = %s", got)
	}
}

func TestHeaderSelfHashIsSHA256OfHeaderXDR(t *testing.T) {
	lcm := sep54test.SyntheticLCM(42, xdr.Hash{}, 0)
	b, err := lcm.V2.LedgerHeader.Header.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if xdr.Hash(sha256.Sum256(b)) != lcm.LedgerHash() {
		t.Fatal("synthetic ledgers must hash like stellar-core")
	}
}

// A frame that declares more than MaxDecodedSize is refused before anything
// is allocated (zstd WithDecoderMaxMemory).
func TestDecompressRefusesOversizedFrames(t *testing.T) {
	frame := func(size uint64) []byte {
		// Magic, single-segment descriptor with an 8-byte content size, then
		// one last RLE block of 1000 zero bytes.
		f := []byte{0x28, 0xB5, 0x2F, 0xFD, 0xE0}
		f = binary.LittleEndian.AppendUint64(f, size)
		return append(f, 0x43, 0x1F, 0x00, 0x00)
	}
	if out, err := sep54.Decompress(frame(1000)); err != nil || len(out) != 1000 {
		t.Fatalf("well-formed frame: %d bytes, %v", len(out), err)
	}
	_, err := sep54.Decompress(frame(sep54.MaxDecodedSize + 1))
	if !errors.Is(err, zstd.ErrDecoderSizeExceeded) {
		t.Fatalf("oversized frame: %v", err)
	}
	if _, _, err := sep54.DecodeLedger(1, frame(1<<40)); !sep54.IsInvalid(err) {
		t.Fatalf("DecodeLedger of an oversized frame: %v", err)
	}
}
