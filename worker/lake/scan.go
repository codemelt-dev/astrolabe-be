package lake

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/klauspost/compress/zstd"
)

// scanObjects is K, the number of newest objects verified after a reboot.
const scanObjects = 100

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

type scanResult struct {
	checked int
	deleted []uint32 // newest first
	holes   []uint32 // deleted objects that sat below a good one
}

// checkBoot runs the checksum scan only when the boot id differs from the
// one recorded in state/boot-id, i.e. after a reboot, power loss or kernel
// panic, then records the current boot id. After a plain crash on the same
// boot, the startup syncfs alone makes every linked object durable.
func (l *Lake) checkBoot(ctx context.Context) (scanResult, error) {
	cur, err := l.opts.BootID()
	if err != nil {
		return scanResult{}, fmt.Errorf("read boot id: %w", err)
	}
	path := filepath.Join(l.stateDir, "boot-id")
	prev, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return scanResult{}, err
	}
	if strings.TrimSpace(string(prev)) == cur {
		l.log.Info("same boot as the last run; skipping the checksum scan", "boot_id", cur)
		return scanResult{}, nil
	}
	l.log.Info("new boot; verifying the newest objects", "boot_id", cur, "previous", strings.TrimSpace(string(prev)), "objects", scanObjects)
	res, err := l.scan(ctx)
	if err != nil {
		return res, err
	}
	if len(res.deleted) > 0 {
		// Make the deletions durable before recording the boot id.
		if err := l.opts.Syncfs(l.cfg.Dir); err != nil {
			return res, fmt.Errorf("syncfs after deleting corrupt objects: %w", err)
		}
	}
	if err := writeFileAtomic(path, []byte(cur+"\n")); err != nil {
		return res, fmt.Errorf("record boot id: %w", err)
	}
	return res, nil
}

// scan verifies the newest scanObjects ledger objects and deletes, through
// the backend, every one that is corrupt. A corrupt suffix is simply
// re-exported by Galexie; a corrupt object below a good one becomes a hole to
// repair with scan-and-fill (galexie/README.md §7.4.8).
func (l *Lake) scan(ctx context.Context) (scanResult, error) {
	var res scanResult
	seqs, err := l.store.newest(scanObjects)
	if err != nil {
		return res, fmt.Errorf("list ledger objects: %w", err)
	}
	dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(maxDecodedObject))
	if err != nil {
		return res, err
	}
	defer dec.Close()

	sawGood := false
	for _, seq := range seqs { // newest first
		reason, err := l.verify(ctx, dec, seq)
		if err != nil {
			return res, err
		}
		res.checked++
		if reason == "" {
			sawGood = true
			continue
		}
		key := l.store.key(seq)
		l.log.Error("corrupt ledger object after reboot; deleting it", "ledger", seq, "reason", reason)
		if _, err := l.px.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(l.bucket), Key: aws.String(key)}); err != nil {
			return res, fmt.Errorf("delete corrupt ledger %d: %w", seq, err)
		}
		res.deleted = append(res.deleted, seq)
		if sawGood {
			res.holes = append(res.holes, seq)
			l.log.Error("the deleted ledger is below a good one: this is a hole; repair it with scan-and-fill (galexie/README.md §7.4.8)", "ledger", seq)
		}
	}
	return res, nil
}

// maxDecodedObject bounds zstd decoder memory; ledgers decompress to ~2.4 MB.
const maxDecodedObject = 256 << 20

// verify returns a non-empty reason if ledger seq is corrupt: its stored
// CRC32C is missing or does not match, its zstd frames (including their
// content checksums) do not decode, or it is empty or truncated.
func (l *Lake) verify(ctx context.Context, dec *zstd.Decoder, seq uint32) (string, error) {
	key := l.store.key(seq)
	head, err := l.px.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket:       aws.String(l.bucket),
		Key:          aws.String(key),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return fmt.Sprintf("unreadable metadata: %v", err), nil
	}
	f, err := os.Open(l.store.path(seq))
	if err != nil {
		return fmt.Sprintf("open: %v", err), nil
	}
	defer f.Close()
	return verifyObject(f, aws.ToString(head.ChecksumCRC32C), dec), nil
}

// verifyObject checks one object's bytes against its stored base64 CRC32C.
func verifyObject(r io.Reader, storedCRC32C string, dec *zstd.Decoder) string {
	if storedCRC32C == "" {
		return "no stored CRC32C checksum"
	}
	crc := crc32.New(castagnoli)
	counted := &countingReader{r: io.TeeReader(r, crc)}
	var decoded int64
	derr := dec.Reset(counted)
	if derr == nil {
		decoded, derr = io.Copy(io.Discard, dec)
	}
	// The decoder reads to EOF looking for further frames; make sure the
	// checksum covers every byte either way.
	if _, err := io.Copy(io.Discard, counted); err != nil {
		return fmt.Sprintf("read: %v", err)
	}
	switch {
	case counted.n == 0:
		return "empty object"
	case derr != nil:
		return fmt.Sprintf("zstd: %v", derr)
	case decoded == 0:
		return "no decompressed data"
	}
	var sum [4]byte
	binary.BigEndian.PutUint32(sum[:], crc.Sum32())
	if got := base64.StdEncoding.EncodeToString(sum[:]); got != storedCRC32C {
		return fmt.Sprintf("CRC32C mismatch: stored %s, computed %s", storedCRC32C, got)
	}
	return ""
}

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// writeFileAtomic replaces path with data durably: temp file, fsync,
// rename, fsync of the directory.
func writeFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
