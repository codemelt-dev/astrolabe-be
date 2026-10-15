package bootstrap

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// Origin is state/bootstrap.json on the lake Volume:
// where the archive starts. It is written once, by the first bootstrap, and
// never rewritten: `--force` and later runs leave it alone.
//
//   - The archiver starts an empty archive bucket at ArchiveOrigin, linked
//     by CheckpointHash.
//   - The launcher starts Galexie at ArchiveOrigin when the lake and the
//     archive are both empty.
//   - The gap check scans [ArchiveOrigin, archive tip].
type Origin struct {
	// Network is the network passphrase.
	Network string `json:"network"`
	// Checkpoint is C, the ledger the state was loaded at (for
	// `bootstrap --history-only --at A`, A − 1).
	Checkpoint uint32 `json:"checkpoint"`
	// CheckpointHash is ledger C's hash, lower-case hex.
	CheckpointHash string `json:"checkpoint_hash"`
	// ArchiveOrigin is C + 1, the first ledger of the archive.
	ArchiveOrigin uint32 `json:"archive_origin"`
}

// OriginFile is the path of bootstrap.json under the lake directory.
func OriginFile(lakeDir string) string { return filepath.Join(lakeDir, "state", "bootstrap.json") }

// NewOrigin describes a bootstrap at ledger c with hash h.
func NewOrigin(network string, c uint32, h xdr.Hash) Origin {
	return Origin{Network: network, Checkpoint: c, CheckpointHash: hex.EncodeToString(h[:]), ArchiveOrigin: c + 1}
}

// Hash returns CheckpointHash decoded.
func (o Origin) Hash() (xdr.Hash, error) {
	var h xdr.Hash
	b, err := hex.DecodeString(o.CheckpointHash)
	if err != nil || len(b) != len(h) {
		return h, fmt.Errorf("checkpoint_hash %q is not a 32-byte hex hash", o.CheckpointHash)
	}
	copy(h[:], b)
	return h, nil
}

func (o Origin) validate() error {
	if o.Network == "" {
		return errors.New("network is empty")
	}
	if o.Checkpoint == 0 || o.ArchiveOrigin != o.Checkpoint+1 {
		return fmt.Errorf("archive_origin %d must be checkpoint %d + 1", o.ArchiveOrigin, o.Checkpoint)
	}
	_, err := o.Hash()
	return err
}

// ErrNoOrigin means bootstrap.json does not exist: `worker bootstrap` has
// not run on this lake, or the file was lost (recreate it with
// `worker bootstrap --recreate-origin`).
var ErrNoOrigin = fmt.Errorf("state/bootstrap.json does not exist: %w", fs.ErrNotExist)

// ReadOrigin reads and validates bootstrap.json.
func ReadOrigin(lakeDir string) (Origin, error) {
	path := OriginFile(lakeDir)
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return Origin{}, fmt.Errorf("%s: %w", path, ErrNoOrigin)
	}
	if err != nil {
		return Origin{}, err
	}
	var o Origin
	if err := json.Unmarshal(b, &o); err != nil {
		return Origin{}, fmt.Errorf("%s: %w", path, err)
	}
	if err := o.validate(); err != nil {
		return Origin{}, fmt.Errorf("%s: %w", path, err)
	}
	return o, nil
}

// WriteOriginIfAbsent creates bootstrap.json atomically unless it exists.
// It returns whether it wrote the file and,
// if not, the existing content. The file is fsynced before it is linked
// into place, and the directory after.
func WriteOriginIfAbsent(lakeDir string, o Origin) (written bool, existing Origin, err error) {
	if err := o.validate(); err != nil {
		return false, Origin{}, err
	}
	path := OriginFile(lakeDir)
	if cur, err := ReadOrigin(lakeDir); err == nil {
		return false, cur, nil
	} else if !errors.Is(err, ErrNoOrigin) {
		return false, Origin{}, err
	}
	b, err := json.MarshalIndent(o, "", "  ")
	if err != nil {
		return false, Origin{}, err
	}
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".bootstrap-*.json")
	if err != nil {
		return false, Origin{}, err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(append(b, '\n')); err != nil {
		tmp.Close()
		return false, Origin{}, err
	}
	if err := tmp.Chmod(0o644); err != nil {
		tmp.Close()
		return false, Origin{}, err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return false, Origin{}, err
	}
	if err := tmp.Close(); err != nil {
		return false, Origin{}, err
	}
	// link(2) fails if the name exists: create-if-absent without a race.
	if err := os.Link(tmp.Name(), path); err != nil {
		if errors.Is(err, fs.ErrExist) {
			cur, rerr := ReadOrigin(lakeDir)
			return false, cur, rerr
		}
		return false, Origin{}, err
	}
	if d, err := os.Open(dir); err == nil {
		err = d.Sync()
		d.Close()
		if err != nil {
			return true, Origin{}, fmt.Errorf("fsync %s: %w", dir, err)
		}
	}
	return true, Origin{}, nil
}
