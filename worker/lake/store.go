package lake

import (
	"cmp"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"time"

	"stellar-explorer/worker/sep54"
)

// store reads the ledger objects straight from the partition directories:
// <s3root>/<bucket>/<prefix>/<partition>/<file>. Names that are not
// canonical SEP-54 ledger objects (the manifest, .sgwtmp/, .vgwlocks/,
// linkat temporaries) are ignored.
type store struct {
	prefixDir string
	prefix    string
}

// partitions returns the start ledgers of the partition directories, ascending.
func (s store) partitions() ([]uint32, error) {
	entries, err := os.ReadDir(s.prefixDir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var starts []uint32
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		if start, ok := sep54.ParsePartitionName(e.Name()); ok {
			starts = append(starts, start)
		}
	}
	slices.SortFunc(starts, cmp.Compare)
	return starts, nil
}

// ledgers returns the ledgers stored in the partition starting at start, ascending.
func (s store) ledgers(start uint32) ([]uint32, error) {
	entries, err := os.ReadDir(filepath.Join(s.prefixDir, sep54.PartitionName(start)))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var seqs []uint32
	for _, e := range entries {
		if !e.Type().IsRegular() {
			continue
		}
		if seq, ok := sep54.ParseFileName(e.Name()); ok && partitionStart(seq) == start {
			seqs = append(seqs, seq)
		}
	}
	slices.SortFunc(seqs, cmp.Compare)
	return seqs, nil
}

func (s store) path(seq uint32) string {
	return filepath.Join(s.prefixDir, sep54.PartitionName(seq), sep54.FileName(seq))
}

func (s store) key(seq uint32) string { return sep54.Key(s.prefix, seq) }

// bounds returns the oldest and newest stored ledgers and the tip's mtime;
// ok is false when no ledger object exists.
func (s store) bounds() (oldest, tip uint32, tipMod time.Time, ok bool, err error) {
	parts, err := s.partitions()
	if err != nil {
		return 0, 0, time.Time{}, false, err
	}
	for i := len(parts) - 1; i >= 0 && !ok; i-- {
		seqs, err := s.ledgers(parts[i])
		if err != nil {
			return 0, 0, time.Time{}, false, err
		}
		if len(seqs) > 0 {
			tip, ok = seqs[len(seqs)-1], true
		}
	}
	if !ok {
		return 0, 0, time.Time{}, false, nil
	}
	for _, p := range parts {
		seqs, err := s.ledgers(p)
		if err != nil {
			return 0, 0, time.Time{}, false, err
		}
		if len(seqs) > 0 {
			oldest = seqs[0]
			break
		}
	}
	fi, err := os.Stat(s.path(tip))
	if err != nil {
		return 0, 0, time.Time{}, false, err
	}
	return oldest, tip, fi.ModTime(), true, nil
}

// newest returns up to k stored ledgers, newest first.
func (s store) newest(k int) ([]uint32, error) {
	parts, err := s.partitions()
	if err != nil {
		return nil, err
	}
	var out []uint32
	for i := len(parts) - 1; i >= 0 && len(out) < k; i-- {
		seqs, err := s.ledgers(parts[i])
		if err != nil {
			return nil, err
		}
		for j := len(seqs) - 1; j >= 0 && len(out) < k; j-- {
			out = append(out, seqs[j])
		}
	}
	return out, nil
}

// partitionStart returns the first ledger of the partition holding seq.
func partitionStart(seq uint32) uint32 {
	const size = sep54.LedgersPerFile * sep54.FilesPerPartition
	return seq / size * size
}
