// Package sep54 holds the SEP-54 ledger-object conventions shared by the lake
// and ingest:
//
//   - key.go: the object key codec for our fixed layout;
//   - manifest.go: the store manifest and LoadSchema;
//   - decode.go: full decode, header-only decode and validation;
//   - source.go: the lake, archive and AWS-lake sources, each with its own
//     explicit S3 client;
//   - tip.go, tipclient.go: the lake's tip protocol and its client;
//   - errors.go: NotFound, unavailable and invalid, the three ways a read can
//     fail.
package sep54

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// The layout is fixed (a new layout needs a new store) and equals the AWS
// Open Data public lake.
const (
	LedgersPerFile    uint32 = 1
	FilesPerPartition uint32 = 64000
	// FileExtension is the SDK compressor name, used in object keys. The
	// manifest says "zstd"; keys say "zst".
	FileExtension = "zst"

	partitionSize = LedgersPerFile * FilesPerPartition
	fileSuffix    = ".xdr." + FileExtension
)

// PartitionName returns the partition directory of ledger seq, e.g.
// "FFFF05FF--64000-127999" for 123456.
func PartitionName(seq uint32) string {
	start := (seq / partitionSize) * partitionSize
	end := uint64(start) + uint64(partitionSize) - 1
	if end > math.MaxUint32 {
		end = math.MaxUint32
	}
	return fmt.Sprintf("%08X--%d-%d", math.MaxUint32-start, start, end)
}

// FileName returns the object file name of ledger seq, e.g.
// "FFFE1DBF--123456.xdr.zst".
func FileName(seq uint32) string {
	return fmt.Sprintf("%08X--%d%s", math.MaxUint32-seq, seq, fileSuffix)
}

// ObjectPath returns the key of ledger seq relative to the store prefix.
func ObjectPath(seq uint32) string {
	return PartitionName(seq) + "/" + FileName(seq)
}

// Key returns the full object key (relative to the bucket) of ledger seq
// under prefix. An empty prefix means the store is the bucket root.
func Key(prefix string, seq uint32) string {
	if prefix == "" {
		return ObjectPath(seq)
	}
	return prefix + "/" + ObjectPath(seq)
}

// ParseFileName returns the ledger sequence of a canonical object file name.
// Only names produced by FileName are accepted (upper-case reverse hex that
// matches the decimal sequence, no leading zeros, our extension).
func ParseFileName(name string) (uint32, bool) {
	rest, ok := strings.CutSuffix(name, fileSuffix)
	if !ok || len(rest) < 11 || rest[8] != '-' || rest[9] != '-' {
		return 0, false
	}
	seq, err := strconv.ParseUint(rest[10:], 10, 32)
	if err != nil {
		return 0, false
	}
	if FileName(uint32(seq)) != name {
		return 0, false
	}
	return uint32(seq), true
}

// ParsePartitionName reports whether name is a canonical partition
// directory name and returns the first ledger of the partition.
func ParsePartitionName(name string) (uint32, bool) {
	if len(name) < 11 || name[8] != '-' || name[9] != '-' {
		return 0, false
	}
	startStr, _, ok := strings.Cut(name[10:], "-")
	if !ok {
		return 0, false
	}
	start, err := strconv.ParseUint(startStr, 10, 32)
	if err != nil {
		return 0, false
	}
	if PartitionName(uint32(start)) != name {
		return 0, false
	}
	return uint32(start), true
}

// ParseObjectPath returns the ledger sequence of a canonical key relative to
// the store prefix ("<partition>/<file>").
func ParseObjectPath(p string) (uint32, bool) {
	part, file, ok := strings.Cut(p, "/")
	if !ok {
		return 0, false
	}
	seq, ok := ParseFileName(file)
	if !ok || PartitionName(seq) != part {
		return 0, false
	}
	return seq, true
}

// ParseKey returns the ledger sequence of a canonical ledger object key
// (relative to the bucket) under prefix. Anything else, including the
// manifest and VersityGW's internal directories, is not a ledger key.
func ParseKey(prefix, key string) (uint32, bool) {
	rel := key
	if prefix != "" {
		var ok bool
		rel, ok = strings.CutPrefix(key, prefix+"/")
		if !ok {
			return 0, false
		}
	}
	return ParseObjectPath(rel)
}
