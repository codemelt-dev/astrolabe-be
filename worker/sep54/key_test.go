package sep54

import (
	"math"
	"testing"

	"github.com/stellar/go-stellar-sdk/support/datastore"
)

var sdkSchema = datastore.DataStoreSchema{LedgersPerFile: LedgersPerFile, FilesPerPartition: FilesPerPartition}

func TestKeyMatchesSDK(t *testing.T) {
	seqs := []uint32{0, 1, 2, 63999, 64000, 64001, 123456, 53312000, 60_000_000, math.MaxUint32 - 64000, math.MaxUint32 - 1, math.MaxUint32}
	for _, seq := range seqs {
		want := sdkSchema.GetObjectKeyFromSequenceNumber(seq)
		if got := ObjectPath(seq); got != want {
			t.Errorf("ObjectPath(%d) = %q, SDK = %q", seq, got, want)
		}
		got, ok := ParseObjectPath(want)
		if !ok || got != seq {
			t.Errorf("ParseObjectPath(%q) = %d, %v", want, got, ok)
		}
		if got, ok := ParseKey("ledgers/pubnet", "ledgers/pubnet/"+want); !ok || got != seq {
			t.Errorf("ParseKey(prefixed %q) = %d, %v", want, got, ok)
		}
		if got, ok := ParseKey("", want); !ok || got != seq {
			t.Errorf("ParseKey(unprefixed %q) = %d, %v", want, got, ok)
		}
	}
}

func TestReadmeExample(t *testing.T) {
	const want = "FFFF05FF--64000-127999/FFFE1DBF--123456.xdr.zst" // galexie/README.md §0.2
	if got := ObjectPath(123456); got != want {
		t.Fatalf("ObjectPath(123456) = %q", got)
	}
	if got := Key("ledgers/pubnet", 123456); got != "ledgers/pubnet/"+want {
		t.Fatalf("Key = %q", got)
	}
	if start, ok := ParsePartitionName("FFFF05FF--64000-127999"); !ok || start != 64000 {
		t.Fatalf("ParsePartitionName = %d, %v", start, ok)
	}
}

func TestParseRejectsNonCanonical(t *testing.T) {
	const prefix = "ledgers/pubnet"
	bad := []string{
		"",
		".config.json",
		prefix + "/.config.json",
		prefix + "/.sgwtmp/x",
		prefix + "/FFFF05FF--64000-127999",
		prefix + "/FFFF05FF--64000-127999/",
		prefix + "/FFFF05FF--64000-127999/FFFE1DBF--123456.xdr.zstd", // wrong extension
		prefix + "/FFFF05FF--64000-127999/fffe1dbf--123456.xdr.zst",  // lower-case hex
		prefix + "/FFFF05FF--64000-127999/FFFE1DBF--0123456.xdr.zst", // leading zero
		prefix + "/FFFF05FF--64000-127999/FFFE1DBE--123456.xdr.zst",  // hex mismatch
		prefix + "/FFFF05FF--64000-127999/FFFE1DBF--123456-123456.xdr.zst",
		prefix + "/FFFF05FE--64000-127999/FFFE1DBF--123456.xdr.zst",     // bad partition hex
		prefix + "/FFFF05FF--0-63999/FFFE1DBF--123456.xdr.zst",          // wrong partition
		prefix + "/FFFF05FF--64000-127999/FFFE1DBF--123456.xdr.zst/x",   // nested
		"other/FFFF05FF--64000-127999/FFFE1DBF--123456.xdr.zst",         // other prefix
		prefix + "x/FFFF05FF--64000-127999/FFFE1DBF--123456.xdr.zst",    // prefix is not a path prefix
		prefix + "/FFFF05FF--64000-127999/FFFE1DBF--4294967296.xdr.zst", // overflow
	}
	for _, k := range bad {
		if seq, ok := ParseKey(prefix, k); ok {
			t.Errorf("ParseKey(%q) accepted as %d", k, seq)
		}
	}
	for _, n := range []string{"FFFF05FF--64000", "FFFF05FF--64000-127998", "ffff05ff--64000-127999", "FFFF05FF-64000-127999", "x"} {
		if _, ok := ParsePartitionName(n); ok {
			t.Errorf("ParsePartitionName(%q) accepted", n)
		}
	}
}

func FuzzParseKey(f *testing.F) {
	f.Add("ledgers/pubnet/FFFF05FF--64000-127999/FFFE1DBF--123456.xdr.zst")
	f.Add("ledgers/pubnet/.config.json")
	f.Fuzz(func(t *testing.T, key string) {
		seq, ok := ParseKey("ledgers/pubnet", key)
		if ok && Key("ledgers/pubnet", seq) != key {
			t.Fatalf("ParseKey(%q) = %d but Key(%d) = %q", key, seq, seq, Key("ledgers/pubnet", seq))
		}
	})
}
