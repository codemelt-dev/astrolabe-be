package bootstrap

import (
	"errors"
	"os"
	"sync"
	"testing"

	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"
)

func TestOriginFile(t *testing.T) {
	dir := newLakeDir(t)
	if _, err := ReadOrigin(dir); !errors.Is(err, ErrNoOrigin) {
		t.Fatalf("missing file: %v", err)
	}
	o := NewOrigin(network.PublicNetworkPassphrase, 63, xdr.Hash{1, 2, 3})
	if o.ArchiveOrigin != 64 {
		t.Fatalf("archive origin %d", o.ArchiveOrigin)
	}
	// Concurrent writers: exactly one creates the file, all see its content.
	var wg sync.WaitGroup
	var mu sync.Mutex
	written := 0
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mine := NewOrigin(network.PublicNetworkPassphrase, 63+uint32(i)*64, xdr.Hash{byte(i)})
			w, existing, err := WriteOriginIfAbsent(dir, mine)
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if w {
				written++
			} else if existing.Checkpoint == 0 {
				t.Error("not written and no existing content")
			}
		}()
	}
	wg.Wait()
	if written != 1 {
		t.Fatalf("%d writers created the file", written)
	}
	got, err := ReadOrigin(dir)
	if err != nil {
		t.Fatal(err)
	}
	if h, err := got.Hash(); err != nil || got.ArchiveOrigin != got.Checkpoint+1 || h[0] != byte((got.Checkpoint-63)/64) {
		t.Fatalf("origin %+v", got)
	}
	fi, err := os.Stat(OriginFile(dir))
	if err != nil || fi.Mode().Perm() != 0o644 {
		t.Fatalf("mode %v %v", fi.Mode(), err)
	}
	entries, _ := os.ReadDir(dir + "/state")
	if len(entries) != 1 {
		t.Fatalf("temporary files left: %v", entries)
	}

	for name, content := range map[string]string{
		"not json":       "{",
		"origin not C+1": `{"network":"n","checkpoint":63,"checkpoint_hash":"` + hashHex(xdr.Hash{}) + `","archive_origin":65}`,
		"bad hash":       `{"network":"n","checkpoint":63,"checkpoint_hash":"xyz","archive_origin":64}`,
		"no network":     `{"checkpoint":63,"checkpoint_hash":"` + hashHex(xdr.Hash{}) + `","archive_origin":64}`,
	} {
		d := newLakeDir(t)
		if err := os.WriteFile(OriginFile(d), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadOrigin(d); err == nil {
			t.Errorf("%s: accepted", name)
		}
		if _, _, err := WriteOriginIfAbsent(d, o); err == nil {
			t.Errorf("%s: write over an invalid file succeeded", name)
		}
	}
}
