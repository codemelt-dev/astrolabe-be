package statefile

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWriteReplacesAtomically(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	type state struct{ N int }
	for n := range 3 {
		if err := WriteJSON(path, state{N: n}); err != nil {
			t.Fatal(err)
		}
		var got state
		if err := ReadJSON(path, &got); err != nil || got.N != n {
			t.Fatalf("read %+v %v", got, err)
		}
	}
	fi, err := os.Stat(path)
	if err != nil || fi.Mode().Perm() != 0o644 {
		t.Fatalf("mode %v %v", fi, err)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 1 {
		t.Fatalf("temporary files left: %v", entries)
	}
	if err := Write(filepath.Join(dir, "missing", "x"), []byte("x"), 0o600); err == nil {
		t.Fatal("wrote into a missing directory")
	}
	var v struct{}
	if err := ReadJSON(filepath.Join(dir, "absent.json"), &v); !os.IsNotExist(err) {
		t.Fatalf("absent file: %v", err)
	}
}
