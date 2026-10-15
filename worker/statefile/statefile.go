// Package statefile writes the small files the oneshot commands leave for
// others: the launcher's START env file, the watchdog's state and the gap
// check's result (read by ingest's metrics). Writes are atomic: a reader
// sees the old content or the new, never a partial file.
package statefile

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// Write replaces path with data through a temporary file in the same
// directory (fsynced), a rename, and an fsync of the directory.
func Write(path string, data []byte, mode os.FileMode) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Chmod(mode); err != nil {
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
	if d, err := os.Open(dir); err == nil {
		err = d.Sync()
		d.Close()
		return err
	}
	return nil
}

// WriteJSON writes v as indented JSON, world-readable.
func WriteJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return Write(path, append(b, '\n'), 0o644)
}

// ReadJSON reads a file written by WriteJSON into v.
func ReadJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}
