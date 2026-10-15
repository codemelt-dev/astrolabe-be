package lake

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// isMountpoint reports whether path is exactly the mount point of the
// filesystem containing it.
func isMountpoint(path string) (bool, error) {
	real, err := filepath.EvalSymlinks(path)
	if err != nil {
		return false, err
	}
	var st unix.Statfs_t
	if err := unix.Statfs(real, &st); err != nil {
		return false, err
	}
	return unix.ByteSliceToString(st.Mntonname[:]) == real, nil
}

// syncFilesystem stands in for Linux syncfs(2) on macOS (development only):
// fsync(2) every file and directory under root, then one F_FULLFSYNC
// ((*os.File).Sync on darwin) to flush the drive cache.
func syncFilesystem(root string) error {
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && !d.Type().IsRegular() {
			return nil
		}
		f, err := os.Open(p)
		if err != nil {
			return err
		}
		defer f.Close()
		if err := unix.Fsync(int(f.Fd())); err != nil {
			return fmt.Errorf("fsync %s: %w", p, err)
		}
		return nil
	})
	if err != nil {
		return err
	}
	f, err := os.Open(root)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// bootID identifies the current boot session.
func bootID() (string, error) {
	id, err := unix.Sysctl("kern.bootsessionuuid")
	if err != nil {
		return "", err
	}
	if id == "" {
		return "", errors.New("empty kern.bootsessionuuid")
	}
	return id, nil
}
