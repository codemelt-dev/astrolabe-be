package lake

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// isMountpoint reports whether path is exactly a mount point, using
// /proc/self/mountinfo (so a bind mount or an empty directory on the root
// disk is told apart from the lake Volume).
func isMountpoint(path string) (bool, error) {
	real, err := filepath.EvalSymlinks(path)
	if err != nil {
		return false, err
	}
	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return false, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		// Field 5 is the mount point, with space, tab, newline and backslash
		// octal-escaped.
		fields := strings.Fields(sc.Text())
		if len(fields) >= 5 && unescapeMountinfo(fields[4]) == real {
			return true, nil
		}
	}
	return false, sc.Err()
}

func unescapeMountinfo(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+4 <= len(s) {
			if v, err := strconv.ParseUint(s[i+1:i+4], 8, 8); err == nil {
				b.WriteByte(byte(v))
				i += 3
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

// syncFilesystem flushes the whole lake filesystem with syncfs(2), which
// (since Linux 5.8) reports writeback errors.
func syncFilesystem(root string) error {
	f, err := os.Open(root)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := unix.Syncfs(int(f.Fd())); err != nil {
		return fmt.Errorf("syncfs %s: %w", root, err)
	}
	return nil
}

// bootID identifies the current boot; it changes after every reboot, power
// loss or kernel panic.
func bootID() (string, error) {
	b, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return "", err
	}
	id := strings.TrimSpace(string(b))
	if id == "" {
		return "", errors.New("empty /proc/sys/kernel/random/boot_id")
	}
	return id, nil
}
