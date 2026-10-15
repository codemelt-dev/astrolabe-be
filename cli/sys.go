package main

// Host helpers: commands, files, packages, users, systemd units, downloads.
// Everything that changes the host goes through these, so it is logged and
// idempotent the same way everywhere.

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// run runs a command, logs it with its output, and returns stdout and stderr
// combined. An error carries the last lines of the output.
func (e *env) run(name string, args ...string) (string, error) { return e.runInput("", name, args...) }

func (e *env) runInput(stdin, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(e.ctx, name, args...)
	// An interrupted run stops the command as systemd would: SIGTERM, then
	// SIGKILL if it is still there a minute later.
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = time.Minute
	cmd.Env = append(os.Environ(), "DEBIAN_FRONTEND=noninteractive", "LC_ALL=C")
	cmd.Dir = "/" // runuser keeps the directory, which the other user may not be able to read
	if stdin != "" {
		cmd.Stdin = strings.NewReader(stdin)
	}
	var out bytes.Buffer // and the log as it comes: tail -f follows a long command
	w := io.MultiWriter(&out, e.log)
	cmd.Stdout, cmd.Stderr = w, w
	fmt.Fprintf(e.log, "$ %s %s\n", name, strings.Join(args, " "))
	start := time.Now()
	err := cmd.Run()
	fmt.Fprintf(e.log, "  (%s, %v)\n", time.Since(start).Round(time.Millisecond), cmd.ProcessState)
	if err != nil {
		return out.String(), fmt.Errorf("%s: %w: %s", name, err, tail(out.String(), 3))
	}
	return strings.TrimSpace(out.String()), nil
}

// probe runs a read-only command without logging it (checks run often).
func probe(name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	cmd.Dir = "/" // as in runInput: psql would warn about it in the output
	out, err := cmd.CombinedOutput()
	return strings.TrimSpace(string(out)), err
}

func tail(s string, n int) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, " | ")
}

// Files

// file is one managed file: its content, mode and "user:group" owner.
type file struct {
	path  string
	data  []byte
	mode  os.FileMode
	owner string
}

// matches reports whether the file is already in place.
func (f file) matches() bool {
	fi, err := os.Lstat(f.path)
	if err != nil || !fi.Mode().IsRegular() || fi.Mode().Perm() != f.mode || !ownedBy(fi, f.owner) {
		return false
	}
	cur, err := os.ReadFile(f.path)
	return err == nil && bytes.Equal(cur, f.data)
}

// write installs the file atomically unless it already matches, and notes
// the change.
func (e *env) write(f file) (bool, error) {
	if f.matches() {
		return false, nil
	}
	if err := os.MkdirAll(filepath.Dir(f.path), 0o755); err != nil {
		return false, err
	}
	tmp, err := os.CreateTemp(filepath.Dir(f.path), "."+filepath.Base(f.path)+".*")
	if err != nil {
		return false, err
	}
	defer os.Remove(tmp.Name())
	uid, gid, err := lookupOwner(f.owner)
	if err == nil {
		_, err = tmp.Write(f.data)
	}
	if err == nil {
		err = tmp.Chmod(f.mode)
	}
	if err == nil {
		err = tmp.Chown(uid, gid)
	}
	if err == nil {
		err = tmp.Sync()
	}
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp.Name(), f.path)
	}
	if err != nil {
		return false, fmt.Errorf("write %s: %w", f.path, err)
	}
	fmt.Fprintf(e.log, "wrote %s (%s %o)\n", f.path, f.owner, f.mode)
	e.note("wrote %s", f.path)
	return true, nil
}

// firstMismatch returns the path of the first file that isn't in place.
func firstMismatch(files ...file) string {
	for _, f := range files {
		if !f.matches() {
			return f.path
		}
	}
	return ""
}

// writeAll writes every file and reports whether any changed.
func (e *env) writeAll(files ...file) (bool, error) {
	changed := false
	for _, f := range files {
		c, err := e.write(f)
		if err != nil {
			return changed, err
		}
		changed = changed || c
	}
	return changed, nil
}

// place writes files like writeAll, but never replaces one it can't
// recognise: without this app's header, and not a package's unmodified
// default (a dpkg conffile, or a postinst's copy of a sample: postinstDefaults).
func (e *env) place(files ...file) (bool, error) {
	for _, f := range files {
		b, err := os.ReadFile(f.path)
		if err == nil && !f.matches() && !bytes.Contains(b[:min(len(b), 300)], []byte("("+tool()+",")) && !pristine(f.path, b) {
			return false, fmt.Errorf("%s isn't managed by %s: compare it with the repository's version, then move it aside", f.path, tool())
		}
	}
	return e.writeAll(files...)
}

func pristine(path string, b []byte) bool {
	if sample, ok := postinstDefaults[path]; ok {
		if d, err := os.ReadFile(sample); err == nil && bytes.Equal(d, b) {
			return true
		}
	}
	owner, err := probe("dpkg-query", "-S", path)
	pkg, _, found := strings.Cut(owner, ": ")
	list, _ := probe("dpkg-query", "-W", "-f=${Conffiles}", pkg)
	sum := md5.Sum(b)
	return err == nil && found && hasConffile(list, path, hex.EncodeToString(sum[:]))
}

// hasConffile reports whether dpkg's Conffiles list (" <path> <md5>" lines,
// trimmed by probe) records path with that md5.
func hasConffile(list, path, md5 string) bool {
	for _, l := range strings.Split(list, "\n") {
		if f := strings.Fields(l); len(f) >= 2 && f[0] == path && f[1] == md5 {
			return true
		}
	}
	return false
}

// symlink points link at target, replacing only a symlink.
func (e *env) symlink(target, link string) error {
	if fi, err := os.Lstat(link); err == nil && fi.Mode()&os.ModeSymlink == 0 {
		return fmt.Errorf("%s exists and is not a symlink", link)
	} else if cur, _ := os.Readlink(link); cur != target {
		e.note("%s → %s", link, target)
		_, err = e.run("ln", "-sfn", target, link)
		return err
	}
	return nil
}

// dir is one managed directory.
type dir struct {
	path  string
	mode  os.FileMode
	owner string
}

func (d dir) matches() bool {
	fi, err := os.Lstat(d.path)
	return err == nil && fi.IsDir() && fi.Mode().Perm() == d.mode && ownedBy(fi, d.owner)
}

func (e *env) mkdir(d dir) error {
	if d.matches() {
		return nil
	}
	uid, gid, err := lookupOwner(d.owner)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(d.path, d.mode); err != nil {
		return err
	}
	if err := os.Chmod(d.path, d.mode); err != nil {
		return err
	}
	if err := os.Chown(d.path, uid, gid); err != nil {
		return err
	}
	e.note("dir %s", d.path)
	return nil
}

func lookupOwner(owner string) (int, int, error) {
	u, g, _ := strings.Cut(owner, ":")
	uu, err := user.Lookup(u)
	if err != nil {
		return 0, 0, err
	}
	gg, err := user.LookupGroup(g)
	if err != nil {
		return 0, 0, err
	}
	uid, _ := strconv.Atoi(uu.Uid)
	gid, _ := strconv.Atoi(gg.Gid)
	return uid, gid, nil
}

func ownedBy(fi os.FileInfo, owner string) bool {
	uid, gid, err := lookupOwner(owner)
	if err != nil {
		return false
	}
	st, isUnix := fi.Sys().(*syscall.Stat_t)
	return isUnix && int(st.Uid) == uid && int(st.Gid) == gid
}

func sha256File(path string) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return ""
	}
	return hex.EncodeToString(h.Sum(nil))
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// Packages

// missing returns the packages that aren't installed.
func missing(pkgs ...string) []string {
	var out []string
	for _, p := range pkgs {
		if s, err := probe("dpkg-query", "-W", "-f=${Status}", p); err != nil || s != "install ok installed" {
			out = append(out, p)
		}
	}
	return out
}

// install installs the missing packages (apt-get update once per run).
func (e *env) install(pkgs ...string) error {
	todo := missing(pkgs...)
	if len(todo) == 0 {
		return nil
	}
	if !e.aptUpdated {
		// apt-get update never waits for the lists lock: let apt-daily's
		// update end first (a oneshot unit is "activating" until then).
		for i := 0; i < 120 && e.ctx.Err() == nil && unitIs("ActiveState", "apt-daily.service") == "activating"; i++ {
			time.Sleep(5 * time.Second)
		}
		if _, err := e.run("apt-get", "update", "-q"); err != nil {
			return err
		}
		e.aptUpdated = true
	}
	// The lock timeout waits for unattended-upgrades instead of failing.
	args := append([]string{"install", "-y", "-q", "-o", "Dpkg::Options::=--force-confold", "-o", "DPkg::Lock::Timeout=600"}, todo...)
	if _, err := e.run("apt-get", args...); err != nil {
		return err
	}
	e.note("installed %s", strings.Join(todo, " "))
	return nil
}

// Users

func userExists(name string) bool { _, err := user.Lookup(name); return err == nil }

// systemUser creates a system user and group without a login shell.
func (e *env) systemUser(name, home string) error {
	if userExists(name) {
		return nil
	}
	_, err := e.run("useradd", "--system", "--user-group", "--home-dir", home, "--no-create-home", "--shell", "/usr/sbin/nologin", name)
	if err == nil {
		e.note("user %s", name)
	}
	return err
}

// systemd

// service is a unit and the files whose change it must pick up: by reload
// when reload is true, otherwise by restart (only with --allow-restart once
// the unit runs). Binaries count as files: a new one is a restart.
type service struct {
	unit   string
	reload bool
	files  []string
}

func unitIs(prop, unit string) string {
	out, _ := probe("systemctl", "show", "-P", prop, unit)
	return out
}

func active(unit string) bool  { return unitIs("ActiveState", unit) == "active" }
func enabled(unit string) bool { s, _ := probe("systemctl", "is-enabled", unit); return s == "enabled" }

// stale reports whether any of the service's files changed after the unit
// last started or, for files a reload picks up, was reloaded by this tool.
func (s service) stale() bool {
	since := startedAt(s.unit)
	if fi, err := os.Stat(reloadStamp(s.unit)); err == nil && fi.ModTime().After(since) && s.reload {
		since = fi.ModTime()
	}
	for _, p := range s.files {
		if fi, err := os.Stat(p); err == nil && fi.ModTime().After(since) {
			return true
		}
	}
	return false
}

func startedAt(unit string) time.Time {
	out, _ := probe("systemctl", "show", "-P", "ActiveEnterTimestamp", "--timestamp=us+utc", unit)
	t, _ := time.Parse("Mon 2006-01-02 15:04:05.999999 MST", out)
	return t
}

func reloadStamp(unit string) string { return "/run/stellar-setup/" + unit + ".reloaded" }

// state is the service's check: enabled, running, and not stale.
func (s service) state() result {
	switch {
	case !enabled(s.unit):
		return change("%s not enabled", s.unit)
	case !active(s.unit):
		return change("%s not running", s.unit)
	case s.stale() && s.reload:
		return change("%s needs a reload", s.unit)
	case s.stale():
		return restart("%s needs a restart (--allow-restart)", s.unit)
	}
	return ok("%s running", s.unit)
}

// ensure enables and starts the unit, and makes a running one pick up
// changed files: reload, or restart when allowed.
func (e *env) ensure(s service) error {
	if !enabled(s.unit) {
		if _, err := e.run("systemctl", "enable", s.unit); err != nil {
			return err
		}
		e.note("enabled %s", s.unit)
	}
	switch {
	case !active(s.unit):
		if _, err := e.run("systemctl", "start", s.unit); err != nil {
			return err
		}
		e.note("started %s", s.unit)
	case s.stale() && s.reload:
		if _, err := e.run("systemctl", "reload", s.unit); err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(reloadStamp(s.unit)), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(reloadStamp(s.unit), nil, 0o644); err != nil {
			return err
		}
		now := time.Now()
		os.Chtimes(reloadStamp(s.unit), now, now)
		e.note("reloaded %s", s.unit)
	case s.stale() && e.allowRestart:
		if _, err := e.run("systemctl", "restart", s.unit); err != nil {
			return err
		}
		e.note("restarted %s", s.unit)
	}
	return nil
}

// daemonReload runs systemctl daemon-reload when changed is true.
func (e *env) daemonReload(changed bool) error {
	if !changed {
		return nil
	}
	_, err := e.run("systemctl", "daemon-reload")
	return err
}

// Downloads

// download fetches url to dest unless dest already has the sha256, and
// verifies the checksum before moving it into place.
func (e *env) download(url, sum, dest string) error {
	if sha256File(dest) == sum {
		return nil
	}
	c := &http.Client{Timeout: 10 * time.Minute}
	req, err := http.NewRequestWithContext(e.ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: %s", url, resp.Status)
	}
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}
	tmp := dest + ".part"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	defer os.Remove(tmp)
	f.Chmod(0o644) // whatever the umask: apt reads keys as _apt
	h := sha256.New()
	_, err = io.Copy(io.MultiWriter(f, h), resp.Body)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != sum {
		return fmt.Errorf("GET %s: sha256 %s, want %s", url, got, sum)
	}
	if err := os.Rename(tmp, dest); err != nil {
		return err
	}
	fmt.Fprintf(e.log, "downloaded %s → %s\n", url, dest)
	e.note("downloaded %s", filepath.Base(dest))
	return nil
}

// get fetches url, dialling addr instead of the URL's host when addr is
// set, and returns the status, the body and the certificate's days left.
func get(url, addr string) (int, []byte, int, error) {
	tr := &http.Transport{}
	if addr != "" {
		tr.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, addr)
		}
	}
	resp, err := (&http.Client{Transport: tr, Timeout: 10 * time.Second}).Get(url)
	if err != nil {
		return 0, nil, 0, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	days := 0
	if resp.TLS != nil {
		days = int(time.Until(resp.TLS.PeerCertificates[0].NotAfter).Hours() / 24)
	}
	return resp.StatusCode, body, days, err
}

// besideSelf returns the path of a file shipped next to this binary (the
// Makefile's dist puts them together), or an error naming it.
func besideSelf(name string) (string, error) {
	self, err := os.Executable()
	if err != nil {
		return "", err
	}
	p := filepath.Join(filepath.Dir(self), name)
	if !exists(p) {
		return "", errors.New(name + " not found next to " + app + " (copy bin/ from `make dist`)")
	}
	return p, nil
}
