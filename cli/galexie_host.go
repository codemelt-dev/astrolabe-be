package main

// The host underneath the pipeline: the private-interface firewall, the
// two disks, and Docker.

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"syscall"
)

// firewall

const nftPath = "/etc/nftables.d/stellar-galexie.nft"

// A reload re-runs nft -f, which replaces the table atomically.
var firewallService = service{unit: "stellar-firewall.service", reload: true, files: []string{nftPath}}

func firewallFiles(e *env) []file {
	return []file{
		{nftPath, e.asset("deploy/galexie-host.nft"), 0o644, "root:root"},
		{"/etc/systemd/system/stellar-firewall.service", e.asset("deploy/systemd/stellar-firewall.service"), 0o644, "root:root"},
	}
}

func galexieFirewallCheck(e *env) result {
	if enabled("nftables.service") {
		return change("disable nftables.service (its stop flushes Docker's chains)")
	}
	if p := firstMismatch(firewallFiles(e)...); p != "" {
		return change("write %s", p)
	}
	return firewallService.state()
}

func galexieFirewallApply(e *env) error {
	if enabled("nftables.service") {
		// No --now: stopping it runs `nft flush ruleset`.
		if _, err := e.run("systemctl", "disable", "nftables.service"); err != nil {
			return err
		}
		e.note("disabled nftables.service")
	}
	// nft -f is atomic: a bad file fails the step and changes no rule.
	files := firewallFiles(e)
	if _, err := e.place(files[0]); err != nil {
		return err
	}
	if changed, err := e.write(files[1]); err != nil {
		return err
	} else if err := e.daemonReload(changed); err != nil {
		return err
	}
	return e.ensure(firewallService)
}

func galexieFirewallHealth(*env) result {
	if _, err := probe("nft", "list", "table", "inet", "stellar_galexie"); err != nil {
		return failed("table inet stellar_galexie is not loaded")
	}
	return ok("table inet stellar_galexie loaded")
}

// disks

const lakeDir, markerPath = "/srv/lake", "/srv/lake/state/lake-id" // the lake refuses to start without the marker

// /srv/core is captive core's (the Galexie container runs as root).
var srvDirs = []dir{{"/srv/core", 0o750, "root:root"}, {"/srv/lake/s3", 0o750, "stellar:stellar"}, {"/srv/lake/state", 0o750, "stellar:stellar"}}

func disksCheck(e *env) result {
	dev := e.set.get("lake_device")
	switch fs, err := blkid(dev); {
	case err != nil:
		return failed("%v", err)
	case fs == "":
		return result{status: statusChange, detail: "format " + dev + " as xfs", ask: "Format " + dev + " as xfs? blkid finds nothing on it."}
	case fs != "xfs":
		return failed("%s holds %s, not xfs: it is never formatted (galexie/README.md §2.2)", dev, fs)
	}
	fstab, _ := os.ReadFile("/etc/fstab")
	want, have, err := fstabEntry(string(fstab), e)
	switch {
	case err != nil:
		return failed("%v", err)
	case !have:
		return change("add to /etc/fstab: %s", want)
	case !mounted(dev):
		return change("mount %s", lakeDir)
	}
	for _, d := range srvDirs {
		if !d.matches() {
			return change("create %s", d.path)
		}
	}
	if !exists(markerPath) {
		return change("write %s", markerPath)
	}
	return ok("/srv/core; %s on %s (xfs)", lakeDir, dev)
}

func disksApply(e *env) error {
	dev := e.set.get("lake_device")
	if fs, err := blkid(dev); err != nil {
		return err
	} else if fs == "" {
		if !e.approved["disks"] {
			return errNotApproved
		}
		// Without -f, mkfs.xfs also refuses anything it recognises.
		if _, err := e.run("mkfs.xfs", dev); err != nil {
			return err
		}
		e.note("formatted %s", dev)
	}
	fstab, _ := os.ReadFile("/etc/fstab")
	if want, have, err := fstabEntry(string(fstab), e); err != nil {
		return err
	} else if !have {
		fstab = []byte(strings.TrimRight(string(fstab), "\n") + "\n" + want + "\n")
		if _, err := e.write(file{"/etc/fstab", fstab, 0o644, "root:root"}); err != nil {
			return err
		}
		if err := e.daemonReload(true); err != nil { // fstab → mount units
			return err
		}
	}
	if !mounted(dev) {
		if err := e.mkdir(dir{lakeDir, 0o755, "root:root"}); err != nil {
			return err
		}
		if _, err := e.run("mount", lakeDir); err != nil {
			return err
		}
		e.note("mounted %s", lakeDir)
	}
	// Never create the lake's directories or marker on the root disk.
	if !mounted(dev) {
		return fmt.Errorf("%s is not mounted from %s", lakeDir, dev)
	}
	for _, d := range srvDirs {
		if err := e.mkdir(d); err != nil {
			return err
		}
	}
	if exists(markerPath) {
		return nil
	}
	_, err := e.write(file{markerPath, []byte(filepath.Base(dev) + "\n"), 0o644, "stellar:stellar"})
	return err
}

// blkid returns a device's filesystem type or partition table; "" for none.
func blkid(dev string) (string, error) {
	if !exists(dev) {
		return "", fmt.Errorf("%s not found: attach the lake volume (galexie/README.md §2.2)", dev)
	}
	out, err := probe("blkid", "-p", "-o", "value", "-s", "TYPE", "-s", "PTTYPE", dev)
	if ee := (*exec.ExitError)(nil); errors.As(err, &ee) && ee.ExitCode() == 2 && out == "" {
		return "", nil
	}
	return out, err
}

// fstabEntry returns the lake's line (deploy/lake.fstab) and whether fstab has
// it. Another entry for the mountpoint or the device (an automount) is left
// to the operator.
func fstabEntry(fstab string, e *env) (want string, have bool, err error) {
	want = strings.TrimSpace(string(e.asset("deploy/lake.fstab")))
	want = want[strings.LastIndexByte(want, '\n')+1:]
	w := strings.Fields(want)
	for _, line := range strings.Split(fstab, "\n") {
		f := strings.Fields(line)
		switch {
		case len(f) < 2 || strings.HasPrefix(f[0], "#"):
		case slices.Equal(f, w):
			have = true
		case f[0] == w[0] || f[1] == w[1]:
			return want, false, fmt.Errorf("/etc/fstab line %q is in the way of %q: fix it by hand", line, want)
		}
	}
	return want, have, nil
}

// mounted reports whether dev (findmnt resolves the symlink) is on /srv/lake.
func mounted(dev string) bool {
	_, err := probe("findmnt", "-n", "--mountpoint", lakeDir, "--source", dev)
	return err == nil
}

// disksHealth shows / (OS, Docker, /srv/core) and the lake; above 85 % pages.
func disksHealth(*env) result {
	r := ok("")
	for _, p := range []string{"/", lakeDir} {
		var st syscall.Statfs_t
		if err := syscall.Statfs(p, &st); err != nil {
			return failed("%s: %v", p, err)
		}
		used := 100 * float64(st.Blocks-st.Bfree) / float64(st.Blocks-st.Bfree+st.Bavail)
		if used > 85 {
			r.status = statusFailed
		}
		r.detail += fmt.Sprintf("%s %.0f %% used, %d GB free  ", p, used, st.Bavail*uint64(st.Bsize)>>30)
	}
	return r
}

// docker

const (
	dockerKey = "/etc/apt/keyrings/docker.asc"
	// sha256 of https://download.docker.com/linux/ubuntu/gpg (fingerprint
	// 9DC8 5822 9FC7 DD38 854A E2D8 8D81 803C 0EBF CD88).
	dockerKeySum = "1500c1f56fa9e26b9b8f42452a553675796ade0807cdce11975eb98170b3a570"
)

var (
	dockerPackages = []string{"docker-ce", "docker-ce-cli", "containerd.io"}
	// daemon.json needs a docker restart (Galexie stops first): --allow-restart.
	dockerService = service{unit: "docker.service", files: []string{"/etc/docker/daemon.json"}}
	imageRe       = regexp.MustCompile(`stellar/stellar-galexie:\S+@(sha256:[0-9a-f]{64})`)
)

func dockerFiles(e *env) []file {
	return []file{
		{"/etc/apt/sources.list.d/docker.list", e.asset("deploy/docker.list"), 0o644, "root:root"},
		{"/etc/apt/apt.conf.d/51stellar-docker", e.asset("deploy/unattended-upgrades-docker.conf"), 0o644, "root:root"},
		{"/etc/docker/daemon.json", e.asset("deploy/daemon.json"), 0o644, "root:root"}, // before docker's first start
	}
}

// galexieImage returns stellar-galexie.service's image, and it by digest.
func galexieImage(e *env) (string, string) {
	m := imageRe.FindStringSubmatch(string(e.asset("deploy/systemd/stellar-galexie.service")))
	return m[0], "stellar/stellar-galexie@" + m[1]
}

func dockerCheck(e *env) result {
	image, byDigest := galexieImage(e)
	if sha256File(dockerKey) != dockerKeySum {
		return change("write %s", dockerKey)
	}
	if p := firstMismatch(dockerFiles(e)...); p != "" {
		return change("write %s", p)
	}
	if m := missing(dockerPackages...); len(m) > 0 {
		return change("install %s", strings.Join(m, " "))
	}
	r := dockerService.state()
	if r.status == statusChange {
		return r
	}
	if _, err := probe("docker", "image", "inspect", byDigest); err != nil {
		return change("pull %s", image)
	}
	if r.status == statusRestart {
		return r
	}
	return ok("docker running; %s", image)
}

func dockerApply(e *env) error {
	if err := e.download("https://download.docker.com/linux/ubuntu/gpg", dockerKeySum, dockerKey); err != nil {
		return err
	}
	if _, err := e.place(dockerFiles(e)...); err != nil {
		return err
	}
	if len(missing(dockerPackages...)) > 0 {
		e.aptUpdated = false // apt doesn't know Docker's repository yet
	}
	if err := e.install(dockerPackages...); err != nil {
		return err
	}
	if err := e.ensure(dockerService); err != nil {
		return err
	}
	// Pulled ahead: the unit's start timeout doesn't cover a pull.
	image, byDigest := galexieImage(e)
	if _, err := probe("docker", "image", "inspect", byDigest); err == nil {
		return nil
	}
	if _, err := e.run("docker", "pull", image); err != nil {
		return err
	}
	e.note("pulled %s", image)
	return nil
}
