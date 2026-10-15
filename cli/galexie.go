package main

import (
	"errors"
	"os"
	"strings"

	galexiefiles "stellar-explorer/galexie"
)

var galexieHost = &host{
	name:     "galexie",
	short:    "Set up the galexie host (Galexie, the worker's lake and ingest), or bring it back to its documented state",
	files:    galexiefiles.Files,
	settings: galexieSettings,
	steps:    galexieSteps,
}

// galexieSettings lists every setting, in galexie/README.md "Example values" order.
var galexieSettings = []setting{
	{key: "private_ip", title: "This host's private IP", example: "10.0.0.2", valid: isIP, steps: []string{"config"}},
	{key: "postgres_ip", title: "psql host's private IP", example: "10.0.1.2", valid: isIP, steps: []string{"firewall", "config"}},
	{key: "private_if", title: "Private network interface (ip -br link)", example: "enp7s0", valid: isName, steps: []string{"firewall"}},
	{key: "lake_device", title: "Lake volume (ls /dev/disk/by-id/)", example: "/dev/disk/by-id/scsi-0HC_Volume_100123456", valid: matching(`^/dev/disk/by-id/[^/\s]+$`, "a /dev/disk/by-id/ path"), steps: []string{"disks"}},
	{key: "archive_bucket", title: "Archive bucket", example: "stellar-ledgers-pubnet", valid: isName, steps: []string{"config"}},
	{key: "archive_endpoint", title: "Archive S3 endpoint", example: "https://hel1.your-objectstorage.com", valid: matching(`^https://[A-Za-z0-9.-]+(:\d+)?$`, "https://host"), steps: []string{"config"}},
	{key: "archive_region", title: "Archive S3 region", example: "hel1", valid: isName, steps: []string{"config"}},
	{key: "archive_access_key", title: "Archive uploader access key", secret: true, steps: []string{"config"}},
	{key: "archive_secret_key", title: "Archive uploader secret key", secret: true, steps: []string{"config"}},
}

// galexieSteps is the step table, in order: each step relies on the ones before.
func galexieSteps() []step {
	return []step{
		{name: "system", about: "packages, UTC, sshd hardening, journald cap, needrestart, user stellar", check: galexieSystemCheck, apply: galexieSystemApply},
		{name: "firewall", about: "nftables table for the private interface, loaded by stellar-firewall", check: galexieFirewallCheck, apply: galexieFirewallApply, health: galexieFirewallHealth},
		{name: "disks", about: "/srv/core; /srv/lake on its own volume (fstab, mount, marker)", check: disksCheck, apply: disksApply, health: disksHealth},
		{name: "docker", about: "Docker from its apt repository, local log driver, the pinned Galexie image", check: dockerCheck, apply: dockerApply},
		{name: "worker", about: "first install of the worker shipped next to this app", check: workerCheck, apply: workerApply},
		{name: "config", about: "/etc/stellar: worker and Galexie configs, keys, the database bundle", check: configCheck, apply: configApply, health: configHealth},
		{name: "units", about: "systemd units of the lake, ingest, Galexie and the timers", check: unitsCheck, apply: unitsApply},
		{name: "migrate", about: "worker migrate while migrations are pending", check: migrateCheck, apply: migrateApply},
		{name: "bootstrap", about: "worker bootstrap once: state from the latest checkpoint, archive_origin", check: bootstrapCheck, apply: bootstrapApply},
		{name: "services", about: "enable and start the lake, ingest, Galexie and both timers", check: servicesCheck, apply: servicesApply, health: pipelineHealth},
	}
}

var isName = matching(`^[A-Za-z0-9][A-Za-z0-9._-]*$`, "letters, digits, '.', '_', '-'")

// system

var galexieSystemPackages = []string{"chrony", "nftables", "xfsprogs", "postgresql-client", "curl", "ca-certificates", "unattended-upgrades"}

func galexieSystemFiles(e *env) []file {
	return []file{
		{"/etc/ssh/sshd_config.d/10-hardening.conf", e.asset("deploy/sshd-hardening.conf"), 0o644, "root:root"},
		{"/etc/systemd/journald.conf.d/60-stellar.conf", e.asset("deploy/journald.conf"), 0o644, "root:root"},
		{"/etc/needrestart/conf.d/60-stellar-docker.conf", e.asset("deploy/needrestart.conf"), 0o644, "root:root"},
	}
}

func galexieSystemCheck(e *env) result {
	if m := missing(galexieSystemPackages...); len(m) > 0 {
		return change("install %s", strings.Join(m, " "))
	}
	if tz, _ := probe("timedatectl", "show", "-p", "Timezone", "--value"); tz != "UTC" {
		return change("timezone %s → UTC", tz)
	}
	if p := firstMismatch(galexieSystemFiles(e)...); p != "" {
		return change("write %s", p)
	}
	if !userExists("stellar") {
		return change("create user stellar")
	}
	return ok("packages, UTC, sshd, journald, needrestart, user stellar")
}

func galexieSystemApply(e *env) error {
	if err := e.install(galexieSystemPackages...); err != nil {
		return err
	}
	if tz, _ := probe("timedatectl", "show", "-p", "Timezone", "--value"); tz != "UTC" {
		if _, err := e.run("timedatectl", "set-timezone", "UTC"); err != nil {
			return err
		}
		e.note("timezone UTC")
	}
	files := galexieSystemFiles(e)
	if fi, err := os.Stat("/root/.ssh/authorized_keys"); !files[0].matches() && (err != nil || fi.Size() == 0) {
		return errors.New("no key in /root/.ssh/authorized_keys: add yours before sshd allows keys only")
	}
	old, rerr := os.ReadFile(files[0].path)
	if changed, err := e.place(files[0]); err != nil {
		return err
	} else if changed {
		// A broken sshd config must never be reloaded: it would lock us out.
		if _, err := e.run("sshd", "-t"); err != nil {
			if os.Remove(files[0].path); rerr == nil {
				os.WriteFile(files[0].path, old, files[0].mode)
			}
			return err
		}
		if _, err := e.run("systemctl", "try-reload-or-restart", "ssh.service"); err != nil {
			return err
		}
	}
	if changed, err := e.place(files[1]); err != nil {
		return err
	} else if changed {
		if _, err := e.run("systemctl", "restart", "systemd-journald"); err != nil {
			return err
		}
	}
	if _, err := e.place(files[2]); err != nil { // read by needrestart on every run
		return err
	}
	return e.systemUser("stellar", "/nonexistent")
}
