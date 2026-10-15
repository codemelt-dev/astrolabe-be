package main

import (
	"os"
	"path/filepath"
	"strings"

	psqlfiles "stellar-explorer/psql"
)

var psqlHost = &host{
	name:     "psql",
	short:    "Set up the psql host (PostgreSQL, PostgREST, Caddy), or bring it back to its documented state",
	files:    psqlfiles.Files,
	settings: psqlSettings,
	steps:    psqlSteps,
}

func (e *env) v(key string) string { return e.set.get(key) }

var onNetwork, onBackups, onMonitor = []string{"network"}, []string{"backups"}, []string{"monitoring"}

// psqlSettings lists every setting, in psql/README.md "Example values" order.
var psqlSettings = []setting{
	{key: "private_ip", title: "This host's private IP", example: "10.0.1.2", valid: isIP},
	{key: "galexie_ip", title: "Galexie host's private IP", example: "10.0.0.2", valid: isIP},
	{key: "private_if", title: "Private network interface (with a VLAN: <vlan_link>.<vlan_id>)", example: "enp5s0.4000"},
	{key: "vlan_id", title: "VLAN ID of the private network, or none", example: "4000", def: "none", valid: isVLAN},
	{key: "vlan_link", title: "Interface that carries the VLAN", example: "enp5s0", when: hasVLAN, steps: onNetwork},
	{key: "vlan_gateway", title: "Gateway of the VLAN subnet", example: "10.0.1.1", when: hasVLAN, steps: onNetwork, valid: isIP},
	{key: "cloud_network", title: "Whole private network, routed via the VLAN gateway", example: "10.0.0.0/16", when: hasVLAN, steps: onNetwork},
	{key: "admin_ips", title: "IPv4 addresses or networks allowed to SSH, comma-separated", example: "203.0.113.10", steps: []string{"firewall"},
		valid: matching(`[0-9./]+(, *[0-9./]+)*`, "IPv4 addresses or networks, comma-separated")},
	{key: "api_hostname", title: "Public API hostname", example: "api.example.com", steps: []string{"postgrest", "caddy"}},
	{key: "cors_origins", title: "Frontend origins allowed by CORS, comma-separated", example: "https://explorer.example.com", steps: []string{"postgrest"}},
	{key: "ops_email", title: "E-mail for Let's Encrypt and alerts", example: "ops@example.com", steps: []string{"caddy", "monitoring"}},
	{key: "backup_endpoint", title: "Backup bucket's S3 endpoint", example: "fsn1.your-objectstorage.com", steps: onBackups},
	{key: "backup_region", title: "Backup bucket's S3 region", example: "fsn1", steps: onBackups},
	{key: "backup_bucket", title: "Backup bucket", example: "stellar-pgbackrest", steps: onBackups},
	{key: "backup_s3_key", title: "Backup bucket's S3 access key", secret: true, steps: onBackups},
	{key: "backup_s3_secret", title: "Backup bucket's S3 secret key", secret: true, steps: onBackups},
	{key: "grafana_hostname", title: "Public hostname for Grafana (its DNS pointing here), or none", example: "grafana.example.com", def: "none", steps: []string{"grafana"},
		valid: matching(`none|[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)+`, "a DNS name in lower case, or none")},
	{key: "clickhouse_hostname", title: "Public hostname for the clickhouse host's SQL endpoint (its DNS pointing here), or none", example: "clickhouse.example.com", def: "none", steps: []string{"clickhouse"},
		valid: matching(`none|[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)+`, "a DNS name in lower case, or none")},
	{key: "clickhouse_ip", title: "clickhouse host's private IP", example: "10.0.2.2", when: clickhouseOn, steps: []string{"clickhouse"}, valid: isIP},
	{key: "monitoring_enabled", title: "Run Prometheus and Alertmanager here (yes or no)", def: "no", valid: isYesNo},
	{key: "smtp_host", title: "SMTP server for alerts (host:port)", example: "smtp.example.com:587", when: monitoring, steps: onMonitor},
	{key: "smtp_user", title: "SMTP user, also the alerts' sender", example: "alerts@example.com", when: monitoring, steps: onMonitor},
	{key: "smtp_password", title: "SMTP password", secret: true, when: monitoring, steps: onMonitor},
	{key: "page_email", title: "E-mail for paging alerts", example: "oncall@example.com", when: monitoring, steps: onMonitor},
	{key: "deadman_url", title: "Dead-man's switch ping URL", secret: true, when: monitoring, steps: onMonitor},
}

func hasVLAN(s settings) bool    { return s["vlan_id"] != "none" }
func monitoring(s settings) bool { return s["monitoring_enabled"] == "yes" }

// psqlSteps is the step table, in order.
func psqlSteps() []step {
	return []step{
		{"system", "packages, UTC, sshd hardening, journald cap, needrestart", psqlSystemCheck, psqlSystemApply, nil},
		{"network", "optional: the VLAN interface with the private IP (netplan)", networkCheck, networkApply, networkHealth},
		{"firewall", "nftables: the whole ruleset, default drop", psqlFirewallCheck, psqlFirewallApply, psqlFirewallHealth},
		{"postgres", "PostgreSQL 18 (PGDG), cluster, conf.d, pg_hba, TLS, huge pages", postgresCheck, postgresApply, postgresHealth},
		{"database", "roles.sql, role passwords, the galexie bundle", databaseCheck, databaseApply, nil},
		{"backups", "pgBackRest: config, stanza, archiving, timers, first full backup", backupsCheck, backupsApply, backupsHealth},
		{"api", "grants.sql and api.sql as explorer_owner, after worker migrate", apiCheck, apiApply, apiHealth},
		{"postgrest", "PostgREST release, config, unit", postgrestCheck, postgrestApply, postgrestHealth},
		{"caddy", "the caddy build next to this app, Caddyfile, unit", caddyCheck, caddyApply, caddyHealth},
		{"monitoring", "optional: exporters, Prometheus, Alertmanager, alert rules", monitoringCheck, monitoringApply, monitoringHealth},
		{"clickhouse", "optional (clickhouse_hostname): the clickhouse host's SQL endpoint in Caddy, its scrape jobs and alerts", psqlCHCheck, psqlCHApply, psqlCHHealth},
		{"jobs", "with the clickhouse site: the worker and its daily state-copy timer", jobsCheck, jobsApply, jobsHealth},
		{"grafana", "optional (lab grafana): Grafana over Prometheus, with dashboard/'s dashboards (and ClickHouse's, with the clickhouse site)", grafanaCheck, grafanaApply, grafanaHealth},
	}
}

var isVLAN, isYesNo = matching(`none|[1-9][0-9]{0,3}`, "a VLAN ID or none"), matching(`yes|no`, "yes or no")

// SQL: psql as postgres over the Unix socket.

func (e *env) sql(db, input string) error {
	_, err := e.runInput(input, "runuser", "-u", "postgres", "--", "psql", "-X", "-v", "ON_ERROR_STOP=1", "-At", "-d", db, "-f", "-")
	return err
}

// query runs one read-only statement, unlogged: for checks.
func query(db, q string) (string, error) {
	return probe("runuser", "-u", "postgres", "--", "psql", "-X", "-v", "ON_ERROR_STOP=1", "-At", "-d", db, "-c", q)
}

// part is what a step installs: packages, system users (home
// /var/lib/<name>), directories, release tarballs, files, symlinks (target,
// link), commands that validate or load the files, and services.
type part struct {
	pkgs     []string
	users    []string
	dirs     []dir
	releases []release
	links    [][2]string
	files    []file
	after    [][]string
	svcs     []service
}

// release is a tarball extracted next to marker, unless marker exists.
type release struct {
	url, sum, marker string
	tar              []string
}

// check reports the first thing that isn't in place, or ok with detail.
func (p part) check(detail string) result {
	if m := missing(p.pkgs...); len(m) > 0 {
		return change("install %s", strings.Join(m, " "))
	}
	for _, u := range p.users {
		if !userExists(u) {
			return change("user %s", u)
		}
	}
	for _, d := range p.dirs {
		if !d.matches() {
			return change("dir %s", d.path)
		}
	}
	for _, l := range p.links {
		if cur, _ := os.Readlink(l[1]); cur != l[0] || !exists(l[0]) {
			return change("install %s", l[0])
		}
	}
	if f := firstMismatch(p.files...); f != "" {
		return change("write %s", f)
	}
	r := ok("%s", detail)
	for _, s := range p.svcs { // a pending restart must not hide a change
		switch sr := s.state(); sr.status {
		case statusChange:
			return sr
		case statusRestart:
			r = sr
		}
	}
	return r
}

func (e *env) apply(p part) error {
	err := e.install(p.pkgs...)
	for _, u := range p.users {
		if err == nil {
			err = e.systemUser(u, "/var/lib/"+u)
		}
	}
	for _, d := range p.dirs {
		if err == nil {
			err = e.mkdir(d)
		}
	}
	for _, r := range p.releases {
		if err == nil && !exists(r.marker) {
			err = e.unpack(r)
		}
	}
	changed := false
	if err == nil {
		changed, err = e.place(p.files...)
	}
	for _, l := range p.links {
		if err == nil {
			err = e.symlink(l[0], l[1])
		}
	}
	if err == nil {
		err = e.daemonReload(changed)
	}
	for _, c := range p.after { // also when nothing changed: an earlier run may have stopped here
		if err == nil {
			_, err = e.run(c[0], c[1:]...)
		}
	}
	for _, s := range p.svcs {
		if err == nil {
			err = e.ensure(s)
		}
	}
	return err
}

// unpack downloads and extracts a release. -m dates the files now, so a
// new binary is newer than the running service.
func (e *env) unpack(r release) error {
	tarball := filepath.Join(filepath.Dir(r.marker), filepath.Base(r.url))
	if err := e.download(r.url, r.sum, tarball); err != nil {
		return err
	}
	defer os.Remove(tarball)
	_, err := e.run("tar", append([]string{"-xmf", tarball, "-C", filepath.Dir(r.marker), "--no-same-owner"}, r.tar...)...)
	return err
}

// secret returns a generated secret without creating it ("" if missing).
func secret(name string) string {
	b, _ := os.ReadFile(filepath.Join(secretsDir, name))
	return strings.TrimSpace(string(b))
}

// system

var psqlSystemPackages = []string{"chrony", "nftables", "curl", "ca-certificates", "unattended-upgrades", "xz-utils"}

func psqlSystemFiles(e *env) []file {
	return []file{
		{"/etc/ssh/sshd_config.d/10-hardening.conf", e.asset("files/sshd-hardening.conf"), 0o644, "root:root"},
		{"/etc/systemd/journald.conf.d/60-stellar.conf", e.asset("files/journald.conf"), 0o644, "root:root"},
		{"/etc/needrestart/conf.d/60-stellar.conf", e.asset("files/needrestart.conf"), 0o644, "root:root"},
	}
}

func psqlSystemCheck(e *env) result {
	if m := missing(psqlSystemPackages...); len(m) > 0 {
		return change("install %s", strings.Join(m, " "))
	}
	if tz, _ := probe("timedatectl", "show", "-p", "Timezone", "--value"); tz != "UTC" {
		return change("timezone %s → UTC", tz)
	}
	if p := firstMismatch(psqlSystemFiles(e)...); p != "" {
		return change("write %s", p)
	}
	return ok("packages, UTC, sshd, journald, needrestart")
}

func psqlSystemApply(e *env) error {
	if err := e.install(psqlSystemPackages...); err != nil {
		return err
	}
	if tz, _ := probe("timedatectl", "show", "-p", "Timezone", "--value"); tz != "UTC" {
		if _, err := e.run("timedatectl", "set-timezone", "UTC"); err != nil {
			return err
		}
		e.note("timezone UTC")
	}
	files := psqlSystemFiles(e)
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
		if _, err := e.run("systemctl", "reload", "ssh"); err != nil {
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
	_, err := e.place(files[2]) // needrestart reads it on its next run
	return err
}
