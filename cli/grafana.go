package main

// grafana: Grafana on the psql host, over its Prometheus, with the
// dashboards in dashboard/ (dashboard/README.md). `lab grafana` runs this
// step alone; once Grafana is installed, `lab psql` keeps it in place too.

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"strings"
	"time"

	dashboardfiles "stellar-explorer/dashboard"
)

const (
	grafanaVersion = "13.2.3"
	grafanaSHA256  = "55ec511999f9c56f3c26b633d0dc31b750c77a682a5ef379edf0d3fe634a7a2f"
	grafanaURL     = "http://127.0.0.1:3002"
	grafanaDash    = "/etc/grafana/dashboards/stellar-explorer"
	// The admin's password, fixed: every run sets it back if it changed
	// (dashboard/README.md).
	grafanaAdminPassword = "codemelt"
	// What lab generated before the password was fixed.
	grafanaOldSecret = secretsDir + "/grafana-admin-password"
)

// grafanaAdminRefused is the check's finding that Grafana refuses the
// admin's password (nil when the check stopped before asking), so that a run
// asks at most once: each wrong try counts towards Grafana's lockout.
var grafanaAdminRefused *bool

// wantGrafana is set by `lab grafana`. Without it, the step leaves a host
// without Grafana as it is.
var wantGrafana bool

// postinstDefaults are files a package's postinst copies from a sample
// instead of shipping them as conffiles; place treats an untouched copy as
// the package's default, which it may replace.
var postinstDefaults = map[string]string{
	"/etc/grafana/grafana.ini": "/usr/share/grafana/conf/sample.ini",
}

// The unit restarts for a new package (dpkg rewrites its file list) or a
// changed configuration; the dashboards are re-read every 30 s without one.
var grafanaService = service{unit: "grafana-server.service", files: []string{
	"/var/lib/dpkg/info/grafana.list", "/etc/grafana/grafana.ini",
	"/etc/grafana/provisioning/datasources/stellar-explorer.yaml",
	"/etc/grafana/provisioning/dashboards/stellar-explorer.yaml", grafanaDropIn,
	chDataSource, chGrafanaPass, chPluginDir + "/plugin.json"}}

// The public site, while grafana_hostname is set: Caddy's site for it and
// the drop-in that gives Grafana its public URL (dashboard/README.md).
const (
	grafanaSite   = "/etc/caddy/sites/grafana.caddy"
	grafanaDropIn = "/etc/systemd/system/grafana-server.service.d/20-public.conf"
)

// grafanaPublic returns the public site's files, or none.
func grafanaPublic(e *env) []file {
	if e.v("grafana_hostname") == "none" {
		return nil
	}
	conf, _ := dashboardfiles.Files.ReadFile("grafana-public.conf")
	return []file{
		{grafanaSite, e.set.render(e.asset("files/grafana.caddy")), 0o644, "root:root"},
		{grafanaDropIn, e.set.render(conf), 0o644, "root:root"},
	}
}

// grafanaFiles returns the configuration (which may replace the package's
// grafana.ini, so it goes through place) and lab's own files: the
// dashboards and the admin password, which have no header to recognise.
func grafanaFiles() (conf, own []file) {
	read := func(n string) []byte { b, _ := dashboardfiles.Files.ReadFile(n); return b }
	g := "root:grafana"
	conf = []file{
		{"/etc/grafana/grafana.ini", read("grafana.ini"), 0o640, g},
		{"/etc/grafana/provisioning/datasources/stellar-explorer.yaml", read("datasources.yaml"), 0o640, g},
		{"/etc/grafana/provisioning/dashboards/stellar-explorer.yaml", read("dashboards.yaml"), 0o640, g},
	}
	for _, n := range grafanaDashboards() {
		own = append(own, file{grafanaDash + "/" + n, read(n), 0o644, "root:root"})
	}
	own = append(own, file{"/etc/grafana/admin-password", []byte(grafanaAdminPassword), 0o640, g})
	return conf, own
}

// grafanaDashboards are dashboard/'s dashboards but ClickHouse's, which
// comes only with the clickhouse site (grafana_clickhouse.go).
func grafanaDashboards() []string {
	var out []string
	names, _ := fs.Glob(dashboardfiles.Files, "*.json")
	for _, n := range names {
		if n != "clickhouse.json" {
			out = append(out, n)
		}
	}
	return out
}

// grafanaCurrent reports whether the installed version is the pinned one
// (packages may carry a "-" or "+" suffix).
func grafanaCurrent(v string) bool {
	return v == grafanaVersion || strings.HasPrefix(v, grafanaVersion+"-") || strings.HasPrefix(v, grafanaVersion+"+")
}

// grafanaInstalled is the installed package version ("" when there is none).
func grafanaInstalled() string {
	v, err := probe("dpkg-query", "-W", "-f=${db:Status-Status} ${Version}", "grafana")
	if err != nil || !strings.HasPrefix(v, "installed ") {
		return ""
	}
	return strings.TrimPrefix(v, "installed ")
}

func grafanaCheck(e *env) result {
	v := grafanaInstalled()
	switch {
	case v == "" && !wantGrafana:
		return ok("not installed (lab grafana installs it)")
	case !monitoring(e.set):
		return waiting("needs monitoring_enabled = yes: Grafana reads this host's Prometheus")
	case !grafanaCurrent(v):
		return change("install Grafana %s (installed: %q)", grafanaVersion, v)
	}
	conf, own := grafanaFiles()
	pub := grafanaPublic(e)
	if p := firstMismatch(append(append(conf, own...), pub...)...); p != "" {
		return change("write %s", p)
	}
	if pub == nil && (exists(grafanaSite) || exists(grafanaDropIn)) {
		return change("remove the public site (grafana_hostname = none)")
	}
	if r := grafanaCHCheck(e); r.status != statusOK {
		return r
	}
	if r := grafanaService.state(); r.status != statusOK {
		return r
	}
	refused := grafanaAdminWrong()
	grafanaAdminRefused = &refused
	if refused || exists(grafanaOldSecret) {
		return change("set the admin password")
	}
	return ok("Grafana %s on %s", grafanaVersion, grafanaURL)
}

func grafanaApply(e *env) error {
	if !grafanaCurrent(grafanaInstalled()) {
		deb := "/var/cache/lab/grafana_" + grafanaVersion + "_amd64.deb"
		url := "https://dl.grafana.com/oss/release/grafana_" + grafanaVersion + "_amd64.deb"
		if err := e.download(url, grafanaSHA256, deb); err != nil {
			return err
		}
		if err := e.install(deb); err != nil { // apt-get resolves its dependencies
			return err
		}
		os.Remove(deb)
	}
	conf, own := grafanaFiles()
	if _, err := e.place(conf...); err != nil {
		return err
	}
	if err := e.mkdir(dir{grafanaDash, 0o755, "root:root"}); err != nil {
		return err
	}
	if _, err := e.writeAll(own...); err != nil {
		return err
	}
	pub := grafanaPublic(e)
	changed, err := e.writeAll(pub...)
	if err != nil {
		return err
	}
	removed := false
	for _, p := range []string{grafanaSite, grafanaDropIn} {
		if pub == nil && exists(p) {
			if err := os.Remove(p); err != nil {
				return err
			}
			e.note("removed %s", p)
			changed, removed = true, true
		}
	}
	if changed {
		if err := e.daemonReload(true); err != nil {
			return err
		}
		// Caddy reads the site at its reload: check the whole configuration first.
		if _, err := e.run("runuser", "-u", "caddy", "--", "/usr/local/bin/caddy", "validate", "--config", "/etc/caddy/Caddyfile"); err != nil {
			return err
		}
		if _, err := e.run("systemctl", "reload", "caddy.service"); err != nil {
			return err
		}
		e.note("reloaded caddy.service")
	}
	chRemoved, err := e.grafanaCHApply()
	if err != nil {
		return err
	}
	removed = removed || chRemoved
	if removed && active(grafanaService.unit) && e.allowRestart { // a removed drop-in leaves nothing newer to see
		if _, err := e.run("systemctl", "restart", grafanaService.unit); err != nil {
			return err
		}
		e.note("restarted %s", grafanaService.unit)
	}
	if err := e.ensure(grafanaService); err != nil {
		return err
	}
	// A first start migrates Grafana's database before it listens, and
	// registers the data source's plugin after: give it a minute, so the
	// step's health sees it up.
	deadline := time.Now().Add(time.Minute)
	for ; time.Now().Before(deadline); time.Sleep(2 * time.Second) {
		if code, _, _, _ := get(grafanaURL+"/api/health", ""); code == http.StatusOK {
			break
		}
	}
	if err := e.grafanaSetAdmin(); err != nil {
		return err
	}
	// Every failed login counts towards Grafana's lockout (5 in a row, then
	// none at all until 5 minutes after the last), so one try, never a loop.
	if grafanaAdminWrong() {
		return errors.New("Grafana refuses the admin password it was just given: failed logins lock it until 5 minutes after the last (journalctl -u grafana-server)")
	}
	for ; time.Now().Before(deadline); time.Sleep(2 * time.Second) {
		if grafanaHealth(e).status == statusOK {
			break
		}
	}
	return nil
}

// grafanaAdminWrong reports whether Grafana refuses the admin's password
// (false while it doesn't answer). A refusal is a failed login: see
// grafanaSetAdmin.
func grafanaAdminWrong() bool {
	code, _, _ := grafanaGet("/api/user")
	return code == http.StatusUnauthorized
}

// grafanaSetAdmin sets the admin's password back to grafanaAdminPassword
// when Grafana refuses it. grafana.ini gives it to Grafana only when
// it creates its database; after that, only Grafana's CLI changes it without
// the old one. It runs as grafana, so the database's files stay Grafana's,
// with the server's paths (the unit's; the defaults would be under
// /usr/share/grafana).
func (e *env) grafanaSetAdmin() error {
	if grafanaAdminRefused == nil {
		refused := grafanaAdminWrong()
		grafanaAdminRefused = &refused
	}
	if *grafanaAdminRefused {
		if _, err := e.run("runuser", "-u", "grafana", "--", "/usr/sbin/grafana", "cli",
			"--homepath", "/usr/share/grafana", "--config", "/etc/grafana/grafana.ini",
			"--configOverrides", "cfg:default.paths.data=/var/lib/grafana cfg:default.paths.logs=/var/log/grafana",
			"admin", "reset-admin-password", grafanaAdminPassword); err != nil {
			return err
		}
		e.note("set the admin password")
	}
	grafanaAdminRefused = nil
	if exists(grafanaOldSecret) {
		if err := os.Remove(grafanaOldSecret); err != nil {
			return err
		}
		e.note("removed %s (the admin password is fixed now)", grafanaOldSecret)
	}
	return nil
}

// grafanaHealth logs in as admin and asks Grafana what the operator would
// look at: is it up, does the data source answer, are the dashboards there.
func grafanaHealth(e *env) result {
	if grafanaInstalled() == "" {
		return ok("not installed")
	}
	if code, _, _, err := get(grafanaURL+"/api/health", ""); err != nil || code != http.StatusOK {
		return failed("%s/api/health: %d %v", grafanaURL, code, err)
	}
	_, ds, err := grafanaGet("/api/datasources/uid/prometheus/health")
	if err != nil || !bytes.Contains(ds, []byte(`"status":"OK"`)) {
		return failed("data source Prometheus: %s %v", tail(string(ds), 1), err)
	}
	want, sources := len(grafanaDashboards()), "Prometheus answers"
	if grafanaCHFiles(e) != nil {
		want, sources = want+1, "Prometheus and ClickHouse answer"
		_, ds, err := grafanaGet("/api/datasources/uid/clickhouse/health")
		if err != nil || !bytes.Contains(ds, []byte(`"status":"OK"`)) {
			return failed("data source ClickHouse: %s %v", tail(string(ds), 1), err)
		}
	}
	_, found, err := grafanaGet("/api/search?tag=stellar-explorer")
	if n := bytes.Count(found, []byte(`"type":"dash-db"`)); err != nil || n < want {
		return failed("%d of %d dashboards provisioned %v", n, want, err)
	}
	if h := e.v("grafana_hostname"); h != "none" {
		code, _, days, err := get("https://"+h+"/api/health", "127.0.0.1:443")
		if err != nil || code != http.StatusOK {
			return failed("https://%s/api/health through 127.0.0.1: %d %v", h, code, err)
		}
		return ok("%s, %d dashboards; https://%s (certificate %d days left)", sources, want, h, days)
	}
	return ok("%s, %d dashboards; ssh -L 3002:127.0.0.1:3002 to open it", sources, want)
}

func grafanaGet(path string) (int, []byte, error) {
	req, err := http.NewRequest(http.MethodGet, grafanaURL+path, nil)
	if err != nil {
		return 0, nil, err
	}
	req.SetBasicAuth("admin", grafanaAdminPassword)
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err == nil && resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("%s: %s", path, resp.Status)
	}
	return resp.StatusCode, body, err
}
