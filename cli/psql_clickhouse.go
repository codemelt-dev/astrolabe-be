package main

// clickhouse on the psql host: the clickhouse host's public, read-only SQL
// endpoint in Caddy (like Grafana's site), and its scrape jobs and alerts in
// Prometheus (clickhouse/README.md §4, §8). Optional: clickhouse_hostname.

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	clickhousefiles "stellar-explorer/clickhouse"
)

const (
	chSite        = "/etc/caddy/sites/clickhouse.caddy"
	chScrape      = "/etc/prometheus/scrape.d/clickhouse.yml"
	chAlerts      = "/etc/prometheus/rules/clickhouse-alerts.yml"
	chCAOnPsql    = "/etc/stellar/clickhouse-ca.crt"
	chPublicOnPsq = secretsDir + "/clickhouse-public.password"
	chReaderOnPsq = secretsDir + "/clickhouse-reader.password"
	chAPIOnPsql   = secretsDir + "/clickhouse-api.password"
	// The frontend's backend's login, in plain in both Caddy
	// sites: the API gives it its own limit, and on the ClickHouse site Caddy
	// logs in as stellar_api for it.
	backendUser, backendPassword = "codemelt", "astrolabe"
)

func clickhouseOn(s settings) bool { return s["clickhouse_hostname"] != "none" }

// chBundleFile reads a bundle file from chBundle while it is there (copied
// from the clickhouse host), else as installed.
func chBundleFile(name, installed string) []byte {
	if b, err := os.ReadFile(filepath.Join(chBundle, name)); err == nil {
		return b
	}
	b, _ := os.ReadFile(installed)
	return b
}

// psqlCHFiles are the step's files: nil while it is off; ready is false
// while the bundle is missing.
func psqlCHFiles(e *env) (files []file, ready bool) {
	if !clickhouseOn(e.set) {
		return nil, true
	}
	ca := chBundleFile("clickhouse-ca.crt", chCAOnPsql)
	public := strings.TrimSpace(string(chBundleFile("clickhouse-public.password", chPublicOnPsq)))
	reader := chBundleFile("clickhouse-reader.password", chReaderOnPsq)
	api := chBundleFile("clickhouse-api.password", chAPIOnPsql)
	if len(ca) == 0 || public == "" || len(reader) == 0 || len(api) == 0 {
		return nil, false
	}
	site := strings.NewReplacer("REPLACE_WITH_PUBLIC_PASSWORD", public, "REPLACE_WITH_API_PASSWORD", strings.TrimSpace(string(api))).
		Replace(string(e.set.render(e.asset("files/clickhouse.caddy"))))
	files = []file{
		{chCAOnPsql, ca, 0o644, "root:root"},
		{chPublicOnPsq, []byte(public + "\n"), 0o600, "root:root"},
		{chReaderOnPsq, reader, 0o600, "root:root"},
		{chAPIOnPsql, api, 0o600, "root:root"},
		{chSite, []byte(site), 0o640, "root:caddy"},
	}
	if monitoring(e.set) {
		alerts, _ := clickhousefiles.Files.ReadFile("files/alerts.yml")
		files = append(files,
			file{chScrape, e.set.render(e.asset("files/clickhouse-scrape.yml")), 0o644, "root:root"},
			file{chAlerts, alerts, 0o644, "root:root"})
	}
	return files, true
}

func psqlCHCheck(e *env) result {
	files, ready := psqlCHFiles(e)
	switch {
	case !clickhouseOn(e.set):
		for _, p := range []string{chSite, chScrape, chAlerts} {
			if exists(p) {
				return change("remove %s (clickhouse_hostname = none)", p)
			}
		}
		return ok("off (clickhouse_hostname = none)")
	case !ready:
		return waiting("needs the clickhouse bundle: scp -3 -r root@<clickhouse host>:%s root@<this host>:/root/", chBundle)
	}
	if p := firstMismatch(files...); p != "" {
		return change("write %s", p)
	}
	if !monitoring(e.set) && (exists(chScrape) || exists(chAlerts)) {
		return change("remove the clickhouse scrape jobs (monitoring off)")
	}
	return ok("https://%s", e.v("clickhouse_hostname"))
}

func psqlCHApply(e *env) error {
	files, ready := psqlCHFiles(e)
	if clickhouseOn(e.set) && !ready {
		return errors.New("the clickhouse bundle is missing")
	}
	var stale []string // what the files no longer want
	if !clickhouseOn(e.set) {
		stale = []string{chSite, chScrape, chAlerts}
	} else if !monitoring(e.set) {
		stale = []string{chScrape, chAlerts}
	}
	removed := map[string]bool{}
	for _, p := range stale {
		if exists(p) {
			if err := os.Remove(p); err != nil {
				return err
			}
			e.note("removed %s", p)
			removed[filepath.Dir(p)] = true
		}
	}
	if err := e.mkdir(dir{"/etc/caddy/sites", 0o755, "root:root"}); err != nil {
		return err
	}
	if monitoring(e.set) {
		if err := e.mkdir(dir{"/etc/prometheus/scrape.d", 0o755, "root:root"}); err != nil {
			return err
		}
	}
	for _, f := range files {
		changed, err := e.writeAll(f)
		if err != nil {
			return err
		}
		if changed {
			removed[filepath.Dir(f.path)] = true
		}
	}
	if removed["/etc/caddy/sites"] {
		// Caddy reads the site at its reload: check the whole configuration first.
		if _, err := e.run("runuser", "-u", "caddy", "--", "/usr/local/bin/caddy", "validate", "--config", "/etc/caddy/Caddyfile"); err != nil {
			return err
		}
		if _, err := e.run("systemctl", "reload", "caddy.service"); err != nil {
			return err
		}
		e.note("reloaded caddy.service")
	}
	if (removed["/etc/prometheus/scrape.d"] || removed["/etc/prometheus/rules"]) && active("prometheus.service") {
		if _, err := e.run("/usr/local/bin/promtool", "check", "config", "/etc/prometheus/prometheus.yml"); err != nil {
			return err
		}
		// Through ensure, which records the reload: the monitoring step
		// watches the same directories.
		return e.ensure(service{unit: "prometheus.service", reload: true,
			files: []string{"/etc/prometheus", "/etc/prometheus/rules", "/etc/prometheus/scrape.d"}})
	}
	return nil
}

// psqlCHHealth asks Caddy what the public and the frontend's backend get:
// /ping, and a query each.
func psqlCHHealth(e *env) result {
	if !clickhouseOn(e.set) {
		return ok("off")
	}
	base := "https://" + e.v("clickhouse_hostname")
	code, body, days, err := get(base+"/ping", "127.0.0.1:443")
	if err != nil || code != 200 || strings.TrimSpace(string(body)) != "Ok." {
		return failed("GET %s/ping through 127.0.0.1: %d %v", base, code, err)
	}
	code, body, _, err = get(base+"/?query="+url.QueryEscape("SELECT count() FROM stellar.ledgers"), "127.0.0.1:443")
	if err != nil || code != 200 {
		return failed("a query through %s: %d %s %v", base, code, strings.TrimSpace(string(body)), err)
	}
	public := strings.TrimSpace(string(body))
	code, body, err = getAs(base+"/?query="+url.QueryEscape("SELECT currentUser()"), "127.0.0.1:443", backendUser, backendPassword)
	if err != nil || code != 200 || strings.TrimSpace(string(body)) != "stellar_api" {
		return failed("a query as %s through %s: %d %s %v", backendUser, base, code, strings.TrimSpace(string(body)), err)
	}
	return ok("%s answers queries (%s ledgers), as %s (stellar_api) too; certificate %d days left", base, public, backendUser, days)
}

// getAs is get with basic auth.
func getAs(url, addr, user, password string) (int, []byte, error) {
	tr := &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, addr)
	}}
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return 0, nil, err
	}
	req.SetBasicAuth(user, password)
	resp, err := (&http.Client{Transport: tr, Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, body, err
}
