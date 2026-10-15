package main

// The pipeline: the worker binary, /etc/stellar, the units, the database
// schema, the one-time bootstrap, and the running services.

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/BurntSushi/toml"
)

const (
	workerBin  = "/usr/local/bin/worker"
	workerConf = "/etc/stellar/worker.toml"
	gatewayEnv = "/etc/stellar/gateway.env"
	bundleDir  = "/root/galexie-bundle" // written by lab psql, copied here with scp
	originPath = "/srv/lake/state/bootstrap.json"
)

// worker

// Both symlinks point at versioned builds; the deploy scripts switch them.
var workerLinks = []string{workerBin, "/usr/local/bin/worker-lake"}

func workerVersion(path string) (string, error) {
	out, err := probe(path, "version")
	if v, found := strings.CutPrefix(out, "worker "); err == nil && found {
		return v, nil
	}
	return "", fmt.Errorf("%s version: %v %s", path, err, out)
}

// workerCheck reports the installed versions (later ones: galexie/README.md §7.2).
func workerCheck(*env) result { return checkWorker(workerLinks) }

func workerApply(e *env) error { return e.installWorker(workerLinks) }

// checkWorker reports the versions behind links (a host's worker symlinks).
func checkWorker(links []string) result {
	var detail []string
	for _, l := range links {
		if _, err := os.Lstat(l); err != nil {
			if _, err := besideSelf("worker"); err != nil {
				return failed("%v", err)
			}
			return change("install %s", l)
		}
		v, err := workerVersion(l)
		if err != nil {
			return failed("%v", err)
		}
		detail = append(detail, filepath.Base(l)+" "+v)
	}
	return ok("%s", strings.Join(detail, ", "))
}

// installWorker installs the shipped worker as a versioned build, never over
// other bytes (as the deploy scripts), and creates the missing symlinks.
func (e *env) installWorker(links []string) error {
	src, err := besideSelf("worker")
	if err != nil {
		return err
	}
	v, err := workerVersion(src)
	if err != nil {
		return err
	}
	dest := "/usr/local/lib/stellar/worker-" + filepath.Base(v)
	if exists(dest) && sha256File(dest) != sha256File(src) {
		return fmt.Errorf("%s exists with different content: build with a new VERSION", dest)
	}
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	if _, err := e.write(file{dest, data, 0o755, "root:root"}); err != nil {
		return err
	}
	for _, l := range links {
		if _, err := os.Lstat(l); err == nil {
			continue
		}
		if err := e.symlink(dest, l); err != nil {
			return err
		}
	}
	return nil
}

// config

// The stellar user reads its files in /etc/stellar.
var etcStellar = dir{"/etc/stellar", 0o755, "root:root"}

// The database bundle as installed (libpq ignores readable password files).
var bundleFiles = []file{
	{path: "/etc/stellar/pg-ca.crt", mode: 0o644, owner: "root:root"},
	{path: "/etc/stellar/pgpass", mode: 0o600, owner: "stellar:stellar"},
	{path: "/etc/stellar/pgpass-owner", mode: 0o600, owner: "root:root"},
}

// configFiles returns /etc/stellar's files and whether the database bundle is
// among them: from bundleDir while it is there, else as installed.
func configFiles(e *env) ([]file, bool, error) {
	gw, err := gatewayKeys()
	keys := strings.NewReplacer("REPLACE_WITH_UPLOADER_ACCESS_KEY", e.set.get("archive_access_key"),
		"REPLACE_WITH_UPLOADER_SECRET_KEY", e.set.get("archive_secret_key"))
	files := []file{
		{workerConf, e.asset("deploy/worker.toml"), 0o640, "root:stellar"},
		{"/etc/stellar/galexie.toml", e.asset("deploy/galexie.toml"), 0o644, "root:root"},
		{"/etc/stellar/galexie-repair.toml", e.asset("deploy/galexie-repair.toml"), 0o644, "root:root"},
		{"/etc/stellar/galexie-archive.toml", e.asset("deploy/galexie-archive.toml"), 0o644, "root:root"},
		{gatewayEnv, gw, 0o640, "root:stellar"},
		{"/etc/stellar/archive.env", []byte(keys.Replace(string(e.asset("deploy/archive.env.example")))), 0o640, "root:stellar"},
		{"/etc/stellar/archive-aws.env", []byte(keys.Replace(string(e.asset("deploy/archive-aws.env.example")))), 0o600, "root:root"},
	}
	n := len(files)
	for _, f := range bundleFiles {
		if f.data, _ = os.ReadFile(filepath.Join(bundleDir, filepath.Base(f.path))); f.data == nil {
			if f.data, _ = os.ReadFile(f.path); f.data == nil {
				return files[:n], false, err // all or nothing
			}
		}
		files = append(files, f)
	}
	return files, true, err
}

var gatewayRe = regexp.MustCompile(`(?m)^AWS_ACCESS_KEY_ID=(\w+)\nAWS_SECRET_ACCESS_KEY=(\w+)$`)

// gatewayKeys returns gateway.env with its keys (never rotated silently), or new.
func gatewayKeys() ([]byte, error) {
	b, err := os.ReadFile(gatewayEnv)
	keys := gatewayRe.FindStringSubmatch(string(b))
	if err != nil {
		keys = []string{"", randomHex(10), randomHex(32)}
	} else if keys == nil || strings.HasPrefix(keys[1], "REPLACE_") {
		return nil, fmt.Errorf("%s has no usable keys: fix or remove it", gatewayEnv)
	}
	return fmt.Appendf(nil, "AWS_ACCESS_KEY_ID=%s\nAWS_SECRET_ACCESS_KEY=%s\nAWS_EC2_METADATA_DISABLED=true\n", keys[1], keys[2]), nil
}

func randomHex(n int) string {
	b := make([]byte, n)
	rand.Read(b) // never fails
	return hex.EncodeToString(b)
}

func configCheck(e *env) result {
	files, found, err := configFiles(e)
	switch p := firstMismatch(files...); {
	case err != nil:
		return failed("%v", err)
	case !etcStellar.matches():
		return change("dir %s", etcStellar.path)
	case p != "":
		return change("write %s", p)
	case !found:
		return waiting("no database bundle: after lab psql, `scp -r root@<psql host>:%s root@<this host>:/root/` from your workstation", bundleDir)
	}
	return ok("worker.toml, galexie*.toml, gateway and archive keys, database bundle")
}

func configApply(e *env) error {
	files, _, err := configFiles(e)
	if err == nil {
		err = e.mkdir(etcStellar)
	}
	if err == nil {
		_, err = e.writeAll(files...)
	}
	return err
}

// dsns returns worker.toml's DSNs: ingest_writer's and explorer_owner's (toml
// matches keys to fields case-insensitively).
func dsns() (writer, owner string, err error) {
	var c struct {
		Postgres struct{ DSN, Migrate_DSN string }
	}
	_, err = toml.DecodeFile(workerConf, &c)
	return c.Postgres.DSN, c.Postgres.Migrate_DSN, err
}

// psql runs one query as a local user, never prompting for a password.
func psql(user, dsn, query string) (string, error) {
	return probe("runuser", "-u", user, "--", "psql", "-w", "-X", "-At", "-c", query, dsn+"&connect_timeout=5")
}

// configHealth logs in like the worker: as ingest_writer and explorer_owner.
func configHealth(*env) result {
	writer, owner, err := dsns()
	if err != nil {
		return failed("%v", err)
	}
	for user, dsn := range map[string]string{"stellar": writer, "root": owner} {
		if out, err := psql(user, dsn, "SELECT 1"); err != nil {
			return failed("Postgres login as %s: %s", user, tail(out, 1))
		}
	}
	return ok("TLS logins to Postgres as ingest_writer and explorer_owner")
}

// units

// unitFiles are deploy/systemd's units but the firewall's (and no .d/ example).
func unitFiles(e *env) []file {
	var out []file
	entries, _ := fs.ReadDir(current.files, "deploy/systemd")
	for _, d := range entries {
		if !d.IsDir() && d.Name() != firewallService.unit {
			out = append(out, file{"/etc/systemd/system/" + d.Name(), e.asset("deploy/systemd/" + d.Name()), 0o644, "root:root"})
		}
	}
	return out
}

func unitsCheck(e *env) result {
	if p := firstMismatch(unitFiles(e)...); p != "" {
		return change("write %s", p)
	}
	return ok("%d units", len(unitFiles(e)))
}

func unitsApply(e *env) error {
	changed, err := e.writeAll(unitFiles(e)...)
	if err != nil {
		return err
	}
	return e.daemonReload(changed)
}

// migrate

func migrateCheck(*env) result {
	if !exists(workerBin) || !exists("/etc/stellar/pgpass-owner") {
		return waiting("needs the worker and the database bundle")
	}
	out, err := probe("env", "PGCONNECT_TIMEOUT=10", workerBin, "migrate", "--check")
	if err == nil {
		return ok("schema current")
	}
	if n := strings.Count("\n"+out, "\npending "); n > 0 {
		return change("%d migration(s) pending", n)
	}
	return failed("worker migrate --check: %s", tail(out, 1))
}

func migrateApply(e *env) error {
	if _, err := e.run(workerBin, "migrate"); err != nil {
		return err
	}
	e.note("ran worker migrate")
	return nil
}

// bootstrap

// cursor returns the explorer cursor ("" when there is none, or no table yet)
// and whether ingest_writer may truncate the state tables, as bootstrap does
// (psql's roles.sql grants it by default privilege; grants.sql catches up).
// Without the tables there is nothing to judge yet: granted is true.
func cursor() (cur string, granted bool, err error) {
	writer, _, err := dsns()
	if err != nil {
		return "", false, err
	}
	out, err := psql("stellar", writer, "SELECT (SELECT ledger_sequence FROM public.ingest_state WHERE pipeline = 'explorer'), has_table_privilege('explorer.accounts', 'TRUNCATE')")
	if err != nil && !strings.Contains(out, `relation "public.ingest_state" does not exist`) {
		return "", false, errors.New(tail(out, 1))
	}
	if cur, grant, found := strings.Cut(out, "|"); found && err == nil {
		return cur, grant == "t", nil
	}
	return "", true, nil // no table before the first migrate
}

func bootstrapCheck(e *env) result {
	if b, err := os.ReadFile(originPath); err == nil {
		return ok("bootstrap.json: %s", originRe.FindString(string(b)))
	}
	if _, found, _ := configFiles(e); !found {
		return waiting("needs the database bundle")
	}
	cur, granted, err := cursor()
	switch {
	case err != nil && exists("/etc/stellar/pgpass") && exists(workerConf):
		return failed("read the cursor: %v", err)
	case err == nil && cur != "":
		return change("no %s, cursor %s: recreate it from Postgres", originPath, cur)
	case err == nil && !granted:
		return waiting("ingest_writer may not truncate: run lab psql on the psql host")
	}
	// No cursor, or the first run, before the database can be read.
	return result{status: statusChange, detail: "no cursor: worker bootstrap",
		ask: "Run worker bootstrap? It loads the current state from the latest checkpoint: it takes a while and much memory (log: " + logPath() + ")."}
}

var originRe = regexp.MustCompile(`"archive_origin":\s*\d+`)

// bootstrapApply runs as stellar, which owns /srv/lake/state.
func bootstrapApply(e *env) error {
	cur, _, err := cursor()
	if err != nil {
		return err
	}
	args := []string{"-u", "stellar", "--", workerBin, "bootstrap"}
	if cur != "" {
		args = append(args, "--recreate-origin") // quick, and never overwrites
	} else if !e.approved["bootstrap"] {
		return errNotApproved
	}
	if _, err = e.run("runuser", args...); err != nil {
		return err
	}
	e.note("ran worker %s", strings.Join(args[4:], " "))
	return nil
}

// services

// Each unit with the files it must be restarted for, only with --allow-restart
// (an unclean Galexie stop costs a captive-core rebuild). A lake restart stops
// Galexie first and starts it after the lake's READY, as deploy-lake.sh does.
var pipelineServices = []service{
	{unit: "stellar-lake.service", files: []string{"/etc/systemd/system/stellar-lake.service", "/usr/local/bin/worker-lake", workerConf, gatewayEnv}},
	{unit: "stellar-ingest.service", files: []string{"/etc/systemd/system/stellar-ingest.service", workerBin, workerConf, gatewayEnv, "/etc/stellar/archive.env", "/etc/stellar/pgpass", "/etc/stellar/pg-ca.crt"}},
	{unit: "stellar-galexie.service", files: []string{"/etc/systemd/system/stellar-galexie.service", "/etc/stellar/galexie.toml", gatewayEnv}},
	{unit: "stellar-watchdog.timer"},
	{unit: "stellar-gapcheck.timer"},
}

func servicesCheck(*env) result {
	if !exists(originPath) { // Galexie's launcher refuses to start without it
		return waiting("needs %s (step bootstrap)", originPath)
	}
	r := ok("lake, ingest, Galexie and both timers running")
	for _, s := range pipelineServices {
		switch sr := s.state(); sr.status {
		case statusChange:
			return sr
		case statusRestart:
			r = sr
		}
	}
	return r
}

func servicesApply(e *env) error {
	for _, s := range pipelineServices {
		if err := e.ensure(s); err != nil {
			return err
		}
	}
	return nil
}

// pipelineHealth reads what an operator would (galexie/README.md §7.1): the lake, the
// last upload, ingest's readiness, cursor and lag, and the archive tip.
func pipelineHealth(*env) result {
	lake, ready := fetch("127.0.0.1:9001/healthz"), fetch("127.0.0.1:9102/readyz")
	m, gx := fetch("127.0.0.1:9102/metrics"), fetch("127.0.0.1:6061/metrics")
	var tip struct{ Tip *uint32 }
	if json.Unmarshal([]byte(fetch("127.0.0.1:9001/v1/tip")), &tip) == nil && tip.Tip != nil {
		lake += fmt.Sprintf(", tip %d", *tip.Tip)
	}
	r := ok("lake %s · galexie uploaded %s · ingest %s, cursor %s (lag %s) · archive tip %s",
		lake, metric(gx, "galexie_uploader_latest_ledger", "nothing yet"), strings.ReplaceAll(ready, "\n", "; "),
		metric(m, `ingest_cursor{pipeline="explorer"}`, "?"), metric(m, `ingest_lag_ledgers{pipeline="explorer"}`, "?"), metric(m, "archive_tip", "?"))
	if !strings.HasPrefix(lake, "ok") || ready != "ready" {
		r.status = statusFailed
	}
	return r
}

// fetch returns a local endpoint's body whatever its status, or "down".
func fetch(url string) string {
	if _, body, _, err := get("http://"+url, ""); err == nil {
		return strings.TrimSpace(string(body))
	}
	return "down"
}

// metric returns a sample's value as an integer (it may read 5.8e+07), or def.
func metric(text, series, def string) string {
	for _, line := range strings.Split(text, "\n") {
		if v, found := strings.CutPrefix(line, series+" "); found {
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				return strconv.FormatFloat(f, 'f', 0, 64)
			}
		}
	}
	return def
}
