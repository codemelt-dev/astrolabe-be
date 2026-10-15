package main

// jobs on the psql host: the worker shipped next to this app, and its daily
// state-copy timer, which copies PostgreSQL's state into ClickHouse and
// computes the rankings there (clickhouse/README.md §10).
// With the clickhouse site only: it needs its CA and the bundle's
// stellar_jobs password.

import (
	"os"
	"strings"
	"time"
)

const (
	stateCopyUnit  = "stellar-state-copy.service"
	stateCopyTimer = "stellar-state-copy.timer"
	chJobsPassword = "/etc/stellar/clickhouse-jobs.password"
	stateCopyMark  = "/var/lib/prometheus/node-exporter/stellar_state_copy.prom"
)

var stateCopyService = service{unit: stateCopyTimer, files: []string{"/etc/systemd/system/" + stateCopyTimer}}

// jobsFiles are the step's files; ready is false while the bundle's
// password is missing.
func jobsFiles(e *env) (files []file, ready bool) {
	pw := chBundleFile("clickhouse-jobs.password", chJobsPassword)
	if len(pw) == 0 || !exists(chCAOnPsql) {
		return nil, false
	}
	unit := func(n string) file {
		return file{"/etc/systemd/system/" + n, e.asset("files/" + n), 0o644, "root:root"}
	}
	return []file{
		{workerConf, e.set.render(e.asset("files/worker.toml")), 0o640, "root:stellar"},
		{chJobsPassword, pw, 0o640, "root:stellar"},
		{"/usr/local/sbin/state-copy-mark-success", e.asset("files/state-copy-mark-success"), 0o755, "root:root"},
		unit(stateCopyUnit), unit(stateCopyTimer),
	}, true
}

func jobsCheck(e *env) result {
	if !clickhouseOn(e.set) {
		return ok("off (clickhouse_hostname = none)")
	}
	files, ready := jobsFiles(e)
	switch {
	case !ready:
		return waiting("needs the clickhouse step and its bundle (clickhouse-jobs.password)")
	case !userExists("stellar"):
		return change("user stellar")
	}
	if r := checkWorker([]string{workerBin}); r.status != statusOK {
		return r
	}
	if p := firstMismatch(files...); p != "" {
		return change("write %s", p)
	}
	return stateCopyService.state()
}

func jobsApply(e *env) error {
	if !clickhouseOn(e.set) {
		return nil
	}
	files, _ := jobsFiles(e)
	err := e.systemUser("stellar", "/nonexistent")
	if err == nil {
		err = e.installWorker([]string{workerBin})
	}
	if err == nil {
		err = e.mkdir(etcStellar)
	}
	changed := false
	if err == nil {
		changed, err = e.writeAll(files...)
	}
	if err == nil {
		err = e.daemonReload(changed)
	}
	if err == nil {
		err = e.ensure(stateCopyService)
	}
	return err
}

// jobsHealth reports the last state copy: its marker's age, and the unit's
// last result.
func jobsHealth(e *env) result {
	if !clickhouseOn(e.set) {
		return ok("off")
	}
	next := unitIs("NextElapseUSecRealtime", stateCopyTimer)
	fi, err := os.Stat(stateCopyMark)
	if err != nil {
		if unitIs("ActiveState", stateCopyUnit) == "activating" {
			return waiting("the first state copy is running (journalctl -u %s)", stateCopyUnit)
		}
		return waiting("no state copy yet; next %s (or: systemctl start %s)", next, stateCopyUnit)
	}
	age := time.Since(fi.ModTime()).Round(time.Minute)
	if res := unitIs("Result", stateCopyUnit); res != "success" || age > 49*time.Hour {
		return failed("last successful state copy %s ago; last result %s (journalctl -u %s)", age, strings.TrimSpace(res), stateCopyUnit)
	}
	return ok("last state copy %s ago; next %s", age, next)
}
