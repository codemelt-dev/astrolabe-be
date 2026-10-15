package main

// ClickHouse in Grafana, while the clickhouse host's site is on
// (clickhouse_hostname, psql step clickhouse): the ClickHouse data source
// plugin, the data source (stellar_reader over native TLS) and the
// dashboard dashboard/clickhouse.json (dashboard/README.md).

import (
	"archive/zip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	dashboardfiles "stellar-explorer/dashboard"
)

const (
	chPluginVersion = "4.22.1"
	chPluginSHA256  = "fc8b4a4ff71e7db614a3560e5b5678bbab523578abca280a195e0bd30d9a52d5"
	chPluginName    = "grafana-clickhouse-datasource"
	chPluginDir     = "/var/lib/grafana/plugins/" + chPluginName
	chDataSource    = "/etc/grafana/provisioning/datasources/stellar-clickhouse.yaml"
	chGrafanaPass   = "/etc/grafana/clickhouse-reader.password"
	chDashboard     = grafanaDash + "/clickhouse.json"
)

// grafanaCHFiles are the data source, its password and the dashboard: nil
// while the clickhouse site is off or its bundle isn't installed yet.
func grafanaCHFiles(e *env) []file {
	pw, err := os.ReadFile(chReaderOnPsq)
	if !clickhouseOn(e.set) || err != nil {
		return nil
	}
	read := func(n string) []byte { b, _ := dashboardfiles.Files.ReadFile(n); return b }
	return []file{
		{chGrafanaPass, pw, 0o640, "root:grafana"},
		{chDataSource, e.set.render(read("clickhouse-datasource.yaml")), 0o640, "root:grafana"},
		{chDashboard, read("clickhouse.json"), 0o644, "root:root"},
	}
}

// chPluginCurrent reports whether the pinned plugin is installed.
func chPluginCurrent() bool {
	var p struct{ Info struct{ Version string } }
	b, err := os.ReadFile(chPluginDir + "/plugin.json")
	return err == nil && json.Unmarshal(b, &p) == nil && p.Info.Version == chPluginVersion
}

func grafanaCHCheck(e *env) result {
	files := grafanaCHFiles(e)
	switch {
	case clickhouseOn(e.set) && files == nil:
		return waiting("the ClickHouse data source needs the psql step clickhouse (its bundle)")
	case files == nil:
		for _, p := range []string{chDataSource, chDashboard, chGrafanaPass} {
			if exists(p) {
				return change("remove %s (clickhouse_hostname = none)", p)
			}
		}
		return ok("")
	case !chPluginCurrent():
		return change("install the ClickHouse data source plugin %s", chPluginVersion)
	}
	if p := firstMismatch(files...); p != "" {
		return change("write %s", p)
	}
	return ok("")
}

// grafanaCHApply installs or removes the data source, its plugin and the
// dashboard. It reports whether it removed the data source, which takes a
// restart that the files' times can't show.
func (e *env) grafanaCHApply() (bool, error) {
	files := grafanaCHFiles(e)
	if files == nil {
		removed := false
		for _, p := range []string{chDataSource, chDashboard, chGrafanaPass} {
			if exists(p) {
				if err := os.Remove(p); err != nil {
					return removed, err
				}
				e.note("removed %s", p)
				removed = removed || p == chDataSource
			}
		}
		return removed, nil
	}
	if !chPluginCurrent() {
		if err := e.installCHPlugin(); err != nil {
			return false, err
		}
	}
	_, err := e.writeAll(files...)
	return false, err
}

// installCHPlugin downloads the plugin (checked by SHA-256) and unpacks it
// beside the installed one, then swaps them, so a failed unpack changes
// nothing.
func (e *env) installCHPlugin() error {
	zipPath := "/var/cache/lab/" + chPluginName + "-" + chPluginVersion + ".linux-amd64.zip"
	url := "https://grafana.com/api/plugins/" + chPluginName + "/versions/" + chPluginVersion + "/download?os=linux&arch=amd64"
	if err := e.download(url, chPluginSHA256, zipPath); err != nil {
		return err
	}
	defer os.Remove(zipPath)
	z, err := zip.OpenReader(zipPath)
	if err != nil {
		return err
	}
	defer z.Close()
	tmp := chPluginDir + ".new"
	os.RemoveAll(tmp)
	for _, f := range z.File {
		rel, ok := strings.CutPrefix(f.Name, chPluginName+"/")
		if !ok || strings.Contains(rel, "..") {
			return fmt.Errorf("%s: unexpected entry %q", zipPath, f.Name)
		}
		dest := filepath.Join(tmp, rel)
		if f.FileInfo().IsDir() {
			if err := os.MkdirAll(dest, 0o755); err != nil {
				return err
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
			return err
		}
		if err := unzipFile(f, dest); err != nil {
			return err
		}
	}
	if _, err := e.run("chown", "-R", "grafana:grafana", tmp); err != nil {
		return err
	}
	if err := os.RemoveAll(chPluginDir); err != nil {
		return err
	}
	if err := os.Rename(tmp, chPluginDir); err != nil {
		return err
	}
	e.note("installed the ClickHouse data source plugin %s", chPluginVersion)
	return nil
}

func unzipFile(f *zip.File, dest string) error {
	r, err := f.Open()
	if err != nil {
		return err
	}
	defer r.Close()
	mode := os.FileMode(0o644)
	if f.Mode()&0o111 != 0 {
		mode = 0o755 // the plugin's backend binary
	}
	w, err := os.OpenFile(dest, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	if _, err := io.Copy(w, r); err != nil {
		w.Close()
		return err
	}
	return w.Close()
}
