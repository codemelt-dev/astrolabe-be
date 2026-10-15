package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	dashboardfiles "stellar-explorer/dashboard"
)

func TestHasConffile(t *testing.T) {
	// As probe returns it: trimmed, so the first line lost its leading space.
	list := "/etc/nftables.conf 4d4241fcd44b9a69b74f69d571f35a20\n /etc/other.conf 0123 obsolete"
	for _, c := range []struct {
		path, md5 string
		want      bool
	}{
		{"/etc/nftables.conf", "4d4241fcd44b9a69b74f69d571f35a20", true},
		{"/etc/nftables.conf", "ffff", false},
		{"/etc/other.conf", "0123", true},
		{"/etc/nftables", "4d4241fcd44b9a69b74f69d571f35a20", false},
	} {
		if got := hasConffile(list, c.path, c.md5); got != c.want {
			t.Errorf("hasConffile(%s, %s) = %v, want %v", c.path, c.md5, got, c.want)
		}
	}
}

func TestPristinePostinstDefault(t *testing.T) {
	dir := t.TempDir()
	sample, path := filepath.Join(dir, "sample.ini"), filepath.Join(dir, "grafana.ini")
	if err := os.WriteFile(sample, []byte("[server]\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	postinstDefaults[path] = sample
	defer delete(postinstDefaults, path)
	if !pristine(path, []byte("[server]\n")) {
		t.Error("an untouched copy of the sample is not pristine")
	}
	if pristine(path, []byte("[server]\nhttp_port = 1\n")) {
		t.Error("an edited copy of the sample is pristine")
	}
}

func TestReplaceToken(t *testing.T) {
	for _, c := range []struct{ in, old, want string }{
		{"host 10.0.0.2/32", "10.0.0.2", "host X/32"},
		{"10.0.0.2:9101, 10.0.0.2.", "10.0.0.2", "X:9101, X."},
		{"10.0.0.20 110.0.0.2 10.0.0.2.5", "10.0.0.2", "10.0.0.20 110.0.0.2 10.0.0.2.5"},
		{"https://10.0.0.2/x", "10.0.0.2", "https://X/x"},
		{"stellar-ledgers-pubnet-staging stellar-ledgers-pubnet", "stellar-ledgers-pubnet", "stellar-ledgers-pubnet-staging X"},
	} {
		if got := replaceToken(c.in, c.old, "X"); got != c.want {
			t.Errorf("replaceToken(%q, %q) = %q, want %q", c.in, c.old, got, c.want)
		}
	}
}

// exampleSettings configures every setting of the current host with its own
// example value.
func exampleSettings() settings {
	s := settings{}
	for _, d := range current.settings {
		s[d.key] = d.example
		if s[d.key] == "" {
			s[d.key] = d.def
		}
	}
	return s
}

// The host folders' files are the templates: rendering them with the example
// values changes nothing, and every example value is used somewhere.
func TestRenderExamplesIsIdentity(t *testing.T) {
	for _, h := range hosts {
		current = h
		set := exampleSettings()
		var all strings.Builder
		err := fs.WalkDir(h.files, ".", func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return err
			}
			b, _ := fs.ReadFile(h.files, path)
			if got := set.render(b); string(got) != string(b) {
				t.Errorf("%s/%s changes when rendered with the example values", h.name, path)
			}
			all.Write(b)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range h.settings {
			if d.example != "" && !d.secret && !strings.Contains(all.String(), d.example) {
				t.Errorf("%s setting %s: example %q is in no file", h.name, d.key, d.example)
			}
		}
	}
}

// The step tables' order: each step relies on the ones before it.
func TestSteps(t *testing.T) {
	for h, want := range map[*host]string{
		galexieHost:    "system firewall disks docker worker config units migrate bootstrap services",
		psqlHost:       "system network firewall postgres database backups api postgrest caddy monitoring clickhouse jobs grafana",
		clickhouseHost: "system network firewall clickhouse worker config migrate init services",
	} {
		var names []string
		for _, s := range h.steps() {
			if s.check == nil || s.apply == nil || s.about == "" {
				t.Errorf("%s step %q is incomplete", h.name, s.name)
			}
			names = append(names, s.name)
		}
		if got := strings.Join(names, " "); got != want {
			t.Errorf("%s steps: %s", h.name, got)
		}
	}
}

// Every step's check and health only read settings their host has: an
// unknown one panics (settings.get). They run here against this machine,
// where they fail, but must not panic.
func TestStepsReadOnlyTheirSettings(t *testing.T) {
	for _, h := range hosts {
		current = h
		e := &env{ctx: context.Background(), set: exampleSettings(), log: io.Discard, approved: map[string]bool{}}
		for _, s := range h.steps() {
			for kind, f := range map[string]func(*env) result{"check": s.check, "health": s.health} {
				if f == nil {
					continue
				}
				func() {
					defer func() {
						if r := recover(); r != nil {
							t.Errorf("%s step %s, %s: panic: %v", h.name, s.name, kind, r)
						}
					}()
					f(e)
				}()
			}
		}
	}
}

// The repository's 20-tuning.conf and sysctl.conf show a 128 GB, 16-core,
// 32-thread server (a Hetzner AX102): rendering them for it is the identity,
// and smaller servers get smaller values.
func TestTuning(t *testing.T) {
	const gb = 1 << 30
	conf, _ := fs.ReadFile(psqlHost.files, "files/20-tuning.conf")
	sysctl, _ := fs.ReadFile(psqlHost.files, "files/sysctl.conf")
	for f, want := range map[facts]string{
		{128 * gb, 16, 32, 3_840_000_000_000}: `shared_buffers = '32GB' effective_cache_size = '90GB' work_mem = '64MB' maintenance_work_mem = '2GB' autovacuum_work_mem = '1GB' io_workers = 8 max_worker_processes = 32 max_parallel_workers = 16 max_parallel_workers_per_gather = 4 max_wal_size = '64GB' min_wal_size = '4GB' autovacuum_max_workers = 6 vm.nr_hugepages = 17200`,
		{16 * gb, 4, 8, 480_000_000_000}:      `shared_buffers = '4GB' effective_cache_size = '11GB' work_mem = '8MB' maintenance_work_mem = '256MB' autovacuum_work_mem = '128MB' io_workers = 3 max_worker_processes = 8 max_parallel_workers = 4 max_parallel_workers_per_gather = 1 max_wal_size = '27GB' min_wal_size = '1728MB' autovacuum_max_workers = 3 vm.nr_hugepages = 2300`,
		{4 * gb, 2, 2, 80_000_000_000}:        `shared_buffers = '1GB' effective_cache_size = '1GB' work_mem = '4MB' maintenance_work_mem = '64MB' autovacuum_work_mem = '64MB' io_workers = 3 max_worker_processes = 8 max_parallel_workers = 2 max_parallel_workers_per_gather = 1 max_wal_size = '4GB' min_wal_size = '256MB' autovacuum_max_workers = 3 vm.nr_hugepages = 700`,
	} {
		got := string(tuned(conf, f)) + string(tuned(sysctl, f))
		if f.cores == 16 && got != string(conf)+string(sysctl) {
			t.Errorf("128 GB: rendering the repository's files changes them:\n%s", got)
		}
		for _, w := range regexp.MustCompile(`[a-z_.]+ = \S+`).FindAllString(want, -1) {
			if !strings.Contains(got, "\n"+w+"\n") && !strings.Contains(got, "\n"+w+" ") {
				t.Errorf("%d GB: no %q", f.ram/gb, w)
			}
		}
	}
}

// A verifier PostgreSQL 18 computed for the password "pencil".
func TestSCRAM(t *testing.T) {
	const v = "SCRAM-SHA-256$4096:Fk6Y8inUR7qJ8uoAfwDPqQ==$fR2E9/ojxBqqmisTwvGvjVwvIqNPMiJJx+O/S+36vzY=:QciJ/PRSyjUG261xFl680e5tcIaJ0HPe/31UA8HbqkE="
	salt, _ := base64.StdEncoding.DecodeString("Fk6Y8inUR7qJ8uoAfwDPqQ==")
	if got := scramVerifier("pencil", salt); got != v {
		t.Errorf("verifier %s, want %s", got, v)
	}
	if !scramMatches(v, "pencil") || scramMatches(v, "pencil2") || scramMatches("md5abc", "pencil") || !scramMatches(scramVerifier("x", nil), "x") {
		t.Error("scramMatches")
	}
}

// The psql host's monitoring installs the galexie host's rules from the
// galexie folder, rendered with the psql host's settings.
func TestGalexieAlertsRender(t *testing.T) {
	current = psqlHost
	b := galexieAlerts()
	if len(b) == 0 || string(exampleSettings().render(b)) != string(b) {
		t.Error("galexie/deploy/alerts.yml is missing or changes when rendered with the psql examples")
	}
}

// The dashboards are valid JSON with unique uids and query only the
// provisioned data source; the configuration files carry the header place
// recognises (they replace the package's files).
func TestDashboards(t *testing.T) {
	current = psqlHost
	conf, own := grafanaFiles()
	for _, f := range conf {
		if !strings.Contains(string(f.data[:min(len(f.data), 300)]), "(lab psql,") {
			t.Errorf("%s: no (lab psql, …) header", f.path)
		}
	}
	// ClickHouse's data source and dashboard come with the clickhouse site
	// (grafana_clickhouse.go): its panels may query either data source.
	ds, _ := dashboardfiles.Files.ReadFile("clickhouse-datasource.yaml")
	if !strings.Contains(string(ds), "(lab psql,") || !strings.Contains(string(ds), "uid: clickhouse") {
		t.Errorf("clickhouse-datasource.yaml: no (lab psql, …) header or not uid clickhouse")
	}
	ch, _ := dashboardfiles.Files.ReadFile("clickhouse.json")
	own = append(own, file{path: chDashboard, data: ch})
	uids := map[string]bool{}
	for _, f := range own {
		if !strings.HasSuffix(f.path, ".json") {
			continue
		}
		var d struct {
			UID    string `json:"uid"`
			Panels []struct {
				Datasource *struct{ UID string } `json:"datasource"`
				Targets    []struct {
					Expr   string `json:"expr"`
					RawSQL string `json:"rawSql"`
				} `json:"targets"`
			} `json:"panels"`
		}
		if err := json.Unmarshal(f.data, &d); err != nil || d.UID == "" || uids[d.UID] {
			t.Errorf("%s: invalid, no uid or a duplicate one (%v)", f.path, err)
		}
		uids[d.UID] = true
		for _, p := range d.Panels {
			if len(p.Targets) == 0 {
				continue
			}
			switch {
			case p.Datasource != nil && p.Datasource.UID == "prometheus":
			case p.Datasource != nil && p.Datasource.UID == "clickhouse" && f.path == chDashboard:
				for _, q := range p.Targets {
					if q.RawSQL == "" {
						t.Errorf("%s: a ClickHouse panel without SQL", f.path)
					}
				}
			default:
				t.Errorf("%s: a panel queries another data source", f.path)
			}
		}
	}
	if len(uids) < 3 {
		t.Errorf("%d dashboards", len(uids))
	}
}

// Caddy passes stellar_api's basic auth through by the header's first 16
// characters: base64("stellar_api:"), the user lab creates on the clickhouse
// host and its users.d file defines.
// The frontend's backend's login is matched whole, in plain, on both Caddy
// sites, before their anonymous handles; on the ClickHouse site Caddy logs
// in as stellar_api for it.
func TestBackendLogin(t *testing.T) {
	auth := `header Authorization "Basic ` + base64.StdEncoding.EncodeToString([]byte(backendUser+":"+backendPassword)) + `"`
	current = psqlHost
	for _, name := range []string{"files/Caddyfile", "files/clickhouse.caddy"} {
		site := string((&env{}).asset(name))
		m, anon := strings.Index(site, "@codemelt "+auth), strings.Index(site, "\thandle {")
		if m < 0 || anon < m {
			t.Errorf("%s does not match %s:%s before its anonymous handle", name, backendUser, backendPassword)
		}
	}
	if site := string((&env{}).asset("files/clickhouse.caddy")); !strings.Contains(site, "X-ClickHouse-User stellar_api\n\t\trequest_header X-ClickHouse-Key REPLACE_WITH_API_PASSWORD") {
		t.Error("clickhouse.caddy does not log in as stellar_api for the backend")
	}
	if strings.Contains(string((&env{}).asset("files/Caddyfile")), "\n\trequest_header -Authorization") {
		t.Error("the Caddyfile drops Authorization at site level, before the backend is recognised")
	}
	if !strings.Contains(chUserList(), " stellar_api ") || !strings.Contains(chUserList(), " stellar_jobs ") {
		t.Error("lab clickhouse does not create stellar_api and stellar_jobs")
	}
	current = clickhouseHost
	users := string((&env{}).asset("files/stellar-users.xml"))
	for _, u := range []string{"<stellar_api>", "<stellar_jobs>"} {
		if !strings.Contains(users, u) {
			t.Errorf("stellar-users.xml has no %s", u)
		}
	}
}

func chUserList() string { return " " + strings.Join(chUsers, " ") + " " }
