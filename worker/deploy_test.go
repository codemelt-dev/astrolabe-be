package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/BurntSushi/toml"
	"go.yaml.in/yaml/v3"

	"stellar-explorer/worker/config"
)

// The deploy files (galexie/deploy) are the source of truth for the host;
// these tests check their invariants and that they agree with each other:
// the units' coupling, restart policies, users and timeouts; the configs
// and env templates against the worker's config; the scripts switch only
// their own symlink and restart only their own unit; and every metric the
// alert rules use is exported.

const deployDir = "../galexie/deploy"

func readDeploy(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(testWD, deployDir, name))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// iniValues parses a systemd unit into section.key → values, joining
// continuation lines.
func iniValues(unit string) map[string][]string {
	out := map[string][]string{}
	section := ""
	for _, line := range strings.Split(strings.ReplaceAll(unit, "\\\n", " "), "\n") {
		line = strings.TrimSpace(line)
		switch {
		case line == "" || strings.HasPrefix(line, "#"):
		case strings.HasPrefix(line, "["):
			section = strings.Trim(line, "[]")
		default:
			k, v, _ := strings.Cut(line, "=")
			out[section+"."+k] = append(out[section+"."+k], strings.Join(strings.Fields(v), " "))
		}
	}
	return out
}

// words splits every value of a key into its space-separated items.
func words(u map[string][]string, key string) []string {
	var out []string
	for _, v := range u[key] {
		out = append(out, strings.Fields(v)...)
	}
	return out
}

var digestRe = regexp.MustCompile(`@sha256:[0-9a-f]{64}`)

// The three long-running units. Their coupling keeps Galexie (whose unclean
// stop costs a captive-core rebuild) away from everything but the lake:
//   - Galexie ↔ lake: Wants= + PartOf= + After=. A lake stop or restart
//     stops Galexie first (it drains into the lake) and starts it after the
//     lake's READY; a held Galexie stays stopped (propagated restarts are
//     try-restarts). Never BindsTo=: in systemd 255 a lake crash would stop
//     Galexie with a pending stop job that suppresses its Restart=always.
//   - ingest ↔ lake: Wants= + After= only, so deploying or crashing ingest
//     never touches the lake or Galexie, and a lake restart doesn't restart
//     ingest (it reconnects).
//
// The lake runs from worker-lake and ingest from worker, so an ingest
// deploy never replaces the lake's binary.
func TestDeployUnits(t *testing.T) {
	cfg := config.Default()
	lake := iniValues(readDeploy(t, "systemd/stellar-lake.service"))
	ingest := iniValues(readDeploy(t, "systemd/stellar-ingest.service"))
	gx := iniValues(readDeploy(t, "systemd/stellar-galexie.service"))
	units := map[string]map[string][]string{"stellar-lake": lake, "stellar-ingest": ingest, "stellar-galexie": gx}

	for name, want := range map[string]map[string]string{
		"stellar-lake": {
			// READY (sd_notify) only once the gateway listens: Galexie and
			// ingest order After= it. The startup scan can take minutes.
			"Service.Type": "notify", "Service.NotifyAccess": "main", "Service.User": "stellar",
			"Service.ExecStart": "/usr/local/bin/worker-lake lake",
			"Service.Restart":   "always", "Service.RestartSec": "1",
			"Service.TimeoutStartSec": "600", "Service.TimeoutStopSec": "30",
			"Service.OOMScoreAdjust": "-900", "Service.LimitNOFILE": "65536",
			// Objects must never land on the root disk.
			"Unit.RequiresMountsFor": cfg.Lake.Dir,
		},
		"stellar-ingest": {
			"Service.User": "stellar", "Service.ExecStart": "/usr/local/bin/worker ingest",
			"Service.Restart": "always", "Service.RestartSec": "5", "Service.TimeoutStopSec": "90",
			"Service.Environment": "GOMEMLIMIT=6GiB",
		},
		"stellar-galexie": {
			// Galexie exits 0 on an upload error, so always restart; docker
			// stops the container within 45 s, inside the unit's 60 s, and the
			// start covers the launcher's bounded archive probe.
			"Service.Restart": "always", "Service.RestartSec": "5",
			"Service.TimeoutStartSec": "120", "Service.TimeoutStopSec": "60",
			"Service.ExecStop":         "-/usr/bin/docker stop -t 45 stellar-galexie",
			"Service.RuntimeDirectory": "stellar-galexie",
			// "-": systemd also reads it before ExecStartPre has written it.
			"Service.EnvironmentFile": "-/run/stellar-galexie/galexie.env",
			"Unit.Requires":           "docker.service",
		},
	} {
		u := units[name]
		for k, v := range want {
			if fmt.Sprint(u[k]) != "["+v+"]" {
				t.Errorf("%s: %s = %q, want %q", name, k, u[k], v)
			}
		}
		// Never give up restarting, and start at boot.
		if fmt.Sprint(u["Unit.StartLimitIntervalSec"]) != "[0]" || fmt.Sprint(u["Install.WantedBy"]) != "[multi-user.target]" {
			t.Errorf("%s: StartLimitIntervalSec %v, WantedBy %v", name, u["Unit.StartLimitIntervalSec"], u["Install.WantedBy"])
		}
		if u["Unit.BindsTo"] != nil || u["Unit.Requisite"] != nil {
			t.Errorf("%s: BindsTo= or Requisite= %v %v", name, u["Unit.BindsTo"], u["Unit.Requisite"])
		}
	}

	// The lake depends on nothing but its mount.
	for _, k := range []string{"Unit.Wants", "Unit.Requires", "Unit.PartOf", "Unit.After"} {
		if lake[k] != nil {
			t.Errorf("stellar-lake: %s = %v", k, lake[k])
		}
	}
	for name, u := range map[string]map[string][]string{"stellar-ingest": ingest, "stellar-galexie": gx} {
		for _, k := range []string{"Unit.Wants", "Unit.After"} {
			if !slices.Contains(words(u, k), "stellar-lake.service") || !slices.Contains(words(u, k), "network-online.target") {
				t.Errorf("%s: %s = %v lacks the lake or network-online.target", name, k, u[k])
			}
		}
	}
	if fmt.Sprint(gx["Unit.PartOf"]) != "[stellar-lake.service]" || !slices.Contains(words(gx, "Unit.After"), "docker.service") {
		t.Errorf("stellar-galexie: PartOf %v, After %v", gx["Unit.PartOf"], gx["Unit.After"])
	}
	if ingest["Unit.PartOf"] != nil || ingest["Unit.Requires"] != nil {
		t.Errorf("stellar-ingest must only Want and order after the lake: %v", ingest)
	}
	// Galexie runs as root: only root runs containers.
	if gx["Service.User"] != nil {
		t.Errorf("stellar-galexie: User = %v", gx["Service.User"])
	}

	// The launcher (worker-lake) writes START before every start, after the
	// orphan cleanup: two containers would share /srv/core.
	env := strings.TrimPrefix(gx["Service.EnvironmentFile"][0], "-")
	if want := []string{"-/usr/bin/docker rm -f stellar-galexie", "/usr/local/bin/worker-lake galexie-start --out " + env}; !slices.Equal(gx["Service.ExecStartPre"], want) ||
		filepath.Dir(env) != "/run/"+gx["Service.RuntimeDirectory"][0] {
		t.Errorf("stellar-galexie: ExecStartPre %q, want %q", gx["Service.ExecStartPre"], want)
	}
	run := gx["Service.ExecStart"][0]
	image := digestRe.FindString(run)
	if image == "" {
		t.Fatal("stellar-galexie.service: the image is not pinned by digest")
	}
	wantRun := "/usr/bin/docker run --rm --name stellar-galexie --init --network host --stop-timeout 45" +
		" -v " + cfg.Metrics.CoreDir + ":" + cfg.Metrics.CoreDir + " -v /etc/stellar/galexie.toml:/config.toml:ro --env-file " + cfg.Credentials.GatewayEnv +
		" stellar/stellar-galexie:<tag>@sha256:<digest> append --start ${START} --config-file /config.toml"
	if got := regexp.MustCompile(`stellar/stellar-galexie:\S+@sha256:[0-9a-f]{64}`).ReplaceAllString(run, "stellar/stellar-galexie:<tag>@sha256:<digest>"); got != wantRun {
		t.Errorf("stellar-galexie: ExecStart\n got %s\nwant %s", got, wantRun)
	}
	if cfg.Galexie.Unit != "stellar-galexie.service" {
		t.Errorf("worker.toml [galexie] unit = %s", cfg.Galexie.Unit)
	}
}

// The timers: oneshot services run as root, the watchdog every 30 s from
// worker-lake, the gap check weekly from worker.
func TestDeployTimers(t *testing.T) {
	for _, tc := range []struct {
		unit, exec string
		timer      map[string]string
	}{
		{"stellar-watchdog", "/usr/local/bin/worker-lake watchdog", map[string]string{"Timer.OnUnitActiveSec": "30s"}},
		{"stellar-gapcheck", "/usr/local/bin/worker gapcheck", map[string]string{"Timer.OnCalendar": "weekly"}},
	} {
		svc := iniValues(readDeploy(t, "systemd/"+tc.unit+".service"))
		if fmt.Sprint(svc["Service.Type"]) != "[oneshot]" || fmt.Sprint(svc["Service.ExecStart"]) != "["+tc.exec+"]" || svc["Service.User"] != nil {
			t.Errorf("%s.service: %v", tc.unit, svc)
		}
		if fmt.Sprint(svc["Service.StateDirectoryMode"]) != "[0755]" {
			t.Errorf("%s.service: its state must be readable by ingest (stellar)", tc.unit)
		}
		timer := iniValues(readDeploy(t, "systemd/"+tc.unit+".timer"))
		for k, v := range tc.timer {
			if fmt.Sprint(timer[k]) != "["+v+"]" {
				t.Errorf("%s.timer %s = %v, want %s", tc.unit, k, timer[k], v)
			}
		}
		if fmt.Sprint(timer["Install.WantedBy"]) != "[timers.target]" {
			t.Errorf("%s.timer is not wanted by timers.target", tc.unit)
		}
	}
	cfg := config.Default()
	for unit, file := range map[string]string{"stellar-watchdog": cfg.Watchdog.StateFile, "stellar-gapcheck": cfg.Gapcheck.ResultFile} {
		dir := iniValues(readDeploy(t, "systemd/"+unit+".service"))["Service.StateDirectory"]
		if fmt.Sprint(dir) != "["+filepath.Base(filepath.Dir(file))+"]" || filepath.Dir(filepath.Dir(file)) != "/var/lib" {
			t.Errorf("%s: StateDirectory %v does not hold %s", unit, dir, file)
		}
	}
	// The M4 drop-in: the orphan cleanup and a fixed START instead of the
	// launcher.
	dropIn := iniValues(readDeploy(t, "systemd/stellar-galexie.service.d/m4-start.conf.example"))
	if !slices.Equal(dropIn["Service.ExecStartPre"], []string{"", "-/usr/bin/docker rm -f stellar-galexie"}) ||
		fmt.Sprint(dropIn["Service.Environment"]) != "[START=<n>]" {
		t.Errorf("M4 drop-in: %v", dropIn)
	}
}

type galexieTOML struct {
	AdminPort       int `toml:"admin_port"`
	DatastoreConfig struct {
		Type   string            `toml:"type"`
		Params map[string]string `toml:"params"`
		Schema struct {
			LedgersPerFile    uint32 `toml:"ledgers_per_file"`
			FilesPerPartition uint32 `toml:"files_per_partition"`
		} `toml:"schema"`
	} `toml:"datastore_config"`
	StellarCoreConfig struct {
		Network     string `toml:"network"`
		StoragePath string `toml:"storage_path"`
	} `toml:"stellar_core_config"`
}

func readGalexieTOML(t *testing.T, name string) galexieTOML {
	t.Helper()
	var g galexieTOML
	md, err := toml.Decode(readDeploy(t, name), &g)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	if u := md.Undecoded(); len(u) > 0 {
		t.Fatalf("%s: unexpected keys %v", name, u)
	}
	return g
}

// worker.toml loads, and the Galexie configs agree with it: the lake's
// bucket path, region and endpoint; the archive's; the pinned image.
func TestDeployConfigsAgree(t *testing.T) {
	cfg, err := config.Load(filepath.Join(testWD, deployDir, "worker.toml"))
	if err != nil {
		t.Fatal(err)
	}
	g := readGalexieTOML(t, "galexie.toml")
	repair := readGalexieTOML(t, "galexie-repair.toml")
	arch := readGalexieTOML(t, "galexie-archive.toml")
	p := g.DatastoreConfig.Params
	if g.AdminPort != 6061 || p["destination_bucket_path"] != cfg.Lake.BucketPath || p["region"] != cfg.Lake.Region ||
		p["endpoint_url"] != "http://"+cfg.Lake.S3Listen || g.DatastoreConfig.Schema.LedgersPerFile != 1 ||
		g.DatastoreConfig.Schema.FilesPerPartition != 64000 || g.StellarCoreConfig.Network != "pubnet" || g.StellarCoreConfig.StoragePath != cfg.Metrics.CoreDir {
		t.Errorf("galexie.toml disagrees with worker.toml: %+v", g)
	}
	want := g
	want.AdminPort = 0
	if fmt.Sprint(repair) != fmt.Sprint(want) {
		t.Errorf("galexie-repair.toml must be galexie.toml with admin_port = 0:\n%+v\n%+v", repair, want)
	}
	ap := arch.DatastoreConfig.Params
	envArchive := parseEnvTemplate(t, "archive.env.example", config.LoadArchiveEnv)
	if arch.AdminPort != 0 || ap["destination_bucket_path"] != cfg.Archive.BucketPath(cfg.Lake) ||
		ap["region"] != envArchive.Region || ap["endpoint_url"] != envArchive.Endpoint || arch.StellarCoreConfig != g.StellarCoreConfig {
		t.Errorf("galexie-archive.toml disagrees: %+v (archive.env %+v)", arch, envArchive)
	}
	image := strings.Fields(strings.Split(readDeploy(t, "systemd/stellar-galexie.service"), "--env-file /etc/stellar/gateway.env \\\n")[1])[0]
	if cfg.Galexie.Image != image || !strings.Contains(readDeploy(t, "galexie-repair.toml"), image) {
		t.Errorf("the pinned image differs: unit %s, worker.toml %s", image, cfg.Galexie.Image)
	}
	if cfg.Archive.Bucket != "stellar-ledgers-pubnet" || cfg.Gapcheck.GalexieConfig != "/etc/stellar/galexie-archive.toml" ||
		cfg.Gapcheck.EnvFile != "/etc/stellar/archive-aws.env" {
		t.Errorf("worker.toml: %+v %+v", cfg.Archive, cfg.Gapcheck)
	}
}

// parseEnvTemplate copies a template to a root-only file and parses it
// like the worker parses the installed one.
func parseEnvTemplate[T any](t *testing.T, name string, load func(string) (T, error)) T {
	t.Helper()
	path := filepath.Join(t.TempDir(), strings.TrimSuffix(name, ".example"))
	if err := os.WriteFile(path, []byte(readDeploy(t, name)), 0o600); err != nil {
		t.Fatal(err)
	}
	v, err := load(path)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return v
}

func TestDeployEnvTemplates(t *testing.T) {
	gw := parseEnvTemplate(t, "gateway.env.example", config.LoadGatewayEnv)
	aws := parseEnvTemplate(t, "archive-aws.env.example", config.LoadGatewayEnv) // the same three AWS_* keys
	arch := parseEnvTemplate(t, "archive.env.example", config.LoadArchiveEnv)
	if gw.AccessKey == "" || aws.AccessKey == "" || arch.AccessKey == "" {
		t.Fatal("empty keys")
	}
	for name, mode := range map[string]string{
		"gateway.env.example": "-m 0640 -o root -g stellar", "archive.env.example": "-m 0640 -o root -g stellar",
		"archive-aws.env.example": "-m 0600 -o root -g root",
	} {
		if !strings.Contains(readDeploy(t, name), "install "+mode+" ") {
			t.Errorf("%s: no ownership note %q", name, mode)
		}
	}
	if strings.Contains(readDeploy(t, "archive.env.example"), "\nAWS_") {
		t.Error("archive.env must never use AWS_* names")
	}
}

// The archive bucket's policy: the uploader may put, get and list; it may
// not delete or touch the policy or the bucket.
func TestDeployArchivePolicy(t *testing.T) {
	var p struct {
		Statement []struct {
			Effect    string
			Principal struct{ AWS []string }
			Action    []string
			Resource  []string
		}
	}
	if err := json.Unmarshal([]byte(readDeploy(t, "archive-policy.json")), &p); err != nil {
		t.Fatal(err)
	}
	actions := map[string][]string{}
	for _, s := range p.Statement {
		actions[s.Effect] = append(actions[s.Effect], s.Action...)
		if len(s.Principal.AWS) != 1 || !strings.HasPrefix(s.Principal.AWS[0], "arn:aws:iam:::user/p<galexie_project_id>:") ||
			!slices.Equal(s.Resource, []string{"arn:aws:s3:::stellar-ledgers-pubnet", "arn:aws:s3:::stellar-ledgers-pubnet/*"}) {
			t.Errorf("statement %+v", s)
		}
	}
	if fmt.Sprint(actions["Allow"]) != "[s3:PutObject s3:GetObject s3:ListBucket]" ||
		fmt.Sprint(actions["Deny"]) != "[s3:DeleteObject s3:PutBucketPolicy s3:DeleteBucketPolicy s3:DeleteBucket]" {
		t.Errorf("actions %v", actions)
	}
}

// The private interface only: metrics from the psql host (Prometheus),
// Postgres out to it, and never a flush of the whole ruleset (Docker's
// chains). The psql host is the one in worker.toml's DSNs.
func TestDeployNftables(t *testing.T) {
	nft := readDeploy(t, "galexie-host.nft")
	for _, want := range []string{"table inet stellar_galexie", "delete table inet stellar_galexie",
		"ip saddr $PSQL_HOST tcp dport { 9101, 9102, 6061 } accept", "ip daddr $PSQL_HOST tcp dport 5432 accept",
		"iifname != $PRIV_IF accept", "oifname != $PRIV_IF accept"} {
		if !strings.Contains(nft, want) {
			t.Errorf("galexie-host.nft lacks %q", want)
		}
	}
	for _, line := range strings.Split(nft, "\n") {
		if l := strings.TrimSpace(line); !strings.HasPrefix(l, "#") && strings.Contains(l, "flush ruleset") {
			t.Error("galexie-host.nft flushes the whole ruleset")
		}
	}
	cfg, err := config.Load(filepath.Join(testWD, deployDir, "worker.toml"))
	if err != nil {
		t.Fatal(err)
	}
	host := regexp.MustCompile(`(?m)^define PSQL_HOST = (\S+)`).FindStringSubmatch(nft)
	for _, dsn := range []string{cfg.Postgres.DSN, cfg.Postgres.MigrationDSN()} {
		if u, err := url.Parse(dsn); err != nil || host == nil || u.Hostname() != host[1] {
			t.Errorf("galexie-host.nft PSQL_HOST %v, worker.toml connects to %s", host, dsn)
		}
	}
	// stellar-firewall.service loads the file from where its header installs
	// it, early at boot, and never flushes (Docker's chains) or opens the
	// interface on stop.
	installed := strings.Fields(strings.SplitN(nft, "\n", 3)[1])[1]
	fw := iniValues(readDeploy(t, "systemd/stellar-firewall.service"))
	if fmt.Sprint(fw["Service.ExecStart"]) != "[/usr/sbin/nft -f "+installed+"]" ||
		fmt.Sprint(fw["Service.ExecReload"]) != "[/usr/sbin/nft -f "+installed+"]" ||
		fw["Service.ExecStop"] != nil || fmt.Sprint(fw["Unit.Before"]) != "[network-pre.target shutdown.target]" ||
		fmt.Sprint(fw["Install.WantedBy"]) != "[sysinit.target]" {
		t.Errorf("stellar-firewall.service does not load %s early and only: %v", installed, fw)
	}
}

var quotedRe = regexp.MustCompile(`"[^"]*"`)

// metricNameRe finds metric names in PromQL expressions.
var metricNameRe = regexp.MustCompile(`\b([a-z_][a-z0-9_]*)\s*(\{|\[|\)|>|<|=|-|/|$| and| or)`)

// exportedMetrics collects the metric names the worker exports (lake and
// ingest) from their descriptors in the source, plus Galexie's.
func exportedMetrics(t *testing.T) map[string]bool {
	t.Helper()
	names := map[string]bool{
		"up": true, "galexie_uploader_latest_ledger": true, "galexie_export_manager_latest_ledger": true,
	}
	re := regexp.MustCompile(`(?:Name:\s*|NewDesc\(\s*)"([a-z_][a-z0-9_]*)"`)
	for _, f := range []string{"lake/metrics.go", "metrics/metrics.go"} {
		b, err := os.ReadFile(filepath.Join(testWD, f))
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range re.FindAllStringSubmatch(string(b), -1) {
			names[m[1]] = true
			names[m[1]+"_bucket"] = true
		}
	}
	return names
}

// jobLabelRe finds job matchers in PromQL expressions.
var jobLabelRe = regexp.MustCompile(`job=~?"([^"]*)"`)

// The alert rules parse, carry a severity and a summary, and use only
// metrics that are exported. Severities and job names are the ones
// Prometheus and Alertmanager on the psql host route and scrape
// (psql/README.md: page or warning; jobs lake, ingest, galexie). (promtool
// check/test rules runs them: make alerts.)
func TestDeployAlertRules(t *testing.T) {
	var rules struct {
		Groups []struct {
			Name  string
			Rules []struct {
				Alert       string
				Expr        string
				Labels      map[string]string
				Annotations map[string]string
			}
		}
	}
	if err := yaml.Unmarshal([]byte(readDeploy(t, "alerts.yml")), &rules); err != nil {
		t.Fatal(err)
	}
	exported := exportedMetrics(t)
	functions := map[string]bool{"max": true, "min": true, "sum": true, "by": true, "on": true, "and": true, "or": true,
		"delta": true, "increase": true, "rate": true, "time": true, "vector": true, "histogram_quantile": true, "le": true,
		"kind": true, "reason": true, "pipeline": true, "phase": true, "job": true, "mount": true}
	n := 0
	for _, g := range rules.Groups {
		for _, r := range g.Rules {
			n++
			if r.Alert == "" || r.Expr == "" || r.Annotations["summary"] == "" ||
				!slices.Contains([]string{"page", "warning"}, r.Labels["severity"]) {
				t.Errorf("rule %+v", r)
			}
			for _, m := range jobLabelRe.FindAllStringSubmatch(r.Expr, -1) {
				if !slices.Contains([]string{"lake", "ingest", "galexie"}, m[1]) {
					t.Errorf("%s matches job %q, which Prometheus does not scrape", r.Alert, m[1])
				}
			}
			expr := quotedRe.ReplaceAllString(r.Expr, `""`) // label values are not metric names
			for _, m := range metricNameRe.FindAllStringSubmatch(expr, -1) {
				if name := m[1]; !functions[name] && !exported[name] {
					t.Errorf("%s uses %s, which nothing exports", r.Alert, name)
				}
			}
		}
	}
	if n < 20 {
		t.Fatalf("%d rules", n)
	}
	for _, want := range []string{"PipelineStall", "NetworkHalt", "LakeFsyncFailed", "LakeVolumeIOStall", "ArchiveGaps", "ArchiveStalled"} {
		if !strings.Contains(readDeploy(t, "alerts_test.yml"), "alertname: "+want) {
			t.Errorf("alerts_test.yml does not simulate %s", want)
		}
	}
}

// shimDir holds stand-ins for the GNU and systemd tools the deploy scripts
// call, recording their arguments in log.
func shimDir(t *testing.T, log string) string {
	t.Helper()
	dir := t.TempDir()
	shims := map[string]string{
		// GNU install -D and mv -T, on any platform.
		"install":   `#!/bin/sh` + "\n" + `mode=0755; while [ $# -gt 2 ]; do case $1 in -m) mode=$2; shift 2;; -D) shift;; *) shift;; esac; done; mkdir -p "$(dirname "$2")" && cp "$1" "$2" && chmod "$mode" "$2"` + "\n",
		"mv":        `#!/bin/sh` + "\n" + `[ "$1" = -Tf ] && shift; exec /bin/mv -f "$@"` + "\n",
		"systemctl": `#!/bin/sh` + "\n" + `echo "systemctl $*" >> "` + log + `"` + "\n",
		"curl":      `#!/bin/sh` + "\n" + `echo "curl $*" >> "` + log + `"; echo ok` + "\n",
	}
	for name, body := range shims {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func fakeWorker(t *testing.T, version, log string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "worker")
	body := fmt.Sprintf("#!/bin/sh\necho \"worker %s $*\" >> %q\n[ \"$1\" = version ] && echo 'worker %s'\nexit 0\n", version, log, version)
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

func runScript(t *testing.T, prefix, shims, script string, args ...string) (string, error) {
	t.Helper()
	cmd := exec.Command("/bin/sh", append([]string{filepath.Join(testWD, deployDir, script)}, args...)...)
	cmd.Env = append(os.Environ(), "PREFIX="+prefix, "PATH="+shims+":/usr/bin:/bin")
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	return out.String(), err
}

// An ingest deploy switches only /usr/local/bin/worker and restarts
// only stellar-ingest: the lake, the launcher, the watchdog and Galexie
// (all worker-lake) are untouched. A lake deploy is the mirror image.
// Versioned installs are never replaced in place.
func TestDeployScripts(t *testing.T) {
	prefix := t.TempDir()
	log := filepath.Join(t.TempDir(), "calls.log")
	shims := shimDir(t, log)
	if err := os.MkdirAll(filepath.Join(prefix, "bin"), 0o755); err != nil {
		t.Fatal(err)
	}
	link := func(name string) string {
		t.Helper()
		dst, err := os.Readlink(filepath.Join(prefix, "bin", name))
		if err != nil {
			return ""
		}
		return dst
	}
	calls := func() []string {
		b, _ := os.ReadFile(log)
		_ = os.Remove(log)
		return strings.Split(strings.TrimSpace(string(b)), "\n")
	}

	// First lake install (deploy-lake.sh), then routine ingest deploys.
	if out, err := runScript(t, prefix, shims, "deploy-lake.sh", fakeWorker(t, "v1", log), "v1"); err != nil {
		t.Fatalf("deploy-lake: %v\n%s", err, out)
	}
	if got := calls(); !slices.Contains(got, "systemctl restart stellar-lake") || slices.ContainsFunc(got, func(c string) bool {
		return strings.Contains(c, "stellar-ingest") || strings.Contains(c, "stellar-galexie") && !strings.HasPrefix(c, "systemctl is-active")
	}) {
		t.Fatalf("deploy-lake calls %q", got)
	}
	lakeTarget := link("worker-lake")
	if lakeTarget != filepath.Join(prefix, "lib/stellar/worker-v1") || link("worker") != "" {
		t.Fatalf("after deploy-lake: worker-lake %q, worker %q", lakeTarget, link("worker"))
	}

	out, err := runScript(t, prefix, shims, "deploy-ingest.sh", fakeWorker(t, "v2", log), "v2", "--migrate")
	if err != nil {
		t.Fatalf("deploy-ingest: %v\n%s", err, out)
	}
	got := calls()
	wantCalls := []string{"worker v2 version", "worker v2 migrate", "systemctl restart stellar-ingest",
		"curl -fsS --max-time 5 http://127.0.0.1:9102/readyz"}
	if !slices.Equal(got, wantCalls) {
		t.Fatalf("deploy-ingest calls\n got %q\nwant %q", got, wantCalls)
	}
	if link("worker") != filepath.Join(prefix, "lib/stellar/worker-v2") || link("worker-lake") != lakeTarget {
		t.Fatalf("after deploy-ingest: worker %q, worker-lake %q", link("worker"), link("worker-lake"))
	}
	if fi, err := os.Stat(filepath.Join(prefix, "lib/stellar/worker-v2")); err != nil || fi.Mode().Perm() != 0o755 {
		t.Fatalf("installed binary: %v %v", fi, err)
	}
	// Without --migrate: no migration.
	if out, err := runScript(t, prefix, shims, "deploy-ingest.sh", fakeWorker(t, "v3", log), "v3"); err != nil {
		t.Fatalf("deploy-ingest v3: %v\n%s", err, out)
	}
	if got := calls(); slices.Contains(got, "worker v3 migrate") || !slices.Contains(got, "systemctl restart stellar-ingest") {
		t.Fatalf("deploy-ingest without --migrate: %q", got)
	}
	// A version is immutable; redeploying the same bytes is fine.
	if out, err := runScript(t, prefix, shims, "deploy-ingest.sh", fakeWorker(t, "other", log), "v2"); err == nil || !strings.Contains(out, "exists with different content") {
		t.Fatalf("replacing v2: %v\n%s", err, out)
	}
	if link("worker") != filepath.Join(prefix, "lib/stellar/worker-v3") {
		t.Fatalf("a refused deploy switched the symlink to %q", link("worker"))
	}
	_ = calls()
	for _, args := range [][]string{{}, {"x"}, {"x", "bad version"}, {"x", "v4", "--nope"}} {
		if _, err := runScript(t, prefix, shims, "deploy-ingest.sh", args...); err == nil {
			t.Errorf("deploy-ingest %q succeeded", args)
		}
	}
}
