package main

// The clickhouse host (clickhouse/README.md): ClickHouse, and the clickhouse
// pipeline, which the worker runs from the archive bucket into it.

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	clickhousefiles "stellar-explorer/clickhouse"
)

var clickhouseHost = &host{
	name:     "clickhouse",
	short:    "Set up the clickhouse host (ClickHouse and the clickhouse pipeline), or bring it back to its documented state",
	files:    clickhousefiles.Files,
	settings: clickhouseSettings,
	steps:    clickhouseSteps,
}

var clickhouseSettings = []setting{
	{key: "private_ip", title: "This host's private IP", example: "10.0.2.2", valid: isIP},
	{key: "psql_ip", title: "psql host's private IP", example: "10.0.1.2", valid: isIP},
	{key: "private_if", title: "Private network interface (with a VLAN: <vlan_link>.<vlan_id>)", example: "enp5s0.4000"},
	{key: "vlan_id", title: "VLAN ID of the private network, or none", example: "4000", def: "none", valid: isVLAN},
	{key: "vlan_link", title: "Interface that carries the VLAN", example: "enp5s0", when: hasVLAN, steps: onNetwork},
	{key: "vlan_gateway", title: "Gateway of the VLAN subnet", example: "10.0.2.1", when: hasVLAN, steps: onNetwork, valid: isIP},
	{key: "cloud_network", title: "Whole private network, routed via the VLAN gateway", example: "10.0.0.0/16", when: hasVLAN, steps: onNetwork},
	{key: "admin_ips", title: "IPv4 addresses or networks allowed to SSH, comma-separated", example: "203.0.113.10", steps: []string{"firewall"},
		valid: matching(`[0-9./]+(, *[0-9./]+)*`, "IPv4 addresses or networks, comma-separated")},
	{key: "archive_bucket", title: "Archive bucket", example: "stellar-ledgers-pubnet", valid: isName, steps: []string{"config"}},
	{key: "archive_endpoint", title: "Archive S3 endpoint", example: "https://hel1.your-objectstorage.com", valid: matching(`^https://[A-Za-z0-9.-]+(:\d+)?$`, "https://host"), steps: []string{"config"}},
	{key: "archive_region", title: "Archive S3 region", example: "hel1", valid: isName, steps: []string{"config"}},
	{key: "archive_access_key", title: "Archive access key (reads the archive)", secret: true, steps: []string{"config"}},
	{key: "archive_secret_key", title: "Archive secret key", secret: true, steps: []string{"config"}},
}

// clickhouseSteps is the step table, in order.
func clickhouseSteps() []step {
	return []step{
		{"system", "packages, UTC, sshd hardening, journald cap, needrestart", psqlSystemCheck, psqlSystemApply, nil},
		{"network", "optional: the VLAN interface with the private IP (netplan)", networkCheck, networkApply, networkHealth},
		{"firewall", "nftables: the whole ruleset, default drop", psqlFirewallCheck, psqlFirewallApply, psqlFirewallHealth},
		{"clickhouse", "ClickHouse " + chVersion + " (apt, pinned), config.d, users.d, TLS, passwords, database stellar", chCheck, chApply, chHealth},
		{"worker", "user stellar; the worker shipped next to this app", chWorkerCheck, chWorkerApply, nil},
		{"config", "/etc/stellar: worker.toml, archive.env, ClickHouse passwords; the clickhouse bundle for the psql host", chConfigCheck, chConfigApply, nil},
		{"migrate", "worker clickhouse-migrate while migrations are pending", chMigrateCheck, chMigrateApply, nil},
		{"init", "worker clickhouse-init once: the cursor before the archive's oldest ledger", chInitCheck, chInitApply, nil},
		{"services", "stellar-clickhouse-ingest", chServicesCheck, chServicesApply, chPipelineHealth},
	}
}

const (
	chVersion    = "26.8.19.9"
	chKeyURL     = "https://packages.clickhouse.com/rpm/lts/repodata/repomd.xml.key"
	chKeySHA256  = "74c1dfd89393b27c5739ee579a5af661ebe628cef9c33222c6fb29d0b8d42ad0"
	chKeyring    = "/usr/share/keyrings/clickhouse-keyring.asc"
	chSources    = "/etc/apt/sources.list.d/clickhouse.sources"
	chConfD      = "/etc/clickhouse-server/config.d/10-stellar.xml"
	chUsersD     = "/etc/clickhouse-server/users.d/10-stellar.xml"
	chPasswords  = "/etc/clickhouse-server/users.d/90-passwords.xml"
	chClientConf = "/root/.clickhouse-client/config.xml"
	chBundle     = "/root/clickhouse-bundle" // copied to the psql host with scp -3
	chIngestUnit = "stellar-clickhouse-ingest.service"
)

var (
	chPackages = []string{"clickhouse-common-static", "clickhouse-server", "clickhouse-client"}
	// node_exporter for the psql host's Prometheus (job clickhouse_node; nftables
	// lets only the psql host reach :9100).
	chHostPackages = []string{"prometheus-node-exporter"}
	// The users, each with a generated password (clickhouse/files/stellar-users.xml).
	chUsers = []string{"default", "stellar_owner", "stellar_writer", "stellar_reader", "stellar_public", "stellar_api", "stellar_jobs"}
	chTLS   = tlsFiles{name: "stellar-explorer clickhouse CA", caCrt: "/etc/stellar/clickhouse-ca.crt", caKey: secretsDir + "/clickhouse-ca.key",
		srvDir: "/etc/clickhouse-server/tls", owner: "root:clickhouse",
		srvCrt: "/etc/clickhouse-server/tls/server.crt", srvKey: "/etc/clickhouse-server/tls/server.key"}
	// ClickHouse re-reads users.d by itself; a new package, the server
	// configuration or the certificate is a restart.
	chService = service{unit: "clickhouse-server.service", files: []string{
		"/var/lib/dpkg/info/clickhouse-server.list", chConfD, chTLS.srvCrt, chTLS.srvKey}}
)

func chPasswordName(user string) string { return "clickhouse-" + user + ".password" }

// chInstalled is clickhouse-server's installed version ("" if none).
func chInstalled() string {
	v, err := probe("dpkg-query", "-W", "-f=${db:Status-Status} ${Version}", "clickhouse-server")
	if err != nil || !strings.HasPrefix(v, "installed ") {
		return ""
	}
	return strings.TrimPrefix(v, "installed ")
}

// chFiles are the configuration lab writes: the apt source, the server's
// and the users' settings (from clickhouse/files), and from the secrets the
// users' password hashes and root's clickhouse-client login.
func chFiles(e *env) (conf, own []file) {
	conf = []file{
		{chSources, e.asset("files/clickhouse.sources"), 0o644, "root:root"},
		{chConfD, e.set.render(e.asset("files/stellar-config.xml")), 0o640, "root:clickhouse"},
		{chUsersD, e.set.render(e.asset("files/stellar-users.xml")), 0o640, "root:clickhouse"},
	}
	var b strings.Builder
	fmt.Fprintf(&b, "<?xml version=\"1.0\"?>\n<!-- %s (%s, step clickhouse): the users' passwords as SHA-256, from\n     %s/clickhouse-*.password. -->\n<clickhouse>\n    <users>\n",
		chPasswords, tool(), secretsDir)
	for _, u := range chUsers {
		sum := sha256.Sum256([]byte(secret(chPasswordName(u))))
		remove := ""
		if u == "default" {
			remove = `<password remove="remove"/>` // the package's empty one
		}
		fmt.Fprintf(&b, "        <%s>%s<password_sha256_hex>%s</password_sha256_hex></%[1]s>\n", u, remove, hex.EncodeToString(sum[:]))
	}
	b.WriteString("    </users>\n</clickhouse>\n")
	client := fmt.Sprintf("<!-- %s (%s, step clickhouse): clickhouse-client logs in as default. -->\n<config>\n    <user>default</user>\n    <password>%s</password>\n</config>\n",
		chClientConf, tool(), secret(chPasswordName("default")))
	own = []file{{chPasswords, []byte(b.String()), 0o640, "root:clickhouse"}, {chClientConf, []byte(client), 0o600, "root:root"}}
	return conf, own
}

func chCheck(e *env) result {
	if m := missing(chHostPackages...); len(m) > 0 {
		return change("install %s", strings.Join(m, " "))
	}
	if v := chInstalled(); v != chVersion {
		return change("install ClickHouse %s (installed: %q)", chVersion, v)
	}
	for _, u := range chUsers {
		if secret(chPasswordName(u)) == "" {
			return change("generate the ClickHouse passwords")
		}
	}
	conf, own := chFiles(e)
	if p := firstMismatch(append(conf, own...)...); p != "" {
		return change("write %s", p)
	}
	if _, err := chTLS.days(e); err != nil {
		return change("%v", err)
	}
	if r := chService.state(); r.status != statusOK {
		return r
	}
	if out, err := probe("clickhouse-client", "--query", "EXISTS DATABASE stellar"); err != nil || out != "1" {
		return change("CREATE DATABASE stellar")
	}
	return ok("ClickHouse %s; database stellar", chVersion)
}

func chApply(e *env) error {
	if err := e.install(chHostPackages...); err != nil {
		return err
	}
	for _, u := range chUsers {
		if _, err := generated(chPasswordName(u)); err != nil {
			return err
		}
	}
	conf, own := chFiles(e)
	if chInstalled() != chVersion {
		if err := e.download(chKeyURL, chKeySHA256, chKeyring); err != nil {
			return err
		}
		if _, err := e.place(conf[0]); err != nil { // the apt source, before the update
			return err
		}
		if _, err := e.run("apt-get", "update", "-q"); err != nil {
			return err
		}
		fresh := chInstalled() == ""
		args := []string{"install", "-y", "-q", "-o", "Dpkg::Options::=--force-confold", "-o", "DPkg::Lock::Timeout=600"}
		for _, p := range chPackages {
			args = append(args, p+"="+chVersion)
		}
		if _, err := e.run("apt-get", args...); err != nil {
			return err
		}
		if _, err := e.run("apt-mark", append([]string{"hold"}, chPackages...)...); err != nil {
			return err
		}
		e.note("installed ClickHouse %s (held)", chVersion)
		if fresh && active(chService.unit) {
			// The package starts it with its defaults: stop it until it is
			// configured, so the first start needs no --allow-restart.
			if _, err := e.run("systemctl", "stop", chService.unit); err != nil {
				return err
			}
		}
	}
	if _, err := e.place(conf...); err != nil {
		return err
	}
	if _, err := e.writeAll(own...); err != nil {
		return err
	}
	if err := e.issue(chTLS); err != nil {
		return err
	}
	if err := e.ensure(chService); err != nil {
		return err
	}
	// It answers once it has loaded its tables.
	for deadline := time.Now().Add(time.Minute); time.Now().Before(deadline); time.Sleep(time.Second) {
		if out, _ := probe("clickhouse-client", "--query", "SELECT 1"); out == "1" {
			break
		}
	}
	if out, err := probe("clickhouse-client", "--query", "EXISTS DATABASE stellar"); err != nil || out != "1" {
		if _, err := e.run("clickhouse-client", "--query", "CREATE DATABASE IF NOT EXISTS stellar"); err != nil {
			return err
		}
		e.note("created database stellar")
	}
	return nil
}

// chHealth asks what the psql host sees: ClickHouse over TLS on the private
// IP, with lab's CA.
func chHealth(e *env) result {
	code, body, days, err := getWithCA("https://"+e.v("private_ip")+":8443/ping", chTLS.caCrt)
	if err != nil || code != http.StatusOK || strings.TrimSpace(body) != "Ok." {
		return failed("https://%s:8443/ping: %d %q %v", e.v("private_ip"), code, strings.TrimSpace(body), err)
	}
	return ok("HTTPS on %s:8443 answers; certificate %d days left", e.v("private_ip"), days)
}

// getWithCA GETs url trusting only the CA in caFile, and returns the
// status, the body and the server certificate's days left.
func getWithCA(url, caFile string) (int, string, int, error) {
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return 0, "", 0, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return 0, "", 0, errors.New(caFile + ": no certificate")
	}
	c := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}}
	resp, err := c.Get(url)
	if err != nil {
		return 0, "", 0, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	days := 0
	if resp.TLS != nil && len(resp.TLS.PeerCertificates) > 0 {
		days = int(time.Until(resp.TLS.PeerCertificates[0].NotAfter).Hours() / 24)
	}
	return resp.StatusCode, string(b), days, nil
}

// worker

var chWorkerLinks = []string{workerBin}

func chWorkerCheck(*env) result {
	if !userExists("stellar") {
		return change("user stellar")
	}
	return checkWorker(chWorkerLinks)
}

func chWorkerApply(e *env) error {
	if err := e.systemUser("stellar", "/nonexistent"); err != nil {
		return err
	}
	return e.installWorker(chWorkerLinks)
}

// config

// chConfigFiles are /etc/stellar's files and the bundle for the psql host:
// lab's CA and the passwords Grafana, Caddy and worker state-copy log in
// with, and stellar_api's, which the psql host's health check uses (the
// frontend team gets it from secretsDir here).
func chConfigFiles(e *env) []file {
	keys := strings.NewReplacer("REPLACE_WITH_UPLOADER_ACCESS_KEY", e.set.get("archive_access_key"),
		"REPLACE_WITH_UPLOADER_SECRET_KEY", e.set.get("archive_secret_key"))
	ca, _ := os.ReadFile(chTLS.caCrt)
	pw := func(u string) []byte { return []byte(secret(chPasswordName(u)) + "\n") }
	return []file{
		{workerConf, e.set.render(e.asset("files/worker.toml")), 0o640, "root:stellar"},
		{"/etc/stellar/archive.env", []byte(keys.Replace(string(e.set.render(e.asset("files/archive.env.example"))))), 0o640, "root:stellar"},
		{"/etc/stellar/clickhouse-writer.password", pw("stellar_writer"), 0o640, "root:stellar"},
		{"/etc/stellar/clickhouse-owner.password", pw("stellar_owner"), 0o600, "root:root"},
		{chBundle + "/clickhouse-ca.crt", ca, 0o644, "root:root"},
		{chBundle + "/clickhouse-reader.password", pw("stellar_reader"), 0o600, "root:root"},
		{chBundle + "/clickhouse-public.password", pw("stellar_public"), 0o600, "root:root"},
		{chBundle + "/clickhouse-jobs.password", pw("stellar_jobs"), 0o600, "root:root"},
		{chBundle + "/clickhouse-api.password", pw("stellar_api"), 0o600, "root:root"},
	}
}

func chConfigCheck(e *env) result {
	if !exists(chTLS.caCrt) || secret(chPasswordName("stellar_writer")) == "" {
		return waiting("needs the clickhouse step: the CA and the passwords")
	}
	if p := firstMismatch(chConfigFiles(e)...); p != "" {
		return change("write %s", p)
	}
	return ok("worker.toml, archive.env, ClickHouse passwords; %s", chBundle)
}

func chConfigApply(e *env) error {
	if err := errors.Join(e.mkdir(etcStellar), e.mkdir(dir{chBundle, 0o700, "root:root"})); err != nil {
		return err
	}
	_, err := e.writeAll(chConfigFiles(e)...)
	return err
}

// migrate and init

func chMigrateCheck(*env) result {
	if !exists(workerBin) || !exists(workerConf) {
		return waiting("needs the worker and its config")
	}
	out, err := probe(workerBin, "clickhouse-migrate", "--check")
	if err == nil {
		return ok("schema current")
	}
	if n := strings.Count("\n"+out, "\npending "); n > 0 {
		return change("%d migration(s) pending", n)
	}
	return failed("worker clickhouse-migrate --check: %s", tail(out, 1))
}

func chMigrateApply(e *env) error {
	if _, err := e.run(workerBin, "clickhouse-migrate"); err != nil {
		return err
	}
	e.note("ran worker clickhouse-migrate")
	return nil
}

func chInitCheck(*env) result {
	if !exists(workerBin) || !exists(workerConf) {
		return waiting("needs the worker and its config")
	}
	out, err := probe(workerBin, "clickhouse-init", "--check")
	switch {
	case err == nil:
		return ok("%s", out)
	case strings.Contains(out, "no clickhouse cursor yet"):
		return change("worker clickhouse-init: the cursor before the archive's oldest ledger")
	}
	return failed("worker clickhouse-init --check: %s", tail(out, 1))
}

func chInitApply(e *env) error {
	if _, err := e.run(workerBin, "clickhouse-init"); err != nil {
		return err
	}
	e.note("ran worker clickhouse-init")
	return nil
}

// services

func chUnitFile(e *env) file {
	return file{"/etc/systemd/system/" + chIngestUnit, e.asset("files/" + chIngestUnit), 0o644, "root:root"}
}

var chIngestService = service{unit: chIngestUnit, files: []string{"/etc/systemd/system/" + chIngestUnit, workerConf, "/etc/stellar/archive.env"}}

func chServicesCheck(e *env) result {
	if f := chUnitFile(e); !f.matches() {
		return change("write %s", f.path)
	}
	return chIngestService.state()
}

func chServicesApply(e *env) error {
	changed, err := e.writeAll(chUnitFile(e))
	if err == nil {
		err = e.daemonReload(changed)
	}
	if err == nil {
		err = e.ensure(chIngestService)
	}
	return err
}

// chPipelineHealth reads the pipeline's own endpoints: readiness, cursor,
// the AWS public lake's tip and the lag, all from one scrape.
func chPipelineHealth(*env) result {
	ready, m := fetch("127.0.0.1:9103/readyz"), fetch("127.0.0.1:9103/metrics")
	r := ok("ingest %s, cursor %s (lag %s) · AWS lake tip %s", strings.ReplaceAll(ready, "\n", "; "),
		metric(m, `ingest_cursor{pipeline="clickhouse"}`, "?"), metric(m, `ingest_lag_ledgers{pipeline="clickhouse"}`, "?"),
		metric(m, "ingest_lake_tip", "?"))
	if ready != "ready" {
		r.status = statusFailed
	}
	return r
}
