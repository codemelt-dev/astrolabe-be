package main

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io/fs"
	"math/big"
	"net"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	pgConf = "/etc/postgresql/18/main"
	pgUnit = "postgresql@18-main.service"
	caCrt  = "/etc/stellar/pg-ca.crt"
	caKey  = secretsDir + "/pg-ca.key"
	srvCrt = "/etc/ssl/postgresql/server.crt"
	srvKey = "/etc/ssl/postgresql/server.key"
	bundle = "/root/galexie-bundle"
)

var pgPackages = []string{"postgresql-18", "postgresql-client-18", "pgbackrest"}

// postgres

func postgresPart(e *env) part {
	f, conf := hostFacts(), func(n string) string { return pgConf + "/" + n }
	return part{
		files: []file{
			// Before the packages: no default cluster; the PGDG repository.
			{"/etc/postgresql-common/createcluster.d/60-stellar.conf", e.asset("files/createcluster.conf"), 0o644, "root:root"},
			{"/etc/apt/sources.list.d/pgdg.sources", e.asset("files/pgdg.sources"), 0o644, "root:root"},
			{"/etc/sysctl.d/60-stellar.conf", tuned(e.asset("files/sysctl.conf"), f), 0o644, "root:root"},
			{"/etc/systemd/system/postgresql@.service.d/10-network-online.conf", e.asset("files/postgresql-network-online.conf"), 0o644, "root:root"},
			{conf("conf.d/10-listen-tls.conf"), e.asset("files/10-listen-tls.conf"), 0o644, "postgres:postgres"},
			{conf("conf.d/20-tuning.conf"), tuned(e.asset("files/20-tuning.conf"), f), 0o644, "postgres:postgres"},
			{conf("conf.d/30-archive.conf"), e.asset("files/30-archive.conf"), 0o644, "postgres:postgres"},
			{conf("pg_hba.conf"), e.asset("files/pg_hba.conf"), 0o640, "postgres:postgres"},
			{conf("pg_ident.conf"), e.asset("files/pg_ident.conf"), 0o640, "postgres:postgres"},
		},
		after: [][]string{{"sysctl", "--load", "/etc/sysctl.d/60-stellar.conf"}}, // before the first start
		svcs: []service{{unit: pgUnit, reload: true, files: []string{conf("conf.d/10-listen-tls.conf"), conf("conf.d/20-tuning.conf"),
			conf("conf.d/30-archive.conf"), conf("pg_hba.conf"), conf("pg_ident.conf"), srvCrt, srvKey}}},
	}
}

func pendingRestart() string {
	p, _ := query("postgres", "SELECT string_agg(name, ', ') FROM pg_settings WHERE pending_restart")
	return p
}

func postgresCheck(e *env) result {
	if m := missing(pgPackages...); len(m) > 0 {
		return change("install %s", strings.Join(m, " "))
	} else if !exists(pgConf + "/postgresql.conf") {
		return change("create cluster 18/main")
	} else if _, err := serverCert(e); err != nil {
		return change("%v", err)
	} else if r := postgresPart(e).check("18/main running"); r.status != statusOK {
		return r
	} else if p := pendingRestart(); p != "" {
		return restart("%s changed: restart pending (--allow-restart)", p)
	}
	return ok("18/main running")
}

func postgresApply(e *env) error {
	p, fresh := postgresPart(e), !exists(pgConf+"/postgresql.conf")
	_, err := e.place(p.files[0])
	if err == nil {
		err = e.install("postgresql-common") // ships the PGDG key
	}
	if err == nil {
		_, err = e.place(p.files[1])
	}
	if err == nil && len(missing(pgPackages...)) > 0 {
		e.aptUpdated = false // fetch the new repository
		err = e.install(pgPackages...)
	}
	if err == nil && !exists(pgConf+"/postgresql.conf") {
		_, err = e.run("pg_createcluster", "--locale=C.UTF-8", "18", "main", "--", "--locale-provider=builtin", "--data-checksums")
	}
	if err == nil && fresh { // created by this run: its pg_hba.conf and pg_ident.conf are ours
		_, err = e.writeAll(p.files[7:]...)
	}
	if err == nil {
		err = e.issueTLS()
	}
	if err == nil {
		err = e.apply(p)
	}
	time.Sleep(time.Second) // a reload is a signal: let the server read it
	if pr := pendingRestart(); err == nil && pr != "" && e.allowRestart {
		if _, err = e.run("systemctl", "restart", pgUnit); err == nil {
			e.note("restarted %s for %s", pgUnit, pr)
		}
	}
	return err
}

func postgresHealth(e *env) result {
	huge, err := query("postgres", "SHOW huge_pages_status")
	days, cerr := serverCert(e)
	if err != nil || cerr != nil {
		return failed("%s %v", tail(huge, 1), cerr)
	}
	return ok("accepting; certificate %d days left; huge pages %s", days, huge)
}

// Tuning

type facts struct{ ram, cores, cpus, disk uint64 }

// hostFacts reads the installed RAM, cores, threads and the data volume.
func hostFacts() facts {
	var kb uint64
	b, _ := os.ReadFile("/proc/meminfo")
	fmt.Sscanf(string(b), "MemTotal: %d kB", &kb)
	// MemTotal is a few percent below the installed RAM: round up to 4 GiB.
	f := facts{ram: (kb>>22 + 1) << 32, cpus: uint64(runtime.NumCPU())}
	f.cores = f.cpus
	if smt, _ := os.ReadFile("/sys/devices/system/cpu/smt/active"); strings.TrimSpace(string(smt)) == "1" {
		f.cores /= 2
	}
	var st syscall.Statfs_t
	if syscall.Statfs("/var/lib/postgresql", &st) == nil || syscall.Statfs("/", &st) == nil {
		f.disk = st.Blocks * uint64(st.Bsize)
	}
	return f
}

// tuned rewrites the computed "name = value" lines of a repository file,
// which shows the values of a 128 GB, 16-core, 32-thread server with a
// 3.84 TB data volume. About 4 GB stay free for PostgREST, Caddy and
// Prometheus; the huge pages hold shared_buffers and its overhead.
func tuned(data []byte, f facts) []byte {
	const mb, gb = 1 << 20, 1 << 30
	clamp := func(v, lo, hi uint64) uint64 { return max(lo, min(hi, v)) }
	size := func(b uint64) string {
		if b%gb == 0 {
			return fmt.Sprintf("'%dGB'", b/gb)
		}
		return fmt.Sprintf("'%dMB'", b/mb)
	}
	shared, wal, par, av := f.ram/4, clamp(f.disk/16/gb*gb, 2*gb, 64*gb), clamp(f.cores/4, 1, 4), uint64(6)
	if f.cores < 12 {
		av = max(3, f.cores/2)
	}
	values := map[string]any{
		"shared_buffers":                   size(shared),
		"effective_cache_size":             size(max(shared, min((f.ram*7/10+gb/2)/gb*gb, f.ram-min(f.ram, 4*gb)))),
		"work_mem":                         size(clamp(f.ram/2048, 4*mb, 256*mb)),
		"maintenance_work_mem":             size(clamp(f.ram/64, 64*mb, 2*gb)),
		"autovacuum_work_mem":              size(clamp(f.ram/128, 64*mb, gb)),
		"io_workers":                       clamp(f.cores/2, 3, 32),
		"max_worker_processes":             max(8, f.cpus),
		"max_parallel_workers":             max(2, f.cores),
		"max_parallel_workers_per_gather":  par,
		"max_parallel_maintenance_workers": par,
		"max_wal_size":                     size(wal),
		"min_wal_size":                     size(wal / 16),
		"autovacuum_max_workers":           av,
		"vm.nr_hugepages":                  ((shared>>21)*104/100 + 128 + 99) / 100 * 100,
	}
	lines := strings.Split(string(data), "\n")
	for i, l := range lines {
		name, rest, _ := strings.Cut(l, " = ")
		if v, ok := values[name]; ok {
			lines[i] = fmt.Sprint(name, " = ", v)
			if _, comment, found := strings.Cut(rest, " # "); found {
				lines[i] = fmt.Sprintf("%-41s # %s", lines[i], comment)
			}
		}
	}
	return []byte(strings.Join(lines, "\n"))
}

// TLS: a local CA (10 years) issues the server certificate (825 days, the
// private IP in its SAN), which is renewed when 30 days are left.

// tlsFiles are a service's local CA and the server certificate it issues.
type tlsFiles struct {
	name           string // the CA's common name
	caCrt, caKey   string
	srvDir, owner  string // the server certificate's directory and owner
	srvCrt, srvKey string
}

var pgTLS = tlsFiles{name: "stellar-explorer postgres CA", caCrt: caCrt, caKey: caKey,
	srvDir: "/etc/ssl/postgresql", owner: "root:postgres", srvCrt: srvCrt, srvKey: srvKey}

// serverCert returns PostgreSQL's certificate's days left, or what has to be done.
func serverCert(e *env) (int, error) { return pgTLS.days(e) }

func (e *env) issueTLS() error { return e.issue(pgTLS) }

// days returns the server certificate's days left, or what has to be done.
func (t tlsFiles) days(e *env) (int, error) {
	ca, err := tls.LoadX509KeyPair(t.caCrt, t.caKey)
	if err != nil {
		return 0, errors.New("create the CA and the server certificate")
	}
	srv, err := tls.LoadX509KeyPair(t.srvCrt, t.srvKey)
	if err != nil {
		return 0, fmt.Errorf("issue %s", t.srvCrt)
	}
	days, c := int(time.Until(srv.Leaf.NotAfter).Hours()/24), srv.Leaf
	if c.CheckSignatureFrom(ca.Leaf) != nil || len(c.IPAddresses) != 1 || !c.IPAddresses[0].Equal(net.ParseIP(e.v("private_ip"))) || days < 30 {
		return days, fmt.Errorf("renew %s (%d days left)", t.srvCrt, days)
	}
	return days, nil
}

// issue creates the CA if there is none and (re)issues the server certificate.
func (e *env) issue(t tlsFiles) error {
	if _, err := t.days(e); err == nil {
		return nil
	}
	if !exists(t.caCrt) && !exists(t.caKey) {
		if err := errors.Join(e.mkdir(dir{secretsDir, 0o700, "root:root"}), e.newCert(t.caKey, t.caCrt, "root:root", t.name, nil)); err != nil {
			return err
		}
	}
	ca, err := tls.LoadX509KeyPair(t.caCrt, t.caKey)
	if err != nil {
		return fmt.Errorf("the CA: %w (restore %s and %s from the password manager)", err, t.caCrt, t.caKey)
	}
	if srv, err := tls.LoadX509KeyPair(t.srvCrt, t.srvKey); err == nil && srv.Leaf.CheckSignatureFrom(ca.Leaf) != nil {
		return fmt.Errorf("%s was not issued by %s: move it aside to have one issued", t.srvCrt, t.caCrt)
	}
	if err := e.mkdir(dir{t.srvDir, 0o750, t.owner}); err != nil {
		return err
	}
	return e.newCert(t.srvKey, t.srvCrt, t.owner, "", &ca)
}

// newCert writes a P-256 key and its certificate: the CA's when ca is nil,
// otherwise the server's, signed by ca. The key goes first: a pair cut short
// doesn't match, and the next run issues it again.
func (e *env) newCert(keyPath, crtPath, owner, caName string, ca *tls.Certificate) error {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	serial, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 127))
	t := &x509.Certificate{SerialNumber: serial, NotBefore: time.Now().Add(-time.Hour), BasicConstraintsValid: true}
	parent, signer, mode := t, crypto.Signer(key), os.FileMode(0o600)
	if ca == nil {
		t.Subject.CommonName, t.NotAfter, t.IsCA = caName, time.Now().AddDate(10, 0, 0), true
		t.KeyUsage = x509.KeyUsageCertSign | x509.KeyUsageCRLSign
	} else {
		ip := net.ParseIP(e.v("private_ip"))
		t.Subject.CommonName, t.NotAfter, t.IPAddresses = ip.String(), time.Now().AddDate(0, 0, 825), []net.IP{ip}
		t.KeyUsage, t.ExtKeyUsage = x509.KeyUsageDigitalSignature, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		parent, signer, mode = ca.Leaf, ca.PrivateKey.(crypto.Signer), 0o640
	}
	der, err := x509.CreateCertificate(rand.Reader, t, parent, &key.PublicKey, signer)
	kb, kerr := x509.MarshalPKCS8PrivateKey(key)
	if err = errors.Join(err, kerr); err != nil {
		return err
	}
	_, err = e.writeAll(file{keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: kb}), mode, owner},
		file{crtPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o644, owner})
	return err
}

// database

var loginRoles = []string{"explorer_owner", "ingest_writer"}

func rolesMarker(e *env) string {
	return fmt.Sprintf("%s: roles.sql sha256 %x", tool(), sha256.Sum256(e.asset("roles.sql")))
}

// bundleExport is what `lab galexie` installs into /etc/stellar.
func bundleExport(e *env) []file {
	ca, _ := os.ReadFile(caCrt)
	pgpass := func(role string) []byte {
		return fmt.Appendf(nil, "%s:5432:explorer:%s:%s\n", e.v("private_ip"), role, secret(role+".password"))
	}
	return []file{{bundle + "/pg-ca.crt", ca, 0o644, "root:root"},
		{bundle + "/pgpass", pgpass("ingest_writer"), 0o600, "root:root"}, {bundle + "/pgpass-owner", pgpass("explorer_owner"), 0o600, "root:root"}}
}

func databaseCheck(e *env) result {
	n, err := query("postgres", "SELECT count(*) FROM pg_roles WHERE rolname IN ('explorer_owner', 'ingest_writer', 'authenticator', 'web_anon', 'postgres_exporter', 'state_reader')")
	c, _ := query("postgres", "SELECT shobj_description(oid, 'pg_database') FROM pg_database WHERE datname = 'explorer'")
	if err != nil || n != "6" || c != rolesMarker(e) {
		return change("apply roles.sql")
	}
	for _, role := range loginRoles {
		if r := password(role); r.status != statusOK {
			return r
		}
	}
	if p := firstMismatch(bundleExport(e)...); p != "" {
		return change("write %s", p)
	}
	return ok("roles, passwords, %s", bundle)
}

// password compares a role's verifier with its secret file. A password that
// is set but not the file's is never overwritten.
func password(role string) result {
	v, err := query("postgres", "SELECT coalesce(rolpassword, '') FROM pg_authid WHERE rolname = '"+role+"'")
	switch {
	case err == nil && v == "":
		return change("set %s's password", role)
	case err != nil || !scramMatches(v, secret(role+".password")):
		return failed("%s's password isn't the one in %s/%s.password: write it there, or ALTER ROLE %[1]s PASSWORD NULL", role, secretsDir, role)
	}
	return ok("")
}

func databaseApply(e *env) error {
	err := e.sql("postgres", string(e.asset("roles.sql"))+fmt.Sprintf("\nCOMMENT ON DATABASE explorer IS '%s';\n", rolesMarker(e)))
	if err == nil {
		e.note("applied roles.sql")
	}
	for _, role := range loginRoles {
		if r := password(role); err == nil && r.status == statusFailed {
			err = errors.New(r.detail)
		} else if err == nil && r.status == statusChange {
			pw, gerr := generated(role + ".password")
			if err = gerr; err == nil {
				err = e.sql("postgres", fmt.Sprintf("ALTER ROLE %s PASSWORD '%s';\n", role, scramVerifier(pw, nil)))
			}
			e.note("%s's password set", role)
		}
	}
	if err == nil {
		err = e.mkdir(dir{bundle, 0o700, "root:root"})
	}
	if err == nil {
		_, err = e.writeAll(bundleExport(e)...)
	}
	return err
}

// SCRAM-SHA-256 verifiers as PostgreSQL stores them (RFC 5802, 7677), so
// that only the verifier reaches the server. Generated passwords are ASCII,
// which SASLprep leaves as it is.

func scramVerifier(password string, salt []byte) string {
	if salt == nil {
		salt = make([]byte, 16)
		rand.Read(salt)
	}
	return fmt.Sprintf("SCRAM-SHA-256$4096:%s$%s", base64.StdEncoding.EncodeToString(salt), scramKeys(password, salt, 4096))
}

func scramMatches(verifier, password string) bool {
	m := regexp.MustCompile(`^SCRAM-SHA-256\$(\d+):([^$]+)\$(.+)$`).FindStringSubmatch(verifier)
	if m == nil {
		return false
	}
	iter, _ := strconv.Atoi(m[1])
	salt, err := base64.StdEncoding.DecodeString(m[2])
	return err == nil && iter > 0 && password != "" && hmac.Equal([]byte(scramKeys(password, salt, iter)), []byte(m[3]))
}

// scramKeys returns "StoredKey:ServerKey", base64.
func scramKeys(password string, salt []byte, iter int) string {
	salted, _ := pbkdf2.Key(sha256.New, password, salt, iter, sha256.Size)
	mac := func(msg string) []byte {
		h := hmac.New(sha256.New, salted)
		h.Write([]byte(msg))
		return h.Sum(nil)
	}
	stored := sha256.Sum256(mac("Client Key"))
	return base64.StdEncoding.EncodeToString(stored[:]) + ":" + base64.StdEncoding.EncodeToString(mac("Server Key"))
}

// backups (pgBackRest)

const fullBackup = "pgbackrest-backup@full.service"

// fullBackupRunning: a oneshot unit is "activating" until its commands end.
func fullBackupRunning() bool { return unitIs("ActiveState", fullBackup) == "activating" }

// backupsPart keeps the secrets out of pgbackrest.conf: pgBackRest also
// reads conf.d/*.conf, and only that file holds them.
func backupsPart(e *env) part {
	secrets := fmt.Sprintf("# /etc/pgbackrest/conf.d/secrets.conf (%s, step backups): from %s.\n[global]\nrepo1-s3-key=%s\nrepo1-s3-key-secret=%s\nrepo1-cipher-pass=%s\n",
		tool(), secretsDir, e.v("backup_s3_key"), e.v("backup_s3_secret"), secret("pgbackrest-cipher-pass"))
	unit := func(n string) file {
		return file{"/etc/systemd/system/" + n, e.asset("files/" + n), 0o644, "root:root"}
	}
	return part{
		files: []file{{"/etc/pgbackrest/pgbackrest.conf", e.asset("files/pgbackrest.conf"), 0o640, "root:postgres"},
			{"/etc/pgbackrest/conf.d/secrets.conf", []byte(secrets), 0o640, "root:postgres"},
			{"/usr/local/sbin/pgbackrest-mark-success", e.asset("files/pgbackrest-mark-success"), 0o755, "root:root"},
			unit("pgbackrest-backup@.service"), unit("pgbackrest-full.timer"), unit("pgbackrest-diff.timer")},
		// The diff timer is installed but off: weekly fulls only. To turn it on:
		// systemctl enable --now pgbackrest-diff.timer (backupsHealth follows).
		svcs: []service{{unit: "pgbackrest-full.timer", files: []string{"/etc/systemd/system/pgbackrest-full.timer"}}},
	}
}

type stanza struct {
	Status struct {
		Code    int
		Message string
	}
	Backup []struct {
		Type      string
		Timestamp struct{ Stop int64 }
	}
}

// info is `pgbackrest info`; status 1 and 3 mean no stanza yet.
func info() (stanza, error) {
	out, err := probe("runuser", "-u", "postgres", "--", "pgbackrest", "--stanza=explorer", "--output=json",
		"--log-level-console=off", "--log-level-stderr=off", "info")
	var in []stanza
	if err == nil {
		err = json.Unmarshal([]byte(out), &in)
	}
	if err != nil || len(in) != 1 {
		return stanza{}, fmt.Errorf("pgbackrest info: %v %s", err, tail(out, 1))
	}
	return in[0], nil
}

// archiving reports whether WAL was archived, with no failure since.
func archiving() bool {
	v, _ := query("postgres", "SELECT last_archived_time >= coalesce(last_failed_time, '-infinity') FROM pg_stat_archiver")
	return v == "t"
}

func backupsCheck(e *env) result {
	r := backupsPart(e).check("")
	if r.status == statusChange {
		return r
	}
	in, err := info()
	switch {
	case err != nil:
		return failed("%v", err)
	case in.Status.Code == 1 || in.Status.Code == 3:
		return change("stanza-create")
	case in.Status.Code != 0 && in.Status.Code != 2:
		return failed("pgbackrest info: %s", in.Status.Message)
	case !archiving():
		return change("pgbackrest check (WAL archiving)")
	case len(in.Backup) == 0 && !fullBackupRunning():
		return change("start the first full backup")
	case r.status == statusRestart: // a timer changed
		return r
	}
	return ok("stanza explorer, %d backups", len(in.Backup))
}

func backupsApply(e *env) error {
	_, err := generated("pgbackrest-cipher-pass")
	if err == nil {
		err = e.apply(backupsPart(e))
	}
	in, ierr := info()
	pgbackrest := func(cmd string) error {
		_, err := e.run("runuser", "-u", "postgres", "--", "pgbackrest", "--stanza=explorer", cmd)
		return err
	}
	if err = errors.Join(err, ierr); err == nil && (in.Status.Code == 1 || in.Status.Code == 3) {
		err = pgbackrest("stanza-create")
	}
	if err == nil && !archiving() {
		err = pgbackrest("check") // switches WAL and waits until it is archived
	}
	if err == nil && len(in.Backup) == 0 && !fullBackupRunning() {
		_, err = e.run("systemctl", "start", "--no-block", fullBackup)
		e.note("first full backup started (journalctl -u %s)", fullBackup)
	}
	return err
}

func backupsHealth(e *env) result {
	in, err := info()
	last, shown := map[string]time.Duration{"full": 1 << 62, "diff": 1 << 62}, map[string]string{"full": "none", "diff": "none"}
	for _, b := range in.Backup {
		if age := time.Since(time.Unix(b.Timestamp.Stop, 0)).Round(time.Minute); age < last[b.Type] {
			last[b.Type], shown[b.Type] = age, age.String()+" ago"
		}
	}
	fails, _ := query("postgres", "SELECT failed_count FROM pg_stat_archiver")
	detail := fmt.Sprintf("last full %s, last diff %s; %s archive failures", shown["full"], shown["diff"], fails)
	switch {
	case err != nil:
		return failed("%v", err)
	case len(in.Backup) == 0 && fullBackupRunning():
		return waiting("first full backup running")
	case last["full"] > 8*24*time.Hour || enabled("pgbackrest-diff.timer") && min(last["full"], last["diff"]) > 30*time.Hour || !archiving():
		return failed("%s", detail)
	}
	return ok("%s", detail)
}

// api

// apiMarker goes into COMMENT ON SCHEMA api. With the schema version in it,
// every migration brings grants.sql and api.sql round again.
func apiMarker(e *env, version string) string {
	return fmt.Sprintf("%s: grants.sql and api.sql sha256 %x at schema version %s", tool(),
		sha256.Sum256(append(e.asset("grants.sql"), e.asset("api.sql")...)), version)
}

// schemaVersion returns explorer's migration version and whether it is the
// one api.sql needs.
func schemaVersion() (string, bool) {
	b, _ := fs.ReadFile(current.files, "api.sql")
	need, _ := strconv.Atoi(string(regexp.MustCompile(`version >= (\d+)`).FindSubmatch(b)[1]))
	v, err := query("explorer", "SELECT max(version) FROM explorer.schema_migrations")
	n, _ := strconv.Atoi(v)
	return v, err == nil && n >= need
}

func apiCheck(e *env) result {
	v, ready := schemaVersion()
	if !ready {
		return waiting("waiting for `worker migrate` on the galexie host")
	}
	// The views must all be there too: dropping schema explorer drops them, not schema api.
	b, _ := fs.ReadFile(current.files, "api.sql")
	c, _ := query("explorer", fmt.Sprintf("SELECT obj_description(oid, 'pg_namespace') FROM pg_namespace WHERE nspname = 'api' "+
		"AND (SELECT count(*) FROM pg_views WHERE schemaname = 'api') = %d", strings.Count(string(b), "CREATE VIEW api.")))
	if !strings.HasSuffix(c, apiMarker(e, v)) {
		return change("apply grants.sql and api.sql")
	}
	return ok("applied at schema version %s", v)
}

func apiApply(e *env) error {
	v, _ := schemaVersion()
	err := e.sql("explorer", "SET ROLE explorer_owner;\n"+string(e.asset("grants.sql"))+"\n"+string(e.asset("api.sql"))+fmt.Sprintf(`
DO $m$ BEGIN
    EXECUTE format('COMMENT ON SCHEMA api IS %%L', obj_description('api'::regnamespace, 'pg_namespace') || E'\n\n%s');
END $m$;
NOTIFY pgrst, 'reload schema';
`, apiMarker(e, v)))
	if err == nil {
		e.note("applied grants.sql and api.sql")
	}
	return err
}

func apiHealth(e *env) result {
	out, err := query("explorer", "SELECT latest_ledger, round(extract(epoch FROM now() - ingested_at)), api_version FROM api.status")
	f := strings.Split(out, "|")
	if err != nil || len(f) != 3 {
		return waiting("no api schema or ingest cursor yet")
	}
	if age, _ := strconv.Atoi(f[1]); age > 300 {
		return failed("ledger %s ingested %ss ago; api version %s", f[0], f[1], f[2])
	}
	return ok("ledger %s ingested %ss ago; api version %s", f[0], f[1], f[2])
}
