package main

import (
	"bytes"
	"fmt"
	"net/netip"
	"os"
	"strings"

	galexiefiles "stellar-explorer/galexie"
)

// network

func netplan(e *env) file {
	return file{"/etc/netplan/60-vswitch.yaml", e.asset("files/vswitch.yaml"), 0o600, "root:root"}
}

func networkCheck(e *env) result {
	switch f := netplan(e); {
	case !hasVLAN(e.set):
		return ok("no VLAN (vlan_id = none)")
	case !f.matches():
		return change("write %s", f.path)
	case !hasPrivateIP(e):
		return change("netplan apply")
	}
	return ok("%s on %s", e.v("private_ip"), e.v("private_if"))
}

func networkApply(e *env) error {
	f := netplan(e)
	old, rerr := os.ReadFile(f.path)
	if _, err := e.place(f); err != nil {
		return err
	}
	if _, err := e.run("netplan", "generate"); err != nil {
		if os.Remove(f.path); rerr == nil { // never leave a file that netplan rejects
			os.WriteFile(f.path, old, f.mode)
		}
		return err
	}
	_, err := e.run("netplan", "apply")
	return err
}

func hasPrivateIP(e *env) bool {
	out, _ := probe("ip", "-o", "addr", "show", "dev", e.v("private_if"))
	return strings.Contains(out, " "+e.v("private_ip")+"/")
}

// networkHealth pings the host this one depends on over the private
// network: the galexie host from the psql host, the psql host from the
// clickhouse host.
func networkHealth(e *env) result {
	peer, name := e.set["galexie_ip"], "galexie host"
	if ip, ok := e.set["psql_ip"]; ok { // a setting of the clickhouse host only
		peer, name = ip, "psql host"
	}
	if !hasPrivateIP(e) {
		return failed("%s has no %s", e.v("private_if"), e.v("private_ip"))
	} else if _, err := probe("ping", "-c1", "-W2", peer); err != nil {
		return failed("%s %s does not answer", name, peer)
	}
	return ok("%s up; %s %s answers", e.v("private_ip"), name, peer)
}

// firewall

func firewallPart(e *env) part {
	return part{files: []file{{"/etc/nftables.conf", e.asset("files/nftables.conf"), 0o755, "root:root"}},
		svcs: []service{{unit: "nftables.service", reload: true, files: []string{"/etc/nftables.conf"}}}}
}

func psqlFirewallCheck(e *env) result {
	if b, _ := os.ReadFile("/etc/ufw/ufw.conf"); bytes.Contains(b, []byte("\nENABLED=yes")) {
		return failed("ufw is enabled: run `ufw disable`, so that only /etc/nftables.conf filters")
	}
	return firewallPart(e).check("/etc/nftables.conf loaded")
}

func psqlFirewallApply(e *env) error {
	// Never load a ruleset that would drop this SSH session.
	if f := strings.Fields(os.Getenv("SSH_CONNECTION")); len(f) > 0 {
		ip, _ := netip.ParseAddr(f[0])
		ip, allowed := ip.Unmap(), false
		for _, a := range strings.Split(e.v("admin_ips"), ",") {
			p, err := netip.ParsePrefix(strings.TrimSpace(a))
			allowed = allowed || p.Contains(ip) || err != nil && strings.TrimSpace(a) == ip.String()
		}
		if !allowed {
			return fmt.Errorf("this SSH session comes from %s, which admin_ips doesn't allow: the ruleset would cut it off", ip)
		}
	}
	p := firewallPart(e)
	if _, err := e.runInput(string(p.files[0].data), "nft", "-c", "-f", "-"); err != nil { // before it can be loaded at boot
		return err
	}
	return e.apply(p)
}

func psqlFirewallHealth(e *env) result {
	if out, err := probe("nft", "list", "table", "inet", "stellar"); err != nil {
		return failed("table inet stellar not loaded: %s", tail(out, 1))
	}
	return ok("table inet stellar loaded")
}

// postgrest

const postgrestBin = "/usr/local/lib/postgrest/postgrest-16.4"

func postgrestPart(e *env) part {
	return part{
		users: []string{"postgrest"},
		releases: []release{{"https://github.com/PostgREST/postgrest/releases/download/v16.4/postgrest-v16.4-linux-static-x86-64.tar.xz",
			"b47ecc82fce1dcebbbc4183d839e52f07f7630c9d7ad0f54db753d1939299354", postgrestBin, []string{"--transform=s,^postgrest$,postgrest-16.4,"}}},
		links: [][2]string{{postgrestBin, "/usr/local/bin/postgrest"}},
		files: []file{{"/etc/postgrest/postgrest.conf", e.asset("files/postgrest.conf"), 0o640, "root:postgrest"},
			{"/etc/systemd/system/postgrest.service", e.asset("files/postgrest.service"), 0o644, "root:root"}},
		// Some keys (CORS origins, the proxy URI) are read only at start: a
		// changed config is a restart.
		svcs: []service{{unit: "postgrest.service", files: []string{"/etc/postgrest/postgrest.conf", "/etc/systemd/system/postgrest.service", "/usr/local/bin/postgrest"}}},
	}
}

func postgrestCheck(e *env) result { return postgrestPart(e).check("PostgREST 16.4") }
func postgrestApply(e *env) error  { return e.apply(postgrestPart(e)) }

func postgrestHealth(e *env) result {
	if code, _, _, err := get("http://127.0.0.1:3001/ready", ""); err != nil || code != 200 {
		return failed("/ready: %d %v", code, err)
	}
	return ok("/ready 200")
}

// caddy

// caddyPart installs the caddy file next to this app, named by its
// checksum so that a new build sits beside the old one.
func caddyPart(e *env) part {
	p := part{
		users: []string{"caddy"},
		dirs:  []dir{{"/var/lib/caddy", 0o750, "caddy:caddy"}, {"/var/log/caddy", 0o750, "caddy:caddy"}, {"/etc/caddy/sites", 0o755, "root:root"}},
		files: []file{{"/etc/caddy/Caddyfile", e.asset("files/Caddyfile"), 0o644, "root:root"},
			{"/etc/systemd/system/caddy.service", e.asset("files/caddy.service"), 0o644, "root:root"}},
		// As caddy: validating opens the access log, which root would own.
		after: [][]string{{"runuser", "-u", "caddy", "--", "/usr/local/bin/caddy", "validate", "--config", "/etc/caddy/Caddyfile"}},
		svcs: []service{{unit: "caddy.service", files: []string{"/etc/systemd/system/caddy.service", "/usr/local/bin/caddy"}},
			{unit: "caddy.service", reload: true, files: []string{"/etc/caddy/Caddyfile"}}},
	}
	if src, err := besideSelf("caddy"); err == nil {
		b, _ := os.ReadFile(src)
		dest := "/usr/local/lib/caddy/caddy-" + sha256File(src)[:12]
		p.files, p.links = append(p.files, file{dest, b, 0o755, "root:root"}), [][2]string{{dest, "/usr/local/bin/caddy"}}
	}
	return p
}

func caddyCheck(e *env) result {
	if _, err := besideSelf("caddy"); err != nil && !exists("/usr/local/bin/caddy") {
		return failed("%v", err)
	} else if r := caddyPart(e).check(""); r.status != statusOK {
		return r
	} else if _, _, _, err := get("https://"+e.v("api_hostname")+"/", "127.0.0.1:443"); err != nil {
		return waiting("no certificate for %s yet: DNS must point here (%v)", e.v("api_hostname"), err)
	}
	return ok("https://%s", e.v("api_hostname"))
}

func caddyApply(e *env) error { return e.apply(caddyPart(e)) }

func caddyHealth(e *env) result {
	code, _, days, err := get("https://"+e.v("api_hostname")+"/", "127.0.0.1:443")
	if err != nil || code != 200 {
		return failed("GET https://%s/ through 127.0.0.1: %d %v", e.v("api_hostname"), code, err)
	}
	return ok("GET / 200; certificate %d days left", days)
}

// monitoring

const (
	promDir = "/usr/local/lib/prometheus/prometheus-3.15.0.linux-amd64"
	amDir   = "/usr/local/lib/alertmanager/alertmanager-0.34.1.linux-amd64"
)

func monitoringPart(e *env) part {
	am, root := "root:alertmanager", "root:root"
	return part{
		pkgs:  []string{"prometheus-node-exporter", "prometheus-postgres-exporter"},
		users: []string{"prometheus", "alertmanager"},
		dirs:  []dir{{"/var/lib/prometheus/metrics2", 0o750, "prometheus:prometheus"}, {"/var/lib/alertmanager", 0o750, "alertmanager:alertmanager"}},
		releases: []release{
			{"https://github.com/prometheus/prometheus/releases/download/v3.15.0/prometheus-3.15.0.linux-amd64.tar.gz",
				"2a542df32eac02ee17b9d844fb2aa1de00dafa5476579ba8a3ba862e9d572ea0", promDir + "/prometheus", []string{"--strip-components=1"}},
			{"https://github.com/prometheus/alertmanager/releases/download/v0.34.1/alertmanager-0.34.1.linux-amd64.tar.gz",
				"265b9d1e55ef0d5306a436018af6d2b686c2ce051f03d968f7464ecb1372a7e8", amDir + "/alertmanager", []string{"--strip-components=1"}},
		},
		links: [][2]string{{promDir + "/prometheus", "/usr/local/bin/prometheus"}, {promDir + "/promtool", "/usr/local/bin/promtool"},
			{amDir + "/alertmanager", "/usr/local/bin/alertmanager"}, {amDir + "/amtool", "/usr/local/bin/amtool"}},
		files: []file{
			{"/etc/default/prometheus-node-exporter", e.asset("files/node-exporter.default"), 0o644, root},
			{"/etc/default/prometheus-postgres-exporter", e.asset("files/postgres-exporter.default"), 0o644, root},
			{"/etc/prometheus/prometheus.yml", e.asset("files/prometheus.yml"), 0o644, root},
			{"/etc/prometheus/rules/alerts.yml", e.asset("files/alerts.yml"), 0o644, root},
			{"/etc/prometheus/rules/galexie-alerts.yml", e.set.render(galexieAlerts()), 0o644, root},
			{"/etc/alertmanager/alertmanager.yml", e.asset("files/alertmanager.yml"), 0o640, am},
			{"/etc/alertmanager/smtp-password", []byte(e.v("smtp_password")), 0o640, am},
			{"/etc/alertmanager/deadman-url", []byte(e.v("deadman_url")), 0o640, am},
			{"/etc/systemd/system/prometheus.service", e.asset("files/prometheus.service"), 0o644, root},
			{"/etc/systemd/system/alertmanager.service", e.asset("files/alertmanager.service"), 0o644, root},
		},
		after: [][]string{{"promtool", "check", "config", "/etc/prometheus/prometheus.yml"}, // with the rules
			{"amtool", "check-config", "/etc/alertmanager/alertmanager.yml"}},
		// The directories change with every file written into them.
		svcs: []service{
			{unit: "prometheus-node-exporter.service", files: []string{"/etc/default/prometheus-node-exporter"}},
			{unit: "prometheus-postgres-exporter.service", files: []string{"/etc/default/prometheus-postgres-exporter"}},
			{unit: "prometheus.service", files: []string{"/etc/systemd/system/prometheus.service", "/usr/local/bin/prometheus"}},
			{unit: "prometheus.service", reload: true, files: []string{"/etc/prometheus", "/etc/prometheus/rules"}},
			{unit: "alertmanager.service", files: []string{"/etc/systemd/system/alertmanager.service", "/usr/local/bin/alertmanager"}},
			{unit: "alertmanager.service", reload: true, files: []string{"/etc/alertmanager"}},
		},
	}
}

func monitoringCheck(e *env) result {
	if !monitoring(e.set) {
		return ok("off (monitoring_enabled = no)")
	}
	return monitoringPart(e).check("exporters, Prometheus 3.15.0, Alertmanager 0.34.1")
}

func monitoringApply(e *env) error {
	p := monitoringPart(e)
	_, err := e.place(p.files[:2]...) // the packages start the exporters with these
	if err == nil {
		err = e.systemUser("alertmanager", "/var/lib/alertmanager")
	}
	if err == nil { // the secrets have no header for place to recognise
		_, err = e.writeAll(p.files[6:8]...)
	}
	if err == nil {
		err = e.apply(p)
	}
	return err
}

func monitoringHealth(e *env) result {
	if !monitoring(e.set) {
		return ok("off")
	}
	_, body, _, err := get("http://127.0.0.1:9090/api/v1/targets?state=active", "")
	up, down := bytes.Count(body, []byte(`"health":"up"`)), bytes.Count(body, []byte(`"health":"down"`))
	if err != nil || down > 0 || up == 0 {
		return failed("Prometheus targets: %d up, %d down %v", up, down, err)
	}
	return ok("%d targets up", up)
}

// galexieAlerts is the galexie host's alert rules, from its own folder.
func galexieAlerts() []byte {
	b, _ := galexiefiles.Files.ReadFile("deploy/alerts.yml")
	return b
}
