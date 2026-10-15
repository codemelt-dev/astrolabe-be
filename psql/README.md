# psql host

Runbook for the explorer's database and API server: **PostgreSQL 18** with the `explorer` database, **PostgREST** and **Caddy** serving the public read-only API, **pgBackRest** backups, and **Prometheus** and **Alertmanager** watching both hosts. The galexie host ([`galexie/README.md`](../galexie/README.md)) writes every ledger into the database over the private network; this host serves it to the internet.

**`lab psql`**, part of the repository's `lab` CLI ([`cli/`](../cli)), does every step on the server: it checks each step, applies what is not in the documented state, and checks again. A second run changes nothing, and `lab psql check` reports the state and the host's health without changing anything. This README says what it does and why, and covers what it can't do: ordering the server, the private network, DNS, the backup bucket, and day-2 operations.

## Contents

1. [What runs here](#1-what-runs-here)
2. [Server](#2-server)
3. [lab psql](#3-lab-psql)
4. [First bring-up across both hosts](#4-first-bring-up-across-both-hosts)
5. [DNS](#5-dns)
6. [PostgreSQL](#6-postgresql)
7. [The API](#7-the-api)
8. [Operations](#8-operations)
9. [Development](#9-development)

---

## 1. What runs here

```text
                internet: HTTPS 443/tcp, HTTP/3 443/udp, 80/tcp (ACME, redirect)
                                   │
┌─ psql host ─ public IP ─ private 10.0.1.2 (vSwitch VLAN) ─────────────────────────────┐
│  caddy                 the only public listener: TLS, read-only methods, rate limit   │
│                                   │ 127.0.0.1:3000                                    │
│  postgrest             PostgREST 16.4, admin 127.0.0.1:3001                           │
│                                   │ Unix socket, peer auth: authenticator → web_anon  │
│  postgresql@18-main    database explorer: schemas explorer (worker), api (api.sql)    │
│                                   ▲ TLS 5432 from 10.0.0.2 only                       │
│  pgbackrest            WAL archiving, weekly full → backup bucket                     │
│  stellar-state-copy    daily timer: the state into ClickHouse on 10.0.2.2:9440        │
│  prometheus, alertmanager, node and postgres exporters: loopback only                 │
└───────────────────────────────────┬───────────────────────────────────────────────────┘
                                    │ private network (Cloud Network + vSwitch)
                       galexie host 10.0.0.2: worker ingest, worker migrate,
                       metrics on 9101 (lake), 9102 (ingest), 6061 (Galexie)
```

- **PostgreSQL 18** (PGDG packages), cluster `18/main`, one database `explorer`. The worker on the galexie host owns schema `explorer` and `public.ingest_state` (its migrations); this folder owns the database, the roles ([`roles.sql`](roles.sql)), the per-object grants ([`grants.sql`](grants.sql)) and the public `api` schema ([`api.sql`](api.sql)).
- **PostgREST** turns the `api` views into a REST API. It runs next to the database and logs in over the Unix socket, so that link needs no password, certificate or network rule.
- **Caddy** terminates HTTPS with a Let's Encrypt certificate, accepts only `GET`, `HEAD` and `OPTIONS`, limits the request rate per client IP, and adds security and cache headers. It is a custom build with the rate-limit module, from [`caddy/`](caddy/).
- **pgBackRest** archives WAL continuously and takes a weekly full backup into an S3 bucket in another location, encrypted client-side.
- **Prometheus and Alertmanager** (optional) scrape this host and the galexie host and send e-mail; an external dead-man's switch pages when they stop.
- **`worker state-copy`** (with the clickhouse site): once a day, a timer copies the current state into ClickHouse, where the asset list, supplies and top holders are computed from it, and appends the day's snapshots ([`clickhouse/README.md`](../clickhouse/README.md) §10). It reads PostgreSQL over the Unix socket as `state_reader`, read only.

**Why PostgREST and Caddy run on the database host.** The `api` schema belongs to the database: `api.sql` runs as `explorer_owner`, which can log in only here or from the galexie host. PostgREST is a stateless translator, and next to the database it reaches PostgreSQL over the Unix socket with peer authentication, so that link needs no password, certificate, CA or `pg_hba` network rule, and no network round trip. That also leaves one server fewer to order, harden and monitor.

The cost is that the database host faces the internet, so it is contained:
- nftables drops everything but 80/443, SSH from the admin IPs, and 5432 from the galexie host on the private interface;
- PostgreSQL listens only on `localhost` and the private IP; PostgREST, its admin port, Prometheus, Alertmanager, Grafana and the exporters listen only on loopback;
- Caddy is the only public listener;
- the anonymous role may only read the `api` views, for at most 2 s per statement.

If API traffic outgrows the host, move Caddy, or Caddy and PostgREST, to a server of their own. PostgREST then connects over TCP with TLS. That is a change of configuration, not of the API.

**Who owns which SQL.** The worker's migrations own schema `explorer`. This folder owns the database, the roles, the per-object grants and the `api` schema, and `lab psql` applies them here: `roles.sql` as the superuser, the other two as `explorer_owner` through `SET ROLE` over the Unix socket. The only owner credential that leaves this host is the one `worker migrate` uses on the galexie host (the bundle, §4.1).

**What an outage of this host means:** the API is down, and ingest on the galexie host retries with backoff while Galexie and the lake keep running and buffer the ledgers. Nothing upstream is lost, so a restart for maintenance is safe.

### 1.1 Traffic

| From | To | Port | What |
|---|---|---|---|
| anyone | public IP | 443/tcp, 443/udp, 80/tcp | the API (80 only redirects and answers ACME challenges) |
| admin IPs | public IP | 22 | SSH |
| galexie host `10.0.0.2` | `10.0.1.2` | 5432 (TLS, verify-full) | `ingest_writer` (ingest, bootstrap, reindex); `explorer_owner` (`worker migrate`) |
| this host (Prometheus) | `10.0.0.2` | 9101, 9102, 6061; ICMP echo | lake, ingest and Galexie metrics; ping (`lab psql check`, the MTU test) |
| this host | backup bucket | 443 | pgBackRest |
| this host | apt, GitHub, Let's Encrypt, SMTP, dead-man's switch | 443, 587 | installs, certificates, alerts |

### 1.2 Example values

The files in [`files/`](files/) are written with these example values, so they read correctly as they are. `lab psql` installs each file with every example replaced by your value (whole tokens only), and asks for missing values on its first run. The keys are the settings in `/etc/stellar/lab-psql.toml`, in the order the app asks for them.

| Setting | Example | What |
|---|---|---|
| `private_ip` | `10.0.1.2` | this host's private IP; the vSwitch subnet is a /24 (§2.3) |
| `galexie_ip` | `10.0.0.2` | the galexie host's private IP |
| `private_if` | `enp5s0.4000` | the interface with the private IP: `<vlan_link>.<vlan_id>` with a VLAN |
| `vlan_id` | `4000` | the vSwitch VLAN ID; default `none` (no VLAN, the network exists already) |
| `vlan_link` | `enp5s0` | the NIC that carries the VLAN (`ip -br link`) |
| `vlan_gateway` | `10.0.1.1` | the vSwitch subnet's gateway, its first IP |
| `cloud_network` | `10.0.0.0/16` | the whole private network, routed through the gateway |
| `admin_ips` | `203.0.113.10` | IPv4 addresses or networks allowed to SSH, comma-separated |
| `api_hostname` | `api.example.com` | the API's public name (§5) |
| `cors_origins` | `https://explorer.example.com` | the frontend origins, comma-separated |
| `ops_email` | `ops@example.com` | Let's Encrypt account; warnings |
| `backup_endpoint` | `fsn1.your-objectstorage.com` | the backup bucket's S3 endpoint (§2.5) |
| `backup_region` | `fsn1` | its region |
| `backup_bucket` | `stellar-pgbackrest` | its name |
| `backup_s3_key`, `backup_s3_secret` | secret | its S3 key pair |
| `grafana_hostname` | `none` | a public name for Grafana (`lab grafana`): a Caddy site in front of it ([dashboard/README.md](../dashboard/README.md#open-it)); `none` keeps it on the tunnel |
| `clickhouse_hostname` | `none` | a public name for the clickhouse host's read-only SQL endpoint: a Caddy site in front of it ([clickhouse/README.md §8](../clickhouse/README.md#8-public-read-only-sql)); `none` leaves it out |
| `clickhouse_ip` | `10.0.2.2` | the clickhouse host's private IP (asked once `clickhouse_hostname` is set) |
| `monitoring_enabled` | `no` | `yes` runs Prometheus and Alertmanager here (§8.5) |
| `smtp_host` | `smtp.example.com:587` | SMTP server for alerts |
| `smtp_user` | `alerts@example.com` | SMTP user, also the sender |
| `smtp_password` | secret | |
| `page_email` | `oncall@example.com` | where paging alerts go |
| `deadman_url` | secret | the dead-man's switch ping URL (§8.5) |

The example private addresses match `galexie/README.md`. Computed values are not settings: [`20-tuning.conf`](files/20-tuning.conf) and `vm.nr_hugepages` in [`sysctl.conf`](files/sysctl.conf) are rewritten from this host's RAM, cores and disk (§6.4).

### 1.3 Pinned versions

| Component | Version | Source |
|---|---|---|
| Ubuntu | 24.04 LTS, amd64 | installimage (§2.2) |
| PostgreSQL | 18 | PGDG apt repository, [`pgdg.sources`](files/pgdg.sources); minor versions by hand (§8.3) |
| pgBackRest | PGDG's current | apt |
| PostgREST | v16.4 | `postgrest-v16.4-linux-static-x86-64.tar.xz`, sha256 `b47ecc82fce1dcebbbc4183d839e52f07f7630c9d7ad0f54db753d1939299354` (in [`cli/psql_web.go`](../cli/psql_web.go)) |
| Caddy | v2.11.4 + `github.com/mholt/caddy-ratelimit` `v0.1.1-0.20260612195517-5625512f24f6` | [`caddy/go.mod`](caddy/go.mod), [`caddy/go.sum`](caddy/go.sum) |
| Prometheus | 3.15.0 | GitHub release, sha256 `2a542df3…72ea0` from its `sha256sums.txt` |
| Alertmanager | 0.34.1 | GitHub release, sha256 `265b9d1e…2a7e8` from its `sha256sums.txt` |
| node_exporter, postgres_exporter | Ubuntu's | apt (`prometheus-node-exporter`, `prometheus-postgres-exporter`) |
| Grafana (optional) | OSS 13.2.3 | `grafana_13.2.3_amd64.deb` from dl.grafana.com, sha256 `55ec5119…34a7a2f` (in [`cli/grafana.go`](../cli/grafana.go)) |

---

## 2. Server

The examples assume Hetzner: a dedicated **AX** server for the database (NVMe in RAID1, much more RAM per euro than cloud servers), joined to the galexie host's **Cloud Network** through a **vSwitch**. `lab psql` itself only assumes Ubuntu 24.04 amd64, systemd, a private network and an S3-compatible bucket.

### 2.1 Order

Robot → **Order → Dedicated root server → AX102**, in **HEL1** next to the galexie host (check availability when ordering): AMD Ryzen 9 7950X3D (16 cores, 32 threads), 128 GB DDR5 ECC, 2 × NVMe (1.92 TB by default, up to 2 × 7.68 TB), 1 Gbit/s. The tuning (§6.4) adapts to another size.

**Drives.** RAID1 means the usable space is one drive. The database holds the current state (~29 GB on pubnet for accounts, trustlines and contract data, plus offers, claimable balances, data entries, pools and contract code) and one row per ledger (~6.4 MB a day): it grows with the network's state, a few GB a year, not with its traffic. The history it used to keep, 13.5–17 GB a day, is the clickhouse host's at a tenth of the size ([`clickhouse/README.md`](../clickhouse/README.md) §2.2). WAL is the volume's other user: about 100 GB a day written and archived, recycled by checkpoints (§6.4). The smallest AX102 drives (2 × 1.92 TB) leave ample room.

Order the largest drives unless the measured growth is much lower, and plan the growth path early (§8.6, *Disk growth*). Add your SSH key to the order; the preinstalled OS doesn't matter.

### 2.2 Install Ubuntu 24.04 with RAID1

Both NVMe drives in software RAID1 (mdadm), LVM on top, `/boot` (and the EFI partition on UEFI machines) outside LVM as installimage requires. `/var/lib/postgresql` is its own XFS volume, so a full database never fills `/`, and it can grow online after a drive upgrade.

1. Robot → your server → **Rescue**: activate *Linux 64 bit* with your key, then **Reset** → *automatic hardware reset*.
2. Log in to the rescue system and look at the machine:
   ```sh
   ssh root@<public IP>
   [ -d /sys/firmware/efi ] && echo UEFI || echo BIOS
   lsblk -d -o NAME,SIZE,MODEL                 # nvme0n1 and nvme1n1
   ls /root/images/ | grep -i 2404             # Ubuntu-2404-noble-amd64-base.tar.gz
   ```
3. Stop old arrays and wipe both drives (**destroys all data on them**): `mdadm --stop /dev/md* 2>/dev/null; wipefs -fa /dev/nvme*n1`
4. Write the installimage config and install. Delete the `PART /boot/efi` line if step 2 printed `BIOS`.
   ```sh
   cat > /root/psql-installimage.conf <<'EOF'
   DRIVE1 /dev/nvme0n1
   DRIVE2 /dev/nvme1n1
   SWRAID 1
   SWRAIDLEVEL 1
   BOOTLOADER grub
   HOSTNAME stellar-psql1
   PART /boot/efi esp 256M
   PART /boot ext3 1024M
   PART lvm vg0 all
   LV vg0 root / ext4 100G
   LV vg0 swap swap swap 8G
   LV vg0 pgdata /var/lib/postgresql xfs all
   IMAGE /root/images/Ubuntu-2404-noble-amd64-base.tar.gz
   EOF
   installimage -a -c /root/psql-installimage.conf && reboot
   ```
5. Log in to the new system with the same key and check it: `cat /proc/mdstat` (every array `[UU]`; the first resync continues in the background), `df -hT / /var/lib/postgresql` (ext4 100G and XFS with the rest).
6. `apt-get update && apt-get -y full-upgrade && reboot`. The rest of the base setup is `lab psql`'s `system` step.

### 2.3 Private network: the vSwitch coupled to the Cloud Network

The galexie host is a cloud server in a Hetzner **Cloud Network** (created in `galexie/README.md`, example `stellar`, `10.0.0.0/16`). A dedicated server joins it through a Robot **vSwitch**, coupled to the network as a subnet of its own. Docs: [Connect dedicated servers (vSwitch)](https://docs.hetzner.com/cloud/networks/connect-dedi-vswitch/).

1. Robot → **Servers → vSwitches → Create vSwitch**: name `stellar`, VLAN ID `4000` (allowed: 4000–4091). **Add server**: this one.
2. Console → Networks → `stellar` → **Subnets → Add subnet**: tick *Enable dedicated server vSwitch connection*, select the vSwitch, IP range `10.0.1.0/24`. Its gateway is its first IP, `10.0.1.1`. The coupling takes a few minutes. *Expose routes to vSwitch* is not needed.
3. Before the first run, set the VLAN in the settings, so that the `network` step writes [`60-vswitch.yaml`](files/vswitch.yaml) (MTU **1400**, the private IP, a route to the whole Cloud Network through the gateway) and runs `netplan apply`:
   ```sh
   mkdir -p /etc/stellar && printf 'vlan_id = "4000"\n' >> /etc/stellar/lab-psql.toml && chmod 0600 /etc/stellar/lab-psql.toml
   ```
   The installer's `01-netcfg.yaml` (the public interface) stays as it is; netplan merges the files.
4. After the run, from this host: `ping -c3 10.0.0.2`, and `ping -c3 -M do -s 1372 10.0.0.2` (1372 + 28 header bytes = 1400: the MTU path works).

`netplan apply` briefly reconfigures every interface; keep a second SSH session open, in case the public interface's sessions don't survive it (untested). On a host without a VLAN (`vlan_id = none`) the step does nothing and `private_if` is the interface that already has the private IP.

### 2.4 Optional: the Robot firewall

nftables on the host is the filter that matters (§3.2, `firewall`). Hetzner's Cloud Firewalls don't apply to dedicated servers; the Robot firewall can add a second, stateless layer on the switch port. It has at most **10 rules per direction**, it **also filters vSwitch traffic** (so the Cloud Network needs a rule), and, being stateless, it needs rules for the replies to this host's own connections. Docs: [Robot firewall](https://docs.hetzner.com/robot/dedicated-server/firewall/).

Robot → your server → **Firewall**: tick **Hetzner Services (incoming)**; leave *Filter IPv6 packets* off (nftables filters IPv6). Incoming rules, in this order; outgoing: one rule, protocol `*`, accept.

| # | Name | IP | Protocol | Source | Source port | Destination port | TCP flags | Action |
|---|---|---|---|---|---|---|---|---|
| 1 | `cloud-network` | IPv4 | * | `10.0.0.0/16` | | | | accept |
| 2 | `ssh-admin` | IPv4 | tcp | `203.0.113.10/32` | | 22 | | accept |
| 3 | `http` | IPv4 | tcp | | | 80 | | accept |
| 4 | `https` | IPv4 | tcp | | | 443 | | accept |
| 5 | `http3` | IPv4 | udp | | | 443 | | accept |
| 6 | `icmp` | IPv4 | icmp | | | | | accept |
| 7 | `tcp-return` | IPv4 | tcp | | | 32768-65535 | `ack` | accept |
| 8 | `ntp-return` | IPv4 | udp | | 123 | 32768-65535 | | accept |

### 2.5 Backup bucket

pgBackRest writes to Hetzner **Object Storage in another location** (here FSN1, the server is in HEL1). Its key must be able to delete (to expire old backups), and Hetzner's S3 keys are valid for every bucket of their project, so the bucket gets **a project of its own**, separate from the ledger archive.

1. Console → **New project** `stellar-backups`.
2. **Object Storage → Create Bucket**: location **Falkenstein (FSN1)**, name `stellar-pgbackrest` (names are global: take `<org>-stellar-pgbackrest` if it's taken), private, no Object Lock, no versioning.
3. **Security → S3 credentials → Generate credentials**. The secret is shown once. `lab psql` asks for both (`backup_s3_key`, `backup_s3_secret`) and keeps them in `/etc/stellar/secrets/`.

The repository's encryption passphrase is generated on the first run (`/etc/stellar/secrets/pgbackrest-cipher-pass`). **Store it in the password manager: without it every backup is unreadable**, and it can't be changed for an existing repository.

---

## 3. lab psql

### 3.1 Build and run

On the workstation (Go 1.26), at the repository root:

```sh
make check                  # gofmt, vet, tests, linux/amd64 builds of lab and Caddy
make dist                   # bin/lab, bin/worker and bin/caddy, linux/amd64; prints their sha256
scp -r bin root@<public IP>:/root/
```

On the server, as root, in an SSH session from an admin IP:

```sh
cd /root/bin
./lab psql                # check every step, list the changes, ask once, apply, check again
./lab psql                # again: every step ✓ (or ◷, §4)
./lab psql check          # read-only: every step and the health probes; exit 0 only if all is ok
```

- **Prompts.** Missing settings are asked in one form and saved to `/etc/stellar/lab-psql.toml` (root, 0600); secrets go to `/etc/stellar/secrets/<key>` (directory 0700, files 0600) and are never shown or logged. With `--yes` nothing is asked: every question is approved, and a missing setting is an error naming it.
- **Flags.** `--only a,b` and `--skip a,b` select steps; `--allow-restart` restarts running services whose binary or configuration changed (§3.4); `--config PATH`; `--version`, `--help`, `completion`, `man`.
- **Output.** One line per step: `✓` ok, `↻` changed, `◷` waiting for something outside this host, `!` restart pending, `✗` failed. Before applying, `•` marks what will change. `check` adds the health probes.
- **Log.** Every command and its output: `/var/log/stellar-setup/psql.log`; a failure prints its last lines.
- **Exit codes.** `run`: 0 when every step is ok or waiting; `check`: 0 only when everything is ok; 1 otherwise; 2 for usage errors.

`lab psql` needs the `caddy` file next to itself (it is in `bin/`). Once Caddy is installed, a copy of the app without it leaves the installed build alone.

### 3.2 Steps

| # | Step | Does | Why | Health in `check` |
|---|---|---|---|---|
| 1 | `system` | packages (chrony, nftables, curl, unattended-upgrades …); UTC; [sshd hardening](files/sshd-hardening.conf) (validated with `sshd -t` before the reload); [journald cap](files/journald.conf); [needrestart exemption](files/needrestart.conf) | keys-only SSH; Ubuntu's security updates stay on, but a library update never restarts PostgreSQL on its own | |
| 2 | `network` | optional: [`60-vswitch.yaml`](files/vswitch.yaml), `netplan generate` (a rejected file is replaced by the previous one, or removed), `netplan apply` | the private IP on the VLAN | private IP present, galexie host answers ping |
| 3 | `firewall` | [`/etc/nftables.conf`](files/nftables.conf) (the whole ruleset, own table `inet stellar`), checked with `nft -c` before it is written; `nftables.service` enabled and reloaded | default drop on a public host. It refuses a ruleset that wouldn't allow this SSH session's address, and stops while ufw is enabled | ruleset loaded |
| 4 | `postgres` | [createcluster.d](files/createcluster.conf), [PGDG](files/pgdg.sources), `postgresql-18`, `pgbackrest`; cluster `18/main`; [sysctl](files/sysctl.conf), [network-online drop-in](files/postgresql-network-online.conf); [`10-listen-tls.conf`](files/10-listen-tls.conf), [`20-tuning.conf`](files/20-tuning.conf) (computed), [`30-archive.conf`](files/30-archive.conf); [`pg_hba.conf`](files/pg_hba.conf), [`pg_ident.conf`](files/pg_ident.conf); CA and server certificate; start or reload | §6 | accepting; certificate days; huge pages |
| 5 | `database` | [`roles.sql`](roles.sql) as postgres (the roles, `state_reader` included); `explorer_owner`'s and `ingest_writer`'s passwords as SCRAM verifiers; the galexie bundle (§4.1) | §6.3 | |
| 6 | `backups` | [`pgbackrest.conf`](files/pgbackrest.conf) plus `conf.d/secrets.conf`; `stanza-create`; `pgbackrest check` until WAL is archived; [timers](files/pgbackrest-full.timer); the first full backup, in the background | §8.4 | last full and diff age; archive failures |
| 7 | `api` | ◷ until `explorer.schema_migrations` has the version `api.sql` needs; then [`grants.sql`](grants.sql) and [`api.sql`](api.sql) as `explorer_owner`; their sha256 and the schema version go into `COMMENT ON SCHEMA api` | §7.2 | newest ledger's age; API version |
| 8 | `postgrest` | release download (sha256), `/usr/local/lib/postgrest/postgrest-16.4`, symlink `/usr/local/bin/postgrest`, user, [config](files/postgrest.conf), [unit](files/postgrest.service) | | `/ready` |
| 9 | `caddy` | the `caddy` file next to the app → `/usr/local/lib/caddy/caddy-<sha256 prefix>`, symlink, user, [Caddyfile](files/Caddyfile) (`caddy validate` as `caddy`), [unit](files/caddy.service); ◷ until a certificate is served | §5 | `GET /` through the public name resolved to 127.0.0.1; certificate days |
| 10 | `monitoring` | optional (`monitoring_enabled = "yes"`): exporters with [their](files/node-exporter.default) [defaults](files/postgres-exporter.default), Prometheus and Alertmanager releases (sha256), [`prometheus.yml`](files/prometheus.yml), [`alerts.yml`](files/alerts.yml), the galexie host's [`alerts.yml`](../galexie/deploy/alerts.yml), [`alertmanager.yml`](files/alertmanager.yml), units; `promtool check config`, `amtool check-config` | §8.5 | every target up |
| 11 | `clickhouse` | optional (`clickhouse_hostname`): the clickhouse bundle (lab's CA for ClickHouse, the passwords Caddy, Grafana and the state copy log in with, and `stellar_api`'s, which Caddy logs in with for the frontend's backend, from `/root/clickhouse-bundle/`), the Caddy site [`clickhouse.caddy`](files/clickhouse.caddy), and with monitoring the clickhouse host's [scrape jobs](files/clickhouse-scrape.yml) and alerts; `none` removes them | [`clickhouse/README.md`](../clickhouse/README.md#4-bring-up-with-the-psql-host) | `/ping` and a query through Caddy, anonymously and as `codemelt` (`stellar_api`); certificate |
| 12 | `jobs` | with the `clickhouse` step on: user `stellar`; the worker shipped next to the app (`/usr/local/lib/stellar/worker-<version>`, symlink `/usr/local/bin/worker`); [`worker.toml`](files/worker.toml); `stellar_jobs`' password; [`stellar-state-copy.service`](files/stellar-state-copy.service) and its [timer](files/stellar-state-copy.timer) (daily, 00:15 UTC), with the [success marker](files/state-copy-mark-success) for node_exporter | [`clickhouse/README.md`](../clickhouse/README.md) §10 | the last copy's age and result |
| 13 | `grafana` | optional, installed by `lab grafana`: Grafana with the [dashboards](../dashboard/) over Prometheus, and with the `clickhouse` step on, the ClickHouse data source plugin, data source and dashboard; left out until then | [`dashboard/README.md`](../dashboard/README.md) | data sources answer; dashboards provisioned |

**Never overwritten.** A file in the way that `lab psql` doesn't recognise (no `(lab psql, …)` header, and not a package's unmodified default) stops its step with a message: compare it with the repository's file, then move it aside. Likewise a server certificate not issued by the local CA, a role password that isn't the one in the secrets directory, an existing cluster (kept with its locale), and a non-symlink where a symlink goes.

### 3.3 Settings and secrets

`/etc/stellar/lab-psql.toml` holds the settings of §1.2 as `key = "value"` lines. The form asks only for missing values; to change one, edit the file and run `lab psql` again. `vlan_id` and `monitoring_enabled` have defaults (`none`, `no`) and are never asked: set them in the file.

| In `/etc/stellar/secrets/` | Entered or generated | What |
|---|---|---|
| `backup_s3_key`, `backup_s3_secret` | entered | the backup bucket's S3 key pair (§2.5) |
| `smtp_password`, `deadman_url` | entered (monitoring only) | §8.5 |
| `explorer_owner.password`, `ingest_writer.password` | generated | the roles' passwords; only their SCRAM verifiers reach the server |
| `pgbackrest-cipher-pass` | generated | the backup repository's passphrase |
| `pg-ca.key` | generated | the local CA's key; its certificate is `/etc/stellar/pg-ca.crt` |

Generated secrets are created once and never rotated silently (§8.7). **Copy the generated ones into the password manager after the first run.** A rebuilt server needs them back before its first run (§8.6, *Server lost*).

Where a daemon needs a secret in a file, the file is generated with a tight mode: pgBackRest reads `/etc/pgbackrest/conf.d/secrets.conf` (root:postgres 0640) next to the secret-free `pgbackrest.conf` (its default `config-include-path`); Alertmanager reads `/etc/alertmanager/smtp-password` and `deadman-url` (root:alertmanager 0640).

### 3.4 Restarts and reloads

A step reloads a service whenever its configuration changed and a reload picks it up. It **restarts a running service only with `--allow-restart`**; without it the step shows `! restart pending`, and the run exits 1 until you run `./lab psql --allow-restart` at a quiet time.

| Change | lab psql does |
|---|---|
| `pg_hba.conf`, `pg_ident.conf`, `conf.d/*.conf`, a renewed certificate | reload PostgreSQL; settings that need a restart (`pg_settings.pending_restart`: `shared_buffers`, `listen_addresses`, …) are a pending restart |
| `postgrest.conf`, the PostgREST binary or unit | restart (CORS origins and the proxy URI are read only at start) |
| Caddyfile | `caddy validate`, then reload through the admin API (no dropped connections) |
| the Caddy build or unit | restart |
| `prometheus.yml`, rules, `alertmanager.yml`, its secrets | check, then reload (SIGHUP) |
| a binary or unit of Prometheus or Alertmanager, an exporter's `/etc/default` file, a backup timer | restart |
| `nftables.conf` | reload |
| sshd and journald files (`system`) | `sshd -t` and reload ssh; restart journald (harmless) |

A service that isn't running yet is simply started, so a new server needs no `--allow-restart`: the cluster's first start already has every setting, `archive_mode` and huge pages included.

---

## 4. First bring-up across both hosts

First order both servers and join their private networks: §2 here, and `galexie/README.md` for the galexie host and the Cloud Network. Then:

1. **This host.** Copy `bin/` over and run `./lab psql` (§3.1). Every step goes ✓ except:
   - `api` ◷, waiting for `worker migrate`;
   - `caddy` ◷, until DNS points at this host (§5);
   - `monitoring` stays `off` until you enable it (§8.5).

   The database bundle is now in `/root/galexie-bundle/`.
2. **Bundle.** Copy it to the galexie host, through the workstation: `scp -3 -r root@<psql public IP>:/root/galexie-bundle root@<galexie public IP>:/root/`
3. **Galexie host.** Copy `bin/` over and run `/root/bin/lab galexie` (`galexie/README.md`): in one run it installs the bundle, runs `worker migrate` as `explorer_owner`, `worker bootstrap` as `ingest_writer` (the default privileges of `roles.sql` already allow it), and starts the services.
4. **This host again.** `./lab psql`: `api` applies `grants.sql` and `api.sql`, and everything is ✓. After the bootstrap's bulk load, refresh the statistics: `runuser -u postgres -- vacuumdb --dbname=explorer --schema=explorer --analyze --jobs=4`.
5. **Verify.** `./lab psql check` here and `/root/bin/lab galexie check` there; reboot both hosts; check both again.

### 4.1 The galexie bundle

| File in `/root/galexie-bundle/` | Mode | lab galexie installs it as | Holds |
|---|---|---|---|
| `pg-ca.crt` | 0644 | `/etc/stellar/pg-ca.crt` | the local CA's certificate: the worker connects with `sslmode=verify-full` |
| `pgpass` | 0600 | `/etc/stellar/pgpass` | `10.0.1.2:5432:explorer:ingest_writer:<password>` |
| `pgpass-owner` | 0600 | `/etc/stellar/pgpass-owner` | `10.0.1.2:5432:explorer:explorer_owner:<password>` |

`lab psql` rewrites the bundle whenever a password or the CA changes; copy it again then (§8.7). Server certificate renewals don't change it. Delete the copy on the galexie host once `lab galexie` has installed it.

---

## 5. DNS

Create `A` and `AAAA` records for `api.example.com` pointing at this server's public IPv4 and IPv6. If you use CAA records, allow `letsencrypt.org`. Caddy obtains the certificate on its own once the name resolves here (HTTP-01 on 80 or TLS-ALPN-01 on 443) and renews it about 30 days before it expires. Until then the `caddy` step shows ◷; after `dig +short api.example.com A AAAA` returns this host, run `./lab psql` again.

Check from outside:

```sh
curl -sI https://api.example.com/status                                   # 200, HSTS, Cache-Control: public, max-age=5
curl -s -o /dev/null -w '%{http_code}\n' -X POST https://api.example.com/ledgers   # 405
curl -sI "https://api.example.com/ledgers?sequence=eq.<a recent ledger>" | grep -i cache-control   # max-age=86400, immutable
```

What the [Caddyfile](files/Caddyfile) does, in short: automatic HTTPS and HTTP/3; `405` for anything but `GET`/`HEAD`/`OPTIONS`; a 1 KB body limit; `Authorization` and `Cookie` dropped, so PostgREST never sees credentials; 100 requests per client IP in any 10 s (IPv6 per /64), then `429`, except for the frontend's backend, which sends its login `codemelt:astrolabe` (basic auth, in plain in the Caddyfile, the same as on the ClickHouse site) and has a budget of its own, 5,000 requests in any 10 s: PostgREST answers about 5,500 a second with every core busy (measured with the frontend's page calls, ingest keeping up), and a single backend serving every visitor would reach the per-IP limit; security headers; `public, max-age=86400, immutable` on non-empty answers pinned to one ledger or hash (one day, because `worker reindex` may rewrite history after a decoder fix), `max-age=5` on everything else; JSON errors with `Access-Control-Allow-Origin: *` for the errors Caddy raises itself (429, 413, 502); the access log in `/var/log/caddy/api.log` (100 MiB × 10, 14 days); metrics on the admin API, `localhost:2019`.

`{client_ip}` is the TCP peer, because nothing sits in front. If a CDN or load balancer is ever added, set `trusted_proxies` in the Caddyfile, or every visitor shares the proxy's rate limit.

---

## 6. PostgreSQL

### 6.1 Cluster, listening, TLS

- **Packages** come from the PGDG repository. `unattended-upgrades` installs only Ubuntu's own security updates, so PostgreSQL, libpq and pgBackRest are never upgraded behind your back (§8.3).
- **Cluster.** [`createcluster.conf`](files/createcluster.conf) stops the package from creating a default cluster; `lab psql` creates `18/main` with `pg_createcluster --locale=C.UTF-8 18 main -- --locale-provider=builtin --data-checksums`. The builtin provider sorts by code point inside PostgreSQL, so an OS upgrade can never change index order. An existing cluster is kept as it is. postgresql-common honours `createcluster.d`, so the cluster gets the builtin provider and data checksums.
- **Paths.** Config `/etc/postgresql/18/main/` (the `conf.d/` files are lab psql's), data `/var/lib/postgresql/18/main`, log `/var/log/postgresql/postgresql-18-main.log`, unit `postgresql@18-main`. lab psql enables the unit itself; it coexists with `postgresql.service`'s generator.
- **Listening** ([`10-listen-tls.conf`](files/10-listen-tls.conf)): `localhost` and the private IP, never the public interface. Two measures make the private IP reliable at boot: `net.ipv4.ip_nonlocal_bind = 1` ([`sysctl.conf`](files/sysctl.conf)) lets PostgreSQL bind it before the VLAN is up, and a [drop-in](files/postgresql-network-online.conf) starts every cluster after `network-online.target`.
- **TLS.** `lab psql` keeps a small CA on this host: the certificate `/etc/stellar/pg-ca.crt`, the key `/etc/stellar/secrets/pg-ca.key` (ECDSA P-256, 10 years). It issues the server certificate `/etc/ssl/postgresql/server.crt` (825 days, the private IP as its only SAN, key `root:postgres 0640`) and renews it when 30 days are left, followed by a reload. The galexie host trusts the CA through the bundle and connects with `sslmode=verify-full`; TLS 1.3 only. The CA key lives on the server it vouches for, which gives an attacker with root here nothing they don't already have; in exchange, renewals need no human.

### 6.2 Client authentication

[`pg_hba.conf`](files/pg_hba.conf) replaces the whole file:

- `local all postgres peer`: administration (`runuser -u postgres -- psql`) and pgBackRest;
- `local explorer authenticator peer map=postgrest`: PostgREST, running as the OS user `postgrest`, logs in as `authenticator` ([`pg_ident.conf`](files/pg_ident.conf)) with no password;
- `local all postgres_exporter peer map=exporter`: the exporter, running as `prometheus`;
- `local explorer state_reader peer map=statecopy`: `worker state-copy`, running as `stellar`, reads as `state_reader`;
- `hostssl explorer ingest_writer,explorer_owner 10.0.0.2/32 scram-sha-256`: the galexie host;
- everything else is rejected.

Test from the galexie host, once its bundle is installed:

```sh
psql "host=10.0.1.2 dbname=explorer user=ingest_writer sslmode=verify-full sslrootcert=/etc/stellar/pg-ca.crt passfile=/etc/stellar/pgpass" \
  -c 'SELECT current_user, ssl, version FROM pg_stat_ssl WHERE pid = pg_backend_pid()'     # ingest_writer | t | TLSv1.3
```

On this host, check that every line parsed: `runuser -u postgres -- psql -Atc "SELECT rule_number, error FROM pg_hba_file_rules WHERE error IS NOT NULL"` (no rows).

### 6.3 Roles and privileges

| Role | Login | Purpose |
|---|---|---|
| `explorer_owner` | password, from the galexie host only | owns the database, the schemas and every object: `worker migrate` runs as it, and `api.sql` here through `SET ROLE` |
| `ingest_writer` | password, from the galexie host only | `worker ingest`, `bootstrap`, `reindex`: DML and `TRUNCATE` on every table through default privileges, `TEMPORARY` for COPY batches |
| `authenticator` | peer, local only, `NOINHERIT` | PostgREST's login: no privileges of its own, switches to `web_anon` per request |
| `web_anon` | no | the anonymous API role: `SELECT` on the `api` views only, `statement_timeout = 2s` |
| `postgres_exporter` | peer, local only | monitoring (`pg_monitor`) |
| `state_reader` | peer, local only (the OS user `stellar`) | `worker state-copy`: `SELECT` on the tables it copies, the cursor and `ledgers` ([`grants.sql`](grants.sql)); read-only transactions |

Who applies what:

- [`roles.sql`](roles.sql) (step `database`, as postgres): the database, the roles and their attributes, `pg_stat_statements`, and default privileges, so that whatever `explorer_owner` creates is usable by `ingest_writer`, `TRUNCATE` included: `worker bootstrap` (which truncates the state tables) can run right after `worker migrate`. It is idempotent and resets attributes; its sha256 is recorded in `COMMENT ON DATABASE explorer`, so a changed file is applied again.
- [`grants.sql`](grants.sql) (step `api`, as `explorer_owner`): `TRUNCATE` on every existing table of schema `explorer` (tables created before the default privilege), and `state_reader`'s `SELECT`s. It runs again after every migration (§7.2).
- **Passwords.** `lab psql` generates them (32 random bytes, hex), keeps them in `/etc/stellar/secrets/`, and sends only their SCRAM-SHA-256 verifiers to the server (computed in Go), so the plaintext never reaches the server's logs or `pg_stat_statements`.

Check the grants by hand (`dml` and `truncate` true for every table):

```sql
-- runuser -u postgres -- psql -d explorer
SELECT c.oid::regclass AS relation,
       (SELECT bool_and(has_table_privilege('ingest_writer', c.oid, p))
          FROM unnest(ARRAY['SELECT', 'INSERT', 'UPDATE', 'DELETE']) AS p) AS dml,
       has_table_privilege('ingest_writer', c.oid, 'TRUNCATE') AS truncate
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r' AND (n.nspname = 'explorer' OR c.relname = 'ingest_state')
ORDER BY 1;
SELECT has_table_privilege('state_reader', 'explorer.trustlines', 'SELECT'),   -- t
       has_table_privilege('state_reader', 'explorer.offers', 'SELECT');      -- f: only what it copies
```

### 6.4 Tuning and huge pages

[`20-tuning.conf`](files/20-tuning.conf) shows the values for an AX102 (128 GB RAM, 16 cores, 32 threads, a 3.84 TB data volume). `lab psql` rewrites the computed lines for this host:

| Setting | Computed as |
|---|---|
| `shared_buffers` | 25 % of RAM (MemTotal rounded up to 4 GiB) |
| `effective_cache_size` | 70 % of RAM, at most RAM − 4 GB (PostgREST, Caddy, Prometheus), at least `shared_buffers` |
| `work_mem`, `maintenance_work_mem`, `autovacuum_work_mem` | RAM / 2048 (4 MB–256 MB), RAM / 64 (64 MB–2 GB), RAM / 128 (64 MB–1 GB) |
| `io_workers` | cores / 2 (3–32) |
| `max_worker_processes`, `max_parallel_workers` | threads (at least 8), cores |
| `max_parallel_workers_per_gather`, `…_maintenance_workers` | cores / 4 (1–4) |
| `autovacuum_max_workers` | 6, or cores / 2 (at least 3) below 12 cores |
| `max_wal_size`, `min_wal_size` | 1/16 of the data volume (2–64 GB), and 1/16 of that |
| `vm.nr_hugepages` ([`sysctl.conf`](files/sysctl.conf)) | `shared_buffers` in 2 MB pages + 4 % + 128 pages, rounded up to 100 |

The rest is fixed: WAL compression and checkpoints for ingest throughput; aggressive autovacuum for the update-heavy state tables; `jit = off` for short API queries; `max_locks_per_transaction = 256`; `pg_stat_statements`. `synchronous_commit` stays on: the ledger commit is the pipeline's point of truth, and at one commit per ledger its latency doesn't matter.

**Huge pages.** The kernel reserves them when the sysctl file is loaded, before the cluster's first start, so `SHOW huge_pages_status` says `on`. On a box whose memory is already fragmented the kernel may reserve fewer; `huge_pages = 'try'` then falls back to normal pages, and `check` shows `huge pages off`: reboot at a quiet time. Compare `grep HugePages_ /proc/meminfo` with `runuser -u postgres -- psql -Atc 'SHOW shared_memory_size_in_huge_pages'` Check the 4 % + 128 margin against that value on a new server size.

---

## 7. The API

[`api.sql`](api.sql) is the public API contract: read-only views over the worker's tables, plus the indexes they need. Everything PostgREST serves is defined there, and nothing else is exposed. Since version 2 it serves **current state and the ledgers**: transactions, operations, events, statistics and rankings are the clickhouse host's ([`clickhouse/README.md`](../clickhouse/README.md) §8).

### 7.1 What it contains (API version 2)

**Part 1, indexes** on `explorer.*`, created only if missing.

**Part 2, two schemas, replaced as a whole** in one transaction that ends with `NOTIFY pgrst, 'reload schema'`:

- `api_types` (not exposed): four **domains** with PostgREST *domain representations*. A view column cast to one keeps its base type for filters, ordering and indexes; PostgREST calls the domain's cast to `json` for the response and, for `hex`, a cast from text to parse filter values. The domains have no constraints, so the casts are no-ops for the planner and the base columns' indexes still apply.
- `api` (exposed): 13 **views**, 6 **computed relationships** for embedding, the `order_book` function, and the grants: `USAGE`, `SELECT` and `EXECUTE` for `web_anon` only. The cast functions are executable by everyone, because PostgREST loads the schema cache as `authenticator`.

| View | Rows | Key | Indexed lookups |
|---|---|---|---|
| `ledgers` | one per ledger, from the state's checkpoint on | `sequence` | `sequence`, `hash`, `closed_at` |
| `accounts` | current state | `account_id` | `account_id` |
| `trustlines` | current state | `(account_id, asset)` | an account's, an asset's |
| `data_entries` | an account's name/value pairs | `(account_id, name)` | an account's |
| `offers` | the DEX's open offers (+ `price`) | `offer_id` | a seller's, a pair's by price |
| `claimable_balances` | current state | `balance_id` | a claimant's (`claimants=cs.[…]`), an asset's |
| `liquidity_pools` | current state | `pool_id` | `pool_id`, a pair |
| `contracts` | one per contract: its instance entry (+ TTL) | `contract_id` | `contract_id`, `wasm_hash`, `asset` (a Stellar Asset Contract) |
| `contract_data` | current state (+ TTL) | `key_hash` | `key_hash`, a contract's |
| `sac_balances` | `contract_data` with a balance holder | `key_hash` | `balance_holder` |
| `contract_code` | current state (+ TTL, + `spec`) | `key_hash` | `key_hash`, `code_hash` |
| `contract_wasm` | the uploaded Wasm, base64 | `code_hash` | `code_hash` |
| `status` | one row: ingest progress | | |

Computed relationships (embed with `select=…,name(…)`): `accounts → trustlines, sac_balances, data_entries, offers, claimable_balances`; `contracts → contract_code`. The function `rpc/order_book?base=…&counter=…&depth=20` aggregates a pair's offers by price level: asks sell `base`, bids buy it, both priced in `counter` per unit of `base`, with amounts in stroops of `base`.

| Values | JSON | Filters |
|---|---|---|
| hashes and keys (`hash`, `prev_hash`, `key_hash`, `code_hash`, `wasm_hash`, `pool_id`, `liquidity_pool_id`) | lower-case hex | hex, any case: `?hash=eq.3389e9f0…` |
| XDR and bytes (`key_xdr`, `val_xdr`, `wasm`, a data entry's `value`) | base64 | not for filtering |
| 64-bit integers that can exceed 2^53 (stroop amounts, sequence numbers, offer ids) | decimal **string** | plain decimal |
| token amounts and prices (`balance` of contract data, `price` of offers, the order book), in base units | decimal string | decimal |
| ledger sequences, counts, flags | numbers | numbers |
| `closed_at` | ISO 8601 with offset (UTC) | `?closed_at=gte.2026-10-01T00:00:00Z` |
| ScVal (`key`, `val` of contract data) | the worker's JSON, e.g. `{"symbol":"transfer"}` | JSON equality, URL-encoded |
| `spec` | the Wasm's `contractspecv0` entries as JSON: functions, their argument and result types, structs, unions, enums, errors, events | JSON containment |
| `claimants` | `[{"destination": "G…", "predicate": …}]` | containment: `claimants=cs.[{"destination":"G…"}]` |

`contract_data.expired` (and `contracts`', `contract_code`'s) is `live_until_ledger` below the newest ingested ledger; `archived` means evicted after the bootstrap.

### 7.2 How it is applied

The `api` step:

1. waits (◷) until `explorer.schema_migrations` reaches the version `api.sql` checks for (12, `state_liquidity_pools`);
2. runs `SET ROLE explorer_owner`, [`grants.sql`](grants.sql) and [`api.sql`](api.sql) in one `psql` session over the Unix socket, so `explorer_owner` owns the views without ever logging in here;
3. appends `lab psql: grants.sql and api.sql sha256 <hex> at schema version <n>` to `COMMENT ON SCHEMA api` and notifies PostgREST again.

It applies them again whenever the files change, a migration raises the schema version, or the `api` views aren't all there (a reset, §8.8, drops them). Part 1 commits on its own; part 2 is all or nothing, so a failure leaves the previous API in place. `NOTIFY` is delivered at commit, and PostgREST reloads its schema cache within a second.

By hand, as the step does it:

```sh
(echo 'SET ROLE explorer_owner;'; cat grants.sql api.sql) | runuser -u postgres -- psql -X -v ON_ERROR_STOP=1 -d explorer -f -
```

A worker migration that changes a column a view uses fails with *cannot alter type of a column used by a view*: drop the API first (`DROP SCHEMA api CASCADE`; the API answers 404 meanwhile), migrate, then run `lab psql`. Such migrations belong together with an `api.sql` release (§7.5).

### 7.3 Querying it

From TypeScript, use [`generated/api.ts`](../generated/api.ts) with `@supabase/postgrest-js`: column names and filters are checked and row types follow the `select` string ([`generated/README.md`](../generated/README.md)).

Every list request should filter or order on an **indexed key**; anything else runs into `web_anon`'s 2 s statement timeout (HTTP 500, code `57014`). Responses carry at most 200 rows. Page with **keyset filters**, never `offset`: on composite keys, repeat the last row's key in an `or=` filter together with a plain bound the index can use.

| Need | Request |
|---|---|
| ingest status, newest ledger | `/status` |
| latest ledgers | `/ledgers?order=sequence.desc&limit=20`, next `&sequence=lt.<last>` |
| a ledger | `/ledgers?sequence=eq.60000000` or `/ledgers?hash=eq.<hex>` |
| the ledger at a time | `/ledgers?closed_at=lte.2026-10-01T00:00:00Z&order=closed_at.desc&limit=1` |
| an account with everything it holds | `/accounts?account_id=eq.G…&select=*,trustlines(*),sac_balances(*),data_entries(*),offers(*),claimable_balances(*)` |
| an account's offers | `/offers?seller_id=eq.G…&order=offer_id&limit=50`, next `&offer_id=gt.<last>` |
| a pair's order book | `/rpc/order_book?base=native&counter=USDC:GA5Z…&depth=20` |
| a pair's offers | `/offers?selling=eq.native&buying=eq.USDC:GA5Z…&order=price,offer_id&limit=50` |
| an account's claimable balances | `/claimable_balances?claimants=cs.[{"destination":"G…"}]` (URL-encoded) |
| a pool, a pair's pools | `/liquidity_pools?pool_id=eq.<hex>`, `/liquidity_pools?asset_a=eq.native&asset_b=eq.USDC:GA5Z…` |
| the contracts | `/contracts?order=contract_id&limit=50`, next `&contract_id=gt.C…` |
| a contract with its functions | `/contracts?contract_id=eq.C…&select=*,contract_code(spec)` |
| a code's Wasm | `/contract_wasm?code_hash=eq.<hex>` |
| a contract's data | `/contract_data?contract_id=eq.C…&order=key_hash&limit=50`, next `&key_hash=gt.<hex>` |
| an asset's trustlines | `/trustlines?asset=eq.USDC:GA5Z…&order=account_id&limit=50`, next `&account_id=gt.G…` |
| an asset's Stellar Asset Contract | `/contracts?asset=eq.USDC:GA5Z…` |

Counting: `Prefer: count=exact` on large views runs into the timeout; use `count=planned` or `count=estimated`. The OpenAPI document at `/` lists every view and column.

### 7.4 Indexes and what they cost

| Index (schema `explorer`) | Serves |
|---|---|
| `api_ledgers_closed_at_idx` | the ledger at a time (17,280 rows a day, < 1 MB) |
| `api_trustlines_asset_account_idx`, `api_contract_data_contract_key_idx`, `api_contract_code_code_hash_idx` | asset, contract and code lookups |
| the migrations' own: `offers_seller_idx`, `offers_pair_idx`, `claimable_balances_claimants_idx` (GIN), `claimable_balances_asset_idx`, `liquidity_pools_assets_idx`, `contract_data_instance_idx`, `contract_data_wasm_hash_idx`, `contract_data_asset_idx`, `contract_data_balance_holder_idx`, `trustlines_asset_idx` | the state tables' lookups |

They grow with the state, not with time: the database holds the state and one row per ledger (~6.4 MB a day).

### 7.5 Changing the API

- **Additive changes** (a new view, a new column at the end, a new index) keep the version.
- **Breaking changes** (renaming or removing a column or view, changing a representation) bump the version in `api.sql`'s header, `api_version` in `api.status`, the schema comment, and §7.7. Coordinate them with the frontend.
- Every new index is a `CREATE INDEX IF NOT EXISTS` in part 1, built first with §7.6 when the table holds data. Remove one with `DROP INDEX IF EXISTS` in part 1.
- Keep view columns explicit (no `SELECT *` over `explorer` tables), so a worker migration never changes the API by accident.
- A worker migration that drops a column the views select needs the new `api.sql` first: apply it with `lab psql`, then run `worker migrate`. In the other order the migration stops, changing nothing, because PostgreSQL won't drop a column a view still selects.
- Computed relationships stay `LANGUAGE sql STABLE … RETURNS SETOF <view>` (inlined by PostgreSQL), and each is **named after the view it returns**: PostgREST 16.4 applies an embedded resource's domain representations only when the names match.

Test a change before it ships: on a workstation with PostgreSQL 18, as §9 describes; then query the views as `web_anon` and through a local PostgREST. Then regenerate the API's TypeScript types (`make types`, [`generated/README.md`](../generated/README.md)) and commit them with the change.

### 7.6 Adding an index to a populated table

`CREATE INDEX` **blocks ingest's writes** to the table until it is built. Build it concurrently first, then let `api.sql` find it:

```sql
-- runuser -u postgres -- psql -X -d explorer
SET ROLE explorer_owner;
CREATE INDEX CONCURRENTLY IF NOT EXISTS api_offers_buying_idx ON explorer.offers (buying, selling, price, offer_id);
SELECT indisvalid FROM pg_index WHERE indexrelid = 'explorer.api_offers_buying_idx'::regclass;   -- t
```

A failed `CREATE INDEX CONCURRENTLY` leaves an invalid index: drop it with `DROP INDEX CONCURRENTLY` and run it again. Then add the matching `CREATE INDEX IF NOT EXISTS` to part 1 of `api.sql`.

### 7.7 Changelog

| API version | Changes |
|---|---|
| 1 | The first version: ledgers, transactions, operations, events, contract events, operation types, accounts, trustlines, contract data, SAC balances, contract code, status; computed relationships; hex, base64, int64 and amount representations. |
| 2 | Current state only: `transactions`, `operations`, `events`, `contract_events` and `operation_types` go, with their computed relationships and indexes; history comes from ClickHouse ([`clickhouse/README.md`](../clickhouse/README.md)). New: `data_entries`, `offers` with `rpc/order_book`, `claimable_balances`, `liquidity_pools`, `contracts`, `contract_code.spec`, `contract_wasm`; `accounts` embeds `data_entries`, `offers` and `claimable_balances`, `contracts` embeds `contract_code`. `ledgers` and `status.oldest_ledger` start at the bootstrap's checkpoint. A new database: the worker's migrations start over (12), loaded by `worker bootstrap` (§8.8). |

---

## 8. Operations

### 8.1 Where things are

| What | Where |
|---|---|
| lab psql | settings `/etc/stellar/lab-psql.toml`, secrets `/etc/stellar/secrets/`, log `/var/log/stellar-setup/psql.log` |
| PostgreSQL | `/etc/postgresql/18/main/` (`conf.d/10-listen-tls.conf`, `20-tuning.conf`, `30-archive.conf`), data `/var/lib/postgresql/18/main`, log `/var/log/postgresql/postgresql-18-main.log`, unit `postgresql@18-main` |
| TLS (database) | `/etc/ssl/postgresql/server.{crt,key}`, CA `/etc/stellar/pg-ca.crt` and `/etc/stellar/secrets/pg-ca.key` |
| kernel, boot order | `/etc/sysctl.d/60-stellar.conf`, `/etc/systemd/system/postgresql@.service.d/10-network-online.conf` |
| pgBackRest | `/etc/pgbackrest/pgbackrest.conf`, `conf.d/secrets.conf`, logs `/var/log/pgbackrest/`, timers `pgbackrest-full.timer` (Sun 01:30 UTC), `pgbackrest-diff.timer` (Mon–Sat 01:30, installed but off) |
| PostgREST | `/usr/local/bin/postgrest` → `/usr/local/lib/postgrest/postgrest-16.4`, `/etc/postgrest/postgrest.conf`, `127.0.0.1:3000`, admin `127.0.0.1:3001` |
| Caddy | `/usr/local/bin/caddy` → `/usr/local/lib/caddy/caddy-<sha256 prefix>`, `/etc/caddy/Caddyfile`, certificates `/var/lib/caddy/.local/share/caddy`, access log `/var/log/caddy/api.log`, admin `localhost:2019` |
| monitoring | `/etc/prometheus/`, data `/var/lib/prometheus/metrics2` (30 days, ≤ 20 GB), `127.0.0.1:9090`; `/etc/alertmanager/`, `127.0.0.1:9093`; exporters `127.0.0.1:9100`, `:9187` |
| Grafana (optional) | `/etc/grafana/` (`grafana.ini`, `provisioning/`), dashboards `/etc/grafana/dashboards/stellar-explorer/`, data `/var/lib/grafana`, `127.0.0.1:3002`, unit `grafana-server` |
| firewall, network | `/etc/nftables.conf` (`nft list ruleset`), `/etc/netplan/60-vswitch.yaml` |
| the galexie bundle | `/root/galexie-bundle/` |

### 8.2 Configuration changes

- **A setting** (§1.2): edit `/etc/stellar/lab-psql.toml`, then `./lab psql`. It lists what changes before it asks.
- **A repository file** (`files/`, the SQL): change it in the repository, `make check dist`, copy `bin/` over, `./lab psql`. Never edit an installed file by hand: the next run puts the repository's version back.
- **Restarts** follow §3.4: look at the `!` lines, then `./lab psql --allow-restart` at a quiet time.

### 8.3 Upgrades and reboots

- **Ubuntu** security updates arrive through unattended-upgrades. When `/var/run/reboot-required` exists, reboot at a quiet time, then `./lab psql check`. needrestart restarts other services after a library update but only lists PostgreSQL ([`needrestart.conf`](files/needrestart.conf)); `needrestart -r l` shows it, and `systemctl restart postgresql@18-main` at a quiet time picks the new library up.
- **PostgreSQL minor releases** (quarterly, or for security fixes): read the release notes (some ask for a REINDEX), then at a quiet time — the package restarts the cluster:
  ```sh
  apt-get update && apt-get install --only-upgrade postgresql-18 postgresql-client-18 libpq5 postgresql-common pgbackrest
  runuser -u postgres -- psql -Atc 'SELECT version()'
  ```
- **PostgreSQL major versions** need a new lab psql (its paths and packages name 18) and `pg_upgradecluster --locale=C.UTF-8 -v 19 -m link 18 main` (the builtin locale must be passed again), with pgBackRest's `stanza-upgrade` and a new full backup. Rehearse it on a drill host (§8.4) first.
- **PostgREST**: read the changelog's migration notes, change the release URL, version and sha256 in [`cli/psql_web.go`](../cli/psql_web.go), `make dist`, then `./lab psql --allow-restart`. The new version is installed beside the old one; roll back with `ln -sfn /usr/local/lib/postgrest/postgrest-<old> /usr/local/bin/postgrest && systemctl restart postgrest`. Test a major version against a local database with `api.sql` first: PostgREST's behaviour is part of the API.
- **Caddy**: bump the pins (`cd psql/caddy && go get github.com/caddyserver/caddy/v2@<version> github.com/mholt/caddy-ratelimit@<commit> && go mod tidy`), `make check dist`, copy `bin/`, `./lab psql --allow-restart`. The new build sits beside the old one; roll back by pointing the symlink at the previous `/usr/local/lib/caddy/caddy-*` and restarting.
- **Prometheus, Alertmanager**: change the URLs, versions and sha256 (from the release's `sha256sums.txt`) in [`cli/psql_web.go`](../cli/psql_web.go), then as for PostgREST. Their data is kept.
- **Exporters** come with Ubuntu's apt updates.

### 8.4 Backups and the restore drill

pgBackRest archives every WAL segment (asynchronously, `archive_timeout = 5min`), takes a **full backup on Sundays**, keeps **1 full** (up to a week of point-in-time recovery: a restore replays up to seven days of WAL), and encrypts everything client-side. Backups expire after each successful backup: the previous full and its WAL go once the new one is complete. A full of a multi-terabyte database takes hours at 1 Gbit/s.

The daily differential backup is **off**: `pgbackrest-diff.timer` is installed but not enabled. A diff copies only the files changed since the last full and shortens a restore's WAL replay to at most a day. To turn it on: `systemctl enable --now pgbackrest-diff.timer`; `lab psql check` and the alert `PostgresBackupStale` then also expect a backup every 30 hours (the alert reads the timer through node_exporter's systemd collector), and `lab psql` leaves the timer as it finds it. A diff that starts while a full still runs fails on the lock, which is harmless.

Backups matter although the ledgers are archived: the current-state tables come back otherwise only from a fresh `worker bootstrap`, with the ledgers since the last checkpoint lost from `ledgers`, and the API down meanwhile.

```sh
runuser -u postgres -- pgbackrest --stanza=explorer info              # backups, sizes, WAL range
systemctl start pgbackrest-backup@full.service                        # a full now (e.g. after the bootstrap)
journalctl -u pgbackrest-backup@full.service -n 20
runuser -u postgres -- psql -c 'SELECT * FROM pg_stat_archiver'       # failed_count, last_failed_wal
```

**Restore drill.** Before go-live, after a major upgrade, then quarterly. It proves the backups, the WAL archive **and** the passphrase, and measures the restore time. While the database is small, do it here as a second cluster on port 5433; once it is large, on a temporary server with enough disk (Ubuntu 24.04, PGDG packages, the same `pgbackrest.conf` and `conf.d/secrets.conf`). A drill host must **never** run `backup`, `stanza-*` or `archive-push`.

```sh
# An empty cluster provides the config directory and a unit; recovery as a hot standby needs
# at least production's max_worker_processes and max_locks_per_transaction (§6.4).
pg_createcluster --locale=C.UTF-8 --port=5433 18 drill -- --locale-provider=builtin
pg_conftool 18 drill set huge_pages off
pg_conftool 18 drill set max_worker_processes 32
pg_conftool 18 drill set max_locks_per_transaction 256
rm -rf /var/lib/postgresql/18/drill && install -d -o postgres -g postgres -m 0700 /var/lib/postgresql/18/drill
time runuser -u postgres -- pgbackrest --stanza=explorer --pg1-path=/var/lib/postgresql/18/drill --archive-mode=off restore
#    a point in time instead: add --type=time "--target=2026-10-01 06:00:00+00" --target-action=promote
systemctl start postgresql@18-drill
runuser -u postgres -- psql -p 5433 -Atc 'SELECT pg_is_in_recovery()'     # repeat until f
runuser -u postgres -- psql -p 5433 -d explorer \
  -c "SELECT pipeline, ledger_sequence, updated_at FROM public.ingest_state" -c "SELECT max(sequence), max(closed_at) FROM explorer.ledgers"
pg_dropcluster --stop 18 drill
```

Record the restore and recovery times: that is the recovery time objective.

### 8.5 Monitoring

**Enable it** once the SMTP account and the heartbeat check exist: set `monitoring_enabled = "yes"` in `/etc/stellar/lab-psql.toml`, then `./lab psql`; it asks for `smtp_host`, `smtp_user`, `smtp_password`, `page_email` and `deadman_url`. The galexie host must allow this host on 9101, 9102 and 6061 (its nftables file).

- **Rules:** [`alerts.yml`](files/alerts.yml) for this host and the monitoring itself; the galexie host's [`alerts.yml`](../galexie/deploy/alerts.yml), which `lab` embeds from the galexie folder.
- **Routing:** `severity: page` → `page_email` (repeated hourly); everything else → `ops_email`; a paging alert silences the warnings of the same alert and target. A pager integration replaces the `page` receiver's `email_configs`.
- **Dead-man's switch.** The always-firing `Watchdog` alert goes every minute to an external heartbeat service (for example healthchecks.io: period 5 minutes, grace 10, with its own notifications), which pages when it stops: this host, Prometheus or Alertmanager down. Test it once: stop Alertmanager past the grace period.
- **External uptime check** of `https://api.example.com/status` (any uptime monitor): it sees DNS, TLS and the certificate's expiry as the internet does.
- **Access** through an SSH tunnel; nothing listens publicly:
  ```sh
  ssh -L 9090:127.0.0.1:9090 -L 9093:127.0.0.1:9093 -L 3002:127.0.0.1:3002 root@<public IP>
  # http://localhost:9090 (Prometheus), http://localhost:9093 (Alertmanager, silences),
  # http://localhost:3002 (Grafana, once `lab grafana` installed it)
  ```
- **Dashboards:** `lab grafana` adds Grafana on `127.0.0.1:3002` with two dashboards over this Prometheus, the pipeline (galexie host) and the database and API (this host), and a third over ClickHouse with the clickhouse host: [`dashboard/README.md`](../dashboard/README.md).

The exporters are Ubuntu's packages. `prometheus-postgres-exporter` (0.15.0) runs as `prometheus` (the `pg_ident.conf` map) and exposes `pg_stat_archiver_*`, `pg_stat_activity_count` and `pg_stat_activity_max_tx_duration` (the long-transaction alert) against PostgreSQL 18; its `long_running_transactions` collector is off by default and not needed. node_exporter's systemd collector, off by default, is switched on for the pgBackRest units ([`node-exporter.default`](files/node-exporter.default)). The mdadm collector (`node_md_*`, alert `RaidDegraded`) is untested. Disabling monitoring again leaves Prometheus and Alertmanager as they are.

### 8.6 Runbooks

**`PostgresDown`, `TargetDown{job="postgres"}`.** `systemctl status postgresql@18-main`, the log, `df -h /var/lib/postgresql`. A full data volume stops PostgreSQL: free WAL by fixing archiving (next item), never by deleting files in `pg_wal`.

**`PostgresWalArchivingFailing`, `PostgresWalArchiveStale`.** WAL piles up in `pg_wal` while archiving fails. Hetzner Object Storage sometimes stalls uploads for a few minutes: pgBackRest's async push times out, PostgreSQL retries, and the backlog goes out in seconds once the store answers. A short burst of failures that stops by itself needs no action. Look at `/var/log/pgbackrest/explorer-archive-push-async.log` and `runuser -u postgres -- pgbackrest --stanza=explorer check`. Usual causes: the bucket's credentials (§8.7), the endpoint, the network. Past `archive-push-queue-max` (256 GiB) pgBackRest drops WAL to protect the disk, which breaks point-in-time recovery until the next full backup: take one once archiving works.

**`PostgresBackupStale`, `PostgresBackupUnitFailed`, `PostgresFullBackupStale`.** `systemctl list-timers 'pgbackrest-*'`, `journalctl -u pgbackrest-backup@full.service`, `pgbackrest info`; start a backup by hand once the cause is fixed.

**`DataDiskUsage`, `DataDiskFullIn30Days` (*Disk growth*).** Where the space goes: `SELECT pg_size_pretty(pg_database_size('explorer'))`, and per table `SELECT relname, pg_size_pretty(pg_total_relation_size(oid)) FROM pg_class WHERE relnamespace = 'explorer'::regnamespace AND relkind = 'r' ORDER BY pg_total_relation_size(oid) DESC`. The database holds the state and one row per ledger, so it grows with the network's state, not with time; WAL and its archive are the usual suspects (the archiving runbook below). Options, cheapest first: bigger drives in this server (support swaps one at a time; re-add the partitions, wait for the resync, then `growpart` both, `mdadm --grow --size=max`, `pvresize`, `lvextend -r -l +100%FREE vg0/pgdata`, as in Hetzner's [software RAID guide](https://docs.hetzner.com/robot/dedicated-server/raid/exchanging-hard-disks-in-a-software-raid/)) (check the partition numbers on the host first); or a larger server, restored from the backup as a standby and promoted at a cut-over, keeping `10.0.1.2` and the secrets.

**`RaidDegraded`.** Identify the drive by serial (`cat /proc/mdstat`, `nvme list`, `smartctl -a`), open a Robot support request for the swap, then re-add its partitions and wait for `[UU]`, per the same guide.

**`PostgresLongRunningTransaction`, `PostgresConnectionsHigh`.** A long transaction holds back vacuum everywhere; a waiting DDL blocks everything queued behind it:

```sql
SELECT pid, usename, client_addr, now() - xact_start AS age, wait_event, left(query, 100)
  FROM pg_stat_activity WHERE xact_start IS NOT NULL AND pid <> pg_backend_pid() ORDER BY xact_start LIMIT 20;
SELECT pid, pg_blocking_pids(pid), left(query, 100) FROM pg_stat_activity WHERE cardinality(pg_blocking_pids(pid)) > 0;
SELECT pg_cancel_backend(<pid>);       -- or pg_terminate_backend: an ingest transaction just rolls back and is retried
```

Top statements: `SELECT calls, round(total_exec_time), round(mean_exec_time::numeric, 1), left(query, 100) FROM pg_stat_statements ORDER BY total_exec_time DESC LIMIT 20`.

**`ApiServerErrors`, `PostgrestPoolTimeouts`, `TargetDown{job="postgrest"}`.** `systemctl status postgrest`, `curl -s http://127.0.0.1:3001/ready`, `journalctl -u postgrest`. A 503 from PostgREST means no database connection (PostgreSQL down, `pg_hba.conf`, the `pg_ident.conf` map); PostgREST reconnects on its own.

| Symptom | Cause and fix |
|---|---|
| `PGRST205` *Could not find the table … in the schema cache* | `api.sql` not applied (`check` shows `api` ◷ or `•`), or applied without the reload: `NOTIFY pgrst, 'reload schema'` |
| `500` with code `57014` | the 2 s statement timeout: the request doesn't use an index (§7.3) |
| `401`, code `42501` | a missing grant: a view created by hand; run `lab psql` (it re-applies `api.sql`) |
| hashes or amounts as `\x…` or JSON numbers | the domain representations weren't loaded: re-apply `api.sql`; PostgREST's startup log lists `5 Domain Representations` |
| no statement timeout in effect | `db-config` was turned off in `postgrest.conf` |
| every browser request fails with CORS | the origin is missing from `cors_origins` (§8.2, a restart) |

**`ApiRateLimitedShare`.** `jq -r 'select(.status==429) | .request.client_ip' /var/log/caddy/api.log | sort | uniq -c | sort -rn | head`. Legitimate traffic (shared NAT, a partner): raise `events` or `window` in the Caddyfile, or add a zone for that source. Abuse from a few addresses: block them in [`nftables.conf`](files/nftables.conf). The database is protected regardless: 2 s and 200 rows per request, 20 connections in total.

**Caddy can't get a certificate.** DNS doesn't point here yet, or 80/443 are blocked (Robot firewall §2.4). `journalctl -u caddy | grep -iE 'certificate|error'`. If `caddy validate` was ever run as root, `/var/log/caddy/api.log` belongs to root: `chown caddy:caddy` it.

**`StateCopyStale`, `StateCopyFailed`, `StateCopyMetricMissing`** (clickhouse rules, about this host's job): `systemctl list-timers stellar-state-copy.timer`, `journalctl -u stellar-state-copy`. A failed run leaves the previous copy, assets and top holders in place; fix the cause (ClickHouse unreachable on 10.0.2.2:9440, `stellar_jobs`' password, PostgreSQL), then `systemctl start stellar-state-copy` (a minute or two; it also writes the day's snapshots, replacing that date's if any). Details in [`clickhouse/README.md`](../clickhouse/README.md) §10.

**Server lost or rebuilt.** The secrets directory is the server's identity towards its clients and its backups. Before the first `lab psql` on the new server, restore from the password manager into `/etc/stellar/secrets/` (directory 0700, files 0600): `explorer_owner.password`, `ingest_writer.password`, `pgbackrest-cipher-pass`, `pg-ca.key`, and `/etc/stellar/pg-ca.crt` (also on the galexie host). Without the CA, the galexie host rejects the server; without the passphrase, the backups are unreadable. Then restore the database (§8.4): `./lab psql --skip api` (an empty cluster; the `backups` step writes its files, then fails on the new cluster's system identifier, as expected), `systemctl stop postgresql@18-main`, `runuser -u postgres -- pgbackrest --stanza=explorer --delta restore`, `systemctl start postgresql@18-main`, and `./lab psql` again.

### 8.7 Credential rotation

- **A role's password** (`ingest_writer`, `explorer_owner`): `openssl rand -hex 32 > /etc/stellar/secrets/<role>.password`, `runuser -u postgres -- psql -c 'ALTER ROLE <role> PASSWORD NULL'`, `./lab psql --only database` (sets the new verifier and rewrites the bundle), then copy the bundle (§4.1), install it with `lab galexie` and restart ingest there (`galexie/README.md`). Existing sessions stay logged in; only new logins fail during the few seconds in between. lab psql never overwrites a password it doesn't know: one set by hand must be written into the secret file, or reset to `NULL` as above.
- **The database server certificate** renews itself (§6.1); nothing changes for the clients.
- **The CA** (10 years): create a new one by hand, put both CA certificates (concatenated) into `pg-ca.crt` on both hosts, restart ingest, then switch the server to a certificate from the new CA (move the old files aside and replace `pg-ca.key` and `pg-ca.crt` here), and later drop the old CA from the bundle.
- **The backup bucket's S3 key**: generate new credentials in the `stellar-backups` project, `rm /etc/stellar/secrets/backup_s3_key /etc/stellar/secrets/backup_s3_secret`, run `./lab psql` (it asks for them), then `runuser -u postgres -- pgbackrest --stanza=explorer check`, and delete the old credentials in the Console.
- **The repository passphrase** can't be changed for an existing repository. To rotate it, add a second repository (`repo2-*`, new path and passphrase) by hand, `stanza-create`, a full backup into it, and remove `repo1` once its retention has passed.
- **SMTP password, dead-man's switch URL:** remove the secret file, run `./lab psql`.

### 8.8 Reset

Starts the database over from an empty schema: at go-live, and when the worker's migrations start over (a new API version that drops tables). With ingest stopped on the galexie host (`systemctl stop stellar-ingest`):

```sql
-- runuser -u postgres -- psql -d explorer: drops all explorer data, the cursor and the api views.
DROP SCHEMA IF EXISTS explorer CASCADE;
DROP TABLE IF EXISTS public.ingest_state;
```

Then, on the galexie host, the new worker and `worker migrate`; `./lab psql` here (the `api` step notices the missing views and applies `grants.sql` and `api.sql` again); `runuser -u stellar -- worker bootstrap` on the galexie host (about half an hour: it loads the state from the latest checkpoint and seeds the cursor there; `state/bootstrap.json` is kept), and `systemctl start stellar-ingest`. Roles and default privileges are unaffected. Take a full backup afterwards (`systemctl start pgbackrest-backup@full.service`).

---

## 9. Development

This folder holds the host: this README, the SQL, `files/`, the Caddy build, and `files.go`, which embeds the SQL and `files/` for the CLI. The setup code is the repository's `lab` CLI (`cli/`, module `stellar-explorer` at the root), which also sets up the galexie host:

| Path | What |
|---|---|
| `cli/main.go`, `run.go`, `sys.go`, `config.go` | the command, the step engine, host helpers (commands, files, apt, users, systemd, downloads), settings, secrets and rendering |
| `cli/psql.go`, `psql_pg.go`, `psql_web.go`, `grafana.go`, `grafana_clickhouse.go` | this host's settings and step table; the PostgreSQL steps (tuning, TLS, SCRAM, backups, api); network, firewall, PostgREST, Caddy, monitoring; Grafana (`lab grafana`) and its ClickHouse data source |
| `cli/cli_test.go` | the tests below |
| `psql/roles.sql`, `grants.sql`, `api.sql` | §6.3, §7 |
| `psql/files/` | every installed file, with the example values and a header naming its path |
| `psql/caddy/` | the nested module for the Caddy build: `main.go`, `go.mod`, `go.sum` |
| `Makefile` (root) | `build`, `test`, `vet`, `fmt`, `check` (gofmt, vet for darwin and linux, tests, linux/amd64 builds of lab and Caddy), `check-all` (`check` plus the worker's), `dist` (`VERSION` from git), `clean` |

**Tests** (`make test` at the repository root, no database needed):

- rendering the repository's files with the example values is the identity, and every example value occurs in some file;
- whole-token replacement (`10.0.0.2` never matches inside `10.0.0.20`);
- the tuning for an AX102 reproduces [`20-tuning.conf`](files/20-tuning.conf) and the hugepages line exactly, and two smaller servers get the expected values;
- the SCRAM verifier against one computed by PostgreSQL 18;
- the step table's names and order;
- the galexie host's rules render unchanged with this host's example settings.

**The SQL against a local PostgreSQL 18** (as a superuser; leave no trace):

```sh
psql -X -v ON_ERROR_STOP=1 -d postgres -f roles.sql                  # twice: the second run changes nothing
psql -X -d postgres -c "ALTER ROLE explorer_owner PASSWORD 'x'"      # any password, for worker migrate
worker migrate --config <worker.toml with [postgres] migrate_dsn = "postgres://explorer_owner:x@localhost/explorer">
(echo 'SET ROLE explorer_owner;'; cat grants.sql api.sql) | psql -X -v ON_ERROR_STOP=1 -d explorer -f -    # twice
# the checks of §6.3, then as web_anon: SET ROLE web_anon; SELECT * FROM api.status;
psql -X -d postgres -c 'DROP DATABASE explorer' -c 'DROP ROLE explorer_owner, ingest_writer, authenticator, web_anon, postgres_exporter'
```

**Other files:** `(cd caddy && go build -o /tmp/caddy .) && /tmp/caddy adapt --config files/Caddyfile` checks the Caddyfile with the real build; `promtool check rules files/alerts.yml ../galexie/deploy/alerts.yml` and `amtool check-config` (with the paths adjusted) check the monitoring files. The app itself runs only as root on Ubuntu 24.04; the end-to-end test is two disposable servers: run it twice (the second run changes nothing), `check`, reboot, `check` again.

**Adding a setting:** a line in `psqlSettings` (`cli/psql.go`) with its example, the example in the files that use it, and a row in §1.2, in the same order.
