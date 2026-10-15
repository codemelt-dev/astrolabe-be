# Stellar explorer backend

The backend of a Stellar **pubnet** block explorer. It ingests every ledger from go-live on: the current state into PostgreSQL, served as a public, read-only REST API, and the history into ClickHouse, served as public read-only SQL, where the asset rankings and daily statistics are computed too. Every ledger is also kept whole in the archive bucket. It runs on three servers, set up by one CLI, `lab`.

```text
                       Stellar network
                             │
  galexie host (private) ────┼──────────────────────────────────────────────┐
  │ Galexie (captive core) → worker lake (/srv/lake) → worker ingest        │
  │                                     archive bucket ← archiver           │
  └──────────────────────────┼──────────────────────────────────────────────┘
                             │ private network, TLS 5432
  psql host (public 80/443) ─┼──────────────────────────────────────────────┐
  │ PostgreSQL 18 ← PostgREST (Unix socket) ← Caddy (HTTPS) ← the internet  │
  │ pgBackRest → backup bucket; Prometheus, Alertmanager, Grafana (loopback)│
  │ worker state-copy (daily) ──────────────────────────┐                   │
  └─────────────────────────────────────────────────────┼───────────────────┘
                                                        │ private network: TLS 9440, 8443 (Caddy, Grafana)
  clickhouse host (private) ────────────────────────────┼───────────────────┐
  │ AWS public lake → worker clickhouse-ingest → ClickHouse 26.8            │
  └─────────────────────────────────────────────────────────────────────────┘
```

| Folder | What |
|---|---|
| [`galexie/`](galexie/) | the ingestion host: runbook ([`README.md`](galexie/README.md)) and its files (`deploy/`) |
| [`psql/`](psql/) | the database and API host: runbook ([`README.md`](psql/README.md)), the SQL, its files, the Caddy build |
| [`clickhouse/`](clickhouse/) | the history host: ClickHouse, fed by the worker's clickhouse pipeline from the AWS public lake, and the state copy for rankings; its files and runbook ([`README.md`](clickhouse/README.md)) |
| [`worker/`](worker/) | the Go worker: S3 gateway (lake), decoder and both pipelines, archiver, pruner, the state copy |
| [`cli/`](cli/) | `lab`, the setup CLI: `lab galexie`, `lab psql`, `lab grafana`, `lab clickhouse` |
| [`dashboard/`](dashboard/) | Grafana's configuration and dashboards ([`README.md`](dashboard/README.md)) |
| [`generated/`](generated/) | the API's TypeScript types ([`README.md`](generated/README.md)) |

All servers are deployed with **`lab`**, this repository's CLI ([below](#the-lab-cli)).

## The `lab` CLI

Every server is set up with `lab` ([`cli/`](cli/)): there are no manual install steps. The runbooks explain what it does on each host and why.

**Getting it onto a server.** `make dist` builds `bin/lab` for linux/amd64, next to `bin/worker` and `bin/caddy`. Copy `bin/` to `/root/bin/` on each server and run `lab` there as root. It needs Ubuntu 24.04 and refuses to run anywhere else.

| Command | Run on | Does |
|---|---|---|
| `lab psql` | the psql host | sets up PostgreSQL, the roles and the API schema, PostgREST, Caddy, backups and (optionally) monitoring |
| `lab galexie` | the galexie host | sets up the firewall, the disks, Docker and Galexie, the worker's lake and ingest, and the timers |
| `lab grafana` | the psql host | adds Grafana with the dashboards (optional) |
| `lab clickhouse` | the clickhouse host | sets up the firewall, ClickHouse, the worker and the clickhouse pipeline |
| `lab psql check`, `lab galexie check`, `lab grafana check` | the same host | changes nothing: reports every step and the host's health; exits 0 only when everything is ok |

**How a run goes.**
1. It asks once for any settings it is missing.
2. It checks every step and lists what it will change.
3. It asks for confirmation once. Formatting a disk and running the bootstrap each get their own question.
4. It applies the steps in order and checks each one again.

Each step prints one line: `✓` ok, `↻` changed, `◷` waiting for something outside this host, `!` restart pending, `✗` failed (the run stops). Run it again whenever you like: on a set-up host, a second run changes nothing.

**Flags:**
- `--yes`: no prompts.
- `--allow-restart`: restart running services whose binary or configuration changed. Without it they show `!`.
- `--only a,b` / `--skip a,b`: run only some steps, or skip some.
- `--config`: use another settings file.

`lab <host> --help` lists the host's steps.

**Where it keeps things:**
- settings: `/etc/stellar/lab-<host>.toml`;
- secrets: `/etc/stellar/secrets/`;
- log of every command it ran: `/var/log/stellar-setup/<host>.log`.

## Deploy

Each step below runs `lab` on one of the hosts, after the servers and buckets exist. Addresses and names are the runbooks' example values (galexie host `10.0.0.2`, psql host `10.0.1.2`, clickhouse host `10.0.2.2`, `api.example.com`); `lab` asks for the real ones on its first run.

### 1. Build

On the workstation, at the repository root. Pick a new version for every build that goes to a server.

```sh
V=$(date +%Y.%m.%d)-1
TEST_DATABASE_URL=postgres://$USER@localhost:5432/stellar_explorer_test?sslmode=disable make check-all VERSION=$V
make dist VERSION=$V          # bin/lab, bin/worker, bin/caddy (linux/amd64) and their sha256
```

Every server gets the same `bin/`.

### 2. Order the servers and the private network

- galexie host: [galexie/README.md §1 Server](galexie/README.md#1-server) and [§2 Disks](galexie/README.md#2-disks) (the Volume).
- psql host: [psql/README.md §2 Server](psql/README.md#2-server) (installimage with RAID1, the vSwitch, optionally the Robot firewall).

Both run Ubuntu 24.04. Check that your SSH key is in `/root/.ssh/authorized_keys`: `lab` switches SSH to keys only.

### 3. Create the buckets

- Archive bucket and its policy: [galexie/README.md §3](galexie/README.md#3-archive-bucket-on-hetzner-object-storage), steps a–c. Use the `-staging` bucket until go-live.
- Backup bucket: [psql/README.md §2.5](psql/README.md#25-backup-bucket).

### 4. psql host: `lab psql`, first run

```sh
scp -r bin root@<psql public IP>:/root/
ssh root@<psql public IP>
/root/bin/lab psql
```

It asks for its settings once. Every step ends ✓ except:
- `api` ◷, which waits for the galexie host's `worker migrate`;
- `caddy` ◷, which waits for DNS.

It writes the database credentials for the galexie host to `/root/galexie-bundle/`.

### 5. DNS

Point `A` and `AAAA` records for the API name at the psql host ([psql/README.md §5](psql/README.md#5-dns)). Caddy gets its certificate once the name resolves; run `/root/bin/lab psql` again then.

### 6. galexie host: `lab galexie`

Copy the bundle across, through the workstation, then `bin/`:

```sh
scp -3 -r root@<psql public IP>:/root/galexie-bundle root@<galexie public IP>:/root/
scp -r bin root@<galexie public IP>:/root/
ssh -t root@<galexie public IP> tmux new -A -s setup     # bootstrap takes about 25 minutes on pubnet
/root/bin/lab galexie
```

It asks for its settings and the archive bucket's keys. Formatting the Volume and running `worker bootstrap` each get their own confirmation. In one run it does the following, and every step ends ✓:
- installs everything;
- runs `worker migrate` and `worker bootstrap` (on pubnet: about 57 million state entries in about 25 minutes, peaking at about 11 GB of memory);
- starts the lake, ingest and Galexie.

Then run the provider check against a scratch bucket: [galexie/README.md §3d](galexie/README.md#3-archive-bucket-on-hetzner-object-storage). Once `lab galexie` has installed the bundle, delete `/root/galexie-bundle` from the galexie host.

### 7. psql host: `lab psql` again

```sh
/root/bin/lab psql            # api now applies grants.sql and api.sql: every step ✓
runuser -u postgres -- vacuumdb --dbname=explorer --schema=explorer --analyze --jobs=4   # after the bootstrap's bulk load
```

### 8. Monitoring and dashboards: `lab psql`, `lab grafana`

1. Set `monitoring_enabled = "yes"` in `/etc/stellar/lab-psql.toml` on the psql host.
2. Run `/root/bin/lab psql`. It asks for the SMTP account and the dead-man's-switch URL ([psql/README.md §8.5](psql/README.md#85-monitoring)).
3. For dashboards, run `/root/bin/lab grafana` ([dashboard/README.md](dashboard/README.md)).

Everything listens on loopback only. Open it through a tunnel:

```sh
ssh -L 9090:127.0.0.1:9090 -L 9093:127.0.0.1:9093 -L 3002:127.0.0.1:3002 root@<psql public IP>
# Prometheus http://localhost:9090, Alertmanager :9093, Grafana :3002
```

For Grafana in a browser without the tunnel, set `grafana_hostname` and run `lab grafana` again: Caddy then serves Grafana at that name, behind Grafana's login ([dashboard/README.md](dashboard/README.md#open-it)).

### 9. Verify: `lab … check`

```sh
/root/bin/lab galexie check                  # galexie host: every step ✓, ingest near the network tip
/root/bin/lab psql check                     # psql host: every step ✓, backups, certificate
curl -s https://api.example.com/status       # from anywhere: the newest ledger and its age
```

Reboot both hosts and run both `check`s again.

**Store the generated secrets** (in `/etc/stellar/secrets/` on each host) in your password manager. Above all, keep `pgbackrest-cipher-pass`: without it no backup can be restored. See [psql/README.md §3.3](psql/README.md#33-settings-and-secrets) and [galexie/README.md §4.2](galexie/README.md#42-settings-and-secrets).

**Go-live:** switch the archive from the staging bucket to production as [galexie/README.md §7.6](galexie/README.md#76-staging--production-cut-over) describes.

### 10. The clickhouse host, `lab clickhouse`

The history, the rankings and the statistics in ClickHouse, with public read-only SQL through the psql host's Caddy ([clickhouse/README.md](clickhouse/README.md)); with it, `lab psql` installs the daily state copy (step `jobs`). Order a server (Ubuntu 24.04) and attach it to the private network at `10.0.2.2` (subnet `10.0.2.0/24`). Then:

```sh
scp -r bin root@<clickhouse public IP>:/root/
ssh root@<clickhouse public IP> /root/bin/lab clickhouse        # asks for its settings and the archive bucket's key
scp -3 -r root@<clickhouse public IP>:/root/clickhouse-bundle root@<psql public IP>:/root/
```

On the psql host, set `clickhouse_hostname` (a DNS name pointing at it, for example `clickhouse.api.example.com`) and `clickhouse_ip` in `/etc/stellar/lab-psql.toml`, then run `/root/bin/lab psql`. `lab clickhouse check` reports the pipeline's cursor and lag; `worker clickhouse-check` compares a range with the ledgers' own counts.

## Operate

| Task | How |
|---|---|
| Change a config file or a setting | edit it in the repository (or the settings file), `make dist`, copy `bin/`, run `lab <host>` again. Running services restart only with `--allow-restart`. |
| Deploy a new worker | the deploy scripts: [galexie/README.md §7.2](galexie/README.md#72-deploys) |
| Upgrade PostgreSQL, PostgREST, Caddy, Prometheus or Grafana | [psql/README.md §8.3](psql/README.md#83-upgrades-and-reboots), [dashboard/README.md](dashboard/README.md#change-it) |
| Change the API | [psql/README.md §7.5](psql/README.md#75-changing-the-api); then `make types` and commit `generated/api.ts` |
| Backups and the restore drill | [psql/README.md §8.4](psql/README.md#84-backups-and-the-restore-drill) |
| Something is wrong | `lab <host> check`, then the runbooks: [galexie/README.md §7.4](galexie/README.md#74-runbooks), [psql/README.md §8.6](psql/README.md#86-runbooks) |

## Design decisions

The decisions that shape more than one folder. Host-specific ones are in the host runbooks.

- **Three servers.** The galexie host ingests and is reachable only over SSH. The psql host runs PostgreSQL, PostgREST and Caddy together, plus backups, monitoring and the daily state copy. The clickhouse host holds the history, reading the AWS public lake, so it can't hold back ingest.
  - **Why together:** the API schema belongs to the database, and PostgREST is a stateless translator that reaches PostgreSQL over the Unix socket, with no password, certificate or network rule. That also leaves one server fewer to run.
  - **The cost:** the database host faces the internet. How it's contained, and how to split it later, is in [psql/README.md §1](psql/README.md#1-what-runs-here).
  - **Monitoring** (Prometheus, Alertmanager, optionally Grafana) runs on the psql host on loopback and scrapes both hosts.
- **Who owns which SQL.**
  - The worker's migrations own the `explorer` schema.
  - [`psql/`](psql/) owns the database, the roles and the public `api` schema, and `lab psql` applies them on the host itself.
  - The only database owner credential that leaves the psql host is the one `worker migrate` uses.
- **One setup CLI, and it is only a setup script.**
  - `lab` brings a new or existing server to the state its runbook describes and reports whether it is there and healthy.
  - It doesn't deploy new worker builds (the [deploy scripts](galexie/README.md#72-deploys) do) or coordinate hosts.
  - The engine exists once, and each host adds its settings and steps. The host folders hold their files and docs but no code.
  - It needs only the Charm libraries (fang, huh, lipgloss), cobra and a TOML parser, and calls `psql`, `systemctl` and `apt` instead of using drivers.
- **The repository's files are the templates.** Config files are written with the runbooks' example values (`10.0.1.2`, `api.example.com`, …), so they read correctly as they are. `lab` replaces each example value with the configured one when it installs a file. Tests check that every file renders unchanged with the examples and that every example is used.
- **Settings and secrets.**
  - Settings live in `/etc/stellar/lab-<host>.toml`.
  - Secrets live in `/etc/stellar/secrets/` (root only). They are never written into the settings file, shown on screen or logged.
  - Generated secrets (passwords, keys, the backup passphrase) are created once and never rotated silently.
- **Provider-neutral code.** `lab` assumes Ubuntu 24.04, systemd, a private network and S3-compatible buckets. Hetzner specifics live only in the runbooks.
- **Three stores, each for what it measured best at:**
  - **PostgreSQL: current state**, looked up by key (an account, its trustlines and offers, a contract), 10–100× faster than ClickHouse. It keeps one row per ledger and no other history, so it grows with the state, not with time.
  - **ClickHouse: history, rankings and statistics**: lists by account, contract, asset or hash, at a tenth of PostgreSQL's size; rankings over the whole state, 7–120× faster, on a daily copy of it; daily aggregates. Nothing expires.
  - **The archive: one transaction or ledger in full**, raw XDR, signatures, return values, per-operation results and diagnostic events, read on demand (~85 ms uncached) after `tx_by_hash` finds the ledger.
- **Trade-offs to keep in mind.**
  - **The public database host:** one public listener, default-drop firewall, a read-only API role with a 2 s timeout. If needed, move Caddy, or Caddy and PostgREST, to their own server; that's a configuration change.
  - **Shared resources on that host:** PostgreSQL's memory is sized with headroom for PostgREST, Caddy and monitoring.
  - **Captive core rebuilds its database on a restart,** which takes tens of minutes: `lab` never restarts Galexie or the lake on its own.


## Develop

```sh
make check            # lab and Caddy: gofmt, vet (macOS and linux), tests, linux builds
make check-all        # and the worker's tests (TEST_DATABASE_URL, local PostgreSQL 18)
make build            # lab for this machine (--help only; it runs on Ubuntu 24.04)
make dist VERSION=…   # bin/ for the servers
make types DATABASE_URL=postgres://localhost:5432/explorer        # regenerate generated/api.ts
make types-check DATABASE_URL=postgres://localhost:5432/explorer  # fail if it is stale
make clean
```

More in each folder: [galexie/README.md §8](galexie/README.md#8-development) (the worker, local end to end with `dev-feed`, `lab`), [psql/README.md §9](psql/README.md#9-development) (`lab psql`, the SQL against a local PostgreSQL), [generated/README.md](generated/README.md).
