# Astrolabe Data Layer

The backend and indexer behind [Astrolabe](https://astrostellar.xyz), a Stellar block explorer and analytics platform. It is built by [Codemelt](https://codemelt.com) and funded by the Stellar Community Fund (SCF #45). Product docs are in the [Astrolabe GitBook](https://codemelt.gitbook.io/astrolabe-docs).

It ingests every ledger from go-live on. The current state goes into PostgreSQL and is served as a public, read-only REST API. The history goes into ClickHouse, which also computes the asset rankings and daily statistics, and is served as public, read-only SQL. Every ledger is also kept whole in the archive bucket.

It runs on three servers, all set up by one CLI, `lab`.

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
| [`clickhouse/`](clickhouse/) | the history host (ClickHouse, fed from the AWS public lake by the worker's clickhouse pipeline) and the state copy for rankings: runbook ([`README.md`](clickhouse/README.md)) and its files |
| [`worker/`](worker/) | the Go worker: S3 gateway (lake), decoder and both pipelines, archiver, pruner, the state copy |
| [`cli/`](cli/) | `lab`, the setup CLI: `lab galexie`, `lab psql`, `lab grafana`, `lab clickhouse` |
| [`dashboard/`](dashboard/) | Grafana's configuration and dashboards ([`README.md`](dashboard/README.md)) |
| [`generated/`](generated/) | the API's TypeScript types ([`README.md`](generated/README.md)) |

## The `lab` CLI

Every server is set up with `lab` ([`cli/`](cli/)). There are no manual install steps; the runbooks explain what it does on each host and why.

`make dist` builds `bin/lab` for linux/amd64, next to `bin/worker` and `bin/caddy`. To get it onto a server, copy `bin/` to `/root/bin/` and run `lab` there as root. It needs Ubuntu 24.04 and refuses to run anywhere else.

| Command | Run on | Does |
|---|---|---|
| `lab psql` | the psql host | sets up PostgreSQL, the roles and the API schema, PostgREST, Caddy, backups and (optionally) monitoring |
| `lab galexie` | the galexie host | sets up the firewall, the disks, Docker and Galexie, the worker's lake and ingest, and the timers |
| `lab grafana` | the psql host | adds Grafana with the dashboards (optional) |
| `lab clickhouse` | the clickhouse host | sets up the firewall, ClickHouse, the worker and the clickhouse pipeline |
| `lab psql check`, `lab galexie check`, `lab grafana check`, `lab clickhouse check` | the same host | changes nothing: reports every step and the host's health; exits 0 only when everything is ok |

A run goes like this:

1. It asks once for any settings it's missing.
2. It checks every step and lists what it will change.
3. It asks for confirmation once. Formatting a disk and running the bootstrap each get their own question.
4. It applies the steps in order and checks each one again.

Each step prints one line: `✓` ok, `↻` changed, `◷` waiting for something outside this host, `!` restart pending, `✗` failed (the run stops). You can run it again whenever you like. On a host that's already set up, a second run changes nothing.

`lab <host> --help` lists a host's steps. The flags are:

- `--yes` (`-y`): never prompt. Every question is approved, and a missing setting is an error.
- `--allow-restart`: restart running services whose binary or configuration changed. Without it, they show `!`.
- `--only a,b` / `--skip a,b`: run only some steps, or skip some.
- `--config`: use another settings file.

`lab` keeps its settings in `/etc/stellar/lab-<host>.toml`, its secrets in `/etc/stellar/secrets/`, and a log of every command it ran in `/var/log/stellar-setup/<host>.log`.

## Deploy

Build once, create the servers and buckets, then run `lab` on each host in the order below. Addresses and names are the runbooks' example values (galexie host `10.0.0.2`, psql host `10.0.1.2`, clickhouse host `10.0.2.2`, `api.example.com`); `lab` asks for the real ones on its first run.

### 1. Build

Run this on the workstation, at the repository root. Pick a new version for every build that goes to a server.

```sh
V=$(date +%Y.%m.%d)-1
TEST_DATABASE_URL=postgres://$USER@localhost:5432/stellar_explorer_test?sslmode=disable make check-all VERSION=$V
make dist VERSION=$V          # bin/lab, bin/worker, bin/caddy (linux/amd64) and their sha256
```

Every server gets the same `bin/`.

### 2. Order the servers and the private network

- galexie host: [galexie/README.md §1 Server](galexie/README.md#1-server) and [§2 Disks](galexie/README.md#2-disks) (the Volume).
- psql host: [psql/README.md §2 Server](psql/README.md#2-server) (installimage with RAID1, the vSwitch, optionally the Robot firewall).

Both run Ubuntu 24.04. Check that your SSH key is in `/root/.ssh/authorized_keys`, because `lab` switches SSH to keys only.

### 3. Create the buckets

- Archive bucket and its policy: [galexie/README.md §3](galexie/README.md#3-archive-bucket-on-hetzner-object-storage), steps a to c. Use the `-staging` bucket until go-live.
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

Point `A` and `AAAA` records for the API name at the psql host ([psql/README.md §5](psql/README.md#5-dns)). Caddy gets its certificate once the name resolves. Run `/root/bin/lab psql` again at that point.

### 6. galexie host: `lab galexie`

Copy the bundle across through the workstation, then copy `bin/`:

```sh
scp -3 -r root@<psql public IP>:/root/galexie-bundle root@<galexie public IP>:/root/
scp -r bin root@<galexie public IP>:/root/
ssh -t root@<galexie public IP> tmux new -A -s setup     # bootstrap takes about 25 minutes on pubnet
/root/bin/lab galexie
```

It asks for its settings and the archive bucket's keys. Formatting the Volume and running `worker bootstrap` each get their own confirmation. In one run it installs everything, runs `worker migrate` and `worker bootstrap`, and starts the lake, ingest and Galexie. Every step ends ✓. On pubnet the bootstrap loads about 57 million state entries in about 25 minutes and peaks at about 11 GB of memory.

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

Everything listens on loopback only, so open it through a tunnel:

```sh
ssh -L 9090:127.0.0.1:9090 -L 9093:127.0.0.1:9093 -L 3002:127.0.0.1:3002 root@<psql public IP>
# Prometheus http://localhost:9090, Alertmanager :9093, Grafana :3002
```

To reach Grafana in a browser without the tunnel, set `grafana_hostname` and run `lab grafana` again. Caddy then serves Grafana at that name, behind Grafana's login ([dashboard/README.md](dashboard/README.md#open-it)).

### 9. Verify: `lab … check`

```sh
/root/bin/lab galexie check                  # galexie host: every step ✓, ingest near the network tip
/root/bin/lab psql check                     # psql host: every step ✓, backups, certificate
curl -s https://api.example.com/status       # from anywhere: the newest ledger and its age
```

Reboot both hosts and run both `check`s again.

**Store the generated secrets** from `/etc/stellar/secrets/` on each host in your password manager. Above all, keep `pgbackrest-cipher-pass`: without it, no backup can be restored. See [psql/README.md §3.3](psql/README.md#33-settings-and-secrets) and [galexie/README.md §4.2](galexie/README.md#42-settings-and-secrets).

At go-live, switch the archive from the staging bucket to production as [galexie/README.md §7.6](galexie/README.md#76-staging--production-cut-over) describes.

### 10. clickhouse host: `lab clickhouse`

This host keeps the history, rankings and statistics in ClickHouse and serves them as public, read-only SQL through the psql host's Caddy ([clickhouse/README.md](clickhouse/README.md)). Once it's in place, `lab psql` also installs the daily state copy (step `jobs`).

Order a server (Ubuntu 24.04) and attach it to the private network at `10.0.2.2` (subnet `10.0.2.0/24`). Then:

```sh
scp -r bin root@<clickhouse public IP>:/root/
ssh root@<clickhouse public IP> /root/bin/lab clickhouse        # asks for its settings and the archive bucket's key
scp -3 -r root@<clickhouse public IP>:/root/clickhouse-bundle root@<psql public IP>:/root/
```

On the psql host, set `clickhouse_hostname` (a DNS name pointing at it, for example `clickhouse.api.example.com`) and `clickhouse_ip` in `/etc/stellar/lab-psql.toml`, then run `/root/bin/lab psql`. `lab clickhouse check` reports the pipeline's cursor and lag, and `worker clickhouse-check` compares a range with the ledgers' own counts.

## Operate

| Task | How |
|---|---|
| Change a config file or a setting | edit it in the repository (or the settings file), `make dist`, copy `bin/`, run `lab <host>` again. Running services restart only with `--allow-restart`. |
| Deploy a new worker | the deploy scripts: [galexie/README.md §7.2](galexie/README.md#72-deploys) |
| Upgrade PostgreSQL, PostgREST, Caddy, Prometheus or Grafana | [psql/README.md §8.3](psql/README.md#83-upgrades-and-reboots), [dashboard/README.md](dashboard/README.md#change-it) |
| Change the API | [psql/README.md §7.5](psql/README.md#75-changing-the-api); then `make types` and commit `generated/api.ts` |
| Backups and the restore drill | [psql/README.md §8.4](psql/README.md#84-backups-and-the-restore-drill) |
| Something is wrong | `lab <host> check`, then the runbooks: [galexie/README.md §7.4](galexie/README.md#74-runbooks), [psql/README.md §8.6](psql/README.md#86-runbooks) |

## Design

These decisions shape more than one folder. Host-specific ones are in the host runbooks.

### Three servers

The galexie host ingests and is reachable only over SSH. The psql host runs PostgreSQL, PostgREST and Caddy together, plus backups, monitoring and the daily state copy. The clickhouse host holds the history and reads it from the AWS public lake, so it can't hold back ingest.

PostgreSQL and PostgREST share a host because the API schema belongs to the database, and PostgREST is a stateless translator that reaches PostgreSQL over the Unix socket. It also leaves one server fewer to run.

Monitoring (Prometheus, Alertmanager and optionally Grafana) runs on the psql host on loopback and scrapes both hosts.

### Who owns which SQL

The worker's migrations own the `explorer` schema. [`psql/`](psql/) owns the database, the roles and the public `api` schema, and `lab psql` applies them on the host itself. The only database owner credential that leaves the psql host is the one `worker migrate` uses.

### One setup CLI, and only a setup script

`lab` brings a new or existing server to the state its runbook describes, and reports whether it's there and healthy. It doesn't deploy new worker builds (the [deploy scripts](galexie/README.md#72-deploys) do that) or coordinate hosts.

There is one engine, and each host adds its own settings and steps. The host folders hold their files and docs but no code. The only dependencies are the Charm libraries (fang, huh, lipgloss), cobra and a TOML parser; for the rest it calls `psql`, `systemctl` and `apt` instead of using drivers.

### The repository's files are the templates

Config files are written with the runbooks' example values (`10.0.1.2`, `api.example.com`, …), so they read correctly as they are. When `lab` installs a file, it replaces each example value with the configured one. Tests check that every file renders unchanged with the examples and that every example is used.

### Settings and secrets

Settings live in `/etc/stellar/lab-<host>.toml`. Secrets live in `/etc/stellar/secrets/`, readable by root only, and are never written into the settings file, shown on screen or logged. Generated secrets (passwords, keys, the backup passphrase) are created once and never rotated silently.

### Provider-neutral code

`lab` assumes Ubuntu 24.04, systemd, a private network and S3-compatible buckets. Anything specific to Hetzner lives only in the runbooks.

### Three stores

Each kind of data goes to the store that measured best for it:

| Store | Holds | Why there |
|---|---|---|
| PostgreSQL | the current state, looked up by key: an account, its trustlines and offers, a contract | 10 to 100× faster than ClickHouse for these lookups. It keeps one row per ledger and no other history, so it grows with the state, not with time. |
| ClickHouse | the history, listed by account, contract, asset or hash; rankings over the whole state, on a daily copy of it; daily aggregates. Nothing expires. | the history takes a tenth of PostgreSQL's space, and rankings run 7 to 120× faster |
| Archive bucket | one transaction or ledger in full: raw XDR, signatures, return values, per-operation results and diagnostic events | read on demand (about 85 ms uncached) after `tx_by_hash` finds the ledger |

### Trade-offs

Captive core rebuilds its database on a restart, which takes tens of minutes, so `lab` never restarts Galexie or the lake on its own.

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
