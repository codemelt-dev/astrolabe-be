# Astrolabe Data Layer

The backend and indexer behind [Astrolabe](https://astrostellar.xyz), a Stellar block explorer and analytics platform. It is built by [Codemelt](https://codemelt.com) and funded by the Stellar Community Fund (SCF #45). Product docs are in the [Astrolabe GitBook](https://codemelt.gitbook.io/astrolabe-docs).

The data layer runs its own ingestion of the Stellar network. It does not depend on Horizon or on public RPC for any data it serves:

- **Every ledger since go-live is ingested** by a self-hosted Galexie (SDF's ledger exporter) with an embedded captive stellar-core, and kept in full in an S3-compatible archive bucket (SEP-54 format).
- **The current state** (accounts, trustlines, offers, claimable balances, liquidity pools, contracts, contract data and code) goes into **PostgreSQL** and is served as a public, read-only **REST API**.
- **The history** (transactions, operations, events, token transfers, contract calls) goes into **ClickHouse**, which also computes asset rankings, top holders and daily statistics, and is served as public, read-only **SQL**.

## Architecture

```text
                          Stellar network
                                │ captive core (peers, history archives)
  galexie host (private) ───────┼──────────────────────────────────────────────────────┐
  │ Galexie → lake (local S3, durable) → worker ingest ──┬──▶ PostgreSQL (psql host)   │
  │                                     └─ archiver ──▶ archive bucket (every ledger)  │
  └─────────────────────────────────────────────────────┼────────────────────────────────┘
                                                        │ private network, TLS
  psql host (public 443) ───────────────────────────────┼────────────────────────────────┐
  │ PostgreSQL 18 ◀── PostgREST ◀── Caddy (HTTPS) ◀── the internet: REST API            │
  │                                 Caddy ──────────▶ ClickHouse: public read-only SQL  │
  │ pgBackRest → backup bucket · Prometheus, Alertmanager, Grafana (loopback)           │
  │ worker state-copy (daily) ──────────────────────────┐                               │
  └─────────────────────────────────────────────────────┼────────────────────────────────┘
                                                        │ private network, TLS
  clickhouse host (private) ────────────────────────────┼────────────────────────────────┐
  │ AWS public lake (SEP-54) → worker clickhouse-ingest → ClickHouse 26.8               │
  └──────────────────────────────────────────────────────────────────────────────────────┘
```

| Host | Role | Details |
|---|---|---|
| galexie host | ingestion: Galexie, the local lake, the PostgreSQL pipeline, archiving to the archive bucket | [`galexie/README.md`](galexie/README.md) |
| psql host | current state and the public endpoints: PostgreSQL, PostgREST, Caddy, backups, monitoring, the daily state copy | [`psql/README.md`](psql/README.md) |
| clickhouse host | history, rankings and statistics: ClickHouse and its pipeline | [`clickhouse/README.md`](clickhouse/README.md) |

All three hosts are set up by one CLI, `lab`, and run on Ubuntu 24.04.

### Where each kind of data lives

| Store | Holds | Why there |
|---|---|---|
| PostgreSQL | current state, looked up by key: an account, its trustlines and offers, a contract and its data; one row per ledger | 10 to 100× faster than ClickHouse for key lookups in the team's measurements. It grows with the network's state, not with time. |
| ClickHouse | history, listed by account, contract, asset or hash; rankings over the whole state (on a daily copy of it); daily aggregates. Nothing expires. | history takes about a tenth of PostgreSQL's space, and rankings run 7 to 120× faster |
| Archive bucket | every ledger in full (`LedgerCloseMeta`): raw XDR, signatures, return values, per-operation results and diagnostic events | the permanent raw record, readable with standard SEP-54 tooling |

### How a ledger flows through the system

1. **Galexie** on the galexie host receives the ledger from captive core and writes it as one compressed SEP-54 file to the **lake**, a local S3 endpoint that acknowledges the write only once it is safely on disk.
2. **`worker ingest`** is notified immediately, decodes the ledger (`processLedger`, shared by both pipelines), validates the hash chain, and commits the ledger row and every state change to **PostgreSQL** in one transaction together with its cursor.
3. The **archiver** copies the same file 1:1 to the **archive bucket**; the pruner removes it from the lake once it is archived and older than 72 hours.
4. On the clickhouse host, **`worker clickhouse-ingest`** reads the same ledger from the AWS public lake (the SDF-maintained public copy of every Stellar ledger), runs the same decoder in history mode, and writes transactions, operations, events, transfers and contract calls to **ClickHouse**.
5. Once a day, **`worker state-copy`** on the psql host copies the current state into ClickHouse, which recomputes the asset list, supplies and top holders and stores a dated snapshot.
6. **Caddy** on the psql host serves the REST API (PostgREST over PostgreSQL) and the read-only SQL endpoint (ClickHouse) to the internet, with rate limits and read-only access.

## Repository layout

| Folder | Contents |
|---|---|
| [`worker/`](worker/) | the Go worker: the lake (embedded S3 server), ledger decoding and both pipelines, archiver, pruner, bootstrap, state copy, migrations for PostgreSQL and ClickHouse ([`worker/README.md`](worker/README.md)) |
| [`cli/`](cli/) | `lab`, the setup CLI for all three hosts ([`cli/README.md`](cli/README.md)) |
| [`galexie/`](galexie/) | the galexie host: documentation and every file installed there (`deploy/`) |
| [`psql/`](psql/) | the psql host: documentation, the database roles and grants, the public API (`api.sql`), installed files, the Caddy build |
| [`clickhouse/`](clickhouse/) | the clickhouse host: documentation and installed files |
| [`dashboard/`](dashboard/) | Grafana configuration and dashboards |
| [`generated/`](generated/) | TypeScript types for the REST API |

## Building

Requirements: Go 1.26; PostgreSQL 18 for the worker's database tests; Node 20+ only to regenerate the TypeScript types.

```sh
make check            # lab and Caddy: gofmt, vet (macOS and Linux), tests, Linux builds
TEST_DATABASE_URL=postgres://$USER@localhost:5432/stellar_explorer_test?sslmode=disable make check-all   # plus the worker's tests
make dist VERSION=…   # bin/lab, bin/worker and bin/caddy for linux/amd64, with their sha256
make types DATABASE_URL=postgres://localhost:5432/explorer        # regenerate generated/api.ts
make types-check DATABASE_URL=postgres://localhost:5432/explorer  # fail if it is stale
```

Every server receives the same `bin/` directory.

## Deploying

`lab` brings a server to the documented state and reports whether it is there and healthy. It needs root on Ubuntu 24.04 amd64 and refuses to run anywhere else.

| Command | Run on | Does |
|---|---|---|
| `lab psql` | psql host | PostgreSQL, roles and API schema, PostgREST, Caddy, backups, optional monitoring |
| `lab galexie` | galexie host | firewall, disks, Docker and Galexie, the worker's lake and ingest, timers |
| `lab clickhouse` | clickhouse host | firewall, ClickHouse, the worker and its pipeline |
| `lab grafana` | psql host | Grafana and the dashboards (optional) |
| `lab <host> check` | the same host | changes nothing: reports every step and the host's health; exits 0 only when all is ok |

A run asks once for any missing settings, checks every step and lists what it will change, asks for confirmation once (formatting a disk and the initial bootstrap each get their own question), then applies the steps in order and checks each one again. Each step prints `✓` ok, `↻` changed, `◷` waiting for something outside the host, `!` restart pending, or `✗` failed. Running it again on a configured host changes nothing. Flags: `--yes` (no prompts), `--allow-restart` (restart services whose binary or configuration changed), `--only` / `--skip` (subsets of steps), `--config`.

Settings live in `/etc/stellar/lab-<host>.toml`, secrets in `/etc/stellar/secrets/` (root only, never shown or logged), and a log of every command in `/var/log/stellar-setup/<host>.log`.

**Prerequisites:** three Ubuntu 24.04 servers on a shared private network (examples: galexie host `10.0.0.2`, psql host `10.0.1.2`, clickhouse host `10.0.2.2`), an S3-compatible bucket for the ledger archive and one for database backups, and DNS names for the public endpoints. Sizing and storage requirements are in each host's README.

**Order:**

1. **psql host:** `lab psql`. It completes every step except the API (which waits for the worker's migrations) and Caddy (which waits for DNS), and writes a database bundle (CA certificate and role passwords) for the galexie host.
2. **DNS:** point the API name at the psql host.
3. **galexie host:** copy the bundle over, then `lab galexie`. It installs everything, runs the migrations and the initial bootstrap (on pubnet about 25 minutes and 11 GB of memory), and starts Galexie, the lake and ingest.
4. **psql host:** `lab psql` again; the API is applied.
5. **clickhouse host:** `lab clickhouse`, then copy its bundle to the psql host and run `lab psql` once more to enable the public SQL endpoint, the state copy and ClickHouse monitoring.
6. **Monitoring (optional):** `monitoring_enabled = "yes"` and `lab psql`, then `lab grafana`.
7. **Verify:** `lab <host> check` on every host, a reboot, and `check` again.

Generated secrets (database passwords, the backup encryption passphrase, the CA keys) must be copied from `/etc/stellar/secrets/` into a password manager after the first run.

## Verifying the data

A reviewer can check the system from outside through the public endpoints:

```sh
curl -s https://api.example.com/status                               # newest ingested ledger and its age
curl -s "https://api.example.com/ledgers?order=sequence.desc&limit=5" # the newest ledgers
curl -s "https://clickhouse.example.com/?query=SELECT+min(sequence),max(sequence),count()+FROM+stellar.ledgers+FORMAT+JSON"
```

On the hosts, `lab <host> check`, the gap queries in [`galexie/README.md`](galexie/README.md#verifying-the-pipeline) and [`clickhouse/README.md`](clickhouse/README.md#verifying-the-data), and `worker clickhouse-check` (which compares each ledger's transaction and operation counts with its header) cover continuity, freshness and completeness.

## Design principles

- **One setup CLI, and only a setup tool.** `lab` brings a new or existing server to the state its README describes, and reports whether it is there and healthy. It does not deploy new worker builds (deploy scripts do) or coordinate hosts. It has one step engine; each host adds its settings and steps. It calls `psql`, `systemctl` and `apt` rather than embedding drivers.
- **The repository's files are the templates.** Every installed file is committed with example values (`10.0.1.2`, `api.example.com`, …), so it reads correctly as it is. When `lab` installs a file it replaces each example value with the configured one. Tests check that every file renders unchanged with the examples and that every example is used.
- **Provider-neutral code.** `lab` assumes Ubuntu 24.04, systemd, a private network and S3-compatible buckets; provider specifics are confined to documentation.
- **Who owns which SQL.** The worker's migrations own the `explorer` schema (PostgreSQL) and the `stellar` database (ClickHouse). [`psql/`](psql/) owns the database, roles and public `api` schema. The only database-owner credential that leaves the psql host is the one `worker migrate` uses.
- **Secrets are generated once and never rotated silently.**
- **Captive core is expensive to restart** (it rebuilds its database, tens of minutes), so `lab` never restarts Galexie or the lake on its own.

## Operating

| Task | Where |
|---|---|
| Change a configuration file or setting | edit it in the repository (or the settings file), `make dist`, copy `bin/`, run `lab <host>`; running services restart only with `--allow-restart` |
| Deploy a new worker | [`galexie/README.md`](galexie/README.md#deploys), [`clickhouse/README.md`](clickhouse/README.md#deploying-a-new-worker) |
| Change the REST API | [`psql/README.md`](psql/README.md#changing-the-api), then `make types` |
| Backups and restore drill | [`psql/README.md`](psql/README.md#backups) |
| Alerts and failure handling | [`galexie/README.md`](galexie/README.md#failure-handling), [`psql/README.md`](psql/README.md#monitoring), [`clickhouse/README.md`](clickhouse/README.md#monitoring-and-alerts) |
