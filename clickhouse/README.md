# clickhouse host

The clickhouse host stores the explorer's **history** in **ClickHouse 26.8 (LTS)**: every transaction, operation, event, token transfer and contract call since go-live, kept indefinitely. It also holds a daily copy of the current state, from which it computes the **asset list, supplies and top holders** (with daily snapshots), and **daily network statistics**.

The history is written by the worker's ClickHouse pipeline (`worker clickhouse-ingest`), which decodes every ledger with the same code as the PostgreSQL pipeline. The psql host's Caddy publishes ClickHouse to the internet as **read-only SQL**. `lab clickhouse` sets the host up; `lab clickhouse check` reports its state and health.

## Contents

1. [Role in the system](#role-in-the-system)
2. [Design decisions](#design-decisions)
3. [Schema](#schema)
4. [The ClickHouse pipeline](#the-clickhouse-pipeline)
5. [State copy, assets and top holders](#state-copy-assets-and-top-holders)
6. [Public read-only SQL](#public-read-only-sql)
7. [Server configuration](#server-configuration)
8. [Setup with `lab clickhouse`](#setup-with-lab-clickhouse)
9. [Integration with the psql host](#integration-with-the-psql-host)
10. [Verifying the data](#verifying-the-data)
11. [Operations](#operations)
12. [Limitations](#limitations)
13. [Development](#development)
14. [Files](#files)

---

## Role in the system

```text
  galexie host                      archive bucket                    AWS public lake (SEP-54)
  Galexie → lake → archiver ──────▶ every ledger since go-live        every ledger since genesis
             └▶ ingest → PostgreSQL       │                                   │
                (psql host)               │ go-live ledger (init only)        │ every ledger
                                          ▼                                   ▼
  clickhouse host ── worker clickhouse-ingest ──────────────────────▶ ClickHouse (database stellar)
        ▲  private network, TLS only, from the psql host:
        │   Prometheus (metrics) · Grafana (stellar_reader) · worker state-copy (stellar_jobs)
        └── Caddy on the psql host: public read-only SQL (stellar_public, stellar_api)
```

| Component | Purpose |
|---|---|
| ClickHouse server | the `stellar` database: history tables, the state copy, rankings, snapshots, statistics |
| `stellar-clickhouse-ingest.service` | runs `worker clickhouse-ingest`: reads ledgers, decodes them, writes history, advances its cursor |
| node_exporter | host metrics for Prometheus |

## Design decisions

- **A dedicated server.** ClickHouse merges and analytical queries use every core and the page cache. Sharing a host with PostgreSQL would make them compete with the database the REST API depends on.
- **History in ClickHouse, current state in PostgreSQL.** Current state means per-key upserts and point lookups, which PostgreSQL serves 10 to 100 times faster in the team's measurements. History and whole-state rankings are the opposite: ClickHouse stores the history in about a tenth of PostgreSQL's space and runs the rankings 7 to 120 times faster.
- **The pipeline reads the AWS public lake.** The AWS Open Data copy of the Stellar ledgers (SEP-54 format, maintained by SDF) holds every ledger since genesis and trails the network by about one ledger. Reading it, rather than the galexie host's lake or the project's own archive bucket, means:
  - the galexie host needs no extra process, port or memory;
  - a slow or stopped ClickHouse can never hold back the PostgreSQL pipeline or the lake's pruning;
  - catch-up is not slowed by the archive bucket's provider, whose object storage was observed to stall on single objects for minutes, which blocks strictly in-order reading;
  - rebuilds and a future backfill run where the data lands.

  The archive bucket is only used once, to find the go-live ledger where the history starts. Data integrity does not depend on the source: every ledger is fully decoded, and the zstd frame checksums, the header hash and the hash chain to the previous ledger are verified.
- **One decoder.** The ClickHouse pipeline runs the worker's `processLedger` in history mode (`explorer.HistoryProcessor`); the PostgreSQL pipeline runs the same function in state mode. Both see a ledger identically.
- **Close time on every row**, so time series need no join; all events are kept, including fee and refund events, with every topic.
- **Nothing expires.** History grows by about 2 GiB a day.
- **No backups.** Everything except the daily snapshots can be rebuilt from the AWS public lake (see [Rebuilding](#rebuilding)).

## Schema

The worker's migrations own the `stellar` database ([`worker/clickhouse/*.sql`](../worker/clickhouse/)), just as they own the `explorer` schema in PostgreSQL. Migrations are checksummed and forward-only, applied by `worker clickhouse-migrate` as `stellar_owner` and recorded in `stellar.schema_migrations`. This folder owns the server, the users and the empty database.

| Table | `ORDER BY` | Written by | Contents |
|---|---|---|---|
| `ledgers` | `sequence` | pipeline | one row per ledger |
| `transactions` | `(ledger_sequence, tx_index)` | pipeline | bloom filters on `hash` and `source_account` |
| `operations` | `(ledger_sequence, tx_index, op_index)` | pipeline | `type_name`, `details` (JSON), the operation's own `result_code`, a Soroban call's `return_value` (JSON) |
| `events` | `(contract_id, ledger_sequence, tx_index, op_index, event_index)` | pipeline | `stage` (fee, refund or operation), all topics |
| `token_transfers` | `(contract_id, ledger_sequence, tx_index, op_index, position)` | pipeline | kind, asset, from, to, `amount Int128` |
| `transfers_by_account` | `(account, ledger_sequence, tx_index, op_index, position)` | materialised view | one row per side; negative when the account sends |
| `tx_by_hash` | `prefix` (first 8 bytes of the hash) | pipeline | hash → ledger and index, including fee-bump inner transactions |
| `account_activity` | `(account, ledger_sequence, tx_index)` | pipeline | every transaction an account or contract took part in, with a role bitmask (source, fee payer, operation source, named in an operation, transfer side, owner of a changed entry) |
| `contract_calls` | `(contract_id, ledger_sequence, tx_index, op_index)` | pipeline | each contract invocation: function, caller, outcome, return value |
| `contract_creations` | `contract_id` | pipeline | creator and creation ledger, including deployments by other contracts |
| `daily_stats` | `day` | refreshable view, every 5 minutes | per-day network statistics (read with `FINAL`) |
| `state_accounts`, `state_trustlines`, `state_sac_balances`, `state_asset_contracts`, `state_claimable_balances`, `state_liquidity_pools` | | `worker state-copy`, daily | PostgreSQL's current state as of one ledger |
| `assets`, `top_holders` | `asset`; `(asset, rank)` | `worker state-copy` | the asset list with supplies; top 500 holders per asset, top 1,000 for XLM |
| `asset_snapshots`, `holder_snapshots` | `(asset, date)`; `(asset, date, rank)` | `worker state-copy --snapshot` | `assets` and `top_holders` by date, kept indefinitely |
| `state_copy_runs` | `started_at` | `worker state-copy` | one row per run |
| `ingest_state` | `pipeline` | pipeline | the cursor |

Conventions for the history tables:

- every row carries `ledger_sequence` and `closed_at`;
- monthly partitions by `toYYYYMM(closed_at)` (`tx_by_hash` by a million ledgers; `contract_creations` unpartitioned);
- where `ledger_sequence` is not the first sort key, a `minmax` index on it keeps ledger-range scans cheap;
- hashes are `FixedString(32)`, account and contract IDs are strkey `String`s, JSON is `String` (read with `JSONExtract*`; an ScVal is an object keyed by its type, e.g. `{"symbol":"transfer"}`), amounts are `Int128` in base units;
- no `Nullable` columns: empty strings and zeros mean "absent".

Notes for querying:

- token transfer amounts can be negative: a fee refund is a negative `fee`;
- `tx_by_hash` is keyed by an 8-byte prefix, so the full hash must be checked against `transactions`;
- aggregates are refreshable views that recompute the most recent days into a `ReplacingMergeTree` keyed by day, which keeps them correct after the recovery step described below.

## The ClickHouse pipeline

`worker clickhouse-ingest` uses the same sequencer as the PostgreSQL pipeline on the galexie host:

| | PostgreSQL pipeline (galexie host) | ClickHouse pipeline (this host) |
|---|---|---|
| processor | `explorer.Processor`: ledger row and current state | `explorer.HistoryProcessor`: ledger row and all history rows |
| sink | PostgreSQL | ClickHouse ([`worker/clickhouse/`](../worker/clickhouse/)) |
| ledger sources | lake → archive bucket → AWS public lake | AWS public lake |
| network tip | the lake's tip | the AWS public lake's newest ledger, polled every 5 s |
| unit | `stellar-ingest.service` | `stellar-clickhouse-ingest.service` |

Batches hold at most 500 ledgers and 512 MiB; at the network tip a batch is one ledger every ~5 s. Transient errors (connection loss, timeouts, a restarting server) are retried; decoding or type errors halt the pipeline for an operator. If the AWS public lake cannot be read for two minutes, the pipeline raises `lake_unavailable` and keeps retrying.

**A batch:**

1. **Guard:** the first ledger must follow the cursor and link to the cursor's hash, otherwise the pipeline halts.
2. **Insert:** one columnar `INSERT` per table for the whole batch, in a fixed order.
3. **Commit:** a row `(pipeline, sequence, hash, time)` is written to `ingest_state`; the current cursor is the row with the highest sequence.

**Exactly-once without transactions.** ClickHouse has no multi-table transactions, so a crash between steps 2 and 3 can leave rows above the cursor. Rows above the cursor are therefore never trusted: on start-up and after a failed commit, the sink deletes them from every table (`DELETE FROM stellar.<table> WHERE ledger_sequence > <cursor>`) and resumes from the cursor. This is safe because this one service is the only writer.

**Starting point.** `worker clickhouse-init` seeds the cursor once, by default just before the archive bucket's oldest ledger (go-live). `--at <sequence>` starts from any other ledger.

## State copy, assets and top holders

Rankings over the whole state (largest holders, asset supplies) are computed in ClickHouse on a daily copy of PostgreSQL's current state. `worker state-copy` ([`worker/statecopy/`](../worker/statecopy/)) runs on the psql host, where PostgreSQL is local, every day at 00:15 UTC (`stellar-state-copy.timer`). One run:

1. reads accounts, trustlines (excluding pool shares), Stellar Asset Contract balances and instances, claimable balances and liquidity pools in a single read-only `REPEATABLE READ` transaction, so the copy reflects one ledger;
2. loads them into `<table>_new` copies of the `state_*` tables over native TLS, 200,000 rows per insert;
3. computes `assets_new` and `top_holders_new` in ClickHouse ([`worker/statecopy/rankings.go`](../worker/statecopy/rankings.go)): classic assets from trustlines, contract balances, claimable balances and pool reserves, with the issuer's flags and home domain; XLM from the ledger's total coins; Soroban-only tokens (SEP-41 contracts without a Stellar Asset Contract) from their transfers since go-live, marked `complete` only if the token was created after go-live; then the top 500 holders per asset and the top 1,000 for XLM;
4. swaps each table in atomically with `EXCHANGE TABLES` and drops the previous version;
5. with `--snapshot` (as the timer runs it), appends `assets` and `top_holders` to the snapshot tables, dated the previous day;
6. records the run in `state_copy_runs`.

A run takes a minute or two. A failed run leaves the previous copy in place. The asset list and top holders are therefore at most a day old; `daily_stats` follows the pipeline within five minutes.

## Public read-only SQL

When `clickhouse_hostname` is set on the psql host, its Caddy serves ClickHouse's HTTP interface under that name ([`psql/files/clickhouse.caddy`](../psql/files/clickhouse.caddy)). ClickHouse itself is never exposed outside the private network.

- **Anyone can run `SELECT` queries**, via `GET /?query=…` or a POST body; ClickHouse's SQL page is at `/play` and a health check at `/ping`. Every other path returns 404.
- **Caddy always logs in on the client's behalf.** It strips `Authorization`, cookies, `X-ClickHouse-*` headers, trace-context headers and the `user`, `password` and `quota_key` parameters, then connects to ClickHouse over TLS as `stellar_public`. A client cannot choose another ClickHouse user.
- **The frontend's backend** authenticates to Caddy with its own HTTP basic-auth login. Caddy replaces it with the `stellar_api` ClickHouse user, which has a larger profile and quota, so `stellar_api`'s password never leaves the two hosts.
- **Limits:** the `stellar_public` profile (read-only, 10 s, 2 GB and 100,000 rows per query; 30,000 queries per hour), plus Caddy's 30 requests per 10 s per client IP. The backend's login has its own budget of 2,000 requests per 10 s.

Integers wider than 53 bits are returned as JSON strings (`output_format_json_quote_64bit_integers`). Query parameters keep values out of the SQL:

```ts
const url = new URL('https://clickhouse.example.com/')
url.searchParams.set('query', 'SELECT sequence, tx_count, closed_at FROM stellar.ledgers WHERE sequence = {seq:UInt32} FORMAT JSON')
url.searchParams.set('param_seq', '64806858')
const { data } = await (await fetch(url)).json()
```

**Capacity.** The backend's limits were sized by replaying the frontend's page queries (transactions by hash, their operations, account histories including the busiest accounts, assets, top holders, statistics) as `stellar_api` from the psql host:

| Concurrent queries | Queries/s | p50 | p99 |
|---|---|---|---|
| 1 | 118 | 6.6 ms | 35 ms |
| 4 | 703 | 3.9 ms | 21 ms |
| 8 | 1,342 | 4.3 ms | 19 ms |
| 16 | 1,362 | 9.3 ms | 37 ms |

The pipeline stayed within two ledgers of the network tip throughout. On this basis `stellar_api` allows 10 s and 1 GB per query, 4 GB in total and 16 concurrent queries, and Caddy's budget of 2,000 requests per 10 s is about 15 % of measured capacity. The hourly quotas set the error limit equal to the query limit, so a burst of refused queries cannot lock a client out for the rest of the hour.

Hardening was verified from outside: credential tricks (`?user=default`, basic auth as another ClickHouse user, `X-ClickHouse-User` headers) still run as `stellar_public`; `CREATE`, `INSERT`, reading `system.query_log` and `?readonly=0` are refused; a runaway query stops at 10 s.

## Server configuration

**Install.** ClickHouse 26.8.19.9 from the official apt repository's LTS channel (repository key verified by sha256). `clickhouse-server`, `clickhouse-client` and `clickhouse-common-static` are pinned and held, so upgrades are always deliberate. A single node: no Keeper, no replication.

**[`files/stellar-config.xml`](files/stellar-config.xml)** → `/etc/clickhouse-server/config.d/10-stellar.xml`:

| Setting | Reason |
|---|---|
| listens on 127.0.0.1 and the private IP | never on the public interface |
| HTTP 8123 and native 9000, plain | loopback only (worker, `clickhouse-client`), enforced by nftables |
| HTTPS 8443 and native TLS 9440 | for the psql host, with a certificate from the host's own CA |
| interserver, MySQL and PostgreSQL ports removed | single server, no other wire protocols |
| Prometheus endpoint on 9363 | metrics |
| `max_server_memory_usage_to_ram_ratio` 0.75 | leaves room for the pipeline and the page cache |
| ZSTD(3) compression | history is written once and read in bulk |
| 14-day TTL on all system logs | most grow without limit by default |

### Users

Users are defined in [`files/stellar-users.xml`](files/stellar-users.xml), not with SQL (`access_management` is off), so `lab` manages them like any other configuration file. Passwords are generated by `lab`; only their SHA-256 hashes are written to the server configuration.

| User | Connects from | Can | Profile | Used by |
|---|---|---|---|---|
| `default` | loopback | everything | | `lab`, administration |
| `stellar_owner` | loopback | DDL on `stellar.*` | | `worker clickhouse-migrate` |
| `stellar_writer` | loopback | `SELECT`, `INSERT`, `ALTER DELETE` on `stellar.*` | 8 GB | the pipeline, `clickhouse-init`, `clickhouse-check` |
| `stellar_reader` | loopback, psql host | `SELECT` on `stellar.*` and schema metadata | read-only; 30 s, 4 GB, 1,000,000 rows; 20,000 queries/hour | Grafana |
| `stellar_public` | loopback, psql host | `SELECT` on `stellar.*` | read-only; 10 s, 2 GB, 100,000 rows; 30,000 queries/hour | the public SQL endpoint |
| `stellar_api` | loopback, psql host | `SELECT` on `stellar.*` | read-only; 10 s, 1 GB (4 GB total), 16 concurrent; 720,000 queries/hour | the frontend's backend, via Caddy |
| `stellar_jobs` | psql host | manage the state copy, rankings and snapshots | 6 GB, 30 min | `worker state-copy` |

Read-only profiles use `readonly = 2` (settings may be adjusted within constraints, nothing can be written), and system tables require an explicit grant, so neither Grafana nor the public can read `system.query_log`.

**TLS.** `lab clickhouse` creates a small CA and a server certificate for the private IP. The psql host only uses the TLS ports and trusts only that CA.

**Firewall** ([`files/nftables.conf`](files/nftables.conf), default drop):

| Port | From | Purpose |
|---|---|---|
| 22 | admin IPs | SSH |
| 8443, 9440 | psql host | ClickHouse over TLS |
| 9363, 9100, 9103 | psql host | ClickHouse, node and pipeline metrics |
| 8123, 9000 | loopback | worker, `clickhouse-client` |

## Setup with `lab clickhouse`

Requirements: Ubuntu 24.04 amd64, a private network shared with the psql host, and read access to the archive bucket (only used to find the go-live ledger). Build `bin/` with `make dist` ([root README](../README.md#building)), copy it to the host and run, as root:

```sh
/root/bin/lab clickhouse          # asks for missing settings once, then applies every step
/root/bin/lab clickhouse check    # read-only: every step and the health probes
```

| # | Step | Does | Health check |
|---|---|---|---|
| 1 | `system` | packages, UTC, key-only SSH, journald cap, needrestart exemptions | |
| 2 | `network` | optional VLAN interface for the private IP ([`files/vswitch.yaml`](files/vswitch.yaml)) | psql host reachable |
| 3 | `firewall` | [`files/nftables.conf`](files/nftables.conf) | ruleset loaded |
| 4 | `clickhouse` | apt repository, pinned packages, node_exporter, server and user configuration, password hashes, TLS, `CREATE DATABASE stellar` | `/ping` over HTTPS; certificate validity |
| 5 | `worker` | user `stellar`; the worker binary shipped next to `lab` | |
| 6 | `config` | `/etc/stellar/worker.toml`, `archive.env`, password files; the bundle for the psql host | |
| 7 | `migrate` | `worker clickhouse-migrate` while migrations are pending | |
| 8 | `init` | `worker clickhouse-init` once | |
| 9 | `services` | [`stellar-clickhouse-ingest.service`](files/stellar-clickhouse-ingest.service) | `/readyz`, cursor and lag |

**Settings** (`/etc/stellar/lab-clickhouse.toml`): `private_ip` (example `10.0.2.2`), `psql_ip` (`10.0.1.2`), the private interface and optional VLAN settings, `admin_ips`, and the archive bucket's name, endpoint and region. **Secrets** (`/etc/stellar/secrets/`, root only): the archive read key (entered once), one generated password per ClickHouse user, and the CA key. Generated secrets are never rotated silently.

Running services are only restarted with `--allow-restart`; otherwise the step reports a pending restart. A ClickHouse restart pauses the pipeline for a few seconds, after which it catches up by itself.

## Integration with the psql host

The psql host reaches ClickHouse over the private network for four things: Prometheus metrics, Grafana's dashboard, the public SQL endpoint and the daily state copy. `lab clickhouse` writes what it needs (the CA certificate and the passwords of `stellar_reader`, `stellar_public`, `stellar_jobs` and `stellar_api`) into a bundle at `/root/clickhouse-bundle/`, which is copied to the psql host:

```sh
scp -3 -r root@<clickhouse host>:/root/clickhouse-bundle root@<psql host>:/root/
# on the psql host: set clickhouse_hostname and clickhouse_ip in /etc/stellar/lab-psql.toml
/root/bin/lab psql       # installs the bundle, the Caddy site, scrape jobs, alerts and the state-copy timer
/root/bin/lab grafana    # adds the ClickHouse data source and dashboard
```

## Verifying the data

```sh
worker clickhouse-check                                 # the last 10,000 ledgers up to the cursor
worker clickhouse-check --from 64806592 --to 64808752   # any range
curl -s localhost:9103/readyz
curl -s localhost:9103/metrics | grep -E '^ingest_(cursor|lag_ledgers|lake_tip)'
```

`clickhouse-check` compares each ledger's transaction and operation counts with the counts recorded in the ledger header, and confirms that no table holds rows above the cursor.

Useful queries for a reviewer, through the public endpoint or `clickhouse-client`:

```sql
-- coverage: first and last ledger, and any gap in the sequence
SELECT min(sequence), max(sequence), count(), max(sequence) - min(sequence) + 1 - count() AS missing
FROM stellar.ledgers;

-- last state-copy runs
SELECT started_at, ledger, rows, snapshot FROM stellar.state_copy_runs ORDER BY started_at DESC LIMIT 5;

-- storage per table
SELECT table, formatReadableSize(sum(bytes_on_disk)) FROM system.parts
WHERE active AND database = 'stellar' GROUP BY table ORDER BY sum(bytes_on_disk) DESC;
```

## Operations

### Storage growth

Measured on pubnet over eight days of history: about **1.7 GiB per day** of history, plus the daily snapshots (~0.2 GiB), so roughly 2 GiB per day or 0.7 TiB per year (±30 %, depending on network activity). The largest tables are `operations` (~550 MiB/day), `transactions` (~400 MiB/day), `events` (~210 MiB/day) and the two transfer tables (~180 MiB/day each). The state copy and rankings are replaced on every run and stay at about 1.9 GiB. Disk alerts fire at 70 % and 85 %, and when the disk is projected to fill within 30 days.

### Deploying a new worker

`lab clickhouse` installs the worker shipped next to it but never replaces a running one. A new version is installed side by side and selected by a symlink:

```sh
install -m 0755 worker-$V /usr/local/lib/stellar/worker-$V
ln -sfn /usr/local/lib/stellar/worker-$V /usr/local/bin/worker
worker clickhouse-migrate                 # if the release adds migrations
systemctl restart stellar-clickhouse-ingest
curl -s localhost:9103/readyz
```

The restart removes any rows above the cursor and resumes. Rolling back means pointing the symlink at the previous version; applied migrations stay.

### Monitoring and alerts

Prometheus on the psql host scrapes ClickHouse (9363), node_exporter (9100) and the pipeline (9103). Alerts ([`files/alerts.yml`](files/alerts.yml)):

| Alert | Severity | Condition |
|---|---|---|
| `ClickHouseDown`, `ClickHousePipelineDown` | page | metrics unreachable for 5 minutes |
| `ClickHousePipelineHalted` | page | the pipeline halted on bad data |
| `ClickHousePipelineBehind` | page | more than 720 ledgers (one hour) behind the network for 15 minutes |
| `ClickHouseTooManyParts` | warning | more than 300 parts in a partition for 15 minutes |
| `ClickHouseDiskUsage`, `ClickHouseDiskCritical`, `ClickHouseDiskFullIn30Days` | warning, page, warning | disk over 70 %, over 85 %, or full within 30 days |
| `StateCopyStale`, `StateCopyFailed`, `StateCopyMetricMissing` | warning | no successful state copy for two days, a failed run, or none ever |

Typical responses: a halted pipeline is deterministic, so it needs a fixed worker (the journal names the ledger and the error); a pipeline that is behind usually means the AWS public lake was unreachable and it catches up by itself; too many parts means merges are behind (check `system.merges` and the disk).

### Rebuilding

Everything except the snapshot tables can be rebuilt from the AWS public lake. A rebuilt server runs `lab clickhouse` (with the previous secrets restored, or a new bundle for the psql host), which migrates, seeds the cursor at go-live and catches up at about 28 ledgers per second: about 10 minutes per day of history, about 2.5 days per year. The next state copy refills the rankings. `asset_snapshots` and `holder_snapshots` from before the rebuild are lost, because nothing else records the state as it was on each day; they should be exported first if they matter.

A deliberate re-ingest (after a decoder fix) stops the pipeline, drops the `stellar` database and runs `lab clickhouse` again.

### Retention

`stellar.*` keeps everything. System logs keep 14 days; server log files are capped at 10 × 100 MB.

## Limitations

- **History before go-live is not loaded yet.** The pipeline starts at go-live. A backfill can read the AWS public lake from an earlier ledger with the same code (`worker clickhouse-init --at <ledger>` on a fresh database), but only back to Protocol 23: the decoder accepts `LedgerCloseMeta` V2 only (`worker/sep54/decode.go`), and earlier ledgers use V0/V1. A backfill to genesis needs decoder support for the older formats first.
- **Worker deploys on this host are manual** (see above); the galexie host has deploy scripts.

## Development

The worker's ClickHouse code is in [`worker/clickhouse/`](../worker/clickhouse/). Unit tests run on real ledgers without a server. Integration tests run against a ClickHouse server given by `TEST_CLICKHOUSE_URL`, in a database of their own that they drop afterwards; without the variable they are skipped.

```sh
cd worker && TEST_CLICKHOUSE_URL="clickhouse://default:<password>@127.0.0.1:9000/default" go test ./clickhouse/...
```

`worker/statecopy`'s integration test needs both `TEST_CLICKHOUSE_URL` and `TEST_DATABASE_URL`. `lab`'s code for this host is [`cli/clickhouse.go`](../cli/clickhouse.go); the psql side is in [`cli/psql_clickhouse.go`](../cli/psql_clickhouse.go), [`cli/psql_jobs.go`](../cli/psql_jobs.go) and [`cli/grafana_clickhouse.go`](../cli/grafana_clickhouse.go).

## Files

| File | Installed as |
|---|---|
| [`files/clickhouse.sources`](files/clickhouse.sources) | `/etc/apt/sources.list.d/clickhouse.sources` |
| [`files/stellar-config.xml`](files/stellar-config.xml) | `/etc/clickhouse-server/config.d/10-stellar.xml` |
| [`files/stellar-users.xml`](files/stellar-users.xml) | `/etc/clickhouse-server/users.d/10-stellar.xml` |
| [`files/nftables.conf`](files/nftables.conf) | `/etc/nftables.conf` |
| [`files/vswitch.yaml`](files/vswitch.yaml) | `/etc/netplan/60-vswitch.yaml`, only with a VLAN |
| [`files/sshd-hardening.conf`](files/sshd-hardening.conf), [`journald.conf`](files/journald.conf), [`needrestart.conf`](files/needrestart.conf) | as on the psql host |
| [`files/worker.toml`](files/worker.toml) | `/etc/stellar/worker.toml` |
| [`files/archive.env.example`](files/archive.env.example) | `/etc/stellar/archive.env`, with the archive read key |
| [`files/stellar-clickhouse-ingest.service`](files/stellar-clickhouse-ingest.service) | `/etc/systemd/system/` |
| [`files/alerts.yml`](files/alerts.yml) | on the psql host: `/etc/prometheus/rules/clickhouse-alerts.yml` |
| `files.go` | not installed: embeds this folder into `lab` |
