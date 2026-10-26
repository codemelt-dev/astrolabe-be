# worker

The Go program that does all of the data layer's ingestion and processing. One binary (`worker`) provides every command; which commands run where is decided by the systemd units each host installs.

Module: `stellar-explorer/worker` (Go 1.26). Main dependencies: `github.com/stellar/go-stellar-sdk` (XDR, ingest helpers, history archives, the CAP-67 token-transfer processor), `github.com/versity/versitygw` (the embedded S3 server behind the lake), `github.com/jackc/pgx/v5`, `github.com/ClickHouse/clickhouse-go/v2`, `github.com/aws/aws-sdk-go-v2`.

## What it does

```text
                     ┌──────────────────── galexie host ─────────────────────┐
Galexie ── S3 PUT ──▶│ lake ──▶ sequencer ──▶ processLedger ──▶ PostgreSQL     │  (current state + ledgers)
                     │   └───▶ archiver ──▶ archive bucket   pruner ──▶ lake  │
                     └────────────────────────────────────────────────────────┘
                     ┌─────────────────── clickhouse host ───────────────────┐
AWS public lake ────▶│ sequencer ──▶ processLedger (history) ──▶ ClickHouse   │  (transactions, operations, events, …)
                     └────────────────────────────────────────────────────────┘
                     ┌────────────────────── psql host ──────────────────────┐
PostgreSQL state ───▶│ state-copy (daily) ──▶ ClickHouse: assets, top holders │
                     └────────────────────────────────────────────────────────┘
```

**Two pipelines, one decoder.** Both pipelines use the same sequencer and the same `processLedger` (`explorer/process.go`):

| | PostgreSQL pipeline (`worker ingest`) | ClickHouse pipeline (`worker clickhouse-ingest`) |
|---|---|---|
| Host | galexie host | clickhouse host |
| Sources | lake → archive bucket → AWS public lake | AWS public lake |
| Produces | the ledger row and all current-state changes | the ledger row and all history rows |
| Commit | rows and cursor in one PostgreSQL transaction | one insert per table, then the cursor; rows above the cursor are deleted on restart |

## Decoding and validation

1. **Read:** SEP-54 objects (one zstd-compressed `LedgerCloseMetaBatch` per ledger). Object integrity is checked by CRC32C, or the ETag where the store keeps no CRC32C.
2. **Decode:** zstd with frame checksums (256 MiB cap), then `xdr.SafeUnmarshal`. The batch must hold exactly the requested ledger, as `LedgerCloseMeta` **V2** (Protocol 23 and later), and `sha256(header)` must equal the ledger hash.
3. **Chain:** each ledger's previous hash must equal the cursor's hash.
4. **Process:** transactions and ledger-entry changes are read with the SDK's ingest readers; token movements (transfers, mints, burns, clawbacks, fees) come from the SDK's CAP-67 token-transfer processor; Stellar Asset Contract balances from its `sac` helpers. ScVals are encoded as typed JSON (`explorer/scval.go`), operation details by a reflection-based XDR-to-JSON encoder (`explorer/xdrjson.go`), and contract interfaces by parsing the Wasm `contractspecv0` section (`explorer/wasmspec.go`).

A ledger that fails validation from one source is retried from the next. If every source fails, the pipeline halts without moving its cursor and waits for an operator; other loops in the process (archiver, pruner) keep running.

## What is produced

| Data | Store | Notes |
|---|---|---|
| Ledgers | PostgreSQL and ClickHouse | one row per ledger |
| Accounts, trustlines, data entries, offers, claimable balances, liquidity pools | PostgreSQL | current state, with evictions handled |
| Contract instances, contract data, contract code (Wasm and decoded spec), TTLs | PostgreSQL | current state, including archived and evicted entries |
| Transactions, operations (all types, details as JSON, result codes, Soroban return values) | ClickHouse | history since go-live |
| Contract and system events (including fee and refund events, every topic) | ClickHouse | diagnostic events are not stored |
| Token transfers, mints, burns, clawbacks, fees | ClickHouse | plus a per-account view of both sides |
| Contract calls (top-level invocations: contract, function, caller, outcome, return value) | ClickHouse | |
| Contract creations, including deployments by other contracts | ClickHouse | |
| Account activity (every transaction an account or contract took part in, with roles) | ClickHouse | |
| Transaction lookup by hash | ClickHouse | |
| Daily network statistics | ClickHouse | refreshed every 5 minutes |
| Asset list, supplies, holder counts, top holders (500 per asset, 1,000 for XLM), daily snapshots | ClickHouse | from the daily state copy |

## Commands

| Command | Host | Purpose |
|---|---|---|
| `lake` | galexie | Galexie's S3 endpoint with the durability layer and tip index |
| `galexie-start` | galexie | computes Galexie's start ledger before every start |
| `watchdog` | galexie | restarts Galexie only if its upload is hung |
| `ingest` | galexie | PostgreSQL pipeline, archiver, pruner, metrics |
| `gapcheck` | galexie | weekly check that the archive has no missing ledgers |
| `bootstrap` | galexie | loads the full current state from an SDF history-archive checkpoint and seeds the cursor (variants: `--force`, `--recreate-origin`, `--history-only --at`) |
| `migrate [--check]` | galexie | PostgreSQL migrations |
| `reindex --from --to [--source] [--dry-run]` | galexie | re-reads a closed range and replaces its ledger rows, or only validates it |
| `smoke-archive` | galexie | S3 provider compatibility test |
| `clickhouse-migrate [--check]` | clickhouse | ClickHouse migrations |
| `clickhouse-init [--at N]` | clickhouse | seeds the ClickHouse cursor (default: go-live) |
| `clickhouse-ingest` | clickhouse | ClickHouse pipeline |
| `clickhouse-check [--from --to]` | clickhouse | consistency check: counts against ledger headers, no rows above the cursor |
| `state-copy [--snapshot]` | psql | copies current state into ClickHouse and computes the rankings |
| `dev-feed` | development | stands in for Galexie by copying from the AWS public lake |
| `version` | any | build version |

Every command takes `--config` (default `/etc/stellar/worker.toml`); unknown configuration keys are rejected. Credentials are read from files by path and never from the process environment.

## Packages

| Package | Purpose |
|---|---|
| `main` (`main.go`, `cmd_*.go`) | command dispatch |
| `config/` | `worker.toml` loading and validation; credential file parsers |
| `lake/` | the embedded S3 server, fsync chain, fail-closed behaviour, tip index, control endpoint, start-up checksum scan |
| `sep54/` | SEP-54 key layout, manifest, decoding and validation, the three ledger sources, tip clients |
| `sequencer/` | the generic ingest loop: routing between sources, decoding, hash-chain checks, batching, commit, halting |
| `explorer/` | `processLedger`; PostgreSQL schema, migrations and sink; XDR and ScVal JSON; Wasm spec parsing |
| `ingest/` | wiring for `worker ingest` |
| `archive/` | archiver, gap check, provider smoke test |
| `prune/` | lake retention |
| `bootstrap/` | state load from history-archive checkpoints |
| `launcher/`, `watchdog/` | Galexie start ledger and hang detection |
| `reindex/`, `devfeed/` | operator and development tools |
| `clickhouse/` | ClickHouse connection, migrations, sink, consistency check |
| `statecopy/` | the daily state copy and ranking SQL |
| `metrics/` | Prometheus metrics and health endpoints |
| `migration/`, `statefile/` | migration file parser; atomic state files |

## Building and testing

```sh
make build         # bin/worker for this machine
make build-linux   # bin/worker-linux-amd64, the servers' binary
make test          # go test -race (lake tests first)
make check         # fmt, vet (macOS and Linux), test, Linux build
make test-live     # opt-in, needs internet: newest 2,000 AWS-lake ledgers decoded, fed and (with TEST_DATABASE_URL) replayed
make alerts        # promtool checks and rule tests for ../galexie/deploy/alerts.yml
```

| Variable | Enables |
|---|---|
| `TEST_DATABASE_URL` | PostgreSQL tests (a local PostgreSQL 18 role with `CREATEDB` and `CREATEROLE`; each test creates and drops its own database) |
| `TEST_CLICKHOUSE_URL` | ClickHouse and state-copy integration tests |

Tests that need a database are skipped when the variable is unset. The test suite includes real pubnet ledgers (`sep54/sep54test/testdata/`), crash tests (`kill -9` of ingest and the lake), and `deploy_test.go`, which checks that the galexie host's systemd units, configuration files, firewall rules, alert rules and deploy scripts agree with each other and with the worker's configuration.

Deployment and operation are described in [`galexie/README.md`](../galexie/README.md), [`clickhouse/README.md`](../clickhouse/README.md) and [`psql/README.md`](../psql/README.md).
