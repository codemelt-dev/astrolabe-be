# galexie host

The galexie host is where ledgers enter the system. It runs **Galexie**, SDF's ledger exporter with an embedded captive stellar-core, and the project's **Go worker** (`worker/`), which stores every ledger locally, archives it to object storage, decodes it and writes the current state to PostgreSQL on the psql host.

`lab galexie` installs every file in [`deploy/`](deploy/) and brings the host to the state described here; `lab galexie check` reports whether it is there and healthy. The deploy files are checked by `go test -run TestDeploy` in `worker/` (unit coupling, restart policies, timeouts, configs agreeing with each other, alert rules using only exported metrics).

## Contents

1. [What runs here](#what-runs-here)
2. [Galexie](#galexie)
3. [The lake](#the-lake)
4. [Ingest: sequencer, archiver, pruner](#ingest-sequencer-archiver-pruner)
5. [The archive bucket](#the-archive-bucket)
6. [Watchdog and gap check](#watchdog-and-gap-check)
7. [Worker commands](#worker-commands)
8. [Setup with `lab galexie`](#setup-with-lab-galexie)
9. [Verifying the pipeline](#verifying-the-pipeline)
10. [Operations](#operations)
11. [Monitoring and alerts](#monitoring-and-alerts)
12. [Development](#development)
13. [Files](#files)

---

## What runs here

```text
Stellar network (peers, history archives) ─► captive stellar-core inside the Galexie container
stellar-galexie.service   Galexie "append": one compressed ledger file per ledger close
        │ S3 PUT to http://127.0.0.1:9000 (If-None-Match: *, CRC32C)
        ▼
stellar-lake.service      worker lake: an embedded S3 server (VersityGW) on /srv/lake
        │                 every write is fsynced before it is acknowledged; publishes the tip
        │ commit notification + read from the page cache
        ▼
stellar-ingest.service    worker ingest
        ├─ sequencer: cursor + 1 → decode → processLedger → one PostgreSQL transaction (psql host, TLS)
        ├─ archiver:  lake → archive bucket, 1:1, strictly in order
        └─ pruner:    deletes old lake files, only once archived and outside retention
stellar-watchdog.timer    every 30 s: restarts Galexie only if its upload is hung
stellar-gapcheck.timer    weekly: verifies the archive has no missing ledgers
```

| Unit | Runs | Purpose |
|---|---|---|
| `stellar-firewall.service` | `nft -f` | filters the private interface |
| `stellar-lake.service` | `worker-lake lake` | Galexie's S3 endpoint `127.0.0.1:9000`; control endpoint `127.0.0.1:9001`; metrics on the private IP, port 9101 |
| `stellar-galexie.service` | `docker run stellar/stellar-galexie … append` | captive core → one SEP-54 file per ledger → the lake |
| `stellar-ingest.service` | `worker ingest` | sequencer, archiver, pruner; metrics on port 9102 |
| `stellar-watchdog.timer` | `worker-lake watchdog` | every 30 s |
| `stellar-gapcheck.timer` | `worker gapcheck` | weekly |

**Galexie depends only on the lake.** Deploying or crashing ingest never touches Galexie or the lake, and a PostgreSQL outage only makes ingest wait while the lake absorbs the backlog. The lake runs its own copy of the worker binary (`worker-lake`), which changes only with a lake deploy.

**Why a local lake.** Galexie writes to S3-compatible storage. Writing straight to a remote bucket would make the live pipeline depend on that provider's latency and availability. The local lake gives Galexie a fast, durable target on the same machine, lets ingest read each ledger from memory the moment it is committed, and lets the archive upload run independently, catching up after a provider outage without holding back ingest.

**How the units are coupled** (checked by `go test -run TestDeployUnits`):

- **Galexie ↔ lake: `Wants=` + `PartOf=` + `After=`.** Stopping or restarting the lake stops Galexie first (it drains into the lake), then starts it again after the lake is ready. A lake crash does not stop Galexie: Galexie is restarted once the lake is back. `BindsTo=` is deliberately not used, because with systemd 255 a lake crash would leave Galexie stopped.
- **Ingest ↔ lake: `Wants=` + `After=` only**, for boot order. Restarting either never restarts the other.
- **Galexie's container** runs with `--init` (signal forwarding and reaping of captive core's children) and a fixed name with a pre-start cleanup, so two captive cores can never share `/srv/core`.

**State on disk:**

| Path | Contents |
|---|---|
| `/srv/core` | captive core's database and buckets (local NVMe) |
| `/srv/lake/s3/stellar/ledgers/pubnet/` | the lake: SEP-54 ledger files and Galexie's manifest `.config.json` |
| `/srv/lake/state/lake-id` | marker: the lake refuses to start without it, so ledger files never land on the root disk |
| `/srv/lake/state/boot-id` | the boot of the last start; a new boot triggers a checksum scan of the newest 100 files |
| `/srv/lake/state/bootstrap.json` | where the archive starts (`archive_origin`), written once by `worker bootstrap` |
| `/run/stellar-galexie/galexie.env` | Galexie's start ledger, computed before every start |
| `/var/lib/stellar-watchdog/state.json`, `/var/lib/stellar-gapcheck/result.json` | watchdog and gap-check results, exported as metrics |

## Galexie

- **What it is.** Galexie runs a captive stellar-core (bundled in the image) that streams `LedgerCloseMeta`, and writes each ledger as a zstd-compressed XDR `LedgerCloseMetaBatch` in the **SEP-54** layout (`ledgers_per_file = 1`, `files_per_partition = 64000`), the same layout as the AWS public lake. Each file carries metadata (start and end ledger and close time, protocol and core version, network passphrase, Galexie version). The manifest `.config.json` pins the schema and the network.
- **Pinned image:** `stellar/stellar-galexie:29.0.0@sha256:5269dfd9…a3` (stellar-core 29.0.0, Protocol 29). The tag and digest are pinned in the unit, in `deploy/worker.toml` and in `deploy/galexie-repair.toml`; tests fail if they disagree.
- **Mode:** `append`: export from a start ledger and follow the network indefinitely, uploading only to the local lake, strictly sequentially, one PUT per ledger with `If-None-Match: *` (a 412 means "already exported").
- **Start ledger.** Galexie resumes at `max(--start, newest file + 1)`. `--start` is computed before every launch by `worker galexie-start`:

  | Lake | Archive | Start |
  |---|---|---|
  | has ledgers | any (probe bounded to 9.5 s) | `max(lake tip, archive tip if known) + 1` |
  | empty | known tip | archive tip + 1 (the lake was lost) |
  | empty | empty, `bootstrap.json` present | `archive_origin` (first run) |
  | empty | empty, no `bootstrap.json` | refuse: `worker bootstrap` must run first |
  | empty | unreachable | refuse and retry every 5 s |

- **Failure behaviour.** On an upload error Galexie exits and systemd restarts it after 5 s (the start ledger is recomputed). A manifest mismatch makes it exit on every start. **Captive core rebuilds its database on every restart**, even after a clean stop: on pubnet that takes about 25 minutes (downloading about 15 GB of buckets and catching up), which is why nothing restarts Galexie casually.
- **Repair tools** (same image, one-off containers): `scan-and-fill --start A --end B` exports only missing ledgers; `detect-gaps` produces a read-only report.

## The lake

`worker lake` embeds [VersityGW](https://github.com/versity/versitygw), an S3 gateway over a POSIX filesystem, and wraps every write in a durability layer (the "tap"):

- **Write path:** PUT → `fsync` of the file → link into place → `fsync` of the directory → publish the new tip. A PUT is acknowledged only once the ledger is durable.
- **Fail-closed:** if an `fsync` fails after a link (which on Linux cannot be safely retried), the lake refuses every further write with 503 until it is restarted and its start-up `syncfs` succeeds.
- **Index:** the lake tracks its newest and oldest ledger and serves them on the control endpoint (`/v1/tip`, with long-polling), so ingest learns about new ledgers immediately.
- **Start-up checks:** `syncfs` on every start; after a reboot, a checksum scan of the newest 100 files, which deletes corrupt files (a corrupt newest file is simply re-exported by Galexie; a corrupt file below the tip is reported as a hole).
- **Storage:** a separate volume (200 GB, XFS) mounted at `/srv/lake`, so the lake and captive core cannot fill each other's disk. XFS supports the extended attributes the lake uses for metadata and checksums.
- **Sizing:** 72 hours of retention is about 10 GB (3.3 GB per day). The rest is headroom for an archive outage: the pruner only deletes archived ledgers, which gives about 55 days on 200 GB.

## Ingest: sequencer, archiver, pruner

`worker ingest` runs three loops in one process.

**Sequencer.** Reads ledger `cursor + 1`, decodes and validates it (sequence, hash chain), runs `processLedger` (the shared decoder in `worker/explorer/`), and commits the ledger row and all state changes in **one PostgreSQL transaction** together with the new cursor. The cursor update is guarded, so a second writer or a replay can never commit twice; an advisory lock prevents two ingest processes from running at all. Catch-up uses batches of at most 500 ledgers and 512 MiB.

**Ledger sources, in order:**

1. the **lake** (the normal case: the ledger is read from the page cache the moment it is committed);
2. the **archive bucket**, if the lake no longer holds the ledger or has been unreachable for 2 minutes;
3. the **AWS public lake**, if the archive does not have it or has failed for 10 minutes. This is a last resort and raises the `reading_from_aws` alert.

A ledger that fails validation from one source is tried from the next. If every source fails, the pipeline halts (`pipeline_halted`) and waits for an operator; the archiver and pruner keep running.

**Archiver.** Copies the lake to the archive bucket 1:1 (same keys, bytes, metadata and manifest), strictly in order, with `If-None-Match: *`. Each upload is verified (CRC32C where the store keeps it, otherwise the ETag/MD5). If the archive already holds a different ledger under the same key, archiving halts rather than overwrite it.

**Pruner.** Deletes lake files once they are archived and older than the retention window (default 51,840 ledgers, about 72 hours), oldest first, always through the lake's S3 interface. Above a disk watermark (80 %) it stops waiting for the sequencer, which then reads those ledgers from the archive instead.

**Protocol upgrades.** Only ingest decodes XDR; the lake and archiver do not, so storing and archiving continue through a protocol upgrade the decoder does not know yet. If ingest halts on unknown XDR, deploying a worker with an updated Stellar Go SDK resumes it from its cursor.

## The archive bucket

The archive is a permanent 1:1 copy of the lake in S3-compatible object storage. Standard SEP-54 tooling can read it; the sequencer reads it when the lake no longer holds a ledger; Galexie starts from its tip if the lake volume is lost.

- **Least privilege.** The key on the server (the uploader key) may put, get and list objects, but not delete objects or change the bucket policy. On the deployment's provider, S3 keys are valid for every bucket in their project, so the archive bucket lives in a separate project and the uploader key reaches it only through the bucket policy ([`deploy/archive-policy.json`](deploy/archive-policy.json)). An admin key that can delete stays off every server.
- **No versioning or object lock**, because conditional PUT (`If-None-Match`) is not supported on such buckets. An overwrite of an existing key by the uploader is the accepted residual risk: the data is public chain data that can be re-exported.
- **Checksums:** the provider accepts but does not store CRC32C, so stored objects are verified by ETag (the MD5 of a single-part upload).
- **Growth:** about 1.2 TB per year.
- `worker smoke-archive --bucket <scratch bucket>` tests a provider's compatibility before use: cross-project listing, checksummed and conditional PUTs, metadata, ETag behaviour, delete denied, path-style and virtual-hosted reads. On the production bucket only the safe `--delete-probe-only` check is allowed.

## Watchdog and gap check

**Watchdog** (every 30 s). Restarts Galexie only when its upload is hung: a PUT has been receiving data for more than 120 s, or the export manager is ahead of the uploader and the uploader has not moved for 180 s. It never restarts while a PUT is in `fsync` (a restart cannot unblock a stuck disk), while Galexie is deliberately stopped, or when it cannot read the lake's metrics; at most once per 15 minutes, and it pages after two restarts in a row without progress.

**Gap check** (weekly). Runs Galexie's `detect-gaps` over `[archive_origin, archive tip]` against the archive. If `detect-gaps` fails, it falls back to `worker reindex --dry-run --source archive`, which downloads and validates every archived ledger.

## Worker commands

One binary, installed twice so that an ingest deploy never touches the lake: `worker-lake` runs `lake`, `galexie-start` and `watchdog`; `worker` runs everything else. Both are symlinks to versioned builds in `/usr/local/lib/stellar/`.

| Command | Run by | Purpose |
|---|---|---|
| `worker lake` | `stellar-lake.service` | Galexie's S3 endpoint with the durability layer |
| `worker ingest` | `stellar-ingest.service` | sequencer, archiver, pruner, metrics |
| `worker galexie-start` | Galexie's pre-start | checks the lake, computes the start ledger |
| `worker watchdog` | timer | restarts a hung Galexie |
| `worker gapcheck` | timer | archive gap check |
| `worker bootstrap` | `lab galexie`, once | loads the full state from the latest history-archive checkpoint and seeds the cursor there |
| `worker bootstrap --force` | operator | state repair; the cursor only moves forward |
| `worker bootstrap --recreate-origin` | `lab galexie` or operator | recreates a lost `bootstrap.json` from PostgreSQL |
| `worker migrate [--check]` | `lab galexie`, deploy script | embedded SQL migrations, as `explorer_owner` |
| `worker reindex --from A --to B [--source lake\|archive\|aws] [--dry-run]` | operator | re-reads a closed range and replaces its ledger rows; `--dry-run` only validates |
| `worker dev-feed --start A [--end B] [--speed X]` | development | behaves like Galexie, copying from the AWS public lake |
| `worker smoke-archive --bucket <name>` | operator | provider compatibility test |
| `worker version` | | prints the build version |

**Bootstrap.** The current state cannot be derived from a few ledgers, so the first run loads it from SDF's history archives at the latest checkpoint: on pubnet about 57 million entries in about 25 minutes, peaking at about 11 GB of memory. The cursor is set to that checkpoint and the archive's origin is recorded in `bootstrap.json`. From then on every ledger is applied incrementally.

## Setup with `lab galexie`

**Requirements:** Ubuntu 24.04 amd64; at least 8 dedicated vCPUs, 32 GB RAM and a 240 GB local NVMe disk for captive core (Galexie's own minimum is 4 vCPU, 16 GB RAM, 100 GB at ≥ 5K IOPS); a separate block volume of at least 200 GB for the lake; a private network shared with the psql host; the archive bucket and its uploader key.

**Public firewall.** Only SSH from admin IPs is allowed inbound on the public interface. Galexie's admin port 6061 and captive core's peer port 11725 bind to all interfaces and must stay closed publicly. The host's own nftables rules ([`deploy/galexie-host.nft`](deploy/galexie-host.nft)) filter the private interface only: inbound metrics ports (9101, 9102, 6061) and ping from the psql host, outbound 5432 to the psql host. They live in their own table and never flush the ruleset, because Docker manages its own chains on this host.

Build `bin/` with `make dist` ([root README](../README.md#building)), copy it to `/root/bin/`, and run as root (the bootstrap takes a while, so a `tmux` session is advisable):

```sh
/root/bin/lab galexie          # checks every step, asks once (plus before formatting the volume and running the bootstrap)
/root/bin/lab galexie check    # read-only: every step and health probe
```

| # | Step | Does | Health check |
|---|---|---|---|
| 1 | `system` | packages, UTC, key-only SSH, journald cap, needrestart exemptions for Docker and the stellar units, service user `stellar` | |
| 2 | `firewall` | the private-interface nftables table and its unit | table loaded |
| 3 | `disks` | `/srv/core`; formats the lake volume (only if blank, after confirmation), adds it to `/etc/fstab`, mounts it, writes the marker | disk usage |
| 4 | `docker` | Docker Engine from Docker's repository (key pinned by sha256), excluded from unattended upgrades; pulls the pinned Galexie image | |
| 5 | `worker` | first install of the worker binary and its two symlinks | |
| 6 | `config` | `/etc/stellar`: `worker.toml`, Galexie configs, generated lake keys, archive credentials, the database bundle from the psql host | TLS logins to PostgreSQL |
| 7 | `units` | the systemd units | |
| 8 | `migrate` | `worker migrate` while migrations are pending | |
| 9 | `bootstrap` | `worker bootstrap` once, after confirmation | |
| 10 | `services` | starts the lake, ingest, Galexie and both timers | lake health and tip, Galexie's last upload, ingest readiness, cursor and lag, archive tip |

**Settings** (`/etc/stellar/lab-galexie.toml`): `private_ip` (example `10.0.0.2`), `postgres_ip` (`10.0.1.2`), `private_if`, `lake_device` (the volume's `/dev/disk/by-id/` path), `archive_bucket`, `archive_endpoint`, `archive_region`. The archive uploader key is a secret, entered once and kept in `/etc/stellar/secrets/`.

**Credentials on the host.** All are files readable only by root and the `stellar` group; no worker process has `AWS_*` variables in its environment.

- `gateway.env`: the lake's own S3 keys, generated once on the host and used only by Galexie, the lake and ingest.
- `archive.env`: the archive endpoint and uploader key (worker), plus `archive-aws.env` with the same key under `AWS_*` names for the gap-check container.
- The database bundle from the psql host: the CA certificate and one password file per role. `pgpass` (for `ingest_writer`) is readable by the `stellar` user; `pgpass-owner` (for `explorer_owner`, migrations only) is readable by root only.

Running services are only restarted with `--allow-restart`, and `lab galexie` never restarts the lake or Galexie on its own, because every Galexie restart costs a captive-core rebuild.

## Verifying the pipeline

```sh
lab galexie check                                     # every step and health probe
curl -s 127.0.0.1:9001/v1/tip                         # lake tip and oldest ledger
curl -s 127.0.0.1:9102/readyz                         # ready, or the failing checks (lake, postgres, lag)
curl -s 127.0.0.1:9102/metrics | grep -E '^(ingest_cursor|ingest_lag_ledgers|stellar_network_tip|archive_tip)[ {]'
cat /var/lib/stellar-gapcheck/result.json             # last gap check: range, gaps, missing, invalid
```

In PostgreSQL (on the psql host), continuity and freshness of the ingested ledgers:

```sql
-- zero missing and zero duplicate ledger sequences since the bootstrap checkpoint
SELECT min(sequence), max(sequence), count(*), count(DISTINCT sequence),
       max(sequence) - min(sequence) + 1 - count(DISTINCT sequence) AS missing
FROM explorer.ledgers;

-- age of the newest ledger
SELECT max(sequence), now() - max(closed_at) AS age FROM explorer.ledgers;
```

The archive can be listed with any S3 client; object keys are derived from the ledger sequence (SEP-54, newest first), e.g. ledger 123456 is `ledgers/pubnet/FFFF05FF--64000-127999/FFFE1DBF--123456.xdr.zst`.

## Operations

### Deploys

Builds are installed as `/usr/local/lib/stellar/worker-<version>` and selected by a symlink; versions are immutable.

```sh
sudo deploy/deploy-ingest.sh /root/worker-$V $V [--migrate]   # routine: switches `worker`, restarts ingest only, waits for /readyz
sudo deploy/deploy-lake.sh   /root/worker-$V $V               # rare: switches `worker-lake`, restarts the lake (Galexie stops first and rebuilds)
```

Rollback re-runs the script with the previous version. Migrations are forward-only; an older build runs against a newer schema as long as the migrations were additive.

### Protocol upgrades

Before a protocol vote, the Galexie image (which bundles the matching stellar-core) is bumped in the unit, `deploy/worker.toml` and `deploy/galexie-repair.toml`, then installed with `lab galexie --allow-restart`. If the protocol changes XDR, `github.com/stellar/go-stellar-sdk` is bumped in `worker/go.mod` and ingest is redeployed. After the vote:

```sql
SELECT protocol_version, min(sequence), max(sequence) FROM explorer.ledgers
WHERE sequence > (SELECT ledger_sequence - 1000 FROM ingest_state WHERE pipeline = 'explorer') GROUP BY 1;
```

### Failure handling

| Situation | Behaviour and response |
|---|---|
| PostgreSQL unavailable | ingest retries with backoff; the lake keeps accepting; archiver and pruner keep running. It catches up by itself. |
| Archive unavailable | the archiver retries; the lake grows by ~3.3 GB/day until it is back (about 55 days of headroom). |
| Lake disk full | Galexie's uploads fail and it restarts in a loop. Usually the archive is behind; the volume can be grown online (`xfs_growfs`), or retention lowered temporarily. Files under `/srv/lake/s3` are never deleted by hand. |
| `fsync` failure | the lake fails closed (503). After investigating the volume, a restart with a successful `syncfs` resumes it. |
| Hole in the lake | the sequencer reads the ledger from the archive. If it was never archived, it is re-exported with `scan-and-fill`. |
| Pipeline halted | every source failed validation for `cursor + 1`. Unknown XDR after a protocol vote needs an SDK bump; a hash mismatch means corruption or the wrong network, and the cursor is never rewound. |
| State drift | `worker bootstrap --force` reloads the state tables from a checkpoint at or above the cursor; the cursor only moves forward. |
| Decoder bug | state: fix, deploy, `bootstrap --force`. Ledger rows: `worker reindex`. History: re-ingest on the clickhouse host. |
| Server lost, volume intact | attach the volume to a new server and run `lab galexie`; nothing is formatted and Galexie resumes at the lake tip + 1. |
| Server and volume lost | a new volume; Galexie resumes at the archive tip + 1, and the sequencer reads from the archive, then the AWS public lake. Nothing is lost that the archive or the AWS lake holds. |

Holding Galexie for maintenance: `systemctl stop stellar-galexie` (it stays stopped across lake restarts); `systemctl enable --now stellar-galexie` releases it. Galexie is never stopped with `docker stop`, so the launcher and the orphan cleanup always run.

## Monitoring and alerts

Prometheus and Alertmanager run on the psql host and scrape this host over the private network (`lake`, `ingest` and `galexie` jobs). Rules are in [`deploy/alerts.yml`](deploy/alerts.yml) with simulations in [`deploy/alerts_test.yml`](deploy/alerts_test.yml) (`make alerts` in `worker/`). Every operator-facing condition is also logged at ERROR with `alert=<kind>`.

| Endpoint | Where | Contents |
|---|---|---|
| lake `/v1/tip`, `/metrics`, `/healthz` | `127.0.0.1:9001` | tip and oldest ledger; `lake_*` metrics |
| lake `/metrics`, `/healthz` | private IP, 9101 | the same metrics for Prometheus |
| ingest `/metrics`, `/healthz`, `/readyz` | `127.0.0.1:9102` and private IP | `ingest_*`, `archive_*`, `archiver_*`, `prune_*`, `stellar_network_tip`, disk usage, alert counters, watchdog and gap-check results |
| Galexie `/metrics` | `:6061` | `galexie_*` |

`/readyz` requires the lake and PostgreSQL to be reachable, a known cursor, and a lag of at most 120 ledgers behind the lake tip.

| Alert | Severity | Meaning |
|---|---|---|
| `LakeDown`, `LakeFsyncFailed`, `LakeVolumeIOStall` | page | the lake is down, failed closed, or its disk is stuck |
| `LakeDiskHigh` / `LakeDiskCritical`, `CoreDiskHigh` / `CoreDiskCritical` | warning / page | disk usage over 70 % / 85 % |
| `IngestDown`, `IngestLakeUnreachable` | page | ingest is down or cannot reach the lake |
| `PipelineStall`, `PipelineHalted` | page | the cursor is not advancing while the network is |
| `PipelineLagBehindLake`, `LedgerLatencyHigh` | warning | ingest is falling behind |
| `NetworkHalt`, `NetworkTipStale` | warning | the network tip stopped advancing, or cannot be read |
| `ArchiveTipUnknown`, `ArchiveStalled`, `ArchiverBlocked`, `ArchiveGaps` | page | archiving is not progressing, or the archive has gaps |
| `GalexieDown`, `GalexieRestarting`, `GalexieUploaderLag` | warning / page | Galexie is not exporting |
| `WatchdogRestartedGalexie`, `WatchdogCondition` | page / warning | the watchdog intervened |
| `ConflictRows`, `StateDrift` | warning | data inconsistencies that should never happen |

## Development

Code lives in `worker/` (module `stellar-explorer/worker`); this folder holds only the host's files and `files.go`, which embeds `deploy/` into `lab`.

```sh
cd worker
make test        # go test -race (lake tests first, then the rest)
make check       # fmt, vet, test, linux build
make test-live   # opt-in, needs internet: decode and validate the newest 2,000 AWS-lake ledgers; with
                 # TEST_DATABASE_URL also a 2,000-ledger replay into PostgreSQL, a rerun (0 conflicts) and a reindex
make alerts      # promtool check rules + rule tests on ../galexie/deploy/alerts.yml
```

Database tests use `TEST_DATABASE_URL` (a local PostgreSQL 18 role with `CREATEDB` and `CREATEROLE`) and are skipped when it is unset; each test creates and drops its own database.

**Local end-to-end run.** `worker dev-feed` stands in for Galexie by copying ledgers from the AWS public lake into a local lake, so the whole pipeline (lake, migrate, bootstrap, ingest, reindex) can run on a workstation against a local PostgreSQL:

```sh
D=/tmp/worker-dev && mkdir -p $D/lake/state $D/etc && echo dev > $D/lake/state/lake-id
( umask 027
  printf 'AWS_ACCESS_KEY_ID=%s\nAWS_SECRET_ACCESS_KEY=%s\nAWS_EC2_METADATA_DISABLED=true\n' \
    "$(openssl rand -hex 10)" "$(openssl rand -hex 32)" > $D/etc/gateway.env
  cat > $D/etc/worker.toml <<EOF
[credentials]
gateway_env = "$D/etc/gateway.env"
archive_env = "$D/etc/archive.env"
[lake]
dir = "$D/lake"
allow_unmounted = true
[archive]
enabled = false
[postgres]
dsn = "postgres://$USER@localhost:5432/worker_dev?sslmode=disable"
[metrics]
core_dir = "$D"
EOF
)
createdb worker_dev
cd worker && make build
w() { c=$1; shift; bin/worker "$c" --config "$D/etc/worker.toml" "$@"; }

w lake &                                                      # local S3 endpoint and control endpoint
w migrate
A=$(( $(curl -s https://horizon.stellar.org/ | sed -n 's/.*"history_latest_ledger": *\([0-9]*\).*/\1/p') - 60 ))
w bootstrap --history-only --at $A                            # seed the cursor without state
w dev-feed --start $A --end $((A + 20)) --speed 0             # 21 ledgers, as fast as possible
w ingest &                                                    # commits them
curl -s 127.0.0.1:9102/metrics | grep -E '^(ingest_cursor|ingest_lag_ledgers)'
w reindex --from $A --to $((A + 5)) --dry-run
pkill -f "$D/etc/worker.toml"; dropdb --force worker_dev; rm -rf $D
```

Some behaviour can only be tested on a Linux host: the lake's `O_TMPFILE` and `syncfs` paths, the real Galexie container under the systemd units, captive-core rebuild timings, nftables, and `lab galexie` end to end (twice on a fresh server, where the second run must change nothing, then `check`, a reboot and `check` again).

## Files

| `deploy/` file | On the host | Step |
|---|---|---|
| `sshd-hardening.conf`, `journald.conf`, `needrestart.conf` | `/etc/ssh/sshd_config.d/`, `/etc/systemd/journald.conf.d/`, `/etc/needrestart/conf.d/` | `system` |
| `galexie-host.nft`, `systemd/stellar-firewall.service` | `/etc/nftables.d/stellar-galexie.nft`, `/etc/systemd/system/` | `firewall` |
| `lake.fstab` | a line in `/etc/fstab` | `disks` |
| `docker.list`, `unattended-upgrades-docker.conf`, `daemon.json` | apt sources, apt config, `/etc/docker/daemon.json` | `docker` |
| `worker.toml`, `galexie.toml`, `galexie-repair.toml`, `galexie-archive.toml` | `/etc/stellar/` | `config` |
| `gateway.env.example`, `archive.env.example`, `archive-aws.env.example` | `/etc/stellar/*.env` (generated or filled in) | `config` |
| `systemd/stellar-{lake,ingest,galexie}.service`, `stellar-{watchdog,gapcheck}.{service,timer}` | `/etc/systemd/system/` | `units` |
| `systemd/stellar-galexie.service.d/m4-start.conf.example` | not installed | an optional fixed start ledger |
| `archive-policy.json` | applied to the bucket from a workstation | |
| `deploy-ingest.sh`, `deploy-lake.sh` | run from a copy | deploys |
| `alerts.yml`, `alerts_test.yml` | psql host, `/etc/prometheus/rules/` | `lab psql` monitoring |
