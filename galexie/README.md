# Galexie host

Runbook for the explorer's ingestion host: **Galexie** (SDF's ledger exporter, with captive stellar-core) and the **Go worker** (the repository's `worker/`) on a Hetzner Cloud **CCX33** in **HEL1**, plus the **archive bucket** on Hetzner Object Storage. It covers what Galexie does here, ordering and preparing the server, the disks, the archive bucket, setting the host up with **`lab galexie`**, the first run, day-2 operations (deploys, protocol upgrades, failure runbooks, metrics and alerts) and development.

The explorer has one other host, the **psql host**, whose runbook is [`psql/README.md`](../psql/README.md): PostgreSQL (the database this host writes, its roles and TLS), the public API, and Prometheus and Alertmanager, which also scrape and alert for this host.

**`lab galexie`** (the repository's CLI, `cli/`) installs every file in [`deploy/`](deploy/) and brings the host to the state this README describes; `lab galexie check` reports whether it is there and healthy (§4). The files are written with the example values (§0.5), so they read correctly as they are, and `go test -run TestDeploy` in `worker/` checks them: the units' coupling, restart policies and timeouts, configs agreeing with each other, alert rules using only exported metrics, deploy scripts touching only their own symlink and unit.

## Contents

0. [Galexie in this setup](#0-galexie-in-this-setup)
1. [Server](#1-server)
2. [Disks](#2-disks)
3. [Archive bucket on Hetzner Object Storage](#3-archive-bucket-on-hetzner-object-storage)
4. [Setup with `lab galexie`](#4-setup-with-lab-galexie)
5. [Configuration](#5-configuration)
6. [First run](#6-first-run)
7. [Operations](#7-operations)
8. [Development](#8-development)
9. [File map](#9-file-map)

---

## 0. Galexie in this setup

### 0.1 What runs here

```text
Stellar network (peers :11625, history archives) ─► captive stellar-core inside the Galexie container
stellar-galexie.service   docker run stellar/stellar-galexie:29.0.0@sha256:… append --start ${START}
        │ one PUT per ledger: http://127.0.0.1:9000, SigV4, If-None-Match: *, CRC32C
        ▼
stellar-lake.service      worker-lake lake: embedded VersityGW on /srv/lake (Hetzner Volume, xfs)
        │                 PUT → fsync → link → fsync → tip; control 127.0.0.1:9001 (/v1/tip long-poll)
        │ commit notification + GET from the page cache
        ▼
stellar-ingest.service    worker ingest
        ├─ sequencer: cursor + 1 → decode → processLedger → one Postgres transaction (10.0.1.2:5432, TLS)
        ├─ archiver:  lake → archive bucket, 1:1, strictly in order (hel1.your-objectstorage.com)
        └─ pruner:    deletes old lake objects, only once archived and outside retention
stellar-watchdog.timer    every 30 s: restarts Galexie only if its upload is hung
stellar-gapcheck.timer    weekly: Galexie's detect-gaps over [archive_origin, archive tip]
```

| Unit | Runs | As | Purpose |
|---|---|---|---|
| `stellar-firewall.service` | `nft -f /etc/nftables.d/stellar-galexie.nft` | root | private-interface filter (§1.4) |
| `stellar-lake.service` | `/usr/local/bin/worker-lake lake` | `stellar` | Galexie's S3 endpoint `127.0.0.1:9000`; control `127.0.0.1:9001`; metrics `10.0.0.2:9101` |
| `stellar-galexie.service` | `docker run … stellar/stellar-galexie … append` | root (container) | captive core → one SEP-54 object per ledger → the lake |
| `stellar-ingest.service` | `/usr/local/bin/worker ingest` | `stellar` | sequencer → Postgres, archiver → archive bucket, pruner; metrics `127.0.0.1:9102` and `10.0.0.2:9102` |
| `stellar-watchdog.timer` → `.service` | `/usr/local/bin/worker-lake watchdog` | root | every 30 s (§7.4.11) |
| `stellar-gapcheck.timer` → `.service` | `/usr/local/bin/worker gapcheck` | root | weekly (§7.4.10) |

**Galexie depends only on the lake.** Deploying or crashing `ingest` never touches Galexie or the lake; a Postgres outage only makes ingest wait while the lake absorbs the backlog. The lake runs its own binary (`worker-lake`), which changes only with a lake deploy (§7.2).

**How the units are coupled** (checked by `go test -run TestDeployUnits` in `worker/`):

- **Galexie ↔ lake: `Wants=` + `PartOf=` + `After=`.** A stop or restart of the lake reaches Galexie, which stops first (it drains into the lake) and starts again only after the lake's READY; a held Galexie stays stopped, because propagated restarts are try-restarts. A lake crash doesn't stop Galexie: at the lake's restart a running Galexie is restarted after READY, and one that has exited comes back through its own `Restart=always` (its launcher refuses until the lake answers). `BindsTo=` is deliberately not used: in systemd 255 a lake crash would stop Galexie with a pending stop job that suppresses its `Restart=always`, and the lake's restart sends only a try-restart, so Galexie would stay down.
- **Ingest ↔ lake: `Wants=` + `After=` only**, for the boot order: an ingest restart never touches the lake or Galexie, and a lake restart doesn't restart ingest (it reconnects).
- **Galexie's container:** the `-` on `EnvironmentFile` is required, since systemd reads the file before `ExecStartPre` writes it; `--name` with the pre-start `docker rm -f` prevents an orphaned container (two cores on `/srv/core`); `--init` forwards signals and reaps captive core's children; `TimeoutStartSec=120` covers the launcher's bounded archive probe.

State on disk:

| Path | What |
|---|---|
| `/srv/core` | captive core's `storage_path` (local NVMe): its DB and buckets |
| `/srv/lake/s3/stellar/ledgers/pubnet/` | the lake: SEP-54 objects and Galexie's manifest `.config.json` (bucket `stellar`, prefix `ledgers/pubnet`) |
| `/srv/lake/s3/stellar/.sgwtmp/`, `/srv/lake/s3/.vgwlocks/` | VersityGW's staging and lock directories; never touch them |
| `/srv/lake/state/lake-id` | marker: the lake refuses to start without it (so objects never land on the root disk) |
| `/srv/lake/state/boot-id` | the boot of the last start; a different boot triggers the checksum scan of the newest 100 objects |
| `/srv/lake/state/bootstrap.json` | where the archive starts (`archive_origin`), written once by `worker bootstrap` |
| `/run/stellar-galexie/galexie.env` | `START=…`, written by `worker galexie-start` before every Galexie start |
| `/var/lib/stellar-watchdog/state.json` | the watchdog's state; ingest exports it as metrics |
| `/var/lib/stellar-gapcheck/result.json` | the last gap check; ingest exports it as metrics |

### 0.2 Galexie

- **What it is.** Galexie runs a **captive stellar-core** (bundled in the image) that streams `LedgerCloseMeta`, and writes each ledger as a zstd-compressed XDR `LedgerCloseMetaBatch` object in the **SEP-54** layout: `ledgers_per_file = 1`, `files_per_partition = 64000`, the same layout as the AWS public lake. Object keys sort newest first: ledger 123456 is `FFFF05FF--64000-127999/FFFE1DBF--123456.xdr.zst`. Each object carries 9 `x-amz-meta-*` keys (start/end ledger and close time, protocol and core version, network passphrase, compression, Galexie version). The manifest `<prefix>/.config.json` pins the schema and the network.
- **Mode.** `append`: export from `--start` and follow the network forever.
- **Where it uploads.** Only to the local lake, `http://127.0.0.1:9000`, bucket path `stellar/ledgers/pubnet`, region `us-east-1`, with the gateway keys from `/etc/stellar/gateway.env` (`docker --env-file`). Uploads are strictly sequential, one PUT per ledger with `If-None-Match: *`; a 412 means "already exported".
- **How it resumes.** At `max(--start, newest_in_store + 1)`, from one `ListObjectsV2`; it never checks holes or hashes. `--start` is computed on every launch by `worker galexie-start`:

  | Lake | Archive | Start |
  |---|---|---|
  | has ledgers | any (the probe is bounded to 9.5 s; an archive outage never blocks the start) | `max(lake tip, archive tip if known) + 1` |
  | empty | known tip | archive tip + 1 (lake lost) |
  | empty | empty, `bootstrap.json` present | `archive_origin` (the first run) |
  | empty | empty, no `bootstrap.json` | refuse: run `worker bootstrap` first |
  | empty | unknown (error or timeout) | refuse and retry every 5 s |

  The Postgres cursor is not an input. If the cursor is ahead, the sequencer waits while Galexie re-exports; if it is behind, the sequencer reads the archive.
- **How it fails.**
  - On an upload error it cancels and **exits 0**, so the unit uses `Restart=always` (5 s), and the launcher recomputes `--start` each time.
  - On SIGTERM it drains for up to 10 s (`docker stop -t 45`, `TimeoutStopSec=60`).
  - A manifest mismatch (schema or passphrase) makes it exit 1 on every start.
  - **Captive core rebuilds its DB** (`new-db` + catchup) on every restart, even after a clean stop: it logs "The next launch will catchup from the network afresh". On pubnet that takes about 25 minutes before uploads resume: downloading the buckets (~15 GB), applying them, then catching up to the network.
- **Tools** (same image, one-off containers):
  - `scan-and-fill --start A --end B`: exports only the missing ledgers of `[A, B]` (with `If-None-Match`); the hole repair (§7.4.8, §7.4.10). `--end` must be greater than `--start`.
  - `detect-gaps --start A --end B --output-file …`: read-only report; run weekly by `worker gapcheck`.
  - `replace`: re-exports and **overwrites** existing objects. Never use it on the lake or the archive in normal operations.

  Bounded modes run captive core in a temporary directory `captive-core-<random>` under `storage_path`, so a repair container can share `/srv/core` with the running Galexie (which uses `captive-core/`). It still runs a full catchup: expect tens of minutes and about 15 GB of buckets in `/srv/core` (untested in this mode).
- **Where to look.** `journalctl -u stellar-galexie` (the container's output; `docker logs stellar-galexie` while it runs), and its admin port `:6061/metrics`: `galexie_export_manager_latest_ledger`, `galexie_uploader_latest_ledger`, `galexie_ingest_captive_stellar_core_new_db`.
- **Upgrades.** Bump the image tag **and** digest (§7.3).

### 0.3 The worker

One Go binary, installed twice so that an ingest deploy never touches the lake: `/usr/local/bin/worker-lake` runs `lake`, `galexie-start` and `watchdog`; `/usr/local/bin/worker` runs everything else. Both are symlinks to versioned builds in `/usr/local/lib/stellar/`. Every command takes `--config` (default `/etc/stellar/worker.toml`); exit status 0 = ok, 1 = error, 2 = usage.

| Command | Run by | What |
|---|---|---|
| `worker lake` | `stellar-lake.service` | Galexie's S3 endpoint: embedded VersityGW, fsync chain, commit tip |
| `worker ingest` | `stellar-ingest.service` | sequencer(s), archiver, pruner, metrics |
| `worker galexie-start --out <file>` | Galexie's `ExecStartPre` | checks the lake answers, computes `--start`, writes `START=…` atomically |
| `worker watchdog` | `stellar-watchdog.timer` | restarts Galexie only if its upload is hung |
| `worker gapcheck` | `stellar-gapcheck.timer` | `detect-gaps` over `[archive_origin, archive tip]` |
| `worker bootstrap` | `lab galexie` (step `bootstrap`), as `stellar` | first run: state from the latest history-archive checkpoint, cursor, `bootstrap.json` |
| `worker bootstrap --force` | operator, ingest stopped | state repair; the cursor only moves forward |
| `worker bootstrap --recreate-origin` | `lab galexie` (step `bootstrap`), or the operator, as `stellar` | recreate a lost `bootstrap.json` from Postgres |
| `worker bootstrap --history-only --at A` | dev/test only | seed the cursor at `A − 1` without state |
| `worker migrate` | `lab galexie` (step `migrate`), `deploy-ingest.sh --migrate`; as root | embedded SQL migrations (as `explorer_owner`) |
| `worker migrate --check` | `lab galexie` | read-only: lists the pending migrations, exit 1 if there are any |
| `worker reindex --from A --to B [--source lake\|archive\|aws] [--dry-run]` | operator | re-read a closed range and replace its `ledgers` rows; `--dry-run` only validates (and is the gap check's fallback) |
| `worker dev-feed --start A [--end B] [--speed X]` | dev/test only | behaves like Galexie, copying from the AWS public lake |
| `worker smoke-archive --bucket <scratch> [--delete-probe-only]` | operator | provider compatibility test (§3d) |
| `worker version` | | prints the build version |

`worker help` and `worker <command> -h` print the same.

### 0.4 Traffic

| From | To | Port | What |
|---|---|---|---|
| Galexie (host network) | `127.0.0.1` | 9000 | S3 PUTs into the lake |
| ingest, launcher, watchdog | `127.0.0.1` | 9000, 9001 | lake S3 and control (`/v1/tip`, `/metrics`) |
| watchdog | `127.0.0.1` | 6061 | Galexie's metrics |
| psql host `10.0.1.2` (Prometheus) | `10.0.0.2` | 9101, 9102, 6061; ICMP echo | lake, ingest and Galexie metrics; ping (`lab psql check`, the MTU test) |
| this host (ingest, migrate, bootstrap, reindex, `lab galexie check`) | psql host `10.0.1.2` | 5432 (TLS) | Postgres |
| this host (archiver, gapcheck) | `hel1.your-objectstorage.com` | 443 | archive bucket |
| this host (ingest, watchdog, dev-feed) | `aws-public-blockchain.s3.us-east-2.amazonaws.com` | 443 | network tip; last-resort ledger source |
| captive core | Stellar peers | 11625 | overlay (outbound only; its own peer port 11725 is closed) |
| captive core, `worker bootstrap` | SDF history archives | 80, 443 | catchup (`http://` in Galexie's config), checkpoint state |
| this host | Docker Hub, `download.docker.com`, apt mirrors | 443 | images, packages |
| admin IP | public IP | 22 | SSH |

### 0.5 Example values

Written literally into the deploy files and snippets, so that they are valid as shown. `lab galexie` asks for the settings below once (§4.2) and installs each file with every example value replaced by yours. The private addresses match psql/README.md.

| Setting | Example | Where it comes from |
|---|---|---|
| `private_ip`: this host's private IP | `10.0.0.2` | Cloud Network `stellar` (psql/README.md, private network), set when attaching (§1.1) |
| `postgres_ip`: the psql host's private IP | `10.0.1.2` | psql/README.md (private network); Postgres and Prometheus both run there |
| `private_if`: the private interface | `enp7s0` | `ip -br link` (`ens10` on older server types) |
| `lake_device`: the lake Volume | `/dev/disk/by-id/scsi-0HC_Volume_100123456` | `ls -l /dev/disk/by-id/` once the Volume is attached (§2.2); `100123456` is its ID in the Console |
| `archive_bucket` | `stellar-ledgers-pubnet` (production), `stellar-ledgers-pubnet-staging` (for testing) | §3 |
| `archive_endpoint` | `https://hel1.your-objectstorage.com` | §3 |
| `archive_region` | `hel1` | §3 |
| admin IP (not a setting) | `203.0.113.10` | your workstation or bastion; only in the Cloud Firewall (§1.2) |
| server project (not a setting) | `stellar` | holds the servers, the Cloud Network, and the archive uploader key |
| archive project (not a setting) | `stellar-archive` | holds only the archive bucket(s) and their admin key (§3) |

The archive uploader key is a secret, not a setting (§4.2).

### 0.6 Pinned versions

| Component | Version | Where it is pinned |
|---|---|---|
| Galexie image | `stellar/stellar-galexie:29.0.0@sha256:5269dfd9034948f3db8dd7595fe8aec88aae5480f712a78e2dea48a841d495a3` (manifest list, from Docker Hub; bundles stellar-core `29.0.0-3589.4eb833373.noble`, Galexie `c927ffc`) | `deploy/systemd/stellar-galexie.service`, `deploy/worker.toml` `[galexie] image`, `deploy/galexie-repair.toml` (comment) |
| Worker | `worker/`: Go 1.26, VersityGW v1.8.0, go-stellar-sdk v0.7.3, pgx v5.11.0 | `worker/go.mod` |
| Docker Engine | current `docker-ce` from Docker's apt repository | excluded from unattended upgrades (§4.3, step `docker`) |
| `lab galexie` | Go 1.26, `charm.land/fang/v2` v2.0.1, `huh/v2` v2.0.3, `lipgloss/v2` v2.0.6, cobra v1.10.2, BurntSushi/toml v1.6.0 | `go.mod` |
| OS | Ubuntu 24.04 LTS | |

---

## 1. Server

### 1.1 Order

Hetzner Console, project `stellar` (the one with the Cloud Network): **Servers → Add server**:

- Location **Helsinki (HEL1)**; image **Ubuntu 24.04**; type **CCX33** (8 dedicated vCPU, 32 GB RAM, 240 GB local NVMe; check availability when ordering). In the EU, CCX is orderable only in NBG1 and HEL1. Galexie's minimum is 16 GB RAM, 4 vCPU and 100 GB at ≥ 5K IOPS.
- Networking: public IPv4 and IPv6. Leave the private network out here (the Console would pick its address). After creation, **Networking → Attach to network** `stellar` with the IP **`10.0.0.2`**, which the psql host's `pg_hba.conf`, firewall and Prometheus expect (the setting `private_ip`).
- SSH key: yours. Name: `stellar-galexie1`.
- Firewall: the one from §1.2. Volume: created separately in §2.2.
- Server backups: not needed (the Volume is not part of them anyway, and everything else is rebuilt from this README).

The local NVMe holds the OS, Docker and `/srv/core`. The lake lives on a Volume (§2.2) so that it survives a server rebuild.

### 1.2 Cloud Firewall

Console → **Firewalls → Create firewall** `stellar-galexie`:

| Direction | Protocol | Port | Source | Why |
|---|---|---|---|---|
| inbound | TCP | 22 | `203.0.113.10/32` (admin) | SSH |

Nothing else is inbound: Galexie's admin port 6061 and captive core's peer port 11725 bind to all interfaces and must stay closed on the public side. Add **no outbound rules**: once any outbound rule exists, outbound becomes deny-by-default. If you ever restrict outbound, allow at least TCP 443 and 80 (SDF's history archives in captive core's config use `http://`), TCP 11625 (peers) and UDP 123 (NTP). Apply the firewall to `stellar-galexie1`.

Cloud Firewalls **don't filter the private network**, so the host filters it (§1.4).

### 1.3 Base setup (step `system`)

Before the first `lab galexie` run, bring the OS up to date and check that your key and the private network are in place:

```sh
ssh root@<public IP>
apt-get update && apt-get -y full-upgrade && reboot   # picks up a new kernel, if any
cat /root/.ssh/authorized_keys           # your key: after the system step, keys are the only way in
ip -br addr                               # enp7s0 (or ens10) UP with 10.0.0.2/32 (Hetzner configures it by DHCP)
ip route | grep 10.0.0.0/16               # 10.0.0.0/16 via 10.0.0.1 dev enp7s0
```

The `system` step (§4.3) then sets the same baseline as on the psql host:

- **Packages:** chrony (time sync), nftables, xfsprogs (the lake's filesystem), postgresql-client (`psql`, for its checks), curl, ca-certificates, unattended-upgrades.
- **UTC** as the timezone.
- **sshd allows keys only** ([`deploy/sshd-hardening.conf`](deploy/sshd-hardening.conf) → `/etc/ssh/sshd_config.d/10-hardening.conf`). The step refuses while `/root/.ssh/authorized_keys` is empty, and checks the configuration with `sshd -t` before it reloads ssh (it puts the previous file back, or removes the new one, if the check fails).
- **The journal is capped** at 2 GB on `/` ([`deploy/journald.conf`](deploy/journald.conf) → `/etc/systemd/journald.conf.d/60-stellar.conf`).
- **needrestart never restarts Docker or the stellar units** ([`deploy/needrestart.conf`](deploy/needrestart.conf) → `/etc/needrestart/conf.d/60-stellar-docker.conf`). Leave `unattended-upgrades` on (Ubuntu security updates) with its default `Automatic-Reboot "false"`; reboots are scheduled by hand (§7.4.16). After each update Ubuntu's `needrestart` restarts the services that still use a replaced library. A restart of `docker` or `containerd` kills Galexie's container, an unclean stop that makes captive core rebuild, so these are listed but left alone; the worker is a static binary and never needs it.
- **The service user `stellar`**: a system user with no home and no shell (`sudo -u stellar` is how operators run commands as it).

### 1.4 Host firewall (step `firewall`)

The rules for the private interface are in [`deploy/galexie-host.nft`](deploy/galexie-host.nft): inbound only TCP 9101 (lake), 9102 (ingest) and 6061 (Galexie) from the psql host (Prometheus) and ping from the psql host (`lab psql check`, the MTU test), plus DHCP; outbound only 5432 to the psql host, plus DHCP and replies. Everything else on that interface is dropped, including captive core's peer port 11725. The public interface is left to the Cloud Firewall, so your SSH session is unaffected.

Docker manages its own nftables chains on this host, so this ruleset **never** runs `flush ruleset`: it lives in its own table `inet stellar_galexie` and replaces only that table, atomically. The `firewall` step:

- installs it as `/etc/nftables.d/stellar-galexie.nft`, with your `private_if` and `postgres_ip`;
- installs [`deploy/systemd/stellar-firewall.service`](deploy/systemd/stellar-firewall.service), which loads it early at boot, and enables and starts it; a changed file is applied with `systemctl reload stellar-firewall` (`nft -f`: a file that doesn't load fails the step and changes no rule);
- disables Ubuntu's `nftables.service` if it is enabled, without stopping it: its `/etc/nftables.conf` starts with `flush ruleset`, and stopping it flushes everything, Docker's chains included.

Keep `ufw` inactive (Ubuntu cloud images ship it inactive; check `ufw status`); the step doesn't look at it. The step rewrites the rules file whenever it differs from the repository's, so change the rules in `deploy/galexie-host.nft` and run `lab galexie` again, not on the host. `lab galexie check` reports whether the table is loaded (`nft list table inet stellar_galexie`).

---

## 2. Disks

| Mount | Device | Contents | Alerts |
|---|---|---|---|
| `/` | local NVMe, 240 GB | OS, Docker, `/srv/core` (captive core) | `CoreDiskHigh` 70 %, `CoreDiskCritical` 85 % |
| `/srv/lake` | Hetzner Volume, 200 GB, xfs | the lake (`s3/`) and its `state/` | `LakeDiskHigh` 70 %, `LakeDiskCritical` 85 % |

Separate filesystems keep captive core safe when the lake fills up, and the other way round.

### 2.1 `/srv/core` (local NVMe)

The `disks` step creates it, `root:root 0750` (the Galexie container runs as root).

### 2.2 `/srv/lake` (Hetzner Volume)

Console → **Volumes → Create volume**: size **200 GB**, location **HEL1**, server `stellar-galexie1`, mount option **Manual**, filesystem **XFS**. Do not use *Automatic*: it mounts under `/mnt/HC_Volume_*`, and the lake must sit exactly on `/srv/lake`.

On the server, `ls -l /dev/disk/by-id/ | grep HC_Volume` shows it (`scsi-0HC_Volume_100123456 -> ../../sdb`); the setting `lake_device` is that path, `/dev/disk/by-id/scsi-0HC_Volume_100123456`. The `disks` step then:

1. formats it with `mkfs.xfs` **only** when `blkid -p` finds nothing on it, and only once you approve (`--yes` approves). A device holding another filesystem or a partition table stops the step; it is never formatted, and `mkfs.xfs` runs without `-f`, so it refuses anything it recognises itself;
2. adds the line of [`deploy/lake.fstab`](deploy/lake.fstab) to `/etc/fstab`: the device by id, `xfs`, `discard,nofail,defaults 0 2`. Any other line for `/srv/lake` or for the same device (the *Automatic* mount, say) stops the step: remove it by hand;
3. mounts `/srv/lake`;
4. once the Volume is mounted there (never on the root disk): creates `s3/` and `state/` (`stellar:stellar 0750`) and, if it is missing, the marker `state/lake-id` (the device's by-id name). An existing marker is never rewritten.

`lab galexie check` shows the usage of both filesystems.

- xfs always supports user xattrs, which the lake needs (it stores the object metadata and checksums in them, and checks support at startup).
- The lake refuses to start unless `/srv/lake` is a mountpoint holding `state/lake-id`. `nofail` lets the server boot without the Volume; `stellar-lake.service` has `RequiresMountsFor=/srv/lake`, so it waits for the mount instead of writing to the root disk.
- **Sizing.** 72 h of retention is ~10 GB (3.3 GB/day). The rest is headroom for an archive outage: the pruner deletes only archived ledgers, so the lake grows ~3.3 GB/day until the archive is back, which gives ~55 days on 200 GB. Grow the Volume for a longer window (§2.3).
- The Volume is replicated 3× by Hetzner and can be re-attached to a replacement server **in the same location** (§7.4.17). It is not covered by server backups or snapshots; the archive bucket is its backup.

### 2.3 Growing the Volume

Online, without stopping anything: Console → Volumes → `…` → **Resize**, then:

```sh
xfs_growfs /srv/lake
df -h /srv/lake
```

---

## 3. Archive bucket on Hetzner Object Storage

The archive is a 1:1 copy of the lake (same keys, bytes, metadata and manifest), kept forever. Stock SEP-54 tooling can read it, the sequencer reads it when the lake no longer holds a ledger, and the launcher starts Galexie from its tip if the Volume is lost.

**Why the setup looks like this:**

- Hetzner S3 keys have read/write on **every** bucket of their project. So the archive bucket lives in its own project, `stellar-archive`, and the key that writes it (the **uploader key**) belongs to the server project `stellar`, reaching the archive only through the bucket policy. Keep the server project free of buckets: the uploader key could do anything to them.
- The uploader may put, get and list, but not delete objects or change the bucket's policy. Residual risk: a PUT to an existing key still overwrites it (the bucket is not versioned), which is accepted because the AWS lake or a re-export can restore public chain data. The archiver itself always uses `If-None-Match: *`.
- Conditional PUT (`If-None-Match`) isn't supported on versioned or Object-Lock buckets, so neither is enabled.
- Hetzner Object Storage accepts the archiver's CRC32C but doesn't keep it: reads return no `x-amz-checksum-crc32c`. Where a store keeps no CRC32C, the archiver checks against the ETag instead, which for a single-part upload is the MD5 of the bytes (`sep54.VerifyStored`): the ETag each PUT returns, and the archive's newest object when it starts. The lake keeps CRC32C and is checked by it.
- NBG1 Object Storage currently throttles with 503s, so the bucket goes in HEL1, next to the servers. Traffic within eu-central is free.
- Growth: ~1.2 TB/year.

The commands below run on your **workstation** with the AWS CLI v2. The admin key never goes to a server. Newer AWS CLI versions send checksum headers that some S3-compatible providers reject; the two `AWS_*_CHECKSUM_*` variables keep the CLI to the classic behaviour (untested with Hetzner):

```sh
export AWS_REQUEST_CHECKSUM_CALCULATION=when_required AWS_RESPONSE_CHECKSUM_VALIDATION=when_required
S3="--endpoint-url https://hel1.your-objectstorage.com --region hel1"
```

**a) Project, bucket, admin key.** Console → **Create project** `stellar-archive`. In it: **Object Storage → Create bucket** `stellar-ledgers-pubnet`, location **Helsinki (hel1)**, visibility **Private**, **no Object Lock** (it can only be chosen at creation). Never enable versioning. Turn on the bucket's **protection** (bucket menu → *Protection*; it prevents only the bucket's deletion). Then **Security → S3 credentials → Generate credentials** `archive-admin`: this is the **admin key**. Store it in the password manager and keep it off every server.

**b) Uploader key.** In the server project `stellar`: **Security → S3 credentials → Generate credentials** `archive-uploader`. Note the project's numeric ID (in the Console URL, `…/projects/<id>/…`) and the access key. `lab galexie` asks for the key once (§4.2) and writes it into two files on the server (§4.4): `/etc/stellar/archive.env` (`root:stellar 0640`; `ARCHIVE_S3_*` names, read by the worker) and `/etc/stellar/archive-aws.env` (`root:root 0600`; `AWS_*` names, read only by the gap check's container).

**c) Bucket policy** ([`deploy/archive-policy.json`](deploy/archive-policy.json)), with the admin key: allow `s3:PutObject`, `s3:GetObject`, `s3:ListBucket` to the uploader, deny it `s3:DeleteObject`, `s3:PutBucketPolicy`, `s3:DeleteBucketPolicy`, `s3:DeleteBucket`. The principal is `arn:aws:iam:::user/p<server project id>:<uploader access key>` (untested).

```sh
read -r AWS_ACCESS_KEY_ID; read -rs AWS_SECRET_ACCESS_KEY; export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY   # the admin key
sed -e 's/<galexie_project_id>/1234567/' -e 's/<uploader_access_key>/UPLOADERACCESSKEY/' \
  galexie/deploy/archive-policy.json > /tmp/archive-policy.json
aws s3api put-bucket-policy $S3 --bucket stellar-ledgers-pubnet --policy file:///tmp/archive-policy.json
aws s3api get-bucket-policy $S3 --bucket stellar-ledgers-pubnet
```

**d) Verify the provider with a scratch bucket** (after the first `lab galexie` run, which installs the worker and `archive.env`; §6 step 4). In the archive project, create `stellar-ledgers-smoke-20261001` exactly like the archive bucket (HEL1, private, no Object Lock), and give it the same policy with its own ARNs:

```sh
sed -e 's/stellar-ledgers-pubnet/stellar-ledgers-smoke-20261001/g' /tmp/archive-policy.json > /tmp/smoke-policy.json
aws s3api put-bucket-policy $S3 --bucket stellar-ledgers-smoke-20261001 --policy file:///tmp/smoke-policy.json
```

On the server, with the uploader key (`archive.env`):

```sh
sudo -u stellar worker smoke-archive --bucket stellar-ledgers-smoke-20261001
```

It prints one PASS/FAIL line per check and exits 1 if any failed: listing with the uploader key (cross-project access), PUT with the SDK's default checksum, the archiver's PUT (CRC32C, `If-None-Match`, 9 metadata keys, Content-Type), ETag = MD5, `ChecksumMode` GET and HEAD (the stored CRC32C, or ETag = MD5 where the store keeps none, as on Hetzner), conditional PUT → 412 (also through the SDK's `PutFileIfNotExists`), `StartAfter` listing, the SDK datastore calls, **delete denied**, and authenticated **path-style** and virtual-hosted reads (`detect-gaps` uses path-style; if path-style fails, set `[gapcheck] method = "reindex-dry-run"`, §7.4.10). The uploader cannot delete the test objects; delete the scratch bucket with the admin key:

```sh
aws s3 rb $S3 s3://stellar-ledgers-smoke-20261001 --force
```

On the archive bucket itself run only the probe that is safe there (deleting a non-existent key must be refused):

```sh
sudo -u stellar worker smoke-archive --bucket stellar-ledgers-pubnet --delete-probe-only
```

`smoke-archive` refuses the full test only for the bucket configured in `worker.toml`. During staging that is the staging bucket, so take care never to point the full test at the production bucket.

**e) Staging** (for testing) uses `stellar-ledgers-pubnet-staging`, created and verified the same way, with the policy rewritten for its ARNs (`sed -e 's/stellar-ledgers-pubnet/stellar-ledgers-pubnet-staging/g'`). On the host it is the setting `archive_bucket = "stellar-ledgers-pubnet-staging"` in `/etc/stellar/lab-galexie.toml`: `lab galexie` renders it into `[archive] bucket` in `/etc/stellar/worker.toml` and into `destination_bucket_path = "stellar-ledgers-pubnet-staging/ledgers/pubnet"` in `/etc/stellar/galexie-archive.toml`. The production bucket is first written at go-live (§7.6).

When done, `unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY` and remove `/tmp/*-policy.json`.

---

## 4. Setup with `lab galexie`

### 4.1 Build, copy, run

On the workstation (Go 1.26; §8.2): run the worker's tests, then `make dist` (at the repository root) builds `bin/lab` and, next to it, `bin/worker` (and `bin/caddy` for the psql host), all for linux/amd64 and stamped with the same version. Pick a new version for every build that goes to a server (letters, digits, `.`, `_`, `+`, `-`):

```sh
V=2026.10.01-1
TEST_DATABASE_URL=postgres://$USER@localhost:5432/stellar_explorer_test?sslmode=disable make -C worker check VERSION=$V
make check dist VERSION=$V
scp -r bin root@<public IP>:/root/
ssh -t root@<public IP> tmux new -A -s setup     # bootstrap takes a while: keep the run in tmux
/root/bin/lab galexie
```

`lab galexie` needs root and Ubuntu 24.04 amd64. It checks every step, lists what it will change and asks once (plus a separate question before it formats the Volume or runs `worker bootstrap`), then applies the steps in order and checks each one again. A second run changes nothing. On a terminal it shows a spinner while a step applies; every command and its full output go to `/var/log/stellar-setup/galexie.log`, and a failure prints its last lines.

```text
lab galexie                run: check every step, apply what is not ok, check again
lab galexie check          read-only: every step's check plus the health probes
  --config PATH              settings file (default /etc/stellar/lab-galexie.toml)
  --only a,b | --skip a,b    a subset of the steps (§4.3)
  --yes                      no prompts: approve every question, fail on a missing setting
  --allow-restart            restart running services whose binary or configuration changed (§4.5)
  --version, --help, completion, man
```

Each step prints one line: `✓` ok, `↻` changed by this run, `◷` waiting for something outside this host (the run goes on), `!` restart pending, `✗` failed (the run stops: later steps rely on earlier ones). The health probes follow. `run` exits 0 when every step is ok or waiting, `check` only when every step and every probe is ok; usage errors exit 2. `check` is also the verification after a reboot or any change (§7.1).

### 4.2 Settings and secrets

The first run asks for the settings of §0.5 in one form and saves them to `/etc/stellar/lab-galexie.toml` (root 0600). Every file it installs is the repository's, with each example value replaced by yours (whole tokens only: `stellar-ledgers-pubnet` does not touch `stellar-ledgers-pubnet-staging`).

The archive uploader key (§3b) is a secret: it is asked once in a password field, as `archive_access_key` and `archive_secret_key`, and kept in `/etc/stellar/secrets/` (root 0700, files 0600). It never goes into the settings file, the terminal or the log.

To change a setting, edit the settings file; to enter a secret again, delete its file in `/etc/stellar/secrets/`. Then run `lab galexie` again.

### 4.3 Steps

| # | Step | Installs and does | Health in `check` |
|---|---|---|---|
| 1 | `system` | packages, UTC, sshd keys only, journal cap, needrestart exemptions, user `stellar` (§1.3) | |
| 2 | `firewall` | `/etc/nftables.d/stellar-galexie.nft` and `stellar-firewall.service`; `nftables.service` disabled (§1.4) | table loaded |
| 3 | `disks` | `/srv/core`; `/srv/lake`: format (asks), `/etc/fstab`, mount, `s3/`, `state/`, `lake-id` (§2) | usage of `/` and `/srv/lake` |
| 4 | `docker` | Docker's apt key and repository ([`deploy/docker.list`](deploy/docker.list)); `docker-ce`, `docker-ce-cli`, `containerd.io`; [`deploy/daemon.json`](deploy/daemon.json) → `/etc/docker/daemon.json`; [`deploy/unattended-upgrades-docker.conf`](deploy/unattended-upgrades-docker.conf) → `/etc/apt/apt.conf.d/51stellar-docker`; the pinned Galexie image | |
| 5 | `worker` | first install only: the `worker` next to the app → `/usr/local/lib/stellar/worker-<version>`, symlinks `worker` and `worker-lake` | |
| 6 | `config` | `/etc/stellar`: `worker.toml`, the three Galexie TOMLs, `gateway.env`, `archive.env`, `archive-aws.env`, the database bundle (§4.4) | TLS logins as `ingest_writer` and `explorer_owner` |
| 7 | `units` | `deploy/systemd/*` → `/etc/systemd/system/`, then `daemon-reload` | |
| 8 | `migrate` | `worker migrate` while `worker migrate --check` lists pending migrations | |
| 9 | `bootstrap` | once: `worker bootstrap` as `stellar` (asks) when there is no `bootstrap.json` and no cursor | |
| 10 | `services` | enables and starts `stellar-lake`, `stellar-ingest`, `stellar-galexie` and both timers | lake `/healthz` and tip, Galexie's last upload, ingest `/readyz`, cursor and lag, archive tip |

The first three steps are explained in §1.3, §1.4 and §2. The others:

- **`docker`.** Docker Engine comes from Docker's apt repository, whose key is checked against a pinned sha256. `daemon.json` selects the `local` log driver and is written before Docker first starts. A `dockerd` restart kills the Galexie container, so Docker is never upgraded unattended (unattended-upgrades only installs from Ubuntu's origins anyway; the blacklist makes it explicit) and needrestart leaves it alone (§1.3): upgrade it by hand with Galexie held (§7.4.18). The image pinned in `stellar-galexie.service` is pulled ahead, because the unit's 120 s start timeout doesn't cover a pull. The `stellar` user is **not** in the `docker` group; only root runs containers.
- **`worker`.** The first install only: later versions go through the deploy scripts (§7.2), which also restart the service and wait for its health. The step's line shows the installed versions. Like the scripts, it never replaces an installed version with other bytes.
- **`config`.** The files, owners and modes are in §4.4 and §9. It waits (◷) until the database bundle is there.
- **`units`.** The units' coupling is explained in §0.1 and checked by `go test -run TestDeployUnits`. `stellar-firewall.service` belongs to the firewall step; `systemd/stellar-galexie.service.d/m4-start.conf.example` (a fixed start ledger instead of the launcher's) is not installed.
- **`migrate`.** Runs as root, so it can read `pgpass-owner`. `check` shows "schema current" once nothing is pending.
- **`bootstrap`.** Runs once (§6). It is long, so it asks first, and its output streams to the log. Bootstrap truncates the state tables; `ingest_writer` may do that through the default privileges lab psql set before `worker migrate` created them, and the step waits (◷) if it can't (a database set up otherwise: lab psql's `api` step catches it up). With a cursor in Postgres but no `bootstrap.json` (a new Volume, §7.4.17) it runs `worker bootstrap --recreate-origin` instead, which is quick and never overwrites.
- **`services`.** Waits (◷) for `bootstrap.json`: without it Galexie's launcher refuses to start. Running services are restarted only as §4.5 says.

**Never overwritten.** A file of the `system`, `firewall` or `docker` step that is in the way and that `lab galexie` doesn't recognise (no `(lab galexie, …)` header, and not a package's unmodified default) stops its step with a message: compare it with the repository's file, then move it aside. JSON has no comments, so `daemon.json` carries no header: once the repository's version changes, move the installed one aside too.

### 4.4 Credentials

All env files are plain `KEY=VALUE` lines, **unquoted** (`docker --env-file` keeps quotes literally), and the worker refuses any file that other users can read. The worker parses them **by path**; no worker process ever has `AWS_*` variables in its environment.

- **Gateway keys** (`/etc/stellar/gateway.env`, `root:stellar 0640`; [`deploy/gateway.env.example`](deploy/gateway.env.example)): the lake's root keys. The `config` step generates the file once, exactly three lines: `AWS_ACCESS_KEY_ID` (10 random bytes, hex), `AWS_SECRET_ACCESS_KEY` (32 random bytes, hex) and `AWS_EC2_METADATA_DISABLED=true`. The file is the keys' only copy: an existing one is kept as it is (keys are never rotated silently; §7.4.19), and one without both keys stops the step. Galexie's container reads it with `--env-file`; the lake, ingest and the launcher parse it.
- **Archive** (`/etc/stellar/archive.env`, `root:stellar 0640`; [`deploy/archive.env.example`](deploy/archive.env.example)): `archive_endpoint`, `archive_region` and the uploader key from §3b, under `ARCHIVE_S3_*` names, never `AWS_*`. Ingest (archiver, sequencer), the launcher, `gapcheck`, `reindex --source archive` and `smoke-archive` read it.
- **Archive for the gap check** (`/etc/stellar/archive-aws.env`, `root:root 0600`; [`deploy/archive-aws.env.example`](deploy/archive-aws.env.example)): the same key under `AWS_*` names, used only by the `detect-gaps` container (and a manual archive repair, §7.4.10).
- **The database bundle.** `lab psql` writes it on the psql host to `/root/galexie-bundle/`: the CA certificate and one libpq password file per role (pgpass format `host:port:database:user:password`). Copy it over from your workstation (§6 step 3):

  ```sh
  scp -r root@<psql host>:/root/galexie-bundle root@<galexie host>:/root/
  ```

  The `config` step installs `pg-ca.crt` (`root:root 0644`), `pgpass` (`stellar:stellar 0600`: ingest, bootstrap and reindex run as `stellar`) and `pgpass-owner` (`root:root 0600`: only `worker migrate`, as root). libpq ignores a password file that its group or others can read, hence 0600; and the `explorer_owner` password stays out of reach of the `stellar` user that runs ingest. Until the bundle is there, the step waits (◷) and prints the `scp` command. Once it is installed, `/root/galexie-bundle` may be deleted; while it is there, a newer bundle replaces the installed files. `check` logs in with `worker.toml`'s DSNs, as `stellar` and as root, over TLS (`sslmode=verify-full`).

### 4.5 Restarts

A service that is not running is started. A running one whose files changed (its unit, binary, configuration or keys) is **not** restarted unless you pass `--allow-restart`: the step shows `! restart pending`, and the run exits 1 until that is done. An unclean Galexie stop costs a captive-core rebuild, so `lab galexie` never restarts the lake or Galexie on its own.

- With `--allow-restart`, the lake is restarted with `systemctl restart stellar-lake`, as `deploy-lake.sh` does: `PartOf=` stops Galexie first (it drains into the still-running lake) and starts it again after the lake's READY. Schedule it like a lake deploy (§7.2).
- Galexie is restarted with `systemctl restart stellar-galexie` (a clean `docker stop`); ingest can be restarted at any time.
- A changed `daemon.json` restarts Docker, which first stops Galexie cleanly through `Requires=docker.service` (untested).
- The firewall is only ever reloaded; sshd is reloaded after `sshd -t`; journald is restarted when its cap changes.

---

## 5. Configuration

### 5.1 `galexie.toml`

`/etc/stellar/galexie.toml` ([`deploy/galexie.toml`](deploy/galexie.toml)), mounted into the container as `/config.toml`:

```toml
admin_port = 6061                      # binds all interfaces → Cloud Firewall (public) + nftables (private)
[datastore_config]
type = "S3"
[datastore_config.params]
destination_bucket_path = "stellar/ledgers/pubnet"
region = "us-east-1"                   # must equal the gateway Region
endpoint_url = "http://127.0.0.1:9000"
[datastore_config.schema]
ledgers_per_file = 1
files_per_partition = 64000
[stellar_core_config]
network = "pubnet"
storage_path = "/srv/core"
```

**Never change the schema or the bucket path of an existing store.** Galexie compares its config with the store's manifest and exits 1 on a mismatch; a new schema means a new prefix or bucket.

Two variants, used only by one-off containers:

- [`galexie-repair.toml`](deploy/galexie-repair.toml): the same with `admin_port = 0` (no clash with the running Galexie), for `scan-and-fill` into the lake (§7.4.8).
- [`galexie-archive.toml`](deploy/galexie-archive.toml): pointed at the archive (`destination_bucket_path = "<archive bucket>/ledgers/pubnet"`, `region = "hel1"`, `endpoint_url = "https://hel1.your-objectstorage.com"`), `admin_port = 0`, for `detect-gaps` (the gap check) and a manual archive repair (§7.4.10).

### 5.2 `worker.toml`

`/etc/stellar/worker.toml` ([`deploy/worker.toml`](deploy/worker.toml)) lists every key with its default; unknown keys are rejected and all paths must be absolute. `lab galexie` sets these from the settings (§0.5):

| Key | Example | Notes |
|---|---|---|
| `lake.metrics_listen` | `10.0.0.2:9101` | private IP; bound best-effort, never delays READY |
| `metrics.private_listen` | `10.0.0.2:9102` | private IP; best-effort |
| `postgres.dsn` | `postgres://ingest_writer@10.0.1.2:5432/explorer?sslmode=verify-full&sslrootcert=/etc/stellar/pg-ca.crt&passfile=/etc/stellar/pgpass` | ingest, bootstrap, reindex |
| `postgres.migrate_dsn` | same with `explorer_owner` and `passfile=/etc/stellar/pgpass-owner` | `worker migrate` only (as root) |
| `archive.bucket` | `stellar-ledgers-pubnet-staging`, then `stellar-ledgers-pubnet` | §3e, §7.6 |

The other sections, with their defaults: `[lake]` (dir `/srv/lake`, bucket path, region, loopback listeners), `[archive]` (`enabled`, `path_style = false`), `[ingest]` (`lake_grace` 2m before failing over, `archive_grace` 10m before the AWS lake, catch-up batches ≤ 500 ledgers and ≤ 512 MiB), `[bootstrap]` (SDF's three history archives, `checkpoint_poll`), `[prune]` (`retention_ledgers` 51,840 = 72 h, allowed 17,280–51,840; `disk_high_watermark` 0.8; `local_readers = ["explorer"]`), `[metrics]` (`ready_max_lag` 120; `network_tip` polls the AWS lake), `[galexie]` (the pinned image, which must equal the unit's: §7.3), `[watchdog]` (the restart rules of §7.4.11; `first_upload_within` 45m, above a restart's catchup, §0.2) and `[gapcheck]` (`method`, file paths).

### 5.3 What must agree

| Value | Where |
|---|---|
| gateway region `us-east-1` | `worker.toml` `lake.region`, `galexie.toml` and `galexie-repair.toml` `region` (a mismatch is a 400 and Galexie exits 1) |
| bucket path `stellar/ledgers/pubnet` | `lake.bucket_path`, both lake-facing Galexie TOMLs |
| archive bucket | `archive.bucket`, `galexie-archive.toml` `destination_bucket_path` (`<bucket>/ledgers/pubnet`), the bucket policy's ARNs |
| archive endpoint and region | `archive.env`, `galexie-archive.toml` |
| Galexie image (tag and digest) | `stellar-galexie.service`, `worker.toml` `galexie.image`; the repair commands read it from `worker.toml` |
| `/srv/core` | `galexie.toml` `storage_path`, `metrics.core_dir`, the unit's and repair commands' `-v /srv/core:/srv/core` |

`go test -run TestDeployConfigsAgree` checks the repository copies, and `lab galexie` installs them with your settings: change them in the repository, not on the host (the next run would put the repository's back).

---

## 6. First run

The two hosts come up in this order; the psql host's side is in psql/README.md.

1. **Order and network.** Order both servers and join their private networks (§1 here, and psql/README.md). Create the archive bucket and its policy (§3a–c).
2. **psql host.** Copy `bin/` over and run `/root/bin/lab psql`. Every step goes ✓ except `api`, which waits (◷) for `worker migrate`, and Caddy, which waits until DNS points at the host. It writes the database bundle to `/root/galexie-bundle/`.
3. **Bundle.** From your workstation: `scp -r root@<psql host>:/root/galexie-bundle root@<galexie host>:/root/` (§4.4).
4. **galexie host.** Copy `bin/` over and run `/root/bin/lab galexie` (§4.1). It installs everything, runs `worker migrate`, asks before `worker bootstrap` and runs it, then starts the services. Galexie starts at `archive_origin`. With the worker and `archive.env` in place, run the provider check now (§3d).
5. **psql host again.** `/root/bin/lab psql`: its `api` step now finds the schema and applies `grants.sql` and `api.sql`.
6. **Verify.** Run `check` on both hosts; then reboot both and run `check` again.

## 7. Operations

### 7.1 Where to look

`lab galexie check` (§4.1) runs every step's check and the health probes. By hand:

```sh
systemctl status stellar-lake stellar-ingest stellar-galexie
systemctl list-timers 'stellar-*'
curl -s 127.0.0.1:9001/v1/tip                         # lake tip and oldest
curl -s 127.0.0.1:9001/healthz                        # ok | starting | stopping | fail-closed: …
curl -s 127.0.0.1:9102/readyz                         # ready, or the failing checks (lake, postgres, lag)
curl -s 127.0.0.1:9102/metrics | grep -E '^(ingest_cursor|ingest_lag_ledgers|stellar_network_tip|archive_tip|archiver_blocked|prune_bound)[ {]'
curl -s 127.0.0.1:6061/metrics | grep -E '^galexie_(export_manager|uploader)_latest_ledger'
journalctl -u stellar-ingest --since -1h | grep 'alert='   # every operator-facing condition is logged with alert=<kind>
cat /var/lib/stellar-watchdog/state.json                    # last decision, restarts, uploader progress
cat /var/lib/stellar-gapcheck/result.json                   # last gap check
df -h / /srv/lake
```

Lake object keys can be computed from the sequence number (for `ls`, a GET, or a DELETE through the lake):

```sh
lakekey() { n=$1; p=$(( n / 64000 * 64000 ))
  printf 'ledgers/pubnet/%08X--%d-%d/%08X--%d.xdr.zst\n' $((0xFFFFFFFF - p)) $p $((p + 63999)) $((0xFFFFFFFF - n)) $n; }
lakekey 123456          # ledgers/pubnet/FFFF05FF--64000-127999/FFFE1DBF--123456.xdr.zst
ls -l /srv/lake/s3/stellar/$(lakekey 123456)
```

Read files under `/srv/lake/s3` freely, but **never** create, change or delete them directly: every write and delete goes through the lake's S3 endpoint, which keeps the fsync chain and the tip/oldest index. For a one-off S3 call use `curl` with SigV4 and the gateway keys (tested with curl 8.7 against the lake; Ubuntu 24.04's 8.5 signs correctly too, against the archive bucket):

```sh
( . /etc/stellar/gateway.env
  curl -fsS -I --aws-sigv4 aws:amz:us-east-1:s3 --user "$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY" \
    "http://127.0.0.1:9000/stellar/$(lakekey 123456)" )        # HEAD: status, x-amz-meta-*
```

### 7.2 Deploys

Each build is installed as `/usr/local/lib/stellar/worker-<version>` and selected by a symlink. Switching a symlink is atomic and a running process keeps its binary. Versions are immutable: the scripts refuse to replace an installed version with different bytes. `lab galexie` installs only the first version (§4.3); later ones go through these scripts. Build as in §4.1 (`make dist VERSION=$V`), then copy the binary and the current `deploy/` to the server:

```sh
scp bin/worker root@<public IP>:/root/worker-$V
scp -r galexie/deploy root@<public IP>:/root/galexie-deploy
```

**Ingest (routine, any time)** — [`deploy/deploy-ingest.sh`](deploy/deploy-ingest.sh): switches `/usr/local/bin/worker`, optionally runs `worker migrate` (as root, the script runs under sudo), restarts `stellar-ingest` only, and waits up to 120 s for `/readyz`. The lake, the launcher, the watchdog and Galexie are untouched (they run `worker-lake`).

```sh
sudo /root/galexie-deploy/deploy-ingest.sh /root/worker-$V $V              # add --migrate when the release has migrations
```

`--migrate` applies the new migrations while the old ingest still runs, then restarts it; after a migration that adds a state table or recreates a function, run `lab psql` on the psql host: its `api` step applies `grants.sql` again. If ingest was stopped for a while, `/readyz` reports the lag until it catches up and the script gives up after `READY_WAIT` seconds (exit 1) although the deploy itself is fine; check `curl -s 127.0.0.1:9102/readyz`, or run with `READY_WAIT=1800`.

**Lake (rare: only when `lake/` or VersityGW changes)** — [`deploy/deploy-lake.sh`](deploy/deploy-lake.sh): switches `/usr/local/bin/worker-lake` (lake, launcher, watchdog), restarts `stellar-lake` and waits for its `/healthz`. `PartOf=` makes systemd stop Galexie first (it drains into the still-running lake), restart the lake, then start Galexie after READY; a held Galexie stays stopped. Ingest is not restarted; it reconnects within its 2 min grace without failing over. Schedule it like maintenance: Galexie stops cleanly, but captive core rebuilds on the restart, about 25 minutes on pubnet (§0.2).

```sh
sudo /root/galexie-deploy/deploy-lake.sh /root/worker-$V $V
```

**Both:** a release that changes the lake and ingest is deployed with both scripts, in either order (the lake's control protocol `v1` stays backward compatible).

After either deploy, check `/readyz` or `/healthz`, then the ledger latency and lag (Prometheus, or §7.1).

**Rollback:** re-run the script with the previous version and its binary (`ls /usr/local/lib/stellar/`). Migrations are forward-only. An older build starts against a newer schema (it checks only the migrations it knows), which is fine for additive migrations; read the release's migrations before rolling back across one.

**Clean up** old versions that neither symlink points to:

```sh
readlink /usr/local/bin/worker /usr/local/bin/worker-lake
ls -l /usr/local/lib/stellar/
```

### 7.3 Protocol upgrades

Only `ingest` decodes XDR; the lake never does, and the archiver validates headers only, so archiving and pruning continue through an upgrade that the SDK doesn't know yet.

1. **Watch** SDF's announcements (protocol votes, stellar-core and Galexie releases on Docker Hub `stellar/stellar-galexie`). The pinned image's stellar-core 29.0.0 supports Protocol 29.
2. **Before the vote, bump the Galexie image** (it bundles the matching core). Find the digest on a host with Docker:
   ```sh
   T=29.0.1                                                         # the new tag
   docker pull stellar/stellar-galexie:$T
   docker inspect --format '{{index .RepoDigests 0}}' stellar/stellar-galexie:$T   # stellar/stellar-galexie@sha256:…
   docker run --rm --entrypoint stellar-core stellar/stellar-galexie:$T version     # the bundled core
   docker run --rm stellar/stellar-galexie:$T version                              # stellar-galexie <commit>
   ```
   In the repository, set `stellar/stellar-galexie:$T@sha256:<digest>` in `deploy/systemd/stellar-galexie.service` (`ExecStart`), `deploy/worker.toml` (`[galexie] image`, used by the gap check and the repair commands) and the comment in `deploy/galexie-repair.toml`; `go test -run TestDeploy` (in `worker/`) fails until they agree. Then build and copy `bin/` (§4.1), and on the server:
   ```sh
   /root/bin/lab galexie                    # pulls the new image; the unit and worker.toml change: restart pending
   /root/bin/lab galexie --allow-restart    # restarts the lake (Galexie stops first, cleanly, and starts after READY) and ingest
   journalctl -u stellar-galexie -f           # uploads resume after core's catchup (~25 min, §0.2)
   ```
   The `docker` step pulls the image before the `units` step installs the new unit, so the restart never waits for a pull. The lake restarts only because `worker.toml` changed; schedule it like a lake deploy (§7.2).
3. **If the protocol changes XDR**, bump `github.com/stellar/go-stellar-sdk` in `worker/go.mod`, rebuild, run `make test` and the replay against the newest AWS-lake ledgers (`make test-live`, §8.5), and deploy ingest only (§7.2).
4. **After the vote**, check that the protocol flipped and nothing halted:
   ```sql
   SELECT protocol_version, min(sequence), max(sequence) FROM explorer.ledgers
    WHERE sequence > (SELECT ledger_sequence - 1000 FROM ingest_state WHERE pipeline = 'explorer') GROUP BY 1;
   ```
   ```sh
   journalctl -u stellar-ingest --since -2h | grep -E 'alert=(validation_failed|pipeline_halted)'
   ```
   If the sequencer halted on unknown XDR, deploying the bumped ingest resumes it from its cursor; the archive kept flowing meanwhile.

### 7.4 Runbooks

The alert names are those of [`deploy/alerts.yml`](deploy/alerts.yml) (§7.5). Ingest, the launcher, the watchdog and the gap check log every operator-facing condition at ERROR with `alert=<kind>`; ingest also counts them in `stellar_alerts_total{kind}`.

#### 7.4.1 Galexie not running or restarting (`GalexieDown`, `GalexieRestarting`)

```sh
systemctl status stellar-galexie
journalctl -u stellar-galexie -n 200 --no-pager
```

- **The launcher refuses** (`ExecStartPre` fails, systemd retries every 5 s):
  - "the lake's S3 endpoint / control endpoint does not answer" → the lake is down or starting (§7.4.6, `systemctl status stellar-lake`);
  - "run `worker bootstrap` first" (`alert=galexie_needs_bootstrap`) → the lake and archive are empty and `bootstrap.json` is missing: first run (§6), or the file was lost (§7.4.15);
  - "the lake holds no ledger and the archive tip is unknown" (`alert=galexie_start_refused`) → the Volume is empty and the archive does not answer: fix archive access (`archive.env`, the provider's status); it starts by itself once the probe succeeds.
- **Galexie exits 0 again and again**: an upload error. The lake refuses PUTs: 503 when fail-closed (§7.4.6), 507 when the disk is full (§7.4.5).
- **Galexie exits 1 again and again**: a config or manifest mismatch (schema, bucket path, passphrase, region). Compare `/etc/stellar/galexie.toml` with §5.1 and the lake's `.config.json`; never change an existing store's schema.
- **Running but no uploads**: captive core is catching up (logs; `galexie_ingest_captive_stellar_core_new_db` increased). Wait; the watchdog alerts `galexie_no_first_upload` after 45 min, and restarts only for a hung upload (§7.4.11).

Never `docker stop`, `docker restart` or `docker kill` the container by hand; use systemctl, so the launcher and the orphan cleanup run.

#### 7.4.2 Holding Galexie

```sh
systemctl stop stellar-galexie            # held: survives lake restarts and lake deploys (propagated restarts are try-restarts)
systemctl disable stellar-galexie         # also keep it stopped across a reboot
systemctl enable --now stellar-galexie    # release
```

While held: nothing new reaches the lake; ingest and the archiver wait at the tip; the watchdog does nothing (`galexie_not_running`). `GalexieDown` warns after 10 min, and `PipelineStall` pages after ~15 min (the cursor more than 120 ledgers behind an advancing network for 5 min); silence them for planned holds on the psql host (`amtool silence add …`). After the release, Galexie resumes at the lake tip + 1.

`lab galexie` starts a stopped or disabled Galexie (step `services`): during a hold, run it with `--skip services`; `check` reports the hold as not ok.

#### 7.4.3 Pipeline halted or lagging (`PipelineHalted`, `PipelineStall`, `PipelineLagBehindLake`, `IngestAlert`)

```sh
journalctl -u stellar-ingest --since -1h | grep 'alert='
curl -s 127.0.0.1:9102/readyz
```

| `alert=` | Meaning | Action |
|---|---|---|
| `pipeline_halted` | every source failed validation for `cursor + 1`, or the sink failed fatally; the cursor did not move | read the preceding `validation_failed` lines; fix, then `systemctl restart stellar-ingest` (a halted pipeline waits for an operator; the archiver and pruner keep running) |
| `validation_failed` | a source served a ledger that failed decoding, sequence or hash checks; the next source is tried | unknown XDR after a vote → §7.3; a hash mismatch means corruption, the wrong network or mixed sources: never rewind the cursor |
| `ledger_missing` | no source holds `cursor + 1` | a hole that was never archived: repair it (§7.4.8) |
| `reading_from_aws` | the lake and the archive don't have the ledger (or the archive failed for 10 min), so the AWS public lake is used | fine for a while; find out why (pruned before ingest read it? archive down?) |
| `lake_unavailable` | the lake didn't answer for 2 min; reads fail over | §7.4.6 |
| `sink_unavailable` | Postgres commits keep failing | §7.4.4 |
| `schema_behind`, `schema_mismatch` | the database lacks this build's migrations, or holds different ones | `worker migrate`, or deploy the matching build |
| `no_cursor` | no `ingest_state` row | `worker bootstrap` (§6) |
| `conflict_rows` | rows were already present (should never happen) | look for a second writer; reindex the range if needed (§7.4.13) |
| `state_drift` | an eviction found no state row | §7.4.12 |

A second `worker ingest` (for example started by hand) exits at once: the advisory lock is held, and the guarded cursor rejects double commits anyway.

#### 7.4.4 Postgres unavailable

Nothing to do here: ingest retries with backoff (`sink_unavailable` after a while), the lake keeps accepting, and the archiver and pruner keep running. Above the disk watermark (0.8) the pruner stops waiting for the cursor, and the sequencer later reads those ledgers from the archive. Fix Postgres (psql/README.md); ingest catches up by itself.

#### 7.4.5 Lake disk high or full (`LakeDiskHigh`, `LakeDiskCritical`)

A full lake returns 507, which Galexie doesn't retry: it exits and restarts in a loop until there is space.

```sh
df -h /srv/lake
curl -s 127.0.0.1:9001/v1/tip
curl -s 127.0.0.1:9102/metrics | grep -E '^(archive_tip|archive_tip_known|archiver_blocked|prune_bound|prune_disk_watermark|prune_last_pass_seconds) '
```

1. **Most likely the archive is behind**: the pruner never deletes unarchived ledgers. Fix the archiver first (§7.4.9).
2. **Grow the Volume** (online, §2.3). This is always safe.
3. **Emergency prune**: lower `[prune] retention_ledgers` to the minimum, 17280 (24 h), and `systemctl restart stellar-ingest`. The pruner still deletes only archived ledgers, oldest first, through the lake. The next `lab galexie` run puts the repository's `worker.toml` back: change `deploy/worker.toml` too, or run it with `--skip config` meanwhile.

Never delete files under `/srv/lake/s3` by hand, and never delete a ledger above the archive tip.

#### 7.4.6 Lake down or fail-closed (`LakeDown`, `LakeFsyncFailed`, `IngestLakeUnreachable`)

- **Down** (crash): systemd restarts it after 1 s; on the same boot that is a `syncfs` and a listen (~1 s), no scan. A running Galexie is restarted after READY, an exited one comes back through its own `Restart=always` (untested). Check `journalctl -u stellar-lake`.
- **Fail-closed**: an fsync failed after a link, so every PUT gets 503 and `/healthz` says `fail-closed`. On Linux a failed fsync cannot be retried safely; on xfs it usually means the filesystem shut down.
  ```sh
  journalctl -u stellar-lake | grep -iE 'fsync|fail'
  dmesg -T | grep -iE 'xfs|i/o error|sd[a-z]'
  ```
  Investigate the Volume (Hetzner status page, `dmesg`). Then restart the lake; its startup `syncfs` must succeed or it refuses to start:
  ```sh
  systemctl restart stellar-lake            # stops Galexie first, starts it after READY
  ```
  If xfs shut down, repair it first, and force the post-crash checksum scan of the newest 100 objects by removing the boot marker:
  ```sh
  systemctl stop stellar-galexie stellar-ingest stellar-lake
  umount /srv/lake && xfs_repair /dev/disk/by-id/scsi-0HC_Volume_100123456 && mount /srv/lake
  rm /srv/lake/state/boot-id                # the next start scans, deletes corrupt objects, reports holes
  systemctl start stellar-lake stellar-ingest stellar-galexie
  curl -s 127.0.0.1:9001/metrics | grep lake_startup_scan
  ```

#### 7.4.7 Volume I/O stall (`LakeVolumeIOStall`, `WatchdogLakeIOStall`)

A ledger PUT has been in fsync (`lake_put_phase_age_seconds{phase="syncing"}`) for more than 30 s: the Volume is stuck. A restart cannot unblock an fsync, so the watchdog never restarts Galexie for this, and restarting the lake or Galexie by hand doesn't help either. Look at `dmesg -T`, `cat /proc/pressure/io`, `iostat -x 5` (package `sysstat`) and Hetzner's status page. When the fsync completes (or fails: §7.4.6), uploads continue by themselves. If the Volume stays stuck, treat it as a lost server (§7.4.17).

#### 7.4.8 Hole in the lake (`LakeStartupHoles`, `ArchiverBlocked{reason="missing"|"invalid"}`, `alert=ledger_missing`)

A ledger is missing or corrupt below the lake tip: the post-reboot scan deleted a corrupt object below good ones, or a ledger failed validation. The sequencer falls back to the archive; the archiver retries the ledger (backoff up to 30 s) and resumes **by itself** once it is repaired. If the ledger was archived already, nothing else is needed. If it was never archived, repair it:

1. Find `n`: `journalctl -u stellar-ingest | grep -E 'alert=archive_(missing|invalid)_ledger'`, or the lake's scan log lines.
2. If the object exists but is corrupt, delete it **through the lake** first (scan-and-fill skips existing keys):
   ```sh
   ( . /etc/stellar/gateway.env
     curl -fsS -X DELETE --aws-sigv4 aws:amz:us-east-1:s3 --user "$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY" \
       "http://127.0.0.1:9000/stellar/$(lakekey $n)" )      # lakekey: §7.1
   ```
3. Re-export it with a one-off container next to the running Galexie (`--end` must be greater than `--start`, existing ledgers are skipped):
   ```sh
   IMAGE=$(sed -n 's/^image *= *"\([^"]*\)".*/\1/p' /etc/stellar/worker.toml)
   docker run --rm --network host --env-file /etc/stellar/gateway.env -v /srv/core:/srv/core \
     -v /etc/stellar/galexie-repair.toml:/config.toml:ro "$IMAGE" \
     scan-and-fill --start $n --end $((n + 1)) --config-file /config.toml
   ```
   This runs a captive-core catchup in a temporary directory under `/srv/core`: tens of minutes and about 15 GB (untested in this mode).
4. Check: `ls -l /srv/lake/s3/stellar/$(lakekey $n)`; `archiver_blocked` returns to 0 and `archive_tip` moves on.

#### 7.4.9 Archive behind or blocked (`ArchiveTipUnknown`, `ArchiveStalled`, `ArchiverBlocked`)

```sh
curl -s 127.0.0.1:9102/metrics | grep -E '^(archive_tip|archive_tip_known|archiver_blocked|archiver_last_archived_seconds) '
journalctl -u stellar-ingest --since -1h | grep -E 'alert=archive_'
```

| `archiver_blocked{reason}` | Meaning | Action |
|---|---|---|
| `unavailable` | the archive (or the lake) does not answer | provider status, `archive.env`, outbound HTTPS; it retries by itself. The lake grows ~3.3 GB/day meanwhile (§7.4.5) |
| `origin_unknown` | the archive is empty and `bootstrap.json` is missing | §7.4.15 |
| `manifest` | the archive's `.config.json` differs from the lake's | wrong bucket or prefix: check `archive.bucket` and the bucket contents; archiving is halted until fixed |
| `missing`, `invalid` | ledger `n` is missing or invalid in the lake | §7.4.8 |
| `halted` | the archive already holds a **different** ledger `n` (another hash) | never overwrite: the bucket belongs to another store (staging/production mix-up?) or the lake is on the wrong network. Fix the configuration; it re-checks every 5 min |

`ArchiveStalled` (the archive tip has not moved for 15 min while the lake's has) and `ArchiveTipUnknown` mean nothing is being archived, so nothing is pruned either. Producer differences are normal: when the archive holds the same ledger with other bytes (e.g. written by `scan-and-fill`), the archiver keeps the remote copy and counts it in `archiver_ledgers_total{result="differs"}`.

#### 7.4.10 Gap check findings (`ArchiveGaps`, `GapcheckFailed`, `GapcheckStale`)

```sh
cat /var/lib/stellar-gapcheck/result.json   # method, range [from, to], gaps, missing, invalid, error
journalctl -u stellar-gapcheck -n 100 --no-pager
systemctl start stellar-gapcheck            # run it now (oneshot; blocks until done; exit 1 when gaps were found)
```

- **Failed**: Docker or the provider. If `detect-gaps` failed (`alert=gapcheck_detect_gaps_failed`), the check fell back to `worker reindex --dry-run --source archive` over the same range, which downloads and validates every archived object; that gets slow as the archive grows (the unit allows 12 h). If the provider rejects `detect-gaps`' path-style addressing for good, set `[gapcheck] method = "reindex-dry-run"`.
- **Gaps**: the archiver copies strictly in order and never goes back below its tip, so a gap means objects disappeared or were never written; repairing the lake does not fill it. Re-export the missing range straight into the archive with Galexie (`If-None-Match`, so existing objects are untouched), using the archive variants of the config and the env file (untested):
  ```sh
  IMAGE=$(sed -n 's/^image *= *"\([^"]*\)".*/\1/p' /etc/stellar/worker.toml)
  docker run --rm --network host --env-file /etc/stellar/archive-aws.env -v /srv/core:/srv/core \
    -v /etc/stellar/galexie-archive.toml:/config.toml:ro "$IMAGE" \
    scan-and-fill --start $first --end $((last + 1)) --config-file /config.toml
  systemctl start stellar-gapcheck          # confirm
  ```
- **Invalid objects** (dry-run method only): the uploader cannot delete, so delete them with the admin key from the workstation (`aws s3api delete-object $S3 --bucket … --key ledgers/pubnet/<key>`), then fill them as above.

#### 7.4.11 Watchdog (`WatchdogRestartedGalexie`, `WatchdogCondition`, `WatchdogNotRunning`)

The watchdog restarts Galexie only when its upload is hung: (a) a ledger PUT has been `receiving` for more than 120 s, or (b) the export manager is ahead of the uploader and the uploader hasn't moved for 180 s. It never restarts while a PUT is `syncing` (§7.4.7), while Galexie is inactive (held), or when it cannot read the lake's metrics; at most once per 15 min (`restart_suppressed_rate_limit`, `alert=galexie_upload_hung`), and it pages after 2 restarts in a row without upload progress.

```sh
cat /var/lib/stellar-watchdog/state.json    # last_decision, restarts_in_a_row, uploader_since, alerts
journalctl -u stellar-watchdog --since -1h | grep -v 'decision=ok'
```

- **Restarted twice in a row**: the restart didn't help. Look at the lake (`lake_put_phase_age_seconds`, `/healthz`), the network (`ss -tnp | grep 9000`), and Galexie's logs.
- **Conditions** (`watchdog_alert{kind}`): `galexie_no_first_upload` (captive core still catching up after 45 min), `galexie_uploader_lag` (uploader > 120 ledgers behind the network), `galexie_upload_hung` (a restart is due but rate-limited), `lake_io_stall`.
- **Not running**: `systemctl status stellar-watchdog.timer stellar-watchdog.service`.

#### 7.4.12 State drift and state repair (`StateDrift`)

An eviction found no state row, so the state tables disagree with the ledger. Repair them from a checkpoint at or above the cursor with `--force`: it waits for that checkpoint to be published (~5–15 min), writes the `ledgers` rows between the cursor and the checkpoint, reloads only the state tables, and moves the cursor forward in the same transaction. The cursor never moves backwards and `bootstrap.json` is never rewritten.

```sh
systemctl stop stellar-ingest              # also pauses the archiver and the pruner; the lake keeps accepting
sudo -u stellar worker bootstrap --force   # in tmux; it refuses while ingest holds the lock
systemctl start stellar-ingest
```

Galexie keeps running next to it, so watch memory during the load (`free -g`): the first pubnet bootstrap alone peaked at about 11 GB, and captive core holds several GB more.

#### 7.4.13 Bug in `processLedger`

PostgreSQL holds the current state and one row per ledger; the history (transactions, operations, events, transfers, activity) is the clickhouse pipeline's, which runs the same `processLedger` on the clickhouse host.

- **A bug in the state tables:** fix it, deploy ingest (§7.2), then repair the state with `bootstrap --force` (§7.4.12).
- **A bug in the history:** fix it and deploy the worker on the clickhouse host, then re-ingest there ([`clickhouse/README.md`](../clickhouse/README.md) §9.6).
- **A bug in the `ledgers` rows:** replace them while live ingestion continues (the cursor is unchanged, ranges above it are refused):

```sh
sudo -u stellar worker reindex --from A --to B --dry-run                  # decode and validate only
sudo -u stellar worker reindex --from A --to B [--source lake|archive|aws]
```

The default source is the lake, which holds only the last 72 h; use `--source archive` for older ranges.

#### 7.4.14 Starting PostgreSQL over

When the worker's migrations start over (as for API version 2), or the database is lost without a usable backup, PostgreSQL is rebuilt from a checkpoint; the lake, the archive and Galexie keep running, and `bootstrap.json` stays (the archive's origin doesn't move):

```sh
systemctl stop stellar-ingest
# psql host: DROP SCHEMA explorer CASCADE; DROP TABLE public.ingest_state; (psql/README.md §8.8)
deploy the new worker (§7.2, without restarting ingest), then:  worker migrate
# psql host: ./lab psql (the api step applies grants.sql and api.sql again)
sudo -u stellar worker bootstrap           # in tmux, ~30 min: the latest checkpoint's state, the cursor there
systemctl start stellar-ingest             # catches up from the checkpoint, from the lake
```

`ledgers` then starts at the checkpoint, and `bootstrap --recreate-origin` (§7.4.15) would rebuild a lost `bootstrap.json` from there rather than from go-live: keep a copy of the file.

#### 7.4.15 Lost `state/bootstrap.json`

It is needed only until the first ledger is archived (the archiver and the launcher then use the archive's own tip) and by the gap check (`archive_origin`). Recreate it from Postgres: `C = min(explorer.ledgers.sequence) − 1`, or the cursor if no ledger was committed yet. It never overwrites an existing file.

```sh
sudo -u stellar worker bootstrap --recreate-origin
```

`lab galexie` does the same in its `bootstrap` step when the file is missing and Postgres has a cursor.

#### 7.4.16 Reboots, power loss, kernel updates

When `/var/run/reboot-required` exists, reboot at a quiet time: `systemctl reboot`. systemd stops Galexie first (clean drain), then the lake and ingest. At boot: the firewall, the Volume mount, the lake (always `syncfs`; after a new boot also the checksum scan of the newest 100 objects), Galexie, ingest.

After any reboot, and especially after a power loss or kernel panic, run `lab galexie check`, and:

```sh
curl -s 127.0.0.1:9001/metrics | grep -E '^lake_startup_scan'   # checked, deleted, holes
journalctl -u stellar-lake -b | grep -iE 'corrupt|hole'
```

Corrupt objects are deleted; a corrupt suffix is simply re-exported by Galexie, a corrupt object below the tip is a hole (§7.4.8). Captive core rebuilds after an unclean stop.

#### 7.4.17 Server lost or rebuilt

- **The Volume survives** (Volumes are replicated and location-bound): order a new CCX33 in **HEL1** (§1.1–§1.3), detach the Volume from the old server if it still exists (Console → Volumes → Detach) and attach it to the new one. Copy the database bundle over again (§4.4) and run `lab galexie` (§4.1) with the same settings and uploader key. It formats nothing (the Volume holds xfs and its marker), `migrate` finds the schema current, `bootstrap` finds `bootstrap.json` on the Volume, and `services` starts everything. `gateway.env` is generated anew, which is fine: only this host uses it. The new boot id triggers the checksum scan; captive core starts fresh (empty `/srv/core`), and the launcher starts Galexie at the lake tip + 1.
- **The Volume is lost too**: create a new Volume (§2.2) and set `lake_device` to it. The `disks` step formats it (it asks first) and writes a new marker; the `bootstrap` step finds the cursor in Postgres and recreates `bootstrap.json` with `worker bootstrap --recreate-origin` (§7.4.15). The launcher starts Galexie at the archive tip + 1; the sequencer reads `cursor + 1` onward from the archive, then the AWS lake. Nothing is lost that the archive or the AWS lake doesn't hold.

#### 7.4.18 Docker upgrades

```sh
systemctl stop stellar-galexie            # clean stop (core keeps its DB)
apt-get update && apt-get install --only-upgrade docker-ce docker-ce-cli containerd.io
systemctl start stellar-galexie
```

#### 7.4.19 Credential rotation

- **Gateway keys** (`gateway.env`): `rm /etc/stellar/gateway.env`, then `lab galexie --allow-restart`: it writes new keys (§4.4) and restarts the lake (which restarts Galexie with the new file after READY) and ingest.
- **Uploader key**: generate a new one in the server project and add it as a second principal to the bucket policy (§3c). Delete `/etc/stellar/secrets/archive_access_key` and `archive_secret_key`, and run `lab galexie --allow-restart`: it asks for the new key, rewrites `archive.env` and `archive-aws.env`, and restarts ingest. Then remove the old principal and delete the old key in the Console.
- **`ingest_writer` / `explorer_owner` passwords and the Postgres CA** are rotated on the psql host (psql/README.md), which writes a new database bundle; during a CA switch its `pg-ca.crt` holds both CAs. Copy the bundle over (§4.4) and run `lab galexie --allow-restart`: it installs the new files and restarts ingest.

### 7.5 Metrics and alerts

| Endpoint | Where | What |
|---|---|---|
| lake `/v1/tip`, `/metrics`, `/healthz` | `127.0.0.1:9001` | tip and oldest (`?after=N` long-polls 30 s); `lake_*` metrics |
| lake `/metrics`, `/healthz` | `10.0.0.2:9101` | the same metrics for Prometheus (no `/v1/tip`) |
| ingest `/metrics`, `/healthz`, `/readyz` | `127.0.0.1:9102`, `10.0.0.2:9102` | `ingest_*`, `archive_*`, `archiver_*`, `prune_*`, `explorer_*`, `stellar_network_tip`, `stellar_disk_bytes{mount}`, `stellar_alerts_total{kind}`, and the watchdog's and gap check's files (`watchdog_*`, `galexie_active`, `galexie_systemd_restarts`, `gapcheck_*`) |
| Galexie `/metrics` | `:6061` | `galexie_*` |

`/healthz` means the process is alive; ingest's `/readyz` also needs the lake and Postgres reachable, a known cursor, and a lag of at most 120 ledgers behind the lake tip.

Prometheus and Alertmanager run on the psql host (loopback only; their UIs through an SSH tunnel) and scrape this host over the private network, as the jobs `lake`, `ingest` and `galexie`. The psql host's `monitoring` step installs them with their rules (psql/README.md), including this host's [`deploy/alerts.yml`](deploy/alerts.yml), which `lab` embeds from this folder. `lab grafana` adds a Grafana dashboard for this host there (dashboard/README.md). Nothing is installed for monitoring on this host.

To change the rules, edit `deploy/alerts.yml` and run `make alerts` in `worker/` (`promtool check rules` plus the simulations in [`deploy/alerts_test.yml`](deploy/alerts_test.yml): pipeline stall versus network halt, fsync failure, Volume I/O stall, gaps, archive stall, watchdog restarts, halted pipeline). Then rebuild `lab` and run `lab psql` on the psql host. Severities are `page` and `warning`, routed by Alertmanager on the psql host; the always-firing heartbeat for the dead-man's switch is in the psql host's own rules.

| Alert | Severity | Runbook |
|---|---|---|
| `LakeDown`, `LakeFsyncFailed` | page | §7.4.6 |
| `LakeVolumeIOStall` | page | §7.4.7 |
| `LakeStartupHoles` | warning | §7.4.8 |
| `LakeDiskHigh` / `LakeDiskCritical` | warning / page | §7.4.5 |
| `IngestDown`, `IngestLakeUnreachable` | page | `journalctl -u stellar-ingest`; §7.4.6 |
| `PipelineStall`, `PipelineHalted` | page | §7.4.3 (`PipelineStall` only while the network advances) |
| `NetworkHalt`, `NetworkTipStale` | warning | the AWS lake's tip stopped (network halt, or AWS lake stall) or can't be read; Galexie and ingest wait |
| `PipelineLagBehindLake`, `LedgerLatencyHigh` | warning | §7.4.3; Postgres load |
| `ConflictRows`, `StateDrift` | warning | §7.4.3, §7.4.12 |
| `CoreDiskHigh` / `CoreDiskCritical` | warning / page | `/srv/core`: leftover `captive-core-*` directories of repair containers, Docker images (`docker image prune`) |
| `IngestAlert` | warning | the `kind` label: §7.4.3, §7.4.9 |
| `ArchiveTipUnknown`, `ArchiveStalled`, `ArchiverBlocked` | page | §7.4.9 |
| `ArchiveGaps` | page | §7.4.10 |
| `GapcheckFailed`, `GapcheckStale` | warning | §7.4.10 |
| `GalexieDown` | warning | §7.4.1, §7.4.2 |
| `GalexieRestarting` | page | §7.4.1 |
| `GalexieUploaderLag` | warning | §7.4.1 |
| `WatchdogRestartedGalexie`, `WatchdogLakeIOStall` | page | §7.4.11, §7.4.7 |
| `WatchdogCondition`, `WatchdogNotRunning` | warning | §7.4.11 |

### 7.6 Staging → production cut-over

Production history and the archive start at the go-live checkpoint. Staging data is not carried over.

1. Stop: `systemctl stop stellar-galexie stellar-ingest stellar-lake` (Galexie first).
2. Reset the database on the psql host (psql/README.md, the pre-go-live reset).
3. Empty the lake, keeping only the marker:
   ```sh
   findmnt /srv/lake                                        # the Volume is mounted
   find /srv/lake/s3 -mindepth 1 -delete
   find /srv/lake/state -mindepth 1 ! -name lake-id -delete
   ```
4. Switch the archive to production: set `archive_bucket = "stellar-ledgers-pubnet"` in `/etc/stellar/lab-galexie.toml` (`lab galexie` renders it into `worker.toml` and `galexie-archive.toml`), and give the production bucket its policy (§3c) and the delete probe (§3d). The production bucket must be empty.
5. Steps 4–5 of the first run (§6): `lab galexie` (migrate, bootstrap, then the lake, ingest and Galexie start), then `lab psql` on the psql host (`grants.sql` and `api.sql`).
6. Check (`lab galexie check`, §6); after the first ledgers are archived, run `systemctl start stellar-gapcheck` once (it replaces the staging result file). Turn the alerts on, and take the full backup and restore drill on the psql host (psql/README.md).

The staging bucket can then be deleted with the admin key, or kept for tests (never point `dev-feed` at it: it refuses buckets not ending in `-dev`).

---

## 8. Development

### 8.1 Layout

This folder holds the host: this README and `deploy/`, plus `files.go`, which embeds `deploy/` for the CLI. The code lives at the repository root: the worker in `worker/` (module `stellar-explorer/worker`), the CLI that sets up both hosts in `cli/` (§8.7; module `stellar-explorer`, at the root). The worker has feature packages, SQL next to the Go that uses it, and tests next to the code. Paths in the table are under `worker/`.

| Path | What |
|---|---|
| `main.go`, `cmd_ingest.go`, `cmd_ops.go` | the CLI (`commands()` table) |
| `config/` | `worker.toml` (defaults, validation, unknown keys rejected) and the env-file parsers |
| `lake/` | embedded VersityGW, the `tap` (fsync chain, fail-closed, tip/oldest index, in-flight phases, length guard), control endpoint, startup `syncfs` and checksum scan; `lake/laketest/` runs a lake for other packages' tests; `lake/testdata/galexieclient/` is a nested module with Galexie's exact `go.mod`/`go.sum`, used as Galexie's client stack in tests |
| `sep54/` | key codec, manifest, decode and header-only validation, the three sources (lake, archive, AWS lake) with explicit clients, the tip client |
| `sequencer/` | the ingest loop, `Sink` interface, batching, source routing (fake sink in tests) |
| `explorer/` | `processLedger` and the Postgres sink; one `.go` + `.sql` per table; migrations; `explorer/explorertest/` creates scratch databases |
| `ingest/` | `worker ingest` wiring (sources, archiver, pruner, metrics, sequencer) |
| `archive/` | archiver, gap check, `smoke-archive` |
| `prune/`, `launcher/`, `watchdog/`, `bootstrap/`, `reindex/`, `devfeed/`, `metrics/`, `statefile/` | one package per command or concern |
| `deploy_test.go` | checks `../galexie/deploy`: the units' invariants (coupling, restart policies, users, mounts, timeouts), the configs and env templates against the worker's config, the bucket policy, the firewall, the alert rules, the deploy scripts |

### 8.2 Build and test

Requirements: Go 1.26 (the module declares `go 1.25`), Postgres 18 for the database tests. No Docker, no stellar-core.

```sh
cd worker
make build            # bin/worker for this machine
make build-linux      # bin/worker-linux-amd64: CGO_ENABLED=0 GOOS=linux GOARCH=amd64 (the host's binary)
make test             # go test -race: lake/ first (its 5 ms latency assertions), then the rest
make vet              # go vet for darwin and GOOS=linux
make fmt              # fails if gofmt would change anything
make check            # fmt, vet, test, build-linux: run before every deploy
make clean
```

`VERSION=…` sets the version printed by `worker version` (default: `git describe`, or `dev`).

Test conventions:

- Database tests use `TEST_DATABASE_URL` and **skip** when it is unset. Every test creates its own database `stellar_gx_<random>` (and roles `gx_*`) and drops it afterwards, so the URL's role needs `CREATEDB` and `CREATEROLE` (a local superuser is fine).
- Only one lake can run per process (VersityGW's package state; `posix.New` changes the working directory), so lake tests run sequentially and use absolute paths. Tests that need a second S3 endpoint (archive, launcher, ingest) call `laketest.Main(m)` in `TestMain` and start it with `laketest.StartProcess`.
- Tests that run ingest give it a free `metrics.listen` and set `metrics.network_tip = false`.

### 8.3 Local Postgres

```sh
brew install postgresql@18 && brew services start postgresql@18
createdb stellar_explorer_test
export TEST_DATABASE_URL=postgres://$USER@localhost:5432/stellar_explorer_test?sslmode=disable
```

### 8.4 Local end to end with `dev-feed`

`worker dev-feed` stands in for Galexie: it copies objects (bytes and the 9 metadata keys) from the AWS public lake into the lake through the same SDK calls Galexie makes, resuming like Galexie at `max(--start, newest + 1)`. It refuses to run unless archiving is disabled or the archive bucket's name ends in `-dev`. It does not model captive-core rebuilds, so runs with the real Galexie on the host stay necessary.

A throwaway setup on the Mac (the lake skips the mountpoint check with `allow_unmounted`, but still needs the marker):

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
w() { c=$1; shift; bin/worker "$c" --config "$D/etc/worker.toml" "$@"; }   # flags follow the command

w lake &                                                      # 127.0.0.1:9000 (S3) and :9001 (control)
w migrate
A=$(( $(curl -s https://horizon.stellar.org/ | sed -n 's/.*"history_latest_ledger": *\([0-9]*\).*/\1/p') - 60 ))
w bootstrap --history-only --at $A                            # cursor A-1 (hash read from the AWS lake); bootstrap.json
w dev-feed --start $A --end $((A + 20)) --speed 0             # 21 ledgers, as fast as possible
w ingest &                                                    # commits them; metrics on 127.0.0.1:9102
curl -s 127.0.0.1:9001/v1/tip; curl -s 127.0.0.1:9102/readyz
curl -s 127.0.0.1:9102/metrics | grep -E '^(ingest_cursor|ingest_lag_ledgers|ingest_ledgers_read_total)'
w galexie-start --out $D/galexie.env && cat $D/galexie.env    # START=<lake tip + 1>
w reindex --from $A --to $((A + 5)) --dry-run
w dev-feed --start $A                                         # follow the network at ledger cadence (Ctrl-C)
pkill -f "$D/etc/worker.toml"; dropdb --force worker_dev; rm -rf $D     # stop the lake and ingest, clean up
```

`--speed 1` paces by the ledgers' own close times, `--speed 10` ten times faster, `--speed 0` as fast as possible; without `--end` it follows the AWS lake's tip. To exercise the archiver locally, run a second lake as the "archive" (another dir and ports) with `[archive] enabled = true`, a bucket name ending in `-dev`, `path_style = true`, and an `archive.env` pointing at it.

### 8.5 Live tests and alert simulations

```sh
make test-live        # opt-in, needs the internet: decode and validate the newest 2,000 AWS-lake ledgers (~400 MB cached
                      # in /tmp, deleted afterwards), feed them through dev-feed, dev-feed at ledger cadence; with
                      # TEST_DATABASE_URL also a 2,000-ledger replay into Postgres, a rerun (0 conflicts), a reindex,
                      # and ledger latency at the live tip
make alerts           # promtool check rules + test rules on ../galexie/deploy/alerts.yml (promtool on PATH, from the
                      # Prometheus release, the same version as on the psql host)
```

### 8.6 What only the host can test

The Mac has no systemd, Docker, nftables, `O_TMPFILE` or `syncfs(2)`. On the macOS POSIX backend the lake stages through `.sgwtmp` with a rename, and `syncfs` is replaced by per-file fsyncs. These need a Linux host:

- the lake suite on Linux (the `O_TMPFILE` link, `syncfs`, `/proc/self/mountinfo`, `boot_id`): `make test` on a Linux machine with Go 1.26 (and gcc for `-race`);
- the real Galexie container against the lake under the deploy units: kill -9 of ingest and the lake, captive-core rebuild timings;
- the systemd coupling: the lake failing its first start, a held Galexie across lake restarts, `systemctl restart --no-block` from the watchdog;
- `nft -c -f`, the provider checks (§3d), `detect-gaps` against the archive, a pubnet bootstrap (duration and memory);
- `lab galexie` end to end: on a fresh Ubuntu 24.04 server, twice (the second run must change nothing), then `check`, a reboot and `check` again; and on a server set up by hand. In particular:
  - `blkid -p -o value -s TYPE -s PTTYPE` prints nothing and exits 2 for an empty Volume, and `xfs` for a formatted one;
  - `findmnt -n --mountpoint /srv/lake --source /dev/disk/by-id/…` matches through the by-id symlink;
  - `systemctl try-reload-or-restart ssh.service` with Ubuntu 24.04's socket-activated ssh, and `sshd -t` from a root shell;
  - `docker image inspect stellar/stellar-galexie@sha256:…` finds the image pulled as `stellar/stellar-galexie:29.0.0@sha256:…`, also with Docker's containerd image store;
  - Ubuntu's `psql` 16 against Postgres 18 with the worker's URI DSNs (`sslrootcert=`, `passfile=`, `connect_timeout=`), run through `runuser -u stellar`;
  - `systemctl restart docker` (a changed `daemon.json` with `--allow-restart`) stops and restarts Galexie through `Requires=`.

### 8.7 Setup app

`lab` (the repository's `cli/`, one `package main`) sets up the hosts: `lab galexie` this one, `lab psql` the psql host, `lab clickhouse` the clickhouse host. It embeds this folder's `deploy/` through `files.go`, and `psql/`'s files the same way.

| File (`cli/`) | What |
|---|---|
| `main.go` | the command line (fang on cobra): `lab <host>`, `lab <host> check`, the flags |
| `run.go` | the step engine: check, ask once, apply, check again; output, health, the lock and the log |
| `sys.go` | commands, atomic files, directories, apt, users, systemd services, downloads |
| `config.go` | the settings file and form, secrets, rendering the example values |
| `galexie.go`, `galexie_host.go`, `galexie_pipeline.go` | this host: the settings and the step table; system, firewall, disks, docker; worker, config, units, migrate, bootstrap, services |
| `psql.go`, `psql_pg.go`, `psql_web.go` | the psql host (psql/README.md) |
| `cli_test.go` | rendering with the examples changes no file and uses every example; the step tables' order |

```sh
# at the repository root
make build            # bin/lab-<os>-<arch> for this machine (--help only: it runs on Ubuntu 24.04 amd64)
make test             # go test
make vet              # go vet for this machine and GOOS=linux
make check            # gofmt, vet, test and the linux/amd64 builds of lab and Caddy
make check-all        # check, plus the worker's make check (needs TEST_DATABASE_URL)
make dist VERSION=…   # bin/lab, bin/worker (through worker/'s build-linux) and bin/caddy, linux/amd64, same version
make clean
```

The app can't run on the Mac (no systemd, apt or Docker); §8.6 lists what its first runs on a server must confirm.

---

## 9. File map

`lab galexie` installs every file below that has a place on the host (§4.3); the step is in the last column.

| `galexie/deploy/` | On the host | Owner, mode | Step, notes |
|---|---|---|---|
| `sshd-hardening.conf` | `/etc/ssh/sshd_config.d/10-hardening.conf` | root 0644 | `system` (§1.3) |
| `journald.conf` | `/etc/systemd/journald.conf.d/60-stellar.conf` | root 0644 | `system` |
| `needrestart.conf` | `/etc/needrestart/conf.d/60-stellar-docker.conf` | root 0644 | `system` |
| `galexie-host.nft` | `/etc/nftables.d/stellar-galexie.nft` | root 0644 | `firewall` (§1.4) |
| `systemd/stellar-firewall.service` | `/etc/systemd/system/` | root 0644 | `firewall`: loads the nftables table at boot |
| `lake.fstab` | a line in `/etc/fstab` | root 0644 | `disks` (§2.2) |
| `docker.list` | `/etc/apt/sources.list.d/docker.list` | root 0644 | `docker` (§4.3), with the key `/etc/apt/keyrings/docker.asc` |
| `unattended-upgrades-docker.conf` | `/etc/apt/apt.conf.d/51stellar-docker` | root 0644 | `docker` |
| `daemon.json` | `/etc/docker/daemon.json` | root 0644 | `docker` |
| `worker.toml` | `/etc/stellar/worker.toml` | root:stellar 0640 | `config` (§5.2) |
| `galexie.toml` | `/etc/stellar/galexie.toml` | root 0644 | `config` (§5.1) |
| `galexie-repair.toml` | `/etc/stellar/galexie-repair.toml` | root 0644 | `config`; lake repair (§7.4.8) |
| `galexie-archive.toml` | `/etc/stellar/galexie-archive.toml` | root 0644 | `config`; gap check, archive repair (§7.4.10) |
| `gateway.env.example` | `/etc/stellar/gateway.env` | root:stellar 0640 | `config`: generated keys (§4.4) |
| `archive.env.example` | `/etc/stellar/archive.env` | root:stellar 0640 | `config`: uploader key, `ARCHIVE_S3_*` |
| `archive-aws.env.example` | `/etc/stellar/archive-aws.env` | root:root 0600 | `config`: uploader key, `AWS_*` |
| (bundle) `pg-ca.crt` | `/etc/stellar/pg-ca.crt` | root 0644 | `config`, from `/root/galexie-bundle/` (§4.4) |
| (bundle) `pgpass`, `pgpass-owner` | `/etc/stellar/pgpass`, `/etc/stellar/pgpass-owner` | stellar 0600, root 0600 | `config`, from `/root/galexie-bundle/` |
| `systemd/stellar-lake.service`, `stellar-ingest.service`, `stellar-galexie.service` | `/etc/systemd/system/` | root 0644 | `units`; invariants in `go test -run TestDeployUnits` |
| `systemd/stellar-watchdog.service`, `.timer` | `/etc/systemd/system/` | root 0644 | `units`: every 30 s, from `worker-lake` |
| `systemd/stellar-gapcheck.service`, `.timer` | `/etc/systemd/system/` | root 0644 | `units`: weekly, from `worker` |
| `systemd/stellar-galexie.service.d/m4-start.conf.example` | — | | not installed: a fixed `START` instead of the launcher's |
| `archive-policy.json` | — (applied to the bucket from the workstation) | | §3c |
| `deploy-ingest.sh`, `deploy-lake.sh` | run from a copy, e.g. `/root/galexie-deploy/` | root | §7.2 |
| `alerts.yml`, `alerts_test.yml` | psql host, `/etc/prometheus/rules/galexie-alerts.yml` (lab psql's `monitoring` step) | | §7.5 |
| (build) `bin/worker` | `/usr/local/lib/stellar/worker-<version>`, symlinked as `/usr/local/bin/worker` and `/usr/local/bin/worker-lake` | root 0755 | `worker` (first install), then §7.2 |
| (build) `bin/lab` | run from a copy, e.g. `/root/bin/` | root | §4.1; settings in `/etc/stellar/lab-galexie.toml` (root 0600), secrets in `/etc/stellar/secrets/` |
