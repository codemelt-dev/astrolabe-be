#!/bin/sh
# deploy-ingest.sh — routine deploy of `worker ingest` (galexie/README.md §7.2).
#
#   sudo ./deploy-ingest.sh <worker binary> <version> [--migrate]
#
# Installs the build as /usr/local/lib/stellar/worker-<version>, switches
# /usr/local/bin/worker to it atomically (a running process keeps its
# binary), runs `worker migrate` when the release has migrations, and
# restarts stellar-ingest only: the lake, the launcher, the watchdog and
# Galexie are untouched (they run /usr/local/bin/worker-lake). Then it
# waits for ingest's /readyz. Check the ledger latency and lag afterwards.
#
# Old versions can be removed once neither symlink points to them.
# Build with: CGO_ENABLED=0 GOOS=linux GOARCH=amd64 (make build-linux).
set -eu

PREFIX=${PREFIX:-/usr/local}
READY_URL=${READY_URL:-http://127.0.0.1:9102/readyz}
READY_WAIT=${READY_WAIT:-120}

usage() { echo "usage: $0 <worker binary> <version> [--migrate]" >&2; exit 2; }
[ $# -ge 2 ] && [ $# -le 3 ] || usage
BIN=$1
V=$2
MIGRATE=no
if [ $# -eq 3 ]; then
	[ "$3" = "--migrate" ] || usage
	MIGRATE=yes
fi
case $V in
	'' | *[!A-Za-z0-9._+-]*) echo "invalid version '$V'" >&2; exit 2 ;;
esac
[ -f "$BIN" ] || { echo "no such file: $BIN" >&2; exit 2; }

DEST=$PREFIX/lib/stellar/worker-$V
if [ -e "$DEST" ]; then
	# Versioned installs are immutable: never replace a binary in place.
	cmp -s "$BIN" "$DEST" || { echo "$DEST exists with different content; pick a new version" >&2; exit 1; }
else
	install -D -m 0755 "$BIN" "$DEST"
fi
"$DEST" version

ln -sfn "$DEST" "$PREFIX/bin/.worker.tmp" && mv -Tf "$PREFIX/bin/.worker.tmp" "$PREFIX/bin/worker"
echo "worker -> $DEST (worker-lake -> $(readlink "$PREFIX/bin/worker-lake" 2>/dev/null || echo none), untouched)"

if [ "$MIGRATE" = yes ]; then
	"$PREFIX/bin/worker" migrate
fi
systemctl restart stellar-ingest

i=0
until curl -fsS --max-time 5 "$READY_URL"; do
	i=$((i + 5))
	if [ "$i" -ge "$READY_WAIT" ]; then
		echo "stellar-ingest is not ready after ${READY_WAIT}s: journalctl -u stellar-ingest" >&2
		exit 1
	fi
	sleep 5
done
