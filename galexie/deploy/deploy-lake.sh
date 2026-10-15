#!/bin/sh
# deploy-lake.sh — rare deploy of `worker lake` (galexie/README.md §7.2):
# only when the lake/ code or VersityGW changes. Schedule it like
# maintenance.
#
#   sudo ./deploy-lake.sh <worker binary> <version>
#
# Installs the build as /usr/local/lib/stellar/worker-<version>, switches
# /usr/local/bin/worker-lake to it atomically (lake, galexie-start,
# watchdog), then restarts stellar-lake. PartOf= makes systemd stop Galexie
# first (it drains into the still-running lake), restart the lake, then
# start Galexie after READY. A held Galexie (`systemctl stop
# stellar-galexie`) stays stopped. ingest is not restarted: it reconnects.
# Then it waits for the lake's /healthz.
set -eu

PREFIX=${PREFIX:-/usr/local}
HEALTH_URL=${HEALTH_URL:-http://127.0.0.1:9001/healthz}
HEALTH_WAIT=${HEALTH_WAIT:-600}

[ $# -eq 2 ] || { echo "usage: $0 <worker binary> <version>" >&2; exit 2; }
BIN=$1
V=$2
case $V in
	'' | *[!A-Za-z0-9._+-]*) echo "invalid version '$V'" >&2; exit 2 ;;
esac
[ -f "$BIN" ] || { echo "no such file: $BIN" >&2; exit 2; }

DEST=$PREFIX/lib/stellar/worker-$V
if [ -e "$DEST" ]; then
	cmp -s "$BIN" "$DEST" || { echo "$DEST exists with different content; pick a new version" >&2; exit 1; }
else
	install -D -m 0755 "$BIN" "$DEST"
fi
"$DEST" version

ln -sfn "$DEST" "$PREFIX/bin/.worker-lake.tmp" && mv -Tf "$PREFIX/bin/.worker-lake.tmp" "$PREFIX/bin/worker-lake"
echo "worker-lake -> $DEST (worker -> $(readlink "$PREFIX/bin/worker" 2>/dev/null || echo none), untouched)"

systemctl restart stellar-lake

i=0
until curl -fsS --max-time 5 "$HEALTH_URL"; do
	i=$((i + 5))
	if [ "$i" -ge "$HEALTH_WAIT" ]; then
		echo "stellar-lake is not healthy after ${HEALTH_WAIT}s: journalctl -u stellar-lake" >&2
		exit 1
	fi
	sleep 5
done
systemctl is-active stellar-galexie || echo "stellar-galexie is not active (held, or still starting): systemctl status stellar-galexie" >&2
