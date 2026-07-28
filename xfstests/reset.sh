#!/bin/bash
# Safely unmount the xfstests DingoFS instances and remove their local runtime
# data. In local mode this also resets the filesystem contents; in MDS mode,
# remote filesystem contents are not deleted.
#
# Usage:  bash reset.sh
#
# Reads /etc/dingofs-xfstests.conf (written by setup.sh). Only clients whose
# DINGOFS_BASE_DIR lives under this setup's BASE_ROOT are waited on — other
# dingo-client instances on the machine are left alone.
set -e

CONF=/etc/dingofs-xfstests.conf
[ -r "$CONF" ] || { echo "ERROR: $CONF not found — run setup.sh first" >&2; exit 1; }
. "$CONF"
[ -n "${BASE_ROOT:-}" ] || { echo "ERROR: BASE_ROOT not set in $CONF" >&2; exit 1; }

ROOT_DIR="$(dirname "$BASE_ROOT")"

for m in "$ROOT_DIR/test" "$ROOT_DIR/scratch"; do
  if mountpoint -q "$m"; then
    sudo umount "$m"
    echo "unmounted $m"
  fi
done

# Wait for this setup's clients to exit. Match by the DINGOFS_BASE_DIR
# environment variable the mount helper sets (works for local and mds mode,
# and does not assume a particular client executable name).
deadline=$((SECONDS + 30))
while :; do
  busy=0
  pid=
  for environ in /proc/[0-9]*/environ; do
    [ -e "$environ" ] || continue
    pid="${environ#/proc/}"
    pid="${pid%/environ}"
    if sudo grep -qzFx \
        -e "DINGOFS_BASE_DIR=$BASE_ROOT/xftest" \
        -e "DINGOFS_BASE_DIR=$BASE_ROOT/xfscratch" \
        "$environ" 2>/dev/null; then
      busy=1
      break
    fi
  done
  [ "$busy" -eq 0 ] && break
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "ERROR: xfstests dingo-client (pid $pid) still running after 30s" >&2
    exit 1
  fi
  sleep 0.2
done

sudo rm -rf "${BASE_ROOT:?}/xftest" "${BASE_ROOT:?}/xfscratch"
echo "removed local runtime data for xftest/xfscratch"
echo "local-mode filesystems will be re-created on next mount"
echo "MDS-mode remote filesystem contents were not deleted"
