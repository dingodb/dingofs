#!/bin/bash
# One-shot setup for running xfstests against DingoFS.
#
# Usage:  bash setup.sh <path-to-xfstests-tree> [root-dir]
#
#   root-dir (default /mnt/dingofs-xfstests) — everything lives under it:
#     <root>/test      TEST_DIR mountpoint
#     <root>/scratch   SCRATCH_MNT mountpoint
#     <root>/runtime   per-fs runtime data (leveldb meta / storage / cache / logs)
#
# Optional environment variables:
#   DINGOFS_CLIENT_BIN        dingo-client binary (default: <repo>/build/bin/dingo-client)
#   DINGOFS_META_URL_TEMPLATE meta-url template, {fsname} substituted
#                             (default: empty = local file backend under <root>/runtime)
#   DINGOFS_EXTRA_FLAGS       extra client flags, space-separated (default: empty)
#
# Example (MDS mode + custom root):
#   DINGOFS_META_URL_TEMPLATE='mds://10.232.10.7:7400/{fsname}' \
#   bash setup.sh ~/workspace/xfstests-dev /mnt/nvme/dingofs-xfstests
#
# Installs the mount helper, writes /etc/dingofs-xfstests.conf, creates the
# root layout, writes local.config into the xfstests tree, and ensures the
# fsgqa users exist. Re-running overwrites conf and local.config.
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
XFSTESTS_DIR="${1:-}"
ROOT_DIR="${2:-${DINGOFS_XFSTESTS_ROOT:-/mnt/dingofs-xfstests}}"
CLIENT_BIN="${DINGOFS_CLIENT_BIN:-$REPO_ROOT/build/bin/dingo-client}"

if [ -z "$XFSTESTS_DIR" ]; then
  echo "Usage: bash setup.sh <path-to-xfstests-tree> [root-dir]" >&2
  exit 1
fi
if [ ! -f "$XFSTESTS_DIR/check" ]; then
  echo "ERROR: $XFSTESTS_DIR does not look like an xfstests tree (no 'check' script)" >&2
  exit 1
fi
[ -x "$CLIENT_BIN" ] || { echo "ERROR: dingo-client not found at $CLIENT_BIN (build first)" >&2; exit 1; }

TEST_DIR="$ROOT_DIR/test"
SCRATCH_MNT="$ROOT_DIR/scratch"
BASE_ROOT="$ROOT_DIR/runtime"

sudo install -m755 "$SCRIPT_DIR/mount.fuse.dingofs" /sbin/mount.fuse.dingofs

# printf %q — the conf is sourced by root (from the mount helper) and
# local.config is sourced by xfstests, so every written value must be
# shell-safe: quotes/spaces/$/backticks in paths or URLs must not break the
# file or execute anything.
q() { printf '%q' "$1"; }

sudo tee /etc/dingofs-xfstests.conf >/dev/null <<EOF
# Written by xfstests/setup.sh — sourced by /sbin/mount.fuse.dingofs.
CLIENT_BIN=$(q "$CLIENT_BIN")
BASE_ROOT=$(q "$BASE_ROOT")
# meta-url template; {fsname} is replaced with the fsname from the device
# string. Default (empty) = local file backend under BASE_ROOT.
# MDS mode example:  META_URL_TEMPLATE='mds://10.232.10.7:7400/{fsname}'
# Per-fs override:   META_URL_xftest='local://xftest?storage=s3&...'
META_URL_TEMPLATE=$(q "${DINGOFS_META_URL_TEMPLATE:-}")
# Extra dingo-client flags, space-separated (word-split on purpose; values
# with spaces are not supported). Example:
# EXTRA_FLAGS='--v=1 --vfs_access_logging=false'
EXTRA_FLAGS=$(q "${DINGOFS_EXTRA_FLAGS:-}")
# Per-fs dummy-server port override; default is a stable per-fsname hash in
# 11900-11989. Ports must differ between the two instances. Example:
# PORT_xftest=12345
EOF

sudo mkdir -p "$TEST_DIR" "$SCRATCH_MNT" "$BASE_ROOT"

cat > "$XFSTESTS_DIR/local.config" <<EOF
# Written by dingofs xfstests/setup.sh — do not edit; re-run setup.sh instead.
# Device strings are also used as the FUSE mount source. Keep them free of ':'.

export FSTYP=fuse
export FUSE_SUBTYP=.dingofs

export TEST_DEV=xftest
export TEST_DIR=$(q "$TEST_DIR")

export SCRATCH_DEV=xfscratch
export SCRATCH_MNT=$(q "$SCRATCH_MNT")

export MOUNT_OPTIONS=""
export TEST_FS_MOUNT_OPTS=""
EOF
echo "local.config -> $XFSTESTS_DIR/local.config"

# Many generic tests require these users and the fsgqa group.
getent group fsgqa >/dev/null 2>&1 || sudo groupadd fsgqa
for u in fsgqa fsgqa2 123456-fsgqa; do
  id "$u" >/dev/null 2>&1 || sudo useradd -m -g fsgqa "$u"
done

echo "Root layout: $ROOT_DIR/{test,scratch,runtime}"
echo "Setup done. Run:  cd $XFSTESTS_DIR && sudo ./check generic/001"
