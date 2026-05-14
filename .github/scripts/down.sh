#!/bin/bash
# Tear down CI stack. MUST work in any state (mount may not exist, MDS may not
# have started, containers may not be up). Every cleanup line wrapped with || true.
set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

RUNNER_TEMP="${RUNNER_TEMP:-/tmp}"
MOUNT_POINT="${RUNNER_TEMP}/dingofs-mount"
RUNTIME_DIR="${RUNNER_TEMP}/dingofs-runtime"

echo "[down] unmount client (lazy, even if IO inflight)..."
if mountpoint -q "${MOUNT_POINT}" 2>/dev/null; then
  sudo fusermount3 -uz "${MOUNT_POINT}" 2>/dev/null \
    || sudo umount -l "${MOUNT_POINT}" 2>/dev/null \
    || true
fi

echo "[down] kill dingo-client..."
if [ -f "${RUNTIME_DIR}/client.pid" ]; then
  CPID=$(cat "${RUNTIME_DIR}/client.pid")
  sudo kill "${CPID}" 2>/dev/null || true
  sleep 2
  sudo kill -9 "${CPID}" 2>/dev/null || true
fi

echo "[down] kill dingo-mds..."
if [ -f "${RUNTIME_DIR}/mds.pid" ]; then
  MPID=$(cat "${RUNTIME_DIR}/mds.pid")
  kill "${MPID}" 2>/dev/null || true
  sleep 2
  kill -9 "${MPID}" 2>/dev/null || true
fi

# Fallback: pkill by full path so we don't touch other dingofs instances
sudo pkill -f "${RUNTIME_DIR}/.*dingo-mds" 2>/dev/null || true
sudo pkill -f "${RUNTIME_DIR}/.*dingo-client" 2>/dev/null || true

echo "[down] docker compose down -v..."
docker compose down -v --remove-orphans 2>&1 | tail -5 || true

echo "[down] remove runtime dir + mount point..."
sudo rm -rf "${RUNTIME_DIR}" "${MOUNT_POINT}" 2>/dev/null || true

echo "[down] ✓"
