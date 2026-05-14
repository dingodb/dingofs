#!/bin/bash
# Sysctl tuning + disk/mem check.
# Sourced by .github/workflows/pr-check.yml (e2e job) AND
# .github/scripts/simulate-locally.sh — single source of truth.
set -euo pipefail

echo "[preflight] sysctl tune for dingo-store..."
sudo sysctl -w vm.overcommit_memory=1 >/dev/null
sudo sysctl -w vm.max_map_count=655360 >/dev/null

# THP — flip 'always' to madvise (dingo-store recommended)
if grep -q '\[always\]' /sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null; then
  echo madvise | sudo tee /sys/kernel/mm/transparent_hugepage/enabled >/dev/null
fi

echo "[preflight] disk:"
df -h / "${RUNNER_TEMP:-/tmp}"
echo "[preflight] mem:"
free -h
echo "[preflight] docker:"
docker info 2>&1 | grep -E "Storage Driver|Server Version" || true
echo "[preflight] ✓"
