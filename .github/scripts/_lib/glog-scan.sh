#!/bin/bash
# Scan client glog for async errors pytest can't see.
# Catches: NoSuchBucket / Retry upload / CacheUnhealthy / Transport endpoint
# Run BEFORE pytest as a fail-fast gate.
# Sourced by .github/workflows/pr-check.yml (e2e job) AND
# .github/scripts/simulate-locally.sh — single source of truth.
set -uo pipefail

GLOG_DIR="${RUNNER_TEMP:-/tmp}/dingofs-runtime/client/log"
GLOG=$(ls -t "${GLOG_DIR}"/dingo-client.*log* 2>/dev/null | head -1 || true)

if [ -z "$GLOG" ]; then
  echo "[glog-scan] no client glog yet (deploy might still be settling)"
  exit 0
fi

HITS=$(sudo tail -200 "$GLOG" | grep -cE "NoSuchBucket|Retry upload|CacheUnhealthy|Transport endpoint" || true)
if [ "$HITS" -gt 0 ]; then
  echo "[glog-scan] ✗ $HITS critical error(s) in $(basename "$GLOG")"
  sudo tail -50 "$GLOG"
  exit 1
fi
echo "[glog-scan] ✓ no critical errors"
