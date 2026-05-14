#!/bin/bash
# Simulate the pr-check.yml e2e job locally.
#
# All preflight / install / glog-scan / up / deploy / down logic is shared via
# .github/scripts/{_lib/*.sh,up.sh,down.sh,deploy-mds-client.sh} — workflow yml
# and this script bash the SAME files. byte-identical alignment, 0 漂移 by design.
#
# This script's job is to:
#   1. set GHA env vars ($GITHUB_WORKSPACE, $RUNNER_TEMP) for local context
#   2. skip GHA-only actions (checkout / download-artifact / upload-artifact)
#   3. drive the same shared scripts in the same order as workflows do
#
# Usage:
#   bash .github/scripts/simulate-locally.sh                    # default — reuse local docker images + local build/
#   PULL=1 bash .github/scripts/simulate-locally.sh             # docker pull latest (real CI cold-start sim)
#   INSTALL=1 bash .github/scripts/simulate-locally.sh          # apt + dingocli + uv install
#
# Differences from real GHA:
#   - actions/checkout: skipped (already in repo dir)
#   - actions/download-artifact: uses local $WS/build/bin
#   - actions/upload-artifact: logs left in $RUNNER_TEMP/ci-logs on failure
set -euo pipefail

# ── Mimic GHA env ─────────────────────────────────────────────────────────
export GITHUB_WORKSPACE="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
export RUNNER_TEMP="${RUNNER_TEMP:-/tmp/dingofs-ci-sim}"
mkdir -p "${RUNNER_TEMP}"

PULL="${PULL:-0}"
INSTALL="${INSTALL:-0}"

t() { date +%s; }
START=$(t)
log() { printf '[sim/%4ds] %s\n' $(($(t)-START)) "$*"; }

trap 'log "(exit trap) tear down"; RUNNER_TEMP="${RUNNER_TEMP}" bash "${GITHUB_WORKSPACE}/.github/scripts/down.sh" 2>&1 | tail -5 || true' EXIT

log "GITHUB_WORKSPACE=${GITHUB_WORKSPACE}"
log "RUNNER_TEMP=${RUNNER_TEMP}"

# ── Preflight: sysctl + disk/mem check (shared with workflow yml) ──────────
log "preflight (_lib/preflight.sh)"
bash "${GITHUB_WORKSPACE}/.github/scripts/_lib/preflight.sh" 2>&1 | sed 's/^/    /'

# ── Install tools (skip if local has them) ─────────────────────────────────
if [ "${INSTALL}" = "1" ] || ! command -v dingo >/dev/null 2>&1 || ! command -v uv >/dev/null 2>&1; then
  log "install (_lib/install.sh)"
  bash "${GITHUB_WORKSPACE}/.github/scripts/_lib/install.sh" 2>&1 | sed 's/^/    /'
else
  log "install: skip (dingo+uv present); INSTALL=1 to force"
fi
export PATH="$HOME/.local/bin:$PATH"

# ── Verify binaries present (sim assumes local build, no artifact download) ──
log "verify local build/bin"
for bin in dingo-mds dingo-mds-client dingo-client; do
  if [ ! -x "${GITHUB_WORKSPACE}/build/bin/${bin}" ]; then
    log "✗ ${GITHUB_WORKSPACE}/build/bin/${bin} missing — build dingofs first"
    exit 1
  fi
done

# ── Pull docker images (digest pinned, see docker-compose.yml) ─────────────
if [ "${PULL}" = "1" ]; then
  log "docker pull pinned digests"
  grep 'image:' "${GITHUB_WORKSPACE}/.github/scripts/docker-compose.yml" \
    | awk '{print $2}' | sort -u | while read -r img; do
      docker pull "$img" 2>&1 | tail -2
    done
else
  log "docker pull: skip (PULL=1 to force)"
  grep 'image:' "${GITHUB_WORKSPACE}/.github/scripts/docker-compose.yml" \
    | awk '{print $2}' | sort -u | while read -r img; do
      docker image inspect "$img" >/dev/null 2>&1 \
        || { log "✗ image $img missing — pass PULL=1"; exit 1; }
    done
fi

# ── Bring up CI stack ──────────────────────────────────────────────────────
T0=$(t)
log "bring up CI stack"
bash "${GITHUB_WORKSPACE}/.github/scripts/up.sh" 2>&1 | sed 's/^/    /'
log "  up.sh took $(($(t)-T0))s"

# ── Deploy MDS + client ────────────────────────────────────────────────────
T0=$(t)
log "deploy MDS + client"
bash "${GITHUB_WORKSPACE}/.github/scripts/deploy-mds-client.sh" 2>&1 | sed 's/^/    /'
log "  deploy took $(($(t)-T0))s"

# ── Pre-test glog scan (shared with workflow yml) ──────────────────────────
log "pre-test glog scan (_lib/glog-scan.sh)"
bash "${GITHUB_WORKSPACE}/.github/scripts/_lib/glog-scan.sh" 2>&1 | sed 's/^/    /'

# ── pytest ─────────────────────────────────────────────────────────────────
T0=$(t)
log "pytest 119 tests"
cd "${GITHUB_WORKSPACE}/test/e2e"
uv sync 2>&1 | tail -3
uv run pytest --tb=short --mount-point="${RUNNER_TEMP}/dingofs-mount" 2>&1 | tail -10
log "  pytest took $(($(t)-T0))s"

log "════════════════════════════════════════"
log "✓ total simulation: $(($(t)-START))s"
log "════════════════════════════════════════"
# Cleanup runs via EXIT trap
