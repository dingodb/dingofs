#!/bin/bash
# apt fuse3 + libibverbs1 + dingocli (latest) + uv install.
# Sourced by .github/workflows/pr-check.yml (e2e job) AND
# .github/scripts/simulate-locally.sh — single source of truth.
set -euo pipefail

echo "[install] apt fuse3 + libibverbs1 (+ ccache for future build-from-source)..."
sudo apt-get update -qq
# libibverbs1 provides libibverbs.so.1: dingo-client/dingo-cache link it
# dynamically (RDMA cache feature). e2e runs TCP-only, but the binaries still
# need the .so present just to load, or they die at startup.
sudo apt-get install -y -qq fuse3 ccache libibverbs1

if ! command -v dingo >/dev/null 2>&1; then
  echo "[install] dingocli (latest)..."
  TMPF=$(mktemp)
  curl -fsSL "https://github.com/dingodb/dingocli/releases/latest/download/dingo" -o "${TMPF}"
  sudo install -m 0755 "${TMPF}" /usr/local/bin/dingo
  rm -f "${TMPF}"
else
  echo "[install] dingocli already present: $(command -v dingo)"
fi

if ! command -v uv >/dev/null 2>&1; then
  echo "[install] uv (astral.sh)..."
  curl -LsSf https://astral.sh/uv/install.sh | sh
  if [ -n "${GITHUB_PATH:-}" ]; then
    echo "${HOME}/.local/bin" >> "${GITHUB_PATH}"
  fi
fi
export PATH="${HOME}/.local/bin:${PATH}"

echo "[install] versions:"
docker --version
docker compose version | head -1
dingo --help 2>&1 | head -1 || true
uv --version || "${HOME}/.local/bin/uv" --version
fusermount3 --version || true

echo "[install] ✓"
