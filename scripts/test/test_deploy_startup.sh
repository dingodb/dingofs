#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
TEMP_ROOT=$(mktemp -d)
trap 'rm -rf -- "${TEMP_ROOT}"' EXIT

export CLUSTER_ID=0 MDS_INSTANCE_START_ID=1001 SERVER_START_PORT=6900
export SERVER_HOST=127.0.0.1 SERVER_LISTEN_HOST=127.0.0.1
export COORDINATOR_ADDR=127.0.0.1:6500
unset FLAGS_role FLAGS_clean_log FLAGS_replace_conf

prepare_package() {
  package="${TEMP_ROOT}/$1"
  mkdir -p "${package}/scripts" "${package}/conf" "${package}/build/bin"
  cp "${ROOT}"/scripts/deploy/{clean_start.sh,stop.sh,deploy.sh,start.sh,shflags} \
    "${package}/scripts/"
  chmod +x "${package}"/scripts/*.sh
  cp "${ROOT}/scripts/dev-mds/mds.template.conf" "${package}/conf/"
}

expect_exit() {
  local expected=$1 actual=0
  bash "${package}/scripts/clean_start.sh" --role=mds \
    >"${package}/console.log" 2>&1 || actual=$?
  if [[ "${actual}" -ne "${expected}" ]]; then
    cat "${package}/console.log" >&2
    cat "${package}/dist/mds/log/mds.log" >&2 2>/dev/null || true
    echo "FAIL: ${package##*/}: expected exit ${expected}, got ${actual}" >&2
    exit 1
  fi
  echo "PASS: ${package##*/}: exit ${actual}"
}

# A real exec failure must not be hidden by a later successful echo.
prepare_package missing-server
expect_exit 127

# Do not proceed to deployment when the stop command cannot execute.
prepare_package stop-failed
chmod -x "${package}/scripts/stop.sh"
expect_exit 126
test ! -e "${package}/dist"

# Do not launch the server when the deployment command cannot execute.
prepare_package deploy-failed
chmod -x "${package}/scripts/deploy.sh"
expect_exit 126
test ! -e "${package}/dist"

# Exercise the real stop/render/start chain with a successful executable.
prepare_package normal-exit
ln -s "$(type -P true)" "${package}/build/bin/dingo-mds"
expect_exit 0
conf="${package}/dist/mds/conf/mds.conf"
if grep -Eq '\$[A-Za-z_][A-Za-z_0-9]*' "${conf}"; then
  echo 'FAIL: unresolved configuration variable' >&2
  exit 1
fi
grep -Eq '^--log_v=-?[0-9]+$' "${conf}"
grep -Eq '^--log_level=(DEBUG|INFO|WARNING|ERROR|FATAL)$' "${conf}"
echo 'PASS: rendered logging configuration is valid'

# A server terminated by a signal must not be reported as a clean exit.
prepare_package signal-exit
printf '%s\n' '#!/bin/bash' 'kill -TERM "$$"' \
  >"${package}/build/bin/dingo-mds"
chmod +x "${package}/build/bin/dingo-mds"
expect_exit 143
