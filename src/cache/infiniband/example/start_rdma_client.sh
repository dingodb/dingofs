#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT=${ROOT:-/home/wine93/workspace/dingofs}
BIN_DIR=${BIN_DIR:-${ROOT}/build/bin}

if [[ -n "${RDMA_CLIENT_BIN:-}" ]]; then
  CLIENT_BIN=${RDMA_CLIENT_BIN}
elif [[ -x "${SCRIPT_DIR}/rdma_client" ]]; then
  CLIENT_BIN=${SCRIPT_DIR}/rdma_client
else
  CLIENT_BIN=${BIN_DIR}/rdma_client
fi

DEVICE=${DEVICE:-mlx5_0}
PORT_NUM=${PORT_NUM:-1}
SERVER_ADDRESS=${SERVER_ADDRESS:-100.64.0.6:18888}
OP=${OP:-rdma_write}
PAYLOAD_SIZE=${PAYLOAD_SIZE:-4194304}
THREADS=${THREADS:-1}
ROUNDS=${ROUNDS:-100}
WARMUP_ROUNDS=${WARMUP_ROUNDS:-10}
VERIFY=${VERIFY:-none}
JSON_RESULT=${JSON_RESULT:-true}
LOG_PER_ROUND=${LOG_PER_ROUND:-false}
LOG_FILE=${LOG_FILE:-}

usage() {
  cat <<EOF
Usage: $(basename "$0") [extra rdma_client flags]

Environment:
  RDMA_CLIENT_BIN       Path to rdma_client. Default: same dir, then ${BIN_DIR}/rdma_client
  DEVICE                Default: ${DEVICE}
  PORT_NUM              Default: ${PORT_NUM}
  SERVER_ADDRESS        host:port of rdma_server. Default: ${SERVER_ADDRESS}
  OP                    ping|rdma_read|rdma_write|mixed|invalid_service|invalid_method|remote_too_small
                        Default: ${OP}
  PAYLOAD_SIZE          Default: ${PAYLOAD_SIZE}
  THREADS               Default: ${THREADS}
  ROUNDS                Default: ${ROUNDS}
  WARMUP_ROUNDS         Default: ${WARMUP_ROUNDS}
  VERIFY                full|markers|none. Default: ${VERIFY}
  JSON_RESULT           true|false. Default: ${JSON_RESULT}
  LOG_PER_ROUND         true|false. Default: ${LOG_PER_ROUND}
  LOG_FILE              Optional file to tee output into.

Examples:
  SERVER_ADDRESS=100.64.0.6:18888 OP=rdma_read THREADS=2 $(basename "$0")
  OP=rdma_write PAYLOAD_SIZE=4194304 THREADS=24 ROUNDS=100 $(basename "$0")
  VERIFY=markers ROUNDS=5 $(basename "$0") --rdma_rpc_timeout_ms=5000
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" || "${1:-}" == "help" ]]; then
  usage
  exit 0
fi

if [[ ! -x "${CLIENT_BIN}" ]]; then
  echo "rdma_client is not executable: ${CLIENT_BIN}" >&2
  exit 1
fi

cmd=(
  "${CLIENT_BIN}"
  --device="${DEVICE}"
  --port_num="${PORT_NUM}"
  --server_address="${SERVER_ADDRESS}"
  --op="${OP}"
  --payload_size="${PAYLOAD_SIZE}"
  --threads="${THREADS}"
  --rounds="${ROUNDS}"
  --warmup_rounds="${WARMUP_ROUNDS}"
  --verify="${VERIFY}"
  --json_result="${JSON_RESULT}"
  --log_per_round="${LOG_PER_ROUND}"
)

cmd+=("$@")

echo "running: ${cmd[*]}"
if [[ -n "${LOG_FILE}" ]]; then
  mkdir -p "$(dirname "${LOG_FILE}")"
  "${cmd[@]}" 2>&1 | tee "${LOG_FILE}"
  exit "${PIPESTATUS[0]}"
fi

exec "${cmd[@]}"
