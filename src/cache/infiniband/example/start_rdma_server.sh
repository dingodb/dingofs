#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT=${ROOT:-/home/wine93/workspace/dingofs}
BIN_DIR=${BIN_DIR:-${ROOT}/build/bin}

if [[ -n "${RDMA_SERVER_BIN:-}" ]]; then
  SERVER_BIN=${RDMA_SERVER_BIN}
elif [[ -x "${SCRIPT_DIR}/rdma_server" ]]; then
  SERVER_BIN=${SCRIPT_DIR}/rdma_server
else
  SERVER_BIN=${BIN_DIR}/rdma_server
fi

DEVICE=${DEVICE:-mlx5_0}
PORT_NUM=${PORT_NUM:-1}
BRPC_PORT=${BRPC_PORT:-18888}
ATTACHMENT_POOL_SIZE=${ATTACHMENT_POOL_SIZE:-512}
ATTACHMENT_BUFFER_SIZE=${ATTACHMENT_BUFFER_SIZE:-4194304}
SERVER_VERIFY=${SERVER_VERIFY:-none}
SERVER_FILL_RESPONSE=${SERVER_FILL_RESPONSE:-false}
LOG_DIR=${LOG_DIR:-/tmp}
LOG_FILE=${LOG_FILE:-${LOG_DIR}/dingofs-rdma-server-${BRPC_PORT}.log}
PID_FILE=${PID_FILE:-${LOG_DIR}/dingofs-rdma-server-${BRPC_PORT}.pid}
START_WAIT_SEC=${START_WAIT_SEC:-2}
FORCE=${FORCE:-0}

usage() {
  cat <<EOF
Usage: $(basename "$0") [start|stop|restart|status] [extra rdma_server flags]

Environment:
  RDMA_SERVER_BIN            Path to rdma_server. Default: same dir, then ${BIN_DIR}/rdma_server
  DEVICE                    Default: ${DEVICE}
  PORT_NUM                  Default: ${PORT_NUM}
  BRPC_PORT                 Default: ${BRPC_PORT}
  ATTACHMENT_POOL_SIZE      Default: ${ATTACHMENT_POOL_SIZE}
  ATTACHMENT_BUFFER_SIZE    Default: ${ATTACHMENT_BUFFER_SIZE}
  SERVER_VERIFY             full|markers|none. Default: ${SERVER_VERIFY}
  SERVER_FILL_RESPONSE      true|false. Default: ${SERVER_FILL_RESPONSE}
  LOG_FILE                  Default: ${LOG_FILE}
  PID_FILE                  Default: ${PID_FILE}
  FORCE=1                   Kill an existing rdma_server on BRPC_PORT before start.

Examples:
  $(basename "$0") start
  SERVER_VERIFY=markers SERVER_FILL_RESPONSE=true $(basename "$0") restart
  $(basename "$0") start --rdma_send_queue_size=8192
  $(basename "$0") stop
EOF
}

read_pid() {
  if [[ -f "${PID_FILE}" ]]; then
    tr -d '[:space:]' <"${PID_FILE}"
  fi
}

is_running() {
  local pid
  pid=$(read_pid || true)
  [[ -n "${pid}" ]] && kill -0 "${pid}" >/dev/null 2>&1
}

status_server() {
  local pid
  pid=$(read_pid || true)
  if [[ -n "${pid}" ]] && kill -0 "${pid}" >/dev/null 2>&1; then
    echo "rdma_server is running: pid=${pid} port=${BRPC_PORT}"
    return 0
  fi
  echo "rdma_server is not running: port=${BRPC_PORT}"
  return 1
}

start_server() {
  if [[ ! -x "${SERVER_BIN}" ]]; then
    echo "rdma_server is not executable: ${SERVER_BIN}" >&2
    return 1
  fi

  mkdir -p "${LOG_DIR}" "$(dirname "${PID_FILE}")"

  if is_running; then
    if [[ "${FORCE}" != "1" ]]; then
      status_server
      return 0
    fi
    stop_server || true
  fi

  if [[ "${FORCE}" == "1" ]]; then
    pkill -f "[r]dma_server .*--brpc_port=${BRPC_PORT}" >/dev/null 2>&1 || true
  fi

  nohup "${SERVER_BIN}" \
    --device="${DEVICE}" \
    --port_num="${PORT_NUM}" \
    --brpc_port="${BRPC_PORT}" \
    --attachment_pool_size="${ATTACHMENT_POOL_SIZE}" \
    --attachment_buffer_size="${ATTACHMENT_BUFFER_SIZE}" \
    --server_verify="${SERVER_VERIFY}" \
    --server_fill_response="${SERVER_FILL_RESPONSE}" \
    "$@" >"${LOG_FILE}" 2>&1 < /dev/null &

  local pid=$!
  echo "${pid}" >"${PID_FILE}"
  sleep "${START_WAIT_SEC}"

  if ! kill -0 "${pid}" >/dev/null 2>&1; then
    echo "rdma_server failed to start. Last log lines:" >&2
    tail -n 80 "${LOG_FILE}" >&2 || true
    return 1
  fi

  echo "rdma_server started: pid=${pid} port=${BRPC_PORT}"
  echo "log: ${LOG_FILE}"
}

stop_server() {
  local pid
  pid=$(read_pid || true)
  if [[ -z "${pid}" ]]; then
    echo "no pid file: ${PID_FILE}"
    return 0
  fi

  if ! kill -0 "${pid}" >/dev/null 2>&1; then
    echo "stale pid file removed: ${PID_FILE}"
    rm -f "${PID_FILE}"
    return 0
  fi

  kill "${pid}" >/dev/null 2>&1 || true
  for _ in {1..30}; do
    if ! kill -0 "${pid}" >/dev/null 2>&1; then
      rm -f "${PID_FILE}"
      echo "rdma_server stopped: pid=${pid}"
      return 0
    fi
    sleep 1
  done

  echo "rdma_server did not stop in time, sending SIGKILL: pid=${pid}" >&2
  kill -9 "${pid}" >/dev/null 2>&1 || true
  rm -f "${PID_FILE}"
}

cmd=${1:-start}
case "${cmd}" in
  -h|--help|help)
    usage
    ;;
  start)
    shift || true
    start_server "$@"
    ;;
  stop)
    stop_server
    ;;
  restart)
    shift || true
    stop_server || true
    start_server "$@"
    ;;
  status)
    status_server
    ;;
  *)
    echo "unknown command: ${cmd}" >&2
    usage >&2
    exit 1
    ;;
esac
