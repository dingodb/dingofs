#!/usr/bin/env bash

set -euo pipefail

ROOT=${ROOT:-/home/wine93/workspace/dingofs}
BIN_DIR=${BIN_DIR:-${ROOT}/build/bin}
DEVICE=${DEVICE:-mlx5_0}
PORT_NUM=${PORT_NUM:-1}

# Defaults match the jg31 -> jg30 fabric used during development. Override
# SERVER_HOST/SERVER_ADDR/CLIENT_HOST/CLIENT_ADDR if hostnames or IPs differ.
SERVER_HOST=${SERVER_HOST:-jg30}
SERVER_ADDR=${SERVER_ADDR:-100.64.0.6}
CLIENT_HOST=${CLIENT_HOST:-localhost}
CLIENT_ADDR=${CLIENT_ADDR:-100.64.0.7}
DIRECTION=${DIRECTION:-forward}  # forward|reverse|both

BASE_PORT=${BASE_PORT:-18888}
SIZES=${SIZES:-"8 4096 4194304"}
THREADS_LIST=${THREADS_LIST:-"1 8 32"}
OPS=${OPS:-"ping rdma_read rdma_write mixed"}
WARMUP_ROUNDS=${WARMUP_ROUNDS:-10}
ROUNDS_SMALL=${ROUNDS_SMALL:-1000}
ROUNDS_LARGE=${ROUNDS_LARGE:-100}
VERIFY=${VERIFY:-markers}
SERVER_VERIFY=${SERVER_VERIFY:-full}
SERVER_FILL_RESPONSE=${SERVER_FILL_RESPONSE:-true}
SERVER_ATTACHMENT_POOL_SIZE=${SERVER_ATTACHMENT_POOL_SIZE:-512}
SERVER_ATTACHMENT_BUFFER_SIZE=${SERVER_ATTACHMENT_BUFFER_SIZE:-4194304}

RUN_FAULTS=${RUN_FAULTS:-1}
RUN_RESTART=${RUN_RESTART:-1}
RUN_SMALL_POOL=${RUN_SMALL_POOL:-1}
RUN_IB_BW=${RUN_IB_BW:-1}
RUN_BRPC_COMPARE=${RUN_BRPC_COMPARE:-1}

declare -a SERVER_PIDS=()

is_local_host() {
  local host=$1
  [[ "${host}" == "localhost" || "${host}" == "127.0.0.1" || "${host}" == "$(hostname -s)" ]]
}

run_host() {
  local host=$1
  shift
  if is_local_host "${host}"; then
    bash -lc "$*"
  else
    ssh "${host}" "$*"
  fi
}

start_rdma_server() {
  local host=$1
  local port=$2
  local pool_size=$3
  local log_file="/tmp/dingofs-rdma-server-${port}.log"
  run_host "${host}" \
    "pkill -f '[r]dma_server.*--brpc_port=${port}' >/dev/null 2>&1 || true; \
	     nohup '${BIN_DIR}/rdma_server' --device='${DEVICE}' --port_num='${PORT_NUM}' \
	       --brpc_port='${port}' --attachment_pool_size='${pool_size}' \
	       --attachment_buffer_size='${SERVER_ATTACHMENT_BUFFER_SIZE}' \
	       --server_verify='${SERVER_VERIFY}' \
	       --server_fill_response='${SERVER_FILL_RESPONSE}' \
	       > '${log_file}' 2>&1 < /dev/null & echo \$!"
}

stop_rdma_server() {
  local host=$1
  local pid=$2
  run_host "${host}" "kill '${pid}' >/dev/null 2>&1 || true"
}

cleanup() {
  local entry host pid
  for entry in "${SERVER_PIDS[@]}"; do
    host=${entry%%:*}
    pid=${entry#*:}
    stop_rdma_server "${host}" "${pid}" || true
  done
}
trap cleanup EXIT

rounds_for_size() {
  local size=$1
  if (( size >= 4194304 )); then
    echo "${ROUNDS_LARGE}"
  else
    echo "${ROUNDS_SMALL}"
  fi
}

run_rdma_client() {
  local client_host=$1
  local server_addr=$2
  local port=$3
  local op=$4
  local size=$5
  local threads=$6
  local rounds=$7
  run_host "${client_host}" \
    "'${BIN_DIR}/rdma_client' --device='${DEVICE}' --port_num='${PORT_NUM}' \
       --server_address='${server_addr}:${port}' --op='${op}' \
       --payload_size='${size}' --threads='${threads}' --rounds='${rounds}' \
       --warmup_rounds='${WARMUP_ROUNDS}' --verify='${VERIFY}' \
       --json_result=true --log_per_round=false"
}

run_direction() {
  local name=$1
  local server_host=$2
  local server_addr=$3
  local client_host=$4
  local port=$5
  local server_pid

  echo "== ${name}: server=${server_host} (${server_addr}) client=${client_host} port=${port} =="
  server_pid=$(start_rdma_server "${server_host}" "${port}" "${SERVER_ATTACHMENT_POOL_SIZE}")
  SERVER_PIDS+=("${server_host}:${server_pid}")
  sleep 3

  for size in ${SIZES}; do
    for threads in ${THREADS_LIST}; do
      local rounds
      rounds=$(rounds_for_size "${size}")
      for op in ${OPS}; do
        echo "-- op=${op} size=${size} threads=${threads} rounds=${rounds}"
        run_rdma_client "${client_host}" "${server_addr}" "${port}" "${op}" \
          "${size}" "${threads}" "${rounds}"
      done
    done
  done

  if [[ "${RUN_FAULTS}" == "1" ]]; then
    echo "-- fault cases"
    run_rdma_client "${client_host}" "${server_addr}" "${port}" invalid_service 4096 1 3
    run_rdma_client "${client_host}" "${server_addr}" "${port}" invalid_method 4096 1 3
    run_rdma_client "${client_host}" "${server_addr}" "${port}" remote_too_small 4096 1 3
  fi

  if [[ "${RUN_RESTART}" == "1" ]]; then
    echo "-- restart/reconnect"
    stop_rdma_server "${server_host}" "${server_pid}" || true
    sleep 2
    server_pid=$(start_rdma_server "${server_host}" "${port}" "${SERVER_ATTACHMENT_POOL_SIZE}")
    SERVER_PIDS+=("${server_host}:${server_pid}")
    sleep 3
    run_rdma_client "${client_host}" "${server_addr}" "${port}" ping 8 1 10
  fi
}

run_small_pool_fault() {
  local server_host=$1
  local server_addr=$2
  local client_host=$3
  local port=$4
  local server_pid

  [[ "${RUN_SMALL_POOL}" == "1" ]] || return 0
  echo "== small attachment pool fault: server=${server_host} port=${port} =="
  server_pid=$(start_rdma_server "${server_host}" "${port}" 1)
  SERVER_PIDS+=("${server_host}:${server_pid}")
  sleep 3
  set +e
  run_rdma_client "${client_host}" "${server_addr}" "${port}" rdma_write 4096 32 8
  local rc=$?
  set -e
  echo "-- small-pool client exit=${rc} (non-zero is expected when pool is exhausted)"
}

run_ib_bw() {
  local server_host=$1
  local server_addr=$2
  [[ "${RUN_IB_BW}" == "1" ]] || return 0
  command -v ib_write_bw >/dev/null 2>&1 || {
    echo "== skip ib_*_bw: perftest not installed locally =="
    return 0
  }

  echo "== ib_write_bw baseline =="
  run_host "${server_host}" \
    "timeout 30 ib_write_bw -d '${DEVICE}' -F --report_gbits \
       > /tmp/dingofs-ib_write_bw.log 2>&1 < /dev/null & echo \$!" >/tmp/dingofs-ib_write_bw.pid
  sleep 2
  timeout 30 ib_write_bw -d "${DEVICE}" -F --report_gbits "${server_addr}" || true
  local pid
  pid=$(cat /tmp/dingofs-ib_write_bw.pid)
  stop_rdma_server "${server_host}" "${pid}" || true

  echo "== ib_read_bw baseline =="
  run_host "${server_host}" \
    "timeout 30 ib_read_bw -d '${DEVICE}' -F --report_gbits \
       > /tmp/dingofs-ib_read_bw.log 2>&1 < /dev/null & echo \$!" >/tmp/dingofs-ib_read_bw.pid
  sleep 2
  timeout 30 ib_read_bw -d "${DEVICE}" -F --report_gbits "${server_addr}" || true
  pid=$(cat /tmp/dingofs-ib_read_bw.pid)
  stop_rdma_server "${server_host}" "${pid}" || true
}

run_brpc_compare() {
  local server_host=$1
  local server_addr=$2
  local port=$3
  [[ "${RUN_BRPC_COMPARE}" == "1" ]] || return 0

  echo "== brpc TCP/RDMA comparison on port=${port} =="
  local mode pid use_rdma
  for mode in tcp rdma; do
    if [[ "${mode}" == "rdma" ]]; then
      use_rdma=true
    else
      use_rdma=false
    fi
    pid=$(run_host "${server_host}" \
      "nohup '${BIN_DIR}/brpc_server' --port='${port}' \
         --attachment_bytes='${SERVER_ATTACHMENT_BUFFER_SIZE}' \
         --use_rdma='${use_rdma}' \
         > /tmp/dingofs-brpc-${mode}-${port}.log 2>&1 < /dev/null & echo \$!")
    sleep 3
    "${BIN_DIR}/brpc_client" --server_address="${server_addr}:${port}" \
      --rounds=100 --use_rdma="${use_rdma}" || true
    stop_rdma_server "${server_host}" "${pid}" || true
    sleep 1
  done
}

case "${DIRECTION}" in
  forward)
    run_direction "forward" "${SERVER_HOST}" "${SERVER_ADDR}" "${CLIENT_HOST}" "${BASE_PORT}"
    run_small_pool_fault "${SERVER_HOST}" "${SERVER_ADDR}" "${CLIENT_HOST}" "$((BASE_PORT + 1))"
    run_ib_bw "${SERVER_HOST}" "${SERVER_ADDR}"
    run_brpc_compare "${SERVER_HOST}" "${SERVER_ADDR}" "$((BASE_PORT + 2))"
    ;;
  reverse)
    run_direction "reverse" "${CLIENT_HOST}" "${CLIENT_ADDR}" "${SERVER_HOST}" "${BASE_PORT}"
    ;;
  both)
    run_direction "forward" "${SERVER_HOST}" "${SERVER_ADDR}" "${CLIENT_HOST}" "${BASE_PORT}"
    run_direction "reverse" "${CLIENT_HOST}" "${CLIENT_ADDR}" "${SERVER_HOST}" "$((BASE_PORT + 10))"
    run_small_pool_fault "${SERVER_HOST}" "${SERVER_ADDR}" "${CLIENT_HOST}" "$((BASE_PORT + 20))"
    run_ib_bw "${SERVER_HOST}" "${SERVER_ADDR}"
    run_brpc_compare "${SERVER_HOST}" "${SERVER_ADDR}" "$((BASE_PORT + 30))"
    ;;
  *)
    echo "Unsupported DIRECTION=${DIRECTION}; use forward|reverse|both" >&2
    exit 1
    ;;
esac
