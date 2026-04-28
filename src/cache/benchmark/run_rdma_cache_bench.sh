#!/usr/bin/env bash

set -euo pipefail

ROOT=${ROOT:-"$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"}
BUILD_DIR=${BUILD_DIR:-"$ROOT/build"}
BIN_DIR=${BIN_DIR:-"$BUILD_DIR/bin"}
DEPLOY_DIR=${DEPLOY_DIR:-/home/wine93/deploy/dingo-cache}
RESULT_DIR=${RESULT_DIR:-"$ROOT/bench_results/$(date +%Y%m%d-%H%M%S)"}

MDS_ADDRS=${MDS_ADDRS:-10.220.88.31:6900}
GROUP_NAME=${GROUP_NAME:-group-1}
FSID=${FSID:-1}

RDMA_DEVICE=${RDMA_DEVICE:-mlx5_0}
RDMA_PORT_NUM=${RDMA_PORT_NUM:-1}
USE_RDMA=${USE_RDMA:-1}
REGISTERED_BUFFERS=${REGISTERED_BUFFERS:-1}
CLIENT_POOL_SIZE=${CLIENT_POOL_SIZE:-4096}
SERVER_POOL_SIZE=${SERVER_POOL_SIZE:-2048}
SERVER_POOL_BUFFER_SIZE=${SERVER_POOL_BUFFER_SIZE:-4194304}

JG29_HOST=${JG29_HOST:-jg29}
JG29_IP=${JG29_IP:-100.64.0.5}
JG29_PORT=${JG29_PORT:-10001}
JG30_HOST=${JG30_HOST:-jg30}
JG30_IP=${JG30_IP:-100.64.0.6}
JG30_PORT=${JG30_PORT:-10002}

THREADS_4K=${THREADS_4K:-"1 8 32 128 256"}
THREADS_4M=${THREADS_4M:-"1 4 8 16 32 64"}
BLOCKS_4K=${BLOCKS_4K:-4096}
BLOCKS_4M=${BLOCKS_4M:-64}
SMOKE_BLOCKS=${SMOKE_BLOCKS:-4}
START_BLOCK_ID=${START_BLOCK_ID:-1000000000}
CACHE_SETTLE_SECONDS=${CACHE_SETTLE_SECONDS:-20}

RUN_BUILD=${RUN_BUILD:-1}
RUN_DEPLOY=${RUN_DEPLOY:-1}
RUN_RESTART=${RUN_RESTART:-1}
RUN_SMOKE=${RUN_SMOKE:-1}
RUN_PERF=${RUN_PERF:-1}
SMOKE_ONLY=${SMOKE_ONLY:-0}
RUN_DROP_CACHES=${RUN_DROP_CACHES:-0}
RUN_IB_BW=${RUN_IB_BW:-0}
CLEAN_REMOTE_LOGS=${CLEAN_REMOTE_LOGS:-0}
KEEP_NODES=${KEEP_NODES:-0}

SSH_OPTS=(-o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=8)
RUN_ID=$(basename "$RESULT_DIR")
REMOTE_RESULT_DIR="$DEPLOY_DIR/rdma-cache-bench-$RUN_ID"

mkdir -p "$RESULT_DIR"/{client_logs,json,remote}

log() {
  printf '[%s] %s\n' "$(date '+%F %T')" "$*"
}

remote() {
  local host=$1
  shift
  ssh "${SSH_OPTS[@]}" "$host" "$@"
}

copy_to_remote() {
  local host=$1
  scp "${SSH_OPTS[@]}" "$BIN_DIR/dingo-cache" "$host:$DEPLOY_DIR/dingo-cache"
  scp "${SSH_OPTS[@]}" "$BIN_DIR/cache-bench" "$host:$DEPLOY_DIR/cache-bench"
}

build_bins() {
  [[ "$RUN_BUILD" == "1" ]] || return 0
  log "Building dingo-cache and cache-bench"
  cmake --build "$BUILD_DIR" --target dingo-cache cache-bench -j "${BUILD_JOBS:-2}"
}

deploy_bins() {
  [[ "$RUN_DEPLOY" == "1" ]] || return 0
  log "Deploying binaries"
  if [[ "$RUN_RESTART" == "1" ]]; then
    stop_remote_node "$JG29_HOST"
    stop_remote_node "$JG30_HOST"
  fi
  mkdir -p "$DEPLOY_DIR"
  install -m 0755 "$BIN_DIR/cache-bench" "$DEPLOY_DIR/cache-bench"
  copy_to_remote "$JG29_HOST"
  copy_to_remote "$JG30_HOST"
}

stop_remote_node() {
  local host=$1
  remote "$host" "sudo -n pkill -TERM -f '$DEPLOY_DIR/[d]ingo-cache' || true; sleep 2; sudo -n pkill -KILL -f '$DEPLOY_DIR/[d]ingo-cache' || true"
}

drop_remote_caches() {
  [[ "$RUN_DROP_CACHES" == "1" ]] || return 0
  local host=$1
  log "Dropping page cache on $host for metadata-cold sensitivity run"
  remote "$host" "sudo -n sync; echo 3 | sudo -n tee /proc/sys/vm/drop_caches >/dev/null"
}

start_remote_node() {
  local host=$1
  local ip=$2
  local port=$3
  log "Starting dingo-cache on $host ($ip:$port)"
  remote "$host" "mkdir -p '$REMOTE_RESULT_DIR'"
  if [[ "$CLEAN_REMOTE_LOGS" == "1" ]]; then
    remote "$host" "sudo -n rm -rf /mnt/disk1/chenjl/logs/* || true"
  fi
  drop_remote_caches "$host"
  remote "$host" "sed 's/^--loglevel=/--log_level=/' '$DEPLOY_DIR/node.conf' > '$REMOTE_RESULT_DIR/node.conf'"
  remote "$host" "cd '$DEPLOY_DIR' && sudo -n setsid -f env LD_PRELOAD=/lib64/libjemalloc.so.2 \
    numactl --cpunodebind=0 --membind=0 \
    '$DEPLOY_DIR/dingo-cache' --conf '$REMOTE_RESULT_DIR/node.conf' \
      --use_rdma=${USE_RDMA} \
      --cache_rdma_device='${RDMA_DEVICE}' \
      --cache_rdma_port_num='${RDMA_PORT_NUM}' \
      --cache_rdma_server_pool_size='${SERVER_POOL_SIZE}' \
    < /dev/null > '$REMOTE_RESULT_DIR/dingo-cache.stdout' 2> '$REMOTE_RESULT_DIR/dingo-cache.stderr'"
}

wait_port() {
  local host=$1
  local port=$2
  for _ in $(seq 1 90); do
    if remote "$host" "ss -ltn | awk '{print \$4}' | grep -q ':$port$'"; then
      return 0
    fi
    sleep 1
  done
  log "Timeout waiting for $host:$port"
  return 1
}

start_remote_monitors() {
  local host=$1
  local ip=$2
  local port=$3
  remote "$host" "mkdir -p '$REMOTE_RESULT_DIR/monitors';
    pid=\$(pgrep -f '$DEPLOY_DIR/[d]ingo-cache.*$REMOTE_RESULT_DIR/node.conf' | head -1);
    echo \"\$pid\" > '$REMOTE_RESULT_DIR/monitors/dingo-cache.pid';
    nohup pidstat -durh -p \"\$pid\" 1 > '$REMOTE_RESULT_DIR/monitors/pidstat.log' 2>&1 & echo \$! > '$REMOTE_RESULT_DIR/monitors/pidstat.pid';
    nohup iostat -xm 1 > '$REMOTE_RESULT_DIR/monitors/iostat.log' 2>&1 & echo \$! > '$REMOTE_RESULT_DIR/monitors/iostat.pid';
    nohup sar -n DEV 1 > '$REMOTE_RESULT_DIR/monitors/sar-net.log' 2>&1 & echo \$! > '$REMOTE_RESULT_DIR/monitors/sar-net.pid';
    (while kill -0 \"\$pid\" 2>/dev/null; do date '+%F %T'; numastat -p \"\$pid\" || true; cat /proc/\"\$pid\"/status || true; sleep 5; done) > '$REMOTE_RESULT_DIR/monitors/proc-status.log' 2>&1 & echo \$! > '$REMOTE_RESULT_DIR/monitors/proc-status.pid';
    curl -s 'http://$ip:$port/vars' > '$REMOTE_RESULT_DIR/monitors/brpc-vars-before.txt' || true"
}

stop_remote_monitors() {
  local host=$1
  local ip=$2
  local port=$3
  remote "$host" "mkdir -p '$REMOTE_RESULT_DIR/monitors';
    curl -s 'http://$ip:$port/vars' > '$REMOTE_RESULT_DIR/monitors/brpc-vars-after.txt' || true;
    for f in '$REMOTE_RESULT_DIR'/monitors/*.pid; do
      [ -f \"\$f\" ] || continue;
      p=\$(cat \"\$f\" 2>/dev/null || true);
      [ -n \"\$p\" ] && kill \"\$p\" 2>/dev/null || true;
    done"
}

restart_nodes() {
  [[ "$RUN_RESTART" == "1" ]] || return 0
  log "Stopping old test nodes"
  stop_remote_node "$JG29_HOST"
  stop_remote_node "$JG30_HOST"
  start_remote_node "$JG29_HOST" "$JG29_IP" "$JG29_PORT"
  start_remote_node "$JG30_HOST" "$JG30_IP" "$JG30_PORT"
  wait_port "$JG29_HOST" "$JG29_PORT"
  wait_port "$JG30_HOST" "$JG30_PORT"
  start_remote_monitors "$JG29_HOST" "$JG29_IP" "$JG29_PORT"
  start_remote_monitors "$JG30_HOST" "$JG30_IP" "$JG30_PORT"
  log "Waiting for cache nodes to join MDS group"
  sleep 8
}

client_common_flags() {
  local size=$1
  printf '%q ' \
    --bench_remote_only=true \
    --cache_group="$GROUP_NAME" \
    --mds_addrs="$MDS_ADDRS" \
    --fsid="$FSID" \
    --use_rdma="$USE_RDMA" \
    --cache_rdma_device="$RDMA_DEVICE" \
    --cache_rdma_port_num="$RDMA_PORT_NUM" \
    --cache_rdma_client_pool_size="$CLIENT_POOL_SIZE" \
    --cache_rdma_client_pool_buffer_size="$size" \
    --bench_rdma_registered_buffers="$REGISTERED_BUFFERS" \
    --blksize="$size" \
    --offset=0 \
    --length="$size" \
    --json_result=true \
    --verify=none \
    --log_dir="$RESULT_DIR/client_logs"
}

start_client_monitors() {
  local case_name=$1
  local pid=$2
  local dir="$RESULT_DIR/client_logs/$case_name"
  mkdir -p "$dir"
  pidstat -durh -p "$pid" 1 > "$dir/pidstat.log" 2>&1 &
  echo $! > "$dir/pidstat.pid"
  iostat -xm 1 > "$dir/iostat.log" 2>&1 &
  echo $! > "$dir/iostat.pid"
  sar -n DEV 1 > "$dir/sar-net.log" 2>&1 &
  echo $! > "$dir/sar-net.pid"
}

stop_client_monitors() {
  local case_name=$1
  local dir="$RESULT_DIR/client_logs/$case_name"
  for f in "$dir"/*.pid; do
    [[ -f "$f" ]] || continue
    kill "$(cat "$f")" 2>/dev/null || true
  done
}

bench_case() {
  local case_name=$1
  local op=$2
  local size=$3
  local threads=$4
  local blocks=$5
  local start_id=$6
  local extra_flags=${7:-}
  local json="$RESULT_DIR/json/$case_name.json"
  local log_path="$RESULT_DIR/client_logs/$case_name.log"
  local common
  common=$(client_common_flags "$size")

  log "Running $case_name op=$op size=$size threads=$threads blocks=$blocks start=$start_id"
  # shellcheck disable=SC2086
  "$DEPLOY_DIR/cache-bench" \
    $common \
    --op="$op" \
    --threads="$threads" \
    --blocks="$blocks" \
    --start_block_id="$start_id" \
    --result_path="$json" \
    $extra_flags \
    > "$log_path" 2>&1 &
  local pid=$!
  start_client_monitors "$case_name" "$pid"
  set +e
  wait "$pid"
  local rc=$?
  set -e
  stop_client_monitors "$case_name"
  if [[ $rc -ne 0 ]]; then
    log "Case $case_name failed with rc=$rc; see $log_path"
    return "$rc"
  fi
  if command -v jq >/dev/null 2>&1 && [[ -f "$json" ]]; then
    local fails
    fails=$(jq -r '.fail // 1' "$json")
    if [[ "$fails" != "0" ]]; then
      log "Case $case_name reported fail=$fails; see $json"
      return 2
    fi
  fi
}

run_smoke() {
  [[ "$RUN_SMOKE" == "1" ]] || return 0
  local start=$START_BLOCK_ID
  bench_case "smoke_4k_cache" cache 4096 1 "$SMOKE_BLOCKS" "$start"
  sleep "$CACHE_SETTLE_SECONDS"
  bench_case "smoke_4k_range_hit" range 4096 1 "$SMOKE_BLOCKS" "$start" "--retrive=false"
  start=$((start + 100000))
  bench_case "smoke_4m_cache" cache 4194304 1 "$SMOKE_BLOCKS" "$start"
  sleep "$CACHE_SETTLE_SECONDS"
  bench_case "smoke_4m_range_hit" range 4194304 1 "$SMOKE_BLOCKS" "$start" "--retrive=false"
}

run_perf_matrix() {
  [[ "$RUN_PERF" == "1" ]] || return 0
  [[ "$SMOKE_ONLY" == "0" ]] || return 0

  local start=$((START_BLOCK_ID + 1000000))
  for threads in $THREADS_4K; do
    bench_case "4k_cache_t${threads}" cache 4096 "$threads" "$BLOCKS_4K" "$start"
    sleep "$CACHE_SETTLE_SECONDS"
    bench_case "4k_range_t${threads}" range 4096 "$threads" "$BLOCKS_4K" "$start" "--retrive=false"
    start=$((start + 1000000))
  done

  for threads in $THREADS_4M; do
    bench_case "4m_cache_t${threads}" cache 4194304 "$threads" "$BLOCKS_4M" "$start"
    sleep "$CACHE_SETTLE_SECONDS"
    bench_case "4m_range_t${threads}" range 4194304 "$threads" "$BLOCKS_4M" "$start" "--retrive=false"
    start=$((start + 1000000))
  done

  for threads in $THREADS_4K; do
    bench_case "4k_put_t${threads}" put 4096 "$threads" "$BLOCKS_4K" "$start" "--writeback=true"
    start=$((start + 1000000))
  done

  for threads in $THREADS_4M; do
    bench_case "4m_put_t${threads}" put 4194304 "$threads" "$BLOCKS_4M" "$start" "--writeback=true"
    start=$((start + 1000000))
  done
}

run_ib_reference() {
  [[ "$RUN_IB_BW" == "1" ]] || return 0
  log "RUN_IB_BW=1 requested, but ib_*_bw orchestration is intentionally left manual"
  log "Run ib_read_bw/ib_write_bw between jg31 and one cache node and save output under $RESULT_DIR/reference"
}

collect_remote_results() {
  log "Collecting remote logs"
  for host in "$JG29_HOST" "$JG30_HOST"; do
    mkdir -p "$RESULT_DIR/remote/$host"
    if remote "$host" "test -d '$REMOTE_RESULT_DIR'"; then
      scp "${SSH_OPTS[@]}" -r "$host:$REMOTE_RESULT_DIR/." "$RESULT_DIR/remote/$host/" || true
    fi
  done
}

write_summary() {
  if command -v jq >/dev/null 2>&1; then
    {
      echo -e "case\top\tthreads\tblksize\tsuccess\tfail\tqps\tMiB/s\tp50_us\tp99_us"
      for f in "$RESULT_DIR"/json/*.json; do
        [[ -f "$f" ]] || continue
        jq -r --arg case "$(basename "$f" .json)" \
          '[$case,.op,.threads,.blksize,.success,.fail,.qps,.mib_per_sec,.lat_us.p50,.lat_us.p99] | @tsv' "$f"
      done
    } > "$RESULT_DIR/summary.tsv"
    log "Wrote $RESULT_DIR/summary.tsv"
  fi
}

cleanup() {
  set +e
  stop_remote_monitors "$JG29_HOST" "$JG29_IP" "$JG29_PORT"
  stop_remote_monitors "$JG30_HOST" "$JG30_IP" "$JG30_PORT"
  collect_remote_results
  write_summary
  if [[ "$KEEP_NODES" != "1" ]]; then
    stop_remote_node "$JG29_HOST"
    stop_remote_node "$JG30_HOST"
  fi
}

trap cleanup EXIT

log "Result dir: $RESULT_DIR"
build_bins
deploy_bins
restart_nodes
run_smoke
run_perf_matrix
run_ib_reference
log "Done"
