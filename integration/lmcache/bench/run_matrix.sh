#!/usr/bin/env bash
# TCP-vs-RDMA benchmark matrix for the real DingoFSConnector.
#
# Each cell is a fresh `client.py` process (process isolation sidesteps the
# one-engine-per-process singleton). The only difference between a TCP and an
# RDMA cell is the --conf file (--use_rdma=false|true); the arena stays
# registered in both, so it is a clean apples-to-apples A/B.
#
#   Latency  : client.py bench --mode seq  -> per-op p50/p99 + per-op MiB/s
#   Bandwidth: client.py watch --op get|put -> sustained MiB/s + per-batch p50/p99
set -u

HERE="$(cd "$(dirname "$0")" && pwd)"
CLIENT="$HERE/../examples/client.py"
PY=python3.11
URL="dingofs://10.220.88.31:6900/group-1"
OUT="$HERE/results/matrix.csv"
LOGDIR="$HERE/results/logs"
mkdir -p "$LOGDIR"

# Full ENV.md size sweep. >1 MiB blocks now work after the io_uring O_DIRECT
# short-completion resubmit fix (device max_hw_sectors_kb=128 KiB short-completes
# large single I/Os; the AIO queue now resubmits the remainder).
SIZES="4096 65536 131072 1048576 2097152 4194304"
CONCS="1 4 16"
LAT_COUNT=30
BW_DUR=8
BW_WARMUP=2
ARENA_MIB=2048

# keys per batched call, tuned per size so a batch moves a sane chunk and the
# arena (ARENA_MIB) comfortably holds concurrency x count x size live buffers.
bw_count() { case "$1" in 4096|65536|131072) echo 256;; 1048576) echo 64;; 2097152) echo 32;; 4194304) echo 16;; esac; }

field() { sed -n "s/.*$1=\\([0-9.]*\\).*/\\1/p" | head -1; }
# any sign of a failed op in the cell's log (so failures can never be masked)
count_fail() { grep -ciE "FAIL|NOT_FOUND|MISMATCH|fail=[1-9]|err round|prime FAIL|RuntimeError" "$1"; }

echo "transport,op,mode,size,concurrency,p50_ms,p99_ms,mibps,fails" > "$OUT"

run_lat() {  # transport conf size
  local tr=$1 conf=$2 size=$3 log="$LOGDIR/lat_${tr}_${size}.log"
  timeout 240 $PY "$CLIENT" --url "$URL" --arena-mib 256 --conf "$conf" \
    bench --mode seq --count $LAT_COUNT --size "$size" --rand-seed 0x1234 \
    >"$log" 2>&1
  local pf=$(grep "put summary" "$log" | sed -n 's/.*failed=\([0-9]*\).*/\1/p' | head -1)
  local gf=$(grep "get summary" "$log" | sed -n 's/.*failed=\([0-9]*\).*/\1/p' | head -1)
  local pl=$(grep "put summary"    "$log" | field p50); local p9=$(grep "put summary" "$log" | field p99)
  local pm=$(grep "put throughput" "$log" | sed -n 's/.*per-op=\([0-9.]*\).*/\1/p' | head -1)
  local gl=$(grep "get summary"    "$log" | field p50); local g9=$(grep "get summary" "$log" | field p99)
  local gm=$(grep "get throughput" "$log" | sed -n 's/.*per-op=\([0-9.]*\).*/\1/p' | head -1)
  echo "$tr,put,lat,$size,1,${pl:-NA},${p9:-NA},${pm:-NA},${pf:-NA}" | tee -a "$OUT"
  echo "$tr,get,lat,$size,1,${gl:-NA},${g9:-NA},${gm:-NA},${gf:-NA}" | tee -a "$OUT"
}

run_bw() {  # transport conf op size conc
  local tr=$1 conf=$2 op=$3 size=$4 conc=$5
  local cnt=$(bw_count "$size") log="$LOGDIR/bw_${tr}_${op}_${size}_c${conc}.log"
  timeout 120 $PY "$CLIENT" --url "$URL" --arena-mib $ARENA_MIB --conf "$conf" \
    watch --op "$op" --count "$cnt" --concurrency "$conc" --size "$size" \
    --duration $BW_DUR --warmup $BW_WARMUP --rand-seed 0x1234 \
    >"$log" 2>&1
  local p50=$(grep "per-batch latency" "$log" | field p50)
  local p99=$(grep "per-batch latency" "$log" | field p99)
  local bw=$(grep "sustained throughput" "$log" | sed -n 's/.*throughput: \([0-9.]*\) MiB.*/\1/p' | head -1)
  local fl=$(count_fail "$log")
  echo "$tr,$op,bw,$size,$conc,${p50:-NA},${p99:-NA},${bw:-NA},${fl}" | tee -a "$OUT"
}

for spec in "rdma:$HERE/rdma_on.conf" "tcp:$HERE/rdma_off.conf"; do
  tr=${spec%%:*}; conf=${spec#*:}
  echo "########## transport=$tr ##########"
  for size in $SIZES; do
    run_lat "$tr" "$conf" "$size"
  done
  for op in put get; do
    for size in $SIZES; do
      for c in $CONCS; do
        run_bw "$tr" "$conf" "$op" "$size" "$c"
      done
    done
  done
done

echo "=== DONE -> $OUT ==="
