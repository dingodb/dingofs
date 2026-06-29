#!/usr/bin/env bash
# Read-only: dump the kernel block-layer knobs that shape realized NVMe perf.
cd "$(dirname "$0")" || exit 1
source ../common/lib.sh
nv_need_root "$@"
nv_section "12 kernel block layer (observe)"
nv_prep_dir
src=$(findmnt -no SOURCE --target "$NVME_DIR" 2>/dev/null)
kname=$(lsblk -no KNAME "$src" 2>/dev/null | head -1)
phys=$kname; [ -d /sys/block/$kname/slaves ] && phys=$(ls /sys/block/$kname/slaves | head -1)
q=/sys/block/$phys/queue
echo "  device $phys"
for f in scheduler max_sectors_kb max_hw_sectors_kb nr_requests read_ahead_kb nomerges rotational io_poll io_poll_delay; do
  [ -e "$q/$f" ] && echo "  $f = $(cat $q/$f 2>/dev/null)"
done
echo "  hw queues = $(ls /sys/block/$phys/mq 2>/dev/null | wc -l)"
cat <<'TXT'
  --
  reads of these knobs:
   scheduler=[none]  -> good for NVMe (mq-deadline/bfq add overhead)
   max_sectors_kb=128 -> a 4 MiB IO is split into 4096/128 = 32 device commands
   read_ahead_kb     -> only matters for buffered IO; O_DIRECT bypasses it
   io_poll=0         -> interrupt-driven; io_uring IOPOLL/SQPOLL can poll instead
TXT
