#!/usr/bin/env bash
# This effect (read << program << erase) is internal to the controller and can't
# be timed at the filesystem layer. This script prints the facts + which runnable
# demos in this set expose its CONSEQUENCES, plus the drive's endurance/wear.
cd "$(dirname "$0")" || exit 1
source ../common/lib.sh
nv_need_root "$@"
nv_section "17 NAND latency hierarchy (concept + where you SEE it)"
nv_prep_dir
src=$(findmnt -no SOURCE --target "$NVME_DIR" 2>/dev/null)
kname=$(lsblk -no KNAME "$src" 2>/dev/null | head -1)
phys=$kname; [ -d /sys/block/$kname/slaves ] && phys=$(ls /sys/block/$kname/slaves | head -1)
ctrl=/dev/${phys%n*}
cat <<'TXT'
  cell-level latency (approx, hidden by the controller's buffer/parallelism):
    read     ~50-100 us       (fastest)
    program  ~hundreds us - ms (TLC multi-pass; upper page slower; QLC slower still)
    erase    ~ms per block     (slowest; whole erase block)
  => writes are physically ~1 order costlier than reads; erase costlier still.
  you SEE the consequences in the runnable demos:
    04-burst-vs-sustained  (buffer hides program; cliff when it can't)
    03-mixed-read-write    (reads wait behind program/erase)
    01-read-after-write    (read a block not yet programmed to NAND)
    08-tail-latency-qos    (a stuck erase => ms read-tail unless suspend supported)
TXT
echo "  this drive's endurance / wear (writes wear NAND):"
nvme smart-log "$ctrl" 2>/dev/null | grep -iE 'percentage_used|data_units_written' | sed 's/^/    /'
