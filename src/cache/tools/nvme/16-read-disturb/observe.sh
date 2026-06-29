#!/usr/bin/env bash
# Read-only-ish: re-read a hot file many times, watch whether nand-writes rise
# (read-disturb forces internal relocation). Reads only; writes one temp file.
cd "$(dirname "$0")" || exit 1
source ../common/lib.sh
nv_need_root "$@"
nv_section "16 read-disturb (observe: do reads cause writes?)"
nv_prep_dir
src=$(findmnt -no SOURCE --target "$NVME_DIR" 2>/dev/null)
kname=$(lsblk -no KNAME "$src" 2>/dev/null | head -1)
phys=$kname; [ -d /sys/block/$kname/slaves ] && phys=$(ls /sys/block/$kname/slaves | head -1)
ctrl=/dev/${phys%n*}
nandw() { nvme intel smart-log-add "$ctrl" 2>/dev/null | awk '/nand_bytes_written/{print $NF; exit}'; }
f="$NVME_DIR/hot.dat"
dd if=/dev/zero of="$f" bs=1M count=256 oflag=direct status=none 2>/dev/null
sync; sleep 12
n0=$(nandw)
echo "  re-reading a 256MiB hot file 50x (O_DIRECT) ..."
for i in $(seq 1 50); do dd if="$f" of=/dev/null bs=1M iflag=direct status=none 2>/dev/null; done
sync; sleep 12
n1=$(nandw)
echo "  nand_bytes_written: before=$n0 after=$n1  delta=$((n1-n0)) (vendor units)"
echo "  (a clear positive delta with NO host writes would indicate read-disturb relocation;"
echo "   on a healthy enterprise drive over a short test this is usually ~0. Real read-disturb"
echo "   needs far more reads + the drive's internal threshold — see README.)"
rm -f "$f"
