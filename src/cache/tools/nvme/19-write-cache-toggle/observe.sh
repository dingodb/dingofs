#!/usr/bin/env bash
# Read-only: show the drive's volatile write cache (VWC) state. Does NOT toggle it.
cd "$(dirname "$0")" || exit 1
source ../common/lib.sh
nv_need_root "$@"
nv_section "19 volatile write cache (observe only — does NOT change it)"
nv_prep_dir
src=$(findmnt -no SOURCE --target "$NVME_DIR" 2>/dev/null)
kname=$(lsblk -no KNAME "$src" 2>/dev/null | head -1)
phys=$kname; [ -d /sys/block/$kname/slaves ] && phys=$(ls /sys/block/$kname/slaves | head -1)
ctrl=/dev/${phys%n*}
echo "  vwc (id-ctrl): $(nvme id-ctrl "$ctrl" 2>/dev/null | awk '$1=="vwc"{print $3; exit}')   (0 = no volatile write cache advertised, typical of PLP drives)"
echo "  -- get-feature 0x6 (Volatile Write Cache enable) --"
nvme get-feature "$ctrl" -f 0x6 -H 2>&1 | head -3 | sed 's/^/  /'
echo "  (Invalid Field => the drive has no toggleable VWC, consistent with PLP. Nothing to change here.)"
