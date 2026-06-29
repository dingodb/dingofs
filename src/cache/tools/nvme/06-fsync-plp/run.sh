#!/usr/bin/env bash
cd "$(dirname "$0")" || exit 1
source ../common/lib.sh
nv_need_root "$@"
nv_section "06 fsync cost vs PLP (power-loss protection)"
nv_prep_dir
nv_device_info
echo "  -- drive PLP / volatile-write-cache hints (nvme id-ctrl) --"
src=$(findmnt -no SOURCE --target "$NVME_DIR" 2>/dev/null)
kname=$(lsblk -no KNAME "$src" 2>/dev/null | head -1)
phys=$kname; [ -d /sys/block/$kname/slaves ] && phys=$(ls /sys/block/$kname/slaves | head -1)
nvme id-ctrl /dev/${phys%n*} 2>/dev/null | grep -iE '^vwc|^cmic|powerloss|^rpmbs' | sed 's/^/    /'
nv_build demo.c demo
./demo "$NVME_DIR" "${1:-300}"
nv_cleanup
