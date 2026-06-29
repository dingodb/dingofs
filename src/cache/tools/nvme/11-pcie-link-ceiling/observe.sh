#!/usr/bin/env bash
# Read-only: show the NVMe's PCIe link capability vs negotiated status.
cd "$(dirname "$0")" || exit 1
source ../common/lib.sh
nv_need_root "$@"
nv_section "11 PCIe link ceiling (observe)"
nv_prep_dir
src=$(findmnt -no SOURCE --target "$NVME_DIR" 2>/dev/null)
kname=$(lsblk -no KNAME "$src" 2>/dev/null | head -1)
phys=$kname; [ -d /sys/block/$kname/slaves ] && phys=$(ls /sys/block/$kname/slaves | head -1)
addr=$(readlink -f /sys/block/$phys/device 2>/dev/null | grep -oE '[0-9a-f]{4}:[0-9a-f]{2}:[0-9a-f]{2}\.[0-9a-f]' | tail -1)
echo "  device $phys @ pci $addr"
lspci -vv -s "$addr" 2>/dev/null | grep -iE 'LnkCap:|LnkSta:' | sed 's/^[[:space:]]*/  /'
cat <<'TXT'
  --
  usable bandwidth ceiling (rough): Gen3 x4 ~3.5 GB/s, Gen4 x4 ~7 GB/s, Gen5 x4 ~14 GB/s
  if LnkSta speed/width < LnkCap, the slot/cable downtrained -> you are capped below the drive.
TXT
