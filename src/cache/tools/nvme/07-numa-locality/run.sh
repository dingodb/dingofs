#!/usr/bin/env bash
cd "$(dirname "$0")" || exit 1
source ../common/lib.sh
nv_need_root "$@"
nv_section "07 NUMA locality (cross-socket IO is slower)"
nv_prep_dir
nv_device_info
nv_build demo.c demo

# find the disk's NUMA node, pick LOCAL = that node, REMOTE = another
src=$(findmnt -no SOURCE --target "$NVME_DIR" 2>/dev/null)
kname=$(lsblk -no KNAME "$src" 2>/dev/null | head -1)
phys=$kname; [ -d /sys/block/$kname/slaves ] && phys=$(ls /sys/block/$kname/slaves | head -1)
disknode=$(cat /sys/block/$phys/device/numa_node 2>/dev/null)
[ -z "$disknode" ] || [ "$disknode" -lt 0 ] && disknode=0
nnodes=$(ls -d /sys/devices/system/node/node[0-9]* 2>/dev/null | wc -l)
remote=$(( (disknode + 1) % nnodes ))
echo "  disk on NUMA node $disknode ; LOCAL=$disknode  REMOTE=$remote  (nodes=$nnodes)"
if [ "$nnodes" -lt 2 ]; then echo "  single NUMA node -> effect N/A on this host"; nv_cleanup; exit 0; fi

./demo "$NVME_DIR" prep 8 >/dev/null            # build the data file once
echo "  LOCAL  (numactl --cpunodebind=$disknode --membind=$disknode):"
L=$(numactl --cpunodebind=$disknode --membind=$disknode ./demo "$NVME_DIR" local 8 | tee /dev/stderr | awk '/^RESULT/{print $3}')
echo "  REMOTE (numactl --cpunodebind=$remote --membind=$remote):"
R=$(numactl --cpunodebind=$remote --membind=$remote ./demo "$NVME_DIR" remote 8 | tee /dev/stderr | awk '/^RESULT/{print $3}')
awk -v l="$L" -v r="$R" 'BEGIN{ if(r>0){ printf "  VERDICT: local vs remote bandwidth -> %.2fx  [%s]\n", l/r, (l/r>=1.08)?"EFFECT CONFIRMED":"effect weak/none" } }'
echo "  (DingoFS deploy currently pins --cpunodebind=0; if disk node != 0 that is the REMOTE case.)"
nv_cleanup
