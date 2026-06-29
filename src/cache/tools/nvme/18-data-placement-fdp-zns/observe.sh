#!/usr/bin/env bash
# Read-only: does this drive support modern data-placement (Streams / FDP / ZNS)?
cd "$(dirname "$0")" || exit 1
source ../common/lib.sh
nv_need_root "$@"
nv_section "18 data placement: Streams / FDP / ZNS (observe support)"
nv_prep_dir
src=$(findmnt -no SOURCE --target "$NVME_DIR" 2>/dev/null)
kname=$(lsblk -no KNAME "$src" 2>/dev/null | head -1)
phys=$kname; [ -d /sys/block/$kname/slaves ] && phys=$(ls /sys/block/$kname/slaves | head -1)
ctrl=/dev/${phys%n*}
echo "  -- ctratt (bit4=Endurance Groups, FDP needs it) / oacs --"
nvme id-ctrl "$ctrl" 2>/dev/null | grep -iE '^ctratt|^oacs' | sed 's/^/  /'
echo "  -- Streams directive --"
nvme dir-receive "$ctrl" -O 1 -D 0 2>&1 | grep -iE 'directive|status|01' | head -2 | sed 's/^/  /'
echo "  -- ZNS? --"
nvme zns id-ctrl "$ctrl" 2>&1 | head -1 | sed 's/^/  /'
echo "  -- FDP? --"
nvme fdp configs "$ctrl" 2>&1 | head -1 | sed 's/^/  /'
echo "  (Invalid Field / required-id errors = NOT supported. This P5510 = conventional drive: no Streams/FDP/ZNS.)"
