#!/usr/bin/env bash
# Read-only: is TRIM happening on the cache disks? (mount discard / fstrim.timer)
cd "$(dirname "$0")" || exit 1
source ../common/lib.sh
nv_need_root "$@"
nv_section "13 TRIM / discard (observe)"
echo "  -- periodic fstrim --"
echo "  fstrim.timer: enabled=$(systemctl is-enabled fstrim.timer 2>&1)  active=$(systemctl is-active fstrim.timer 2>&1)"
echo "  -- mount options (look for 'discard') --"
for m in /mnt/cache0 /mnt/cache1 "$NVME_DIR"; do
  findmnt -no TARGET,FSTYPE,OPTIONS --target "$m" 2>/dev/null | sed 's/^/  /'
done | sort -u
echo "  -- device discard support (DISC-MAX != 0 = supported) --"
lsblk -D -o NAME,MOUNTPOINT,DISC-GRAN,DISC-MAX 2>/dev/null | grep -iE 'NAME|nvme|cache' | sed 's/^/  /'
cat <<'TXT'
  --
  verdict: if fstrim.timer is disabled AND no mount has 'discard', the SSD never
  learns which blocks the filesystem freed -> over time WAF rises, sustained
  writes degrade. To fix (batched, off-peak):  fstrim -v /mnt/cache0 /mnt/cache1
  NOTE: this script does NOT run fstrim (it changes drive state). Run it yourself.
TXT
