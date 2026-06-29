#!/usr/bin/env bash
# Read-only: current fill level of the cache disks (fuller => higher WAF / slower).
cd "$(dirname "$0")" || exit 1
source ../common/lib.sh
nv_need_root "$@"
nv_section "10 capacity / over-provisioning (observe)"
df -h /mnt/cache0 /mnt/cache1 2>/dev/null | sed 's/^/  /'
cat <<'TXT'
  --
  rule of thumb: keep a churning SSD cache below ~80% used. As it approaches full,
  GC has fewer free erase blocks -> write amplification rises, sustained write
  bandwidth and p99 degrade. More free space (or under-partitioning) = more OP = faster.
  To actually MEASURE the fuller=slower effect, see this dir's README (needs a long fill).
TXT
