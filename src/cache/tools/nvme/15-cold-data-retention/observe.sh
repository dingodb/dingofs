#!/usr/bin/env bash
# Read-only: SMART wear / retry counters (proxies for aging/retention pressure).
cd "$(dirname "$0")" || exit 1
source ../common/lib.sh
nv_need_root "$@"
nv_section "15 cold-data / retention (observe SMART)"
nv_prep_dir
src=$(findmnt -no SOURCE --target "$NVME_DIR" 2>/dev/null)
kname=$(lsblk -no KNAME "$src" 2>/dev/null | head -1)
phys=$kname; [ -d /sys/block/$kname/slaves ] && phys=$(ls /sys/block/$kname/slaves | head -1)
ctrl=/dev/${phys%n*}
echo "  -- standard smart-log --"
nvme smart-log "$ctrl" 2>/dev/null | grep -iE 'percentage_used|media_errors|num_err|temperature' | sed 's/^/  /'
echo "  -- vendor smart-log-add (Intel) --"
nvme intel smart-log-add "$ctrl" 2>/dev/null | grep -iE 'wear|retr|media|temperature|timed_workload' | sed 's/^/  /'
echo "  (track these over weeks; rising retry/wear = more aging-related read overhead)"
