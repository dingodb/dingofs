#!/usr/bin/env bash
# Observe that O_DIRECT alone does NOT emit a cache FLUSH; fdatasync does.
# Uses blktrace if available; otherwise prints the conceptual check.
cd "$(dirname "$0")" || exit 1
source ../common/lib.sh
nv_need_root "$@"
nv_section "14 O_DIRECT != durable (observe: which writes emit a FLUSH?)"
nv_prep_dir
src=$(findmnt -no SOURCE --target "$NVME_DIR" 2>/dev/null)
kname=$(lsblk -no KNAME "$src" 2>/dev/null | head -1)
phys=$kname; [ -d /sys/block/$kname/slaves ] && phys=$(ls /sys/block/$kname/slaves | head -1)
f="$NVME_DIR/dur.dat"

if command -v blktrace >/dev/null 2>&1 && command -v blkparse >/dev/null 2>&1; then
  echo "  tracing /dev/$phys for FLUSH/FUA while doing: O_DIRECT write, then fsync ..."
  ( blktrace -d "/dev/$phys" -o - -w 5 2>/dev/null | blkparse -i - 2>/dev/null \
      | grep -iE 'FWFS|FLUSH|FUA|  F ' | head -8 | sed 's/^/    /' ) &
  tracer=$!
  sleep 1
  dd if=/dev/zero of="$f" bs=4M count=4 oflag=direct status=none 2>/dev/null  # O_DIRECT write
  sleep 1
  sync                                                                         # FLUSH happens here
  sleep 3
  wait "$tracer" 2>/dev/null
  echo "  ^ FLUSH/FUA lines appear around the sync, NOT the bare O_DIRECT write."
else
  echo "  (blktrace not installed — conceptual check:)"
  echo "  * O_DIRECT write returns when data reaches the DEVICE, not when it is durable."
  echo "  * durability needs fsync/fdatasync (issues a FLUSH) — and fsync the PARENT DIR after rename."
fi
rm -f "$f"
