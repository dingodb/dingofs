#!/usr/bin/env bash
# Shared shell helpers for the tools/nvme demos.
# Each demo's run.sh does: source "$(dirname "$0")/../common/lib.sh"

# Test directory (a real NVMe-backed dir; default = DingoFS cache0 on jg30).
# Override with:  NVME_DIR=/path ./run.sh
: "${NVME_DIR:=/mnt/cache0/wine93/nvme-demo}"

# Auto-elevate: most demos need root (drop_caches, root-owned cache dir, nvme-cli).
nv_need_root() {
  if [ "$(id -u)" -ne 0 ]; then
    echo "[*] re-exec under sudo ..."
    exec sudo -E "$0" "$@"
  fi
}

nv_build() { # nv_build <src.c> <out>
  gcc -O2 -pthread -o "$2" "$1" || { echo "build failed: $1"; exit 1; }
}

nv_section() { printf '\n========== %s ==========\n' "$*"; }

# Print the backing device + key queue params of $NVME_DIR (context for results).
# Best-effort: never aborts the caller even if a probe fails.
nv_device_info() {
  local src kname phys s
  src=$(findmnt -no SOURCE --target "$NVME_DIR" 2>/dev/null) || true
  kname=$(lsblk -no KNAME "$src" 2>/dev/null | head -1) || true   # dm-0 or nvme3n1
  phys=$kname
  if [ -d "/sys/block/$kname/slaves" ]; then          # dm/LVM -> physical nvme
    s=$(ls "/sys/block/$kname/slaves" 2>/dev/null | head -1) || true
    [ -n "$s" ] && phys=$s
  fi
  echo "  dir=$NVME_DIR  src=${src:-?}  dev=${phys:-?}"
  if [ -n "$phys" ] && [ -e "/sys/block/$phys" ]; then
    echo "  model=$(cat /sys/block/$phys/device/model 2>/dev/null | tr -s ' ')" \
      "numa_node=$(cat /sys/block/$phys/device/numa_node 2>/dev/null)" \
      "sched=$(cat /sys/block/$phys/queue/scheduler 2>/dev/null)" \
      "max_sectors_kb=$(cat /sys/block/$phys/queue/max_sectors_kb 2>/dev/null)"
  fi
  return 0
}

nv_prep_dir() { mkdir -p "$NVME_DIR"; }
nv_cleanup()  { rm -rf "${NVME_DIR:?}/"* 2>/dev/null; }
