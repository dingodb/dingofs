#!/usr/bin/env bash
cd "$(dirname "$0")" || exit 1
source ../common/lib.sh
nv_need_root "$@"
nv_section "01 read-after-write (same-block flush hazard)"
nv_prep_dir
nv_device_info
nv_build demo.c demo
./demo "$NVME_DIR" "${1:-200}" "${2:-4194304}"
nv_cleanup
