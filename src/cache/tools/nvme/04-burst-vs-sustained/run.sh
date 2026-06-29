#!/usr/bin/env bash
cd "$(dirname "$0")" || exit 1
source ../common/lib.sh
nv_need_root "$@"
nv_section "04 burst vs sustained write (buffer absorption + GC cliff)"
nv_prep_dir
nv_device_info
nv_build demo.c demo
./demo "$NVME_DIR" "${1:-17179869184}"
nv_cleanup
