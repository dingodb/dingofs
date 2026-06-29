#!/usr/bin/env bash
cd "$(dirname "$0")" || exit 1
source ../common/lib.sh
nv_need_root "$@"
nv_section "03 mixed read+write (write load inflates read latency)"
nv_prep_dir
nv_device_info
nv_build demo.c demo
./demo "$NVME_DIR" "${1:-200}"
nv_cleanup
