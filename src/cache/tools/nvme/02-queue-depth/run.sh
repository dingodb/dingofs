#!/usr/bin/env bash
cd "$(dirname "$0")" || exit 1
source ../common/lib.sh
nv_need_root "$@"
nv_section "02 queue-depth (QD1 cannot fill the device)"
nv_prep_dir
nv_device_info
nv_build demo.c demo
./demo "$NVME_DIR" "${1:-4294967296}"
nv_cleanup
