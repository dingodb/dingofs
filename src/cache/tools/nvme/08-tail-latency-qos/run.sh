#!/usr/bin/env bash
cd "$(dirname "$0")" || exit 1
source ../common/lib.sh
nv_need_root "$@"
nv_section "08 tail latency / QoS (mean hides the spikes)"
nv_prep_dir
nv_device_info
nv_build demo.c demo
./demo "$NVME_DIR" "${1:-5000}"
nv_cleanup
