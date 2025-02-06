#!/usr/bin/env bash

for ((i=1;i<=25000;i++)); do dd if=/dev/zero of=0_5_${i}_0_0  bs=1048576 count=4; done

for ((i=0;i<=9;i++)); do ./bench_client -block_ino=${i} -first_block_id=1 -last_block_id=25000 >/dev/null 2>&1 & done