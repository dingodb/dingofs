#!/bin/bash

base_dir=/home/dengzihui/mount-test/dengzh_hash_01-1
cd $base_dir

for ((i=1; i<=10000; i++)); do
  # fio --name=test --filename=${i}.pt --ioengine=libaio --iodepth=1 --rw=randwrite --bs=4k --size=1G --runtime=160

  test_dir=$base_dir/test_$i
  mkdir -p $test_dir
  fio --ioengine=libaio --iodepth=1  --direct=1 --rw=read --bs=128KB --size=8GB --numjobs=32 --group_reporting --name=test --directory=$test_dir

  sleep 10
  sudo rm -f ${test_dir}/test*
  sleep 10
done