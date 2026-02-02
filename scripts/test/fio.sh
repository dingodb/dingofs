#!/bin/bash

cd /home/dengzihui/mount-test/dengzh_hash_01-1/test20

for ((i=1; i<=10000; i++)); do
  fio --name=test --filename=${i}.pt --ioengine=libaio --iodepth=1 --rw=randwrite --bs=4k --size=1G --runtime=160

  sleep 10
  sudo rm -f ${i}.pt
  sleep 10
done