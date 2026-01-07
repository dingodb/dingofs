#!/bin/bash

cd /home/dengzihui/mount-test/dengzh_hash_01-1/work/dynamo/lib/bindings/python
for ((i=1; i<=10000; i++)); do
  # strace -ttt -ff -o /tmp/dengzihui/compile_dynamo.trace.$i  maturin -vv develop --uv > /tmp/dengzihui/compile_dynamo.log.$i 2>&1
  maturin -vv develop --uv > /tmp/dengzihui/compile_dynamo.log.$i 2>&1

  sleep 30
  mv ./target ./target_$i
  sleep 300
done