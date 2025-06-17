cache benchmark
===

Quick Start
---

```bash
cache-bench --flagfile bench.conf
```

`bench.conf`:

```
--threads=3
--op=put
--fsid=1
--ino=1
--blksize=4194304
--blocks=100
--writeback=false
--retrive=true
--async_batch=128
--runtime=300
--time_based=true

# Any other client flags here
```

Output
---

```
libaio_read: (g=0): rw=read, bs=(R) 4096KiB-4096KiB, (W) 4096KiB-4096KiB, (T) 4096KiB-4096KiB, ioengine=libaio, iodepth=1
...
fio-3.28
Starting 4 processes

...
[10%]    put:    584 op/s   2336 MB/s  lat(0.013706 0.042489 0.002988)
[11%]    put:    563 op/s   2253 MB/s  lat(0.014187 0.044417 0.003051)
...
```

The output shows the performance of the cache benchmark, including operations per second (op/s), throughput in megabytes per second (MB/s), and latency statistics which include average, maximum and minimum latency in seconds.
