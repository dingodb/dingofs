cache-bench
===

A client-view benchmark for the v2 block cache: it boots the whole
`BlockCacheImpl` facade (runtime, tiers, remote group, object storage) and
drives it through the async API the real client uses, so the numbers include
the inbox hop, the shard work and the worker-pool completion hop.

Each of `--threads` submitting threads keeps `--iodepth` requests in flight;
total concurrency is `threads * iodepth`. Latency is measured per operation,
from right before `AsyncPut`/`AsyncGet` to the completion callback.

Quick Start
---

```bash
cache-bench --flagfile bench.conf
```

`bench.conf`:

```
--op=put
--threads=3
--iodepth=8
--fsid=1
--blksize=4194304
--blocks=100
--time_based=false
--runtime=300

# Cluster / cache shape: the standard v2 client flags
--mds_addrs=192.168.1.1:7400
--cache_store=disk
--cache_dir=/mnt/nvme0/cache
--cache_group=group-2
--remote_rdma=true
```

Run `--op=put` first to populate the keyspace, then `--op=get` with the same
`--threads/--fsid/--blksize/--blocks` so the reads hit what the puts wrote.

Flags
---

| Flag | Default | Meaning |
|------|---------|---------|
| `--op` | put | `put` or `get` |
| `--threads` | 1 | submitting threads, each with its own key range |
| `--iodepth` | 1 | in-flight requests per thread |
| `--fsid` | 1 | fs the blocks belong to |
| `--blksize` | 4194304 | block size in bytes |
| `--blocks` | 1 | blocks per thread |
| `--offset` | 0 | get: offset within the block |
| `--length` | 4194304 | get: bytes per operation, clamped to the block end |
| `--stage` | false | put: stage to the local disk first |
| `--retrieve_storage` | true | get: fall back to the object storage on miss |
| `--time_based` | false | loop over the blocks until `--runtime` elapses |
| `--runtime` | 300 | seconds a time-based run lasts |

Output
---

```
put: threads=3 iodepth=8 fsid=1 blksize=4194304 blocks=100 time_based=false runtime=300
...
Starting 3 workers
...
 [10.28%]  put:    584 op/s   2336 MB/s  lat(0.013706 0.042489 0.002988)
 [20.51%]  put:    563 op/s   2253 MB/s  lat(0.014187 0.044417 0.003051)

Summary (3 workers):
  Avg(put):  563 op/s  2253 MB/s  lat(0.014187 0.044417 0.003051)
```

op/s and MB/s are per 3-second interval; lat() is average, max and min in
seconds over the interval.
