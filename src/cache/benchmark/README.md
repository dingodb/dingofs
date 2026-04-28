cache benchmark
===

`cache-bench` is a remote cache-group benchmark client. In this workspace it
is intentionally remote-only: it constructs `RemoteBlockCacheImpl` directly and
does not fall back to a local cache or storage client.

Build
---

```bash
cmake --build build --target cache-bench dingo-cache -j2
```

Single Run
---

```bash
build/bin/cache-bench \
  --bench_remote_only=true \
  --cache_group=group-1 \
  --mds_addrs=10.220.88.31:6900 \
  --use_rdma=true \
  --cache_rdma_device=mlx5_0 \
  --cache_rdma_port_num=1 \
  --bench_rdma_registered_buffers=true \
  --op=range \
  --retrive=false \
  --fsid=1 \
  --start_block_id=1000000000 \
  --threads=8 \
  --blksize=4194304 \
  --length=4194304 \
  --blocks=64 \
  --json_result=true \
  --result_path=/tmp/cache-bench.json
```

Operations:

- `cache`: admission/prefill path. With RDMA, the server performs RDMA_READ
  from the advertised client buffer and returns after admitting async cache
  write work. It is not a disk-write-completion latency.
- `put`: write-completion path. With RDMA, the server performs RDMA_READ and
  then writes the block through the stage path before returning.
- `range`: read path. Use `--retrive=false` for cache-hit-only measurement.
  The client advertises a pooled RDMA buffer and the server RDMA_WRITEs the
  block directly into it (zero copy on a cache hit).

Important flags:

- `--bench_remote_only=true`: required for this benchmark client.
- `--start_block_id=N`: first slice id for the run. Use a different value per
  case to avoid old data pollution.
- `--bench_rdma_registered_buffers=true`: each worker's request block comes
  from the shared RDMA-registered client pool, enabling a zero-copy server
  RDMA_READ for put/cache (range destinations are always pooled buffers).
- `--json_result=true --result_path=...`: emit attempts, success/fail, qps,
  MiB/s, and min/mean/p50/p90/p99/max latency in microseconds.

jg29/jg30/jg31 Matrix
---

```bash
src/cache/benchmark/run_rdma_cache_bench.sh
```

Useful variants:

```bash
SMOKE_ONLY=1 src/cache/benchmark/run_rdma_cache_bench.sh
USE_RDMA=0 RESULT_DIR=/tmp/cache-bench-tcp src/cache/benchmark/run_rdma_cache_bench.sh
RUN_DROP_CACHES=1 SMOKE_ONLY=1 src/cache/benchmark/run_rdma_cache_bench.sh
KEEP_NODES=1 THREADS_4M="1 8 32" src/cache/benchmark/run_rdma_cache_bench.sh
```

The script builds and deploys `dingo-cache` and `cache-bench`, restarts jg29
and jg30 with RDMA enabled, runs smoke and performance matrices from jg31, and
collects `pidstat`, `iostat`, `sar -n DEV`, `numastat`, `/proc/<pid>/status`,
and brpc `/vars` into `bench_results/<timestamp>`.

Main Results To Judge
---

- 4MB `range --retrive=false` should scale with concurrency toward the 200Gb
  NIC ceiling when disks are balanced across the two nodes.
- 4KB `range` is mostly latency, qps, CPU/CQ, and bthread scheduling overhead;
  it is not expected to fill the link.
- `put` includes RDMA_READ, O_DIRECT write, rename/link/metadata, and cache
  manager work. Compare it with disk write bandwidth, CPU, and iowait.
- `cache` is admission/prefill only; wait for async writes to settle before
  using `range --retrive=false` as a read-hit benchmark.
