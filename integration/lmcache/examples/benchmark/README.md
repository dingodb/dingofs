# Benchmark & Tuning Guide

Compare throughput under different configurations to find the optimal settings.

## Dimensions

- **Worker scaling**: 1 / 4 / 8 / 16 threads
- **Chunk size**: 256KB / 1MB / 4MB
- **O_DIRECT**: on vs off
- **Sync mode**: `always` (fdatasync) vs `none`

## Prerequisites

```bash
cd integration/lmcache
pip install .
```

## Run

```bash
# Default (temp directory, all benchmarks)
python examples/benchmark/benchmark.py

# Specify DingoFS mount point
python examples/benchmark/benchmark.py --base-path /mnt/dingofs/bench

# Write-only or read-only
python examples/benchmark/benchmark.py --write-only
python examples/benchmark/benchmark.py --read-only
```

## Sample Output

```
--- Worker Scaling (1MB x 32 chunks) ---
  workers= 1 | WRITE  280 MB/s | READ  4000 MB/s
  workers= 4 | WRITE  290 MB/s | READ 15000 MB/s
  workers= 8 | WRITE  300 MB/s | READ 28000 MB/s
  workers=16 | WRITE  310 MB/s | READ 40000 MB/s

--- Sync Mode (1MB x 32 chunks, 8 workers) ---
  sync=always | WRITE  280 MB/s
  sync=none   | WRITE  950 MB/s
```

> Actual numbers depend on storage media. Local SSD reads hit page cache,
> producing very high throughput. Test on a DingoFS mount for realistic numbers.

## Tuning Recommendations

**Worker count** (`dingofs_num_workers`):
- DingoFS performs best under high concurrency. Use 8-16 workers.
- Read throughput scales nearly linearly with thread count.
- Write throughput is bounded by `fdatasync` latency.

**O_DIRECT** (`dingofs_use_odirect`):
- Bypasses kernel page cache. Best for read-once workloads (e.g., prefetch).
- Requires buffer address and size aligned to filesystem block size (typically 4096).
- On DingoFS, reduces memory copies in the FUSE layer.

**Sync mode** (`dingofs_sync_mode`):
- `always` (default): fdatasync after every write. Safe but slower.
- `none`: skip fdatasync. Much faster writes; acceptable when DingoFS provides its own durability guarantees.

**DingoFS mount options**:
- Increase read/write buffer size to match KV cache chunk size (typically 1-4 MB).
- Enable parallel I/O for higher throughput.

## Quick Reference

| Workload | Recommended Settings |
|----------|---------------------|
| General KV cache | `num_workers=8`, `sync_mode=always` |
| High-throughput writes | `num_workers=16`, `sync_mode=none` |
| Prefetch (read-once) | `num_workers=8`, `use_odirect=true` |
