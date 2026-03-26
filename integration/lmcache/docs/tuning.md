# Tuning

## Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `dingofs_num_workers` | int | 8 | Number of I/O worker threads |
| `dingofs_use_odirect` | bool | false | Use O_DIRECT for file I/O |
| `dingofs_sync_mode` | str | `"always"` | Write sync strategy |

## Worker Count (`dingofs_num_workers`)

DingoFS performs best under high concurrency. Read throughput scales nearly linearly with thread count.

- **Recommended**: 8-16 workers
- Write throughput is bounded by `fdatasync` latency, so more workers help reads more than writes

## O_DIRECT (`dingofs_use_odirect`)

Bypasses the kernel page cache, reducing memory copies in the FUSE layer.

- **Best for**: Read-once workloads (e.g., prefetch)
- **Requirement**: Buffer address and size must align to filesystem block size (typically 4096)
- **Default**: false (easier buffer management)

## Sync Mode (`dingofs_sync_mode`)

Controls whether `fdatasync` is called after every write.

- **`"always"`** (default): Safe, prevents data loss on crash. Slower writes.
- **`"none"`**: Skip fdatasync. Much faster writes. Acceptable when DingoFS provides its own durability guarantees.

## Recommended Configurations

| Workload | Settings |
|----------|----------|
| General KV cache | `num_workers=8`, `sync_mode=always` |
| High-throughput writes | `num_workers=16`, `sync_mode=none` |
| Prefetch (read-once) | `num_workers=8`, `use_odirect=true` |

## Benchmark Reference

Results from the benchmark example (`examples/benchmark/benchmark.py`):

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

Run your own benchmark:

```bash
python examples/benchmark/benchmark.py
python examples/benchmark/benchmark.py --base-path /mnt/dingofs/bench
python examples/benchmark/benchmark.py --write-only
python examples/benchmark/benchmark.py --read-only
```
