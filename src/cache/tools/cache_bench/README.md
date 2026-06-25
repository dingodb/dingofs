# cache-bench

`cache-bench` is a small tool for benchmarking the DingoFS cache path. It uses `--mds_addrs` and `--fsid` to fetch filesystem storage settings from MDS, then runs `put` or `range` against `TierBlockCache`.

## Build

```bash
cmake --build build --target cache-bench
```

The binary is generated at:

```bash
build/bin/cache-bench
```

## Run

Write blocks:

```bash
build/bin/cache-bench --mds_addrs=127.0.0.1:7400 --fsid=1 --op=put \
  --threads=8 --blocks=100 --block_size=4MiB
```

Read ranges:

```bash
build/bin/cache-bench --mds_addrs=127.0.0.1:7400 --fsid=1 --op=range \
  --threads=8 --blocks=100 --block_size=4MiB --range_length=1MiB
```

You can also use a flagfile:

```text
--mds_addrs=127.0.0.1:7400
--fsid=1
--op=put
--threads=8
--blocks=100
--block_size=4MiB
--report_interval=3
```

```bash
build/bin/cache-bench --flagfile=bench.conf
```

## Main Options

| Option | Description |
| --- | --- |
| `--op=put|range` | Benchmark write or range-read. |
| `--mds_addrs=HOST:PORT` | MDS address. |
| `--fsid=ID` | Filesystem ID. |
| `--threads=N` | Worker count. |
| `--blocks=N` | Blocks per worker. |
| `--block_size=SIZE` | Whole block size, for example `4MiB`. |
| `--range_offset=SIZE` | Range-read offset. |
| `--range_length=SIZE` | Range-read length; empty means `block_size - range_offset`. |
| `--writeback=true|false` | For `put`, write cache first and then storage. |
| `--retrieve_storage=true|false` | For `range`, fall back to storage on cache miss. |
| `--report_interval=N` | Progress interval in seconds. |
