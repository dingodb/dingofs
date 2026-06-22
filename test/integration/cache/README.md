# Cache integration tests

Real end-to-end tests for the cache layer. Unlike the unit tests
(`test/unit/cache`, which exercise classes in-process with mocks), these spawn
the **real `dingo-mds` and `dingo-cache` binaries** as child processes and drive
the cache client over real RPC.

Like `test/unit/cache`, everything compiles into a **single binary**
(`integration_test_cache`); pick what to run with `--gtest_filter`.

| Suite (filter) | What it covers |
|----------------|----------------|
| `NodeTest.*` | Cache-node lifecycle: startup, Ping, registration (ListMembers → Online), multi-member, deregistration, group isolation. |
| `LocalCacheTest.*` / `LocalCacheRawTest.*` | Client-side local on-disk cache: Put/Range round-trips, sub-ranges, reflow, async, prefetch, reload, TTL expiry, eviction, concurrency. |
| `DistributedCacheTest.*` / `DistributedSmallCacheTest.*` | Client → remote cache node over RPC: Put/Range/Cache/Prefetch, multi-node consistent hashing, node leave/restart, eviction. |

## Prerequisites

- **An active RDMA device** (InfiniBand/RoCE, e.g. `mlx5_0`). The on-disk cache
  pins its O_DIRECT buffers in the global RDMA slab pool, both in the test
  process and in the spawned `dingo-cache`. Without a device every test
  `GTEST_SKIP`s (the run still passes).
- Pre-built `dingo-mds` and `dingo-cache`, located next to the test binary
  (`<bin>/../dingo-mds`) by default; override with `--dingo_mds_bin=<path>` /
  `--dingo_cache_bin=<path>`.

No external services are required: the MDS runs with the in-memory `dummy`
storage engine and the filesystem uses a `LOCALFILE` backend (a temp dir), so
there is no TiKV / dingo-store / MinIO dependency.

## Step 1: build

```bash
cmake -S . -B build \
  -DBUILD_INTEGRATION_TESTS=ON \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
&& cmake --build build --target integration_test_cache dingo-mds dingo-cache -j 32
```

## Step 2: run

```bash
./build/bin/test/integration_test_cache --gtest_filter='*'
```

Notes:
- Each test starts real processes; individual cases take ~5–28s. Use
  `--gtest_filter` while iterating rather than running the whole suite.
- A run on a host without an RDMA device skips every test instead of failing.
