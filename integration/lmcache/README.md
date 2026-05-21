# DingoFS Connector for LMCache

A high-performance KV-cache backend that lets [LMCache](https://github.com/LMCache/LMCache)
push and pull KV-cache chunks to/from a distributed [DingoFS](https://github.com/dingodb/dingofs)
cache cluster. Two LMCache extension points are implemented:

* **RemoteConnector** — same-process backend, `dingofs://...` URL,
  async + sync `get/put/exists/list/close` plus all batched hot-path methods.
* **L2 storage adapter** — MP-server mode, three eventfds, bitmap results,
  client-side `lookup_and_lock` refcount. Wired by reusing LMCache's
  `NativeConnectorL2Adapter` shim over our `IStorageConnector`.

Both paths share a single pybind11 native engine (`_native.NativeCacheEngine`)
that internally drives DingoFS's `Upstream` client (`MDSClient` member discovery
+ consistent-hash ring + brpc channels) — there is no Python-side gRPC code.

## Architecture

```
┌── LMCache (Python) ───────────────────────────────────────────┐
│  Remote path                       L2 path (MP server)        │
│  DingoFSRemoteConnector            NativeConnectorL2Adapter   │
│  (RemoteConnector subclass)        (LMCache built-in shim)    │
│           └──────────┬─────────────────────┘                  │
└──────────────────────┼────────────────────────────────────────┘
                       │  pybind11 IStorageConnector ABI
┌──────────────────────▼────────────────────────────────────────┐
│  _native.NativeCacheEngine  (subclass of IStorageConnector)   │
│    - submit_batch_get / set / exists                          │
│    - event_fd() + drain_completions()                         │
└──────────────────────┬────────────────────────────────────────┘
                       │  in-process C++
┌──────────────────────▼────────────────────────────────────────┐
│  Upstream  (src/cache/remotecache/upstream.cc)                │
│   ├─ MDSClient::ListMembers → CacheGroupMember[]              │
│   ├─ PeerGroup (consistent-hash by TensorKey::Filename)       │
│   └─ Peer → brpc → dingo-cache servers                        │
└───────────────────────────────────────────────────────────────┘
```

The wire-level proto change is `BlockContext.key` becoming a `oneof
{ BlockKey, TensorKey }`. Old FUSE / block clients still send `BlockKey` on
tag 1 and stay wire-compatible; new connectors send `TensorKey` on tag 3 and
get routed through the disk-cache only (no S3 staging).

## Requirements

* Python ≥ 3.9
* DingoFS source tree (this repo) builds successfully — `cmake` + a C++17
  toolchain plus the dingofs third-party stack at
  `~/.local/dingo-eureka` (or as overridden by `THIRD_PARTY_INSTALL_PATH`).
* `pybind11` ≥ 2.12.
* `lmcache` 0.3.6 (installed; the L2 adapter framework requires a newer
  LMCache main and degrades gracefully when absent).
* The `dingo-cache` server must report `service_version ≥ 1` (i.e. it must
  include the `TensorKey` oneof). The engine constructor pings on startup
  and refuses to start otherwise.

## Build & install

### From source (default)

```bash
# From the dingofs repo root:
pip install ./integration/lmcache
```

scikit-build-core invokes CMake which `add_subdirectory()`s the parent dingofs
tree, links against `cache_lib`, and emits a wheel containing:

```
dingofs_connector/
  __init__.py
  _native.cpython-3X-x86_64-linux-gnu.so
  adapter.py
  connector.py
  l2_factory.py
lmcache_dingofs/
  url.py
  config.py
  key_mapper.py
  errors.py
```

### Standalone CMake (for development)

```bash
# In the dingofs root:
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
      -DBUILD_UNIT_TESTS=OFF ..
make cache_lib -j 8
# Build just the python extension:
cmake --build . --target _native
```

The `.so` will land in `build/integration/lmcache/`. Copy it next to the Python
sources, or invoke `pip install ./integration/lmcache` once for a clean wheel.

## Usage

### A. RemoteConnector (single-process vLLM)

In your LMCache config:

```yaml
# lmcache_config.yaml
remote_url: "external://_/dingofs_connector.adapter/?connector_name=DingoFSConnectorAdapter&dingofs_url=dingofs%3A%2F%2F10.0.0.1%3A6700%2Fkvcache%3Ffs_id%3D1"
```

or equivalently, programmatically:

```python
from lmcache.v1.config import LMCacheEngineConfig

cfg = LMCacheEngineConfig.from_defaults(
    remote_url=(
        "dingofs://10.0.0.1:6700,10.0.0.2:6700/kvcache"
        "?fs_id=1&num_workers=16&timeout_ms=5000"
    ),
)
```

vLLM will then create a `DingoFSRemoteConnector` for every KV-cache chunk
operation, hashing by `TensorKey::Filename()` onto the consistent-hash ring
of cache nodes registered in the `kvcache` group.

### B. L2 storage adapter (MP-server mode)

Requires LMCache main (post-0.3.6) with the `lmcache.v1.distributed.l2_adapters`
package present.

```bash
vllm serve <model> \
  --kv-transfer-config '{"kv_connector":"LMCacheConnectorV1MP", ...}' \
  --l2-adapter '{"type":"dingofs",
                 "mds_addrs":["10.0.0.1:6700"],
                 "group_name":"kvcache",
                 "fs_id":1,
                 "num_workers":16,
                 "request_timeout_ms":5000}'
```

In Python:

```python
from lmcache_dingofs.config import DingoFSL2AdapterConfig
import dingofs_connector.l2_factory  # registers the factory side-effectfully

cfg = DingoFSL2AdapterConfig.from_dict({
    "type": "dingofs",
    "mds_addrs": ["10.0.0.1:6700"],
    "group_name": "kvcache",
    "fs_id": 1,
    "num_workers": 16,
})
```

The factory creates one `_native.NativeCacheEngine` per adapter and wraps it
in LMCache's `NativeConnectorL2Adapter`. That shim handles the three-eventfd
demux, lookup-and-lock refcount, and bitmap synthesis.

## Operating the cache cluster

Run `dingo-cache` nodes per the main dingofs docs — they need an MDS endpoint
and must register with the `--group_name` that the connector queries.

```bash
dingo-cache --flagfile=cache.conf --listen_ip=0.0.0.0 --listen_port=6701 \
            --group_name=kvcache --mds_addrs=10.0.0.1:6700 --id=node1 \
            --cache_dir=/var/cache/dingo --cache_size_mb=102400
```

Tensor data lands on disk under `/var/cache/dingo/tensor/XX/XXXX/...` where
`XX/XXXX` is a two-level prefix bucketing by `chunk_hash`. Files coexist with
regular block traffic (`/var/cache/dingo/blocks/...`) — the LRU and inflight
tracker treat them as separate cache populations because their `Filename()`
formats are disjoint.

## Tuning knobs

| URL query / config key | Default | Effect |
|---|---|---|
| `num_workers` | 16 | Worker thread pool size; fan-out of batched submissions. Increase for high in-flight depth. |
| `fs_id` | 0 | Routing id stamped on each request. Use a different id per logical filesystem if you partition cache. |
| `timeout_ms` | 5000 | Per-RPC timeout. Bump for high-latency network. |

Observe via dingofs bvars: `dingofs_remote_node_group_put_*`,
`*_range_*`, and the cache-side `dingofs_cache_hit_count` /
`dingofs_cache_miss_count`.

## FAQ

### "dingo-cache server is too old: service_version=0"
The proto schema bumped to add `TensorKey` and `service_version`. Old dingo-cache
binaries can't parse the new request shape. Rebuild and redeploy the cache
nodes from a dingofs revision that contains commit *Introduce TensorKey
oneof in BlockCacheService* or later.

### "DingoFS connector requires init_chunk_meta() to have been called"
LMCache must call `init_chunk_meta(config, metadata)` on the connector
before any `get` — this is what populates the chunk shape/dtype used to
allocate buffers. The base `ConnectorManager` does this automatically; if
you instantiate the connector by hand for testing, call it yourself.

### Why does it use bound threads instead of bthread on the client?
`lmcache::connector::ConnectorBase` is a `std::thread`-based pool. brpc
supports being called from non-bthread threads (it parks the calling pthread
on a bthread internally), so this is the path of least resistance. If we
later need bthread end-to-end, swap the worker loop without breaking the
Python ABI.

### What about non-tensor traffic?
Untouched. Old FUSE / block clients keep sending `BlockKey` on proto tag 1;
the server's `oneof key` resolves to the `block_key` case and routes through
the original S3-backed disk-cache path.

## Limitations

* `list()` — DingoFS cache doesn't enumerate keys. The base
  `RemoteConnector.list()` raises `NotImplementedError`.
* `lookup_and_lock` is client-local refcounting; the cache server has no
  pin/unpin RPC yet. This matches the Redis adapter's semantics in LMCache.
* No GPU-direct transport. Bytes go through CPU.
