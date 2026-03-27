# DingoFS Connector for LMCache

A high-performance storage connector for LLM KV cache on [DingoFS](https://github.com/dingodb/dingofs), built with a native C++ I/O engine and Python ctypes bindings.

## Quick Start

### 1. Build Wheel (on build machine)

```bash
cd integration/lmcache
pip install build
python -m build --wheel
```

Output: `dist/dingofs_connector-0.1.0-cp3*-linux_x86_64.whl`

### 2. Install (on target machine)

```bash
# Copy wheel to target machine, then:
pip install dingofs_connector-0.1.0-cp3*-linux_x86_64.whl
```

### 3. Configure LMCache

Add to your LMCache YAML config:

```yaml
chunk_size: 256
remote_url: "dingofs://host:0/mnt/dingofs/kv_cache"
remote_serde: "naive"
remote_storage_plugins:
  - dingofs
extra_config:
  remote_storage_plugin.dingofs.module_path: "dingofs_connector.adapter"
  remote_storage_plugin.dingofs.class_name: "DingoFSConnectorAdapter"
  dingofs_num_workers: 8
```

More details see [docs](docs) and [examples](examples).
