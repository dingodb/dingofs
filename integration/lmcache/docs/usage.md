# Usage

## Install

### From Source

```bash
cd integration/lmcache

# Install (automatically compiles C++ and bundles .so into the package)
pip install .

# With LMCache dependency
pip install ".[lmcache]"

# With test dependencies
pip install ".[test]"
```

### From Wheel

Build a wheel locally, then copy to the target machine:

```bash
# Build wheel
pip install build
python -m build --wheel

# Copy to target machine and install
pip install dist/dingofs_connector-0.1.0-*.whl
```

### Development Mode

```bash
# Build C++ library first
make

# Editable install (loads .so from build/ directory)
pip install -e .
```

### Library Discovery Order

`NativeIOEngine` locates `libdingofs_connector.so` in this order:

1. Bundled in the package directory (wheel install)
2. `DINGOFS_CONNECTOR_LIB` environment variable (explicit override)
3. `build/libdingofs_connector.so` relative to the project root (dev workflow)
4. System library path (`ldconfig`)

## Configuration

### URL Format

```
dingofs://host:port/mount/path
         ^^^^^^^^^  ^^^^^^^^^^
         placeholder DingoFS mount point
```

`host:port` is required for URL compatibility but not used for connections. The `/mount/path` portion is the local DingoFS mount point where cache files are stored.

Example: `dingofs://host:0/mnt/dingofs/kv_cache`

### LMCache Plugin Config

Register the connector in your LMCache YAML config:

```yaml
remote_url: "dingofs://host:0/mnt/dingofs/kv_cache"
remote_serde: "naive"
remote_storage_plugins:
  - dingofs
extra_config:
  remote_storage_plugin.dingofs.module_path: "dingofs_connector.adapter"
  remote_storage_plugin.dingofs.class_name: "DingoFSConnectorAdapter"
  dingofs_num_workers: 8
  dingofs_use_odirect: false
  dingofs_sync_mode: "always"
```

### Parameters

All parameters are set via `extra_config`:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `dingofs_num_workers` | int | 8 | Number of I/O worker threads |
| `dingofs_use_odirect` | bool | false | Use O_DIRECT for file I/O (requires aligned buffers) |
| `dingofs_sync_mode` | str | `"always"` | `"always"`: fdatasync after every write; `"none"`: skip fdatasync |

### NativeIOEngine Direct Usage

When using `NativeIOEngine` directly (without LMCache):

```python
engine = NativeIOEngine(
    base_path="/mnt/dingofs/cache",  # Storage directory
    num_workers=8,                    # I/O thread count
    use_odirect=False,                # O_DIRECT flag
    sync_mode=SYNC_ALWAYS,            # SYNC_ALWAYS or SYNC_NONE
    loop=asyncio_loop,                # Required for async operations
)
```

## Run Tests

### C++ tests

```bash
make test
```

### Python tests

```bash
# Unit tests (NativeIOEngine)
pytest tests/dingofs_connector/test_native_engine.py -xvs

# Integration tests (requires lmcache)
pytest tests/dingofs_connector/test_connector.py -xvs

# Performance tests
pytest tests/dingofs_connector/test_performance.py -xvs

# All Python tests
pytest tests/dingofs_connector/ -xvs
```
