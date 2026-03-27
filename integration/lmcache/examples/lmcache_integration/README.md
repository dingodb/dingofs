# LMCache Integration

Use DingoFS as a remote storage backend for LMCache.

## Prerequisites

```bash
cd integration/lmcache
pip install ".[lmcache]"
```

## Method 1: YAML Config (recommended)

See [lmcache_config.yaml](lmcache_config.yaml) for a complete example.

Key fields:

| Field | Value |
|-------|-------|
| `remote_url` | `dingofs://host:0/mount/path` |
| `remote_storage_plugins` | `[dingofs]` |
| `remote_storage_plugin.dingofs.module_path` | `dingofs_connector.adapter` |
| `remote_storage_plugin.dingofs.class_name` | `DingoFSConnectorAdapter` |

See [Tuning](../../docs/tuning.md) for all tuning parameters.

## Method 2: Python API

```bash
python examples/lmcache_integration/lmcache_integration.py
```

## Method 3: vLLM

```bash
vllm serve meta-llama/Llama-3-8b \
  --kv-cache-config '{"remote_url": "dingofs://host:0/mnt/dingofs/cache", ...}'
```

Or via environment variables:

```bash
export LMCACHE_REMOTE_URL="dingofs://host:0/mnt/dingofs/cache"
export LMCACHE_REMOTE_SERDE="naive"
vllm serve meta-llama/Llama-3-8b
```
