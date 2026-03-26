# SPDX-License-Identifier: Apache-2.0
"""LMCache integration — creating config programmatically.

Usage:
    python examples/lmcache_integration/lmcache_integration.py
"""


def main() -> None:
    try:
        from lmcache.v1.config import LMCacheEngineConfig
    except ImportError:
        print("[lmcache_integration] Skipped: lmcache not installed.")
        print("  Install with: pip install -e '.[lmcache]'")
        return

    config = LMCacheEngineConfig.from_dict(
        {
            "chunk_size": 256,
            "remote_url": "dingofs://host:0/mnt/dingofs/kv_cache",
            "remote_serde": "naive",
            "remote_storage_plugins": ["dingofs"],
            "extra_config": {
                "remote_storage_plugin.dingofs.module_path": "dingofs_connector.adapter",
                "remote_storage_plugin.dingofs.class_name": "DingoFSConnectorAdapter",
                "dingofs_num_workers": 8,
                "dingofs_use_odirect": False,
                "dingofs_sync_mode": "always",
            },
        }
    )
    print(f"[python_api] Config created: remote_url={config.remote_url}")


if __name__ == "__main__":
    main()
