# SPDX-License-Identifier: Apache-2.0
"""Serialization of LMCache keys to the native connector's wire format.

The native module accepts strings shaped as

    "{model_name}@{kv_rank_hex}@{chunk_hash_hex}"

where `kv_rank_hex` packs (world_size << 24) | (worker_id << 16) and
`chunk_hash_hex` is at least 5 hex chars (dingofs shards on the first 2/4).

This format matches lmcache.v1.distributed.l2_adapters.native_connector_l2_adapter
._object_key_to_string verbatim, so the upstream NativeConnectorL2Adapter can
talk to our native client without modification.

dtype is intentionally not encoded: chunk_hash is already a content hash, so
two CacheEngineKeys with the same model+rank+hash but different dtypes are
effectively the same chunk. The native side fills dtype with a fixed
placeholder when it reconstructs the dingofs TensorKey.
"""

from __future__ import annotations

from lmcache.utils import CacheEngineKey

__all__ = ["cache_engine_key_to_native_str", "pack_kv_rank"]


def pack_kv_rank(world_size: int, worker_id: int) -> int:
    """Pack (world_size, worker_id) into the 32-bit kv_rank shape used by
    the upstream native adapter contract."""
    if not (0 <= world_size < 256) or not (0 <= worker_id < 256):
        raise ValueError(
            f"world_size/worker_id must fit in one byte each "
            f"(got world_size={world_size}, worker_id={worker_id})"
        )
    return (world_size << 24) | (worker_id << 16)


def cache_engine_key_to_native_str(key: CacheEngineKey) -> str:
    """Render a CacheEngineKey as the native module's wire format.

    chunk_hash is truncated to its low 32 bits (8 hex chars) to match the
    upstream ObjectKey.chunk_hash byte layout (api.py IntHash2Bytes, 4 bytes).
    That keeps RemoteConnector and L2 adapter keys interchangeable.
    """
    kv_rank = pack_kv_rank(key.world_size, key.worker_id)
    chunk_hash_low = key.chunk_hash & 0xFFFFFFFF
    return f"{key.model_name}@{kv_rank:08x}@{chunk_hash_low:08x}"
