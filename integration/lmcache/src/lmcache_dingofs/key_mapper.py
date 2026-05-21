# SPDX-License-Identifier: Apache-2.0
"""Convert between LMCache key types and the on-wire 5-segment string format.

The native engine parses keys as ``model@world_size@worker_id@chunk_hash@dtype``
— exactly five `@`-separated segments. We map LMCache's two key types onto
that shape so dingofs server stores them under ``tensor/XX/XXXX/...`` paths.

Module-local: no `@` is allowed inside any field. LMCache itself parses by
`@` already, so model_name is already `@`-free in practice.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from lmcache.utils import CacheEngineKey
    from lmcache.v1.distributed.api import ObjectKey


_L2_DTYPE_TAG = "L2"


def cache_engine_key_to_native(key: "CacheEngineKey") -> str:
    """Encode a :class:`CacheEngineKey` (RemoteConnector path) into the native
    5-segment format.

    Layout::

        {model}@{world_size}@{worker_id}@{chunk_hash_hex}@{fmt}

    ``fmt`` lives in the ``dtype`` slot — semantically it's just a tag that
    differentiates encodings, and the server treats the whole filename as
    opaque so this is safe.
    """
    if "@" in key.model_name:
        raise ValueError(
            f"model_name {key.model_name!r} must not contain '@' — LMCache "
            "key (de)serialization assumes the same"
        )
    return (
        f"{key.model_name}@{key.world_size}@{key.worker_id}"
        f"@{key.chunk_hash:x}@{key.fmt}"
    )


def object_key_to_native(key: "ObjectKey") -> str:
    """Encode an :class:`ObjectKey` (L2 path) into the native 5-segment format.

    Layout::

        {model}@0@{kv_rank}@{chunk_hash_hex}@L2

    ``world_size`` is filled with 0 because :class:`ObjectKey` only stores the
    derived ``kv_rank`` (= world_size * worker_id + worker_id). The trailing
    ``L2`` tag separates these keys from RemoteConnector keys so a single
    cache directory can host both without collision.
    """
    if "@" in key.model_name:
        raise ValueError(
            f"model_name {key.model_name!r} must not contain '@'"
        )
    return (
        f"{key.model_name}@0@{key.kv_rank}@{key.chunk_hash.hex()}"
        f"@{_L2_DTYPE_TAG}"
    )
