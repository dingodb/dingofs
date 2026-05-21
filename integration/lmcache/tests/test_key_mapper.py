# SPDX-License-Identifier: Apache-2.0
"""Key encoder unit tests — must produce strings the C++ key_codec can parse."""

from __future__ import annotations

import pytest

# These imports do not require the C++ extension.
from lmcache.utils import CacheEngineKey

from lmcache_dingofs.key_mapper import cache_engine_key_to_native


def test_cache_engine_key_roundtrip_shape():
    k = CacheEngineKey(
        fmt="kv_2ltd",
        model_name="llama-3",
        world_size=2,
        worker_id=0,
        chunk_hash=0xDEADBEEF,
    )
    s = cache_engine_key_to_native(k)
    parts = s.split("@")
    assert len(parts) == 5
    assert parts[0] == "llama-3"
    assert parts[1] == "2"
    assert parts[2] == "0"
    assert parts[3] == "deadbeef"
    assert parts[4] == "kv_2ltd"


def test_model_name_with_at_sign_rejected():
    k = CacheEngineKey(
        fmt="kv_2ltd",
        model_name="bad@model",
        world_size=1,
        worker_id=0,
        chunk_hash=1,
    )
    with pytest.raises(ValueError):
        cache_engine_key_to_native(k)


def test_distinct_keys_distinct_strings():
    a = CacheEngineKey(fmt="x", model_name="m", world_size=1, worker_id=0, chunk_hash=1)
    b = CacheEngineKey(fmt="x", model_name="m", world_size=1, worker_id=0, chunk_hash=2)
    assert cache_engine_key_to_native(a) != cache_engine_key_to_native(b)
