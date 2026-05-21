# SPDX-License-Identifier: Apache-2.0
"""Key mapper tests — verifies the wire format we share with the upstream
NativeConnectorL2Adapter."""

import importlib.util

import pytest

from dingofs_connector.key_mapper import (
    cache_engine_key_to_native_str,
    pack_kv_rank,
)

# CacheEngineKey lives in lmcache; if it isn't installed we can still test
# pack_kv_rank but skip the higher-level mapping tests.
_HAVE_LMCACHE = importlib.util.find_spec("lmcache") is not None


def test_pack_kv_rank_layout():
    assert pack_kv_rank(0, 0) == 0
    assert pack_kv_rank(1, 0) == (1 << 24)
    assert pack_kv_rank(0, 1) == (1 << 16)
    assert pack_kv_rank(2, 3) == (2 << 24) | (3 << 16)


@pytest.mark.parametrize("ws,wid", [(-1, 0), (0, -1), (256, 0), (0, 256)])
def test_pack_kv_rank_rejects_overflow(ws, wid):
    with pytest.raises(ValueError):
        pack_kv_rank(ws, wid)


@pytest.mark.skipif(not _HAVE_LMCACHE, reason="lmcache not installed")
def test_format_matches_upstream_object_key_to_string():
    # The format must match
    # lmcache.v1.distributed.l2_adapters.native_connector_l2_adapter
    # ._object_key_to_string exactly, otherwise RemoteConnector puts and
    # L2Adapter gets would land on different server-side paths.
    import torch
    from lmcache.utils import CacheEngineKey

    k = CacheEngineKey(
        model_name="meta-llama/Llama-3-8B",
        world_size=2,
        worker_id=1,
        chunk_hash=0xDEADBEEF,
        dtype=torch.float16,
    )
    s = cache_engine_key_to_native_str(k)

    # Expected: "{model}@{kv_rank:08x}@{chunk_hash_low:08x}"
    expected_kv_rank = (2 << 24) | (1 << 16)
    expected = f"meta-llama/Llama-3-8B@{expected_kv_rank:08x}@deadbeef"
    assert s == expected


@pytest.mark.skipif(not _HAVE_LMCACHE, reason="lmcache not installed")
def test_format_chunk_hash_low_32_bits_only():
    import torch
    from lmcache.utils import CacheEngineKey

    # Use a chunk_hash that exceeds 32 bits — we should only encode the low 32.
    k = CacheEngineKey(
        model_name="m",
        world_size=0,
        worker_id=0,
        chunk_hash=(0xAABBCCDD_DEADBEEF),
        dtype=torch.float16,
    )
    s = cache_engine_key_to_native_str(k)
    assert s.endswith("@deadbeef")
