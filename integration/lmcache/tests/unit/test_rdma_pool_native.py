# SPDX-License-Identifier: Apache-2.0
"""End-to-end binding tests for the ``rdma_pools`` argument.

Verifies that ``RemoteCache(rdma_pools=...)`` accepts the documented shapes
without typing or argument errors at the pybind11 boundary. Since unit-test
construction always fails at the unrelated ``StartCache`` step (no MDS at
``127.0.0.1:1``), the engine handle isn't observable after the exception
— so the round-trip through ``rdma_pools()`` accessor is left to
integration tests with a live cluster. Here we just confirm: (a) valid
shapes don't blow up at the boundary; (b) malformed shapes are rejected
before any I/O happens.
"""

from __future__ import annotations

import ctypes

import pytest

native = pytest.importorskip("dingofs_connector._dingofs_native")

_BAD_MDS = {"mds_addrs": "127.0.0.1:1", "cache_group": "lmcache-test"}


def _expect_no_rdma_error(rdma_pools):
    """RemoteCache must always raise here (no MDS), but the message must NOT
    name rdma_pools — i.e. the binding accepted the argument and failed
    later at StartCache, not at the boundary."""
    with pytest.raises(Exception) as exc_info:
        native.RemoteCache(rdma_pools=rdma_pools, **_BAD_MDS)
    assert "rdma_pools" not in str(exc_info.value).lower()


# --- valid shapes pass through to native -----------------------------------


def test_default_no_pools():
    # Omitting the kwarg → pybind default (empty vector) → accepted.
    with pytest.raises(Exception) as exc_info:
        native.RemoteCache(**_BAD_MDS)
    assert "rdma_pools" not in str(exc_info.value).lower()


def test_empty_list_accepted():
    _expect_no_rdma_error([])


def test_single_region():
    buf = ctypes.create_string_buffer(64 * 1024)
    _expect_no_rdma_error([(ctypes.addressof(buf), 64 * 1024)])


def test_multiple_regions():
    bufs = [ctypes.create_string_buffer(n) for n in (4096, 8192, 65536)]
    pools = [(ctypes.addressof(b), n) for b, n in zip(bufs, (4096, 8192, 65536))]
    _expect_no_rdma_error(pools)


# --- malformed shapes rejected at the binding boundary ----------------------


def test_rejects_bare_int():
    with pytest.raises(TypeError):
        native.RemoteCache(rdma_pools=[42], **_BAD_MDS)


def test_rejects_string_addr():
    with pytest.raises(TypeError):
        native.RemoteCache(rdma_pools=[("not_an_int", 4096)], **_BAD_MDS)
