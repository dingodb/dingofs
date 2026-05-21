# SPDX-License-Identifier: Apache-2.0
"""Smoke tests for the C++ native engine.

These tests only verify the Python ↔ C++ wiring (constructor, attribute
access, error paths). End-to-end RPC tests against a live dingo-cache
cluster live in ``tests/integration/`` (manual; not run on CI by default).
"""

from __future__ import annotations

import pytest

pytest.importorskip(
    "dingofs_connector._native",
    reason="native extension not built; run `pip install ./integration/lmcache`",
)


def test_engine_rejects_empty_mds():
    from dingofs_connector import NativeCacheEngine

    with pytest.raises(Exception):  # std::invalid_argument from C++
        NativeCacheEngine(mds_addrs=[], group_name="kv", fs_id=1)


def test_engine_rejects_empty_group_name():
    from dingofs_connector import NativeCacheEngine

    with pytest.raises(Exception):
        NativeCacheEngine(
            mds_addrs=["127.0.0.1:6700"], group_name="", fs_id=1
        )


def test_module_imports_clean():
    """Smoke test: importing the package doesn't blow up before any RPC."""
    import dingofs_connector

    assert hasattr(dingofs_connector, "NativeCacheEngine")
    assert hasattr(dingofs_connector, "DingoFSConnectorAdapter")
    assert hasattr(dingofs_connector, "DingoFSRemoteConnector")
