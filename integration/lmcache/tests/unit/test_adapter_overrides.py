# SPDX-License-Identifier: Apache-2.0
"""Tests for ``DingoFSConnectorAdapter._resolve_overrides``.

Pure-Python: no native module, no lmcache runtime — we hand-roll a tiny
fake of LMCache's ConnectorContext so we can exercise the pure parser.
"""

from types import SimpleNamespace

import pytest

from dingofs_connector.adapter import DingoFSConnectorAdapter


def _ctx(*, url: str, extra_config=None, plugin_name=None):
    """Minimal stand-in for ``lmcache.v1.storage_backend.connector.ConnectorContext``."""
    return SimpleNamespace(
        url=url,
        plugin_name=plugin_name,
        config=SimpleNamespace(extra_config=extra_config),
    )


resolve = DingoFSConnectorAdapter._resolve_overrides


# --- legacy dingofs:// URL pass-through ----------------------------------


def test_legacy_url_pass_through():
    o = resolve(_ctx(url="dingofs://mds:6700/grp"))
    assert o.target_url == "dingofs://mds:6700/grp"
    assert o.conf_path is None
    assert o.max_chunk_mib is None


def test_legacy_url_ignores_extra_config():
    # On the legacy path we have no plugin name to key under, so the
    # adapter doesn't read extra_config — even if conf / max_chunk_mib are set.
    o = resolve(_ctx(
        url="dingofs://mds:6700/grp",
        extra_config={
            "remote_storage_plugin.dingofs.conf": "/etc/x.conf",
            "remote_storage_plugin.dingofs.max_chunk_mib": 32,
        },
    ))
    assert o.conf_path is None
    assert o.max_chunk_mib is None


# --- plugin:// path requires url in extra_config -------------------------


def test_plugin_requires_url():
    with pytest.raises(ValueError, match="requires extra_config"):
        resolve(_ctx(url="plugin://dingofs", plugin_name="dingofs",
                     extra_config={}))


def test_plugin_url_must_be_dingofs_scheme():
    with pytest.raises(ValueError, match="must be a string starting with"):
        resolve(_ctx(url="plugin://dingofs", plugin_name="dingofs",
                     extra_config={"remote_storage_plugin.dingofs.url": "redis://x:1/y"}))


def test_plugin_basic():
    o = resolve(_ctx(
        url="plugin://dingofs", plugin_name="dingofs",
        extra_config={"remote_storage_plugin.dingofs.url":
                      "dingofs://mds:6700/grp"},
    ))
    assert o.target_url == "dingofs://mds:6700/grp"
    assert o.conf_path is None
    assert o.max_chunk_mib is None


# --- conf field ----------------------------------------------------------


def test_conf_path_passed_through():
    o = resolve(_ctx(
        url="plugin://dingofs", plugin_name="dingofs",
        extra_config={
            "remote_storage_plugin.dingofs.url": "dingofs://mds:6700/grp",
            "remote_storage_plugin.dingofs.conf": "/etc/dingofs/lmc.conf",
        },
    ))
    assert o.conf_path == "/etc/dingofs/lmc.conf"


def test_conf_path_empty_string_treated_as_none():
    # Empty string → falsy → no conf file loaded by native side.
    o = resolve(_ctx(
        url="plugin://dingofs", plugin_name="dingofs",
        extra_config={
            "remote_storage_plugin.dingofs.url": "dingofs://mds:6700/grp",
            "remote_storage_plugin.dingofs.conf": "",
        },
    ))
    assert o.conf_path is None


def test_conf_path_rejects_non_string():
    with pytest.raises(ValueError, match="must be a filesystem path string"):
        resolve(_ctx(
            url="plugin://dingofs", plugin_name="dingofs",
            extra_config={
                "remote_storage_plugin.dingofs.url": "dingofs://mds:6700/grp",
                "remote_storage_plugin.dingofs.conf": 42,
            },
        ))


# --- max_chunk_mib field -------------------------------------------------


def test_max_chunk_mib_parsed():
    o = resolve(_ctx(
        url="plugin://dingofs", plugin_name="dingofs",
        extra_config={
            "remote_storage_plugin.dingofs.url": "dingofs://mds:6700/grp",
            "remote_storage_plugin.dingofs.max_chunk_mib": 16,
        },
    ))
    assert o.max_chunk_mib == 16


def test_max_chunk_mib_accepts_numeric_string():
    o = resolve(_ctx(
        url="plugin://dingofs", plugin_name="dingofs",
        extra_config={
            "remote_storage_plugin.dingofs.url": "dingofs://mds:6700/grp",
            "remote_storage_plugin.dingofs.max_chunk_mib": "32",
        },
    ))
    assert o.max_chunk_mib == 32


@pytest.mark.parametrize("bad", ["abc", "4.5", 0, -1, []])
def test_max_chunk_mib_rejects_invalid(bad):
    with pytest.raises(ValueError):
        resolve(_ctx(
            url="plugin://dingofs", plugin_name="dingofs",
            extra_config={
                "remote_storage_plugin.dingofs.url": "dingofs://mds:6700/grp",
                "remote_storage_plugin.dingofs.max_chunk_mib": bad,
            },
        ))


# --- plugin instance name (dingofs.primary etc.) -------------------------


def test_plugin_instance_name_namespaces_keys():
    # When LMCache plugin_name is "dingofs.primary", config keys must be
    # under remote_storage_plugin.dingofs.primary.* — keys under just
    # "dingofs" should be invisible to this instance.
    o = resolve(_ctx(
        url="plugin://dingofs.primary", plugin_name="dingofs.primary",
        extra_config={
            "remote_storage_plugin.dingofs.primary.url":
                "dingofs://mds1:6700/grp",
            "remote_storage_plugin.dingofs.primary.max_chunk_mib": 8,
            # Decoy: belongs to a different instance, must be ignored.
            "remote_storage_plugin.dingofs.url": "dingofs://wrong:1/wrong",
        },
    ))
    assert o.target_url == "dingofs://mds1:6700/grp"
    assert o.max_chunk_mib == 8
