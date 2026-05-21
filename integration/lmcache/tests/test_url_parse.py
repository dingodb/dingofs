# SPDX-License-Identifier: Apache-2.0
"""URL parser unit tests — runs without any dingo-cache server."""

from __future__ import annotations

import pytest

from lmcache_dingofs.errors import DingoFSURLError
from lmcache_dingofs.url import parse_dingofs_url


def test_single_host_default_port():
    ep = parse_dingofs_url("dingofs://mds-a/kvcache")
    assert ep.mds_addrs == ["mds-a:6700"]
    assert ep.group_name == "kvcache"
    assert ep.fs_id == 0
    assert ep.num_workers == 16
    assert ep.request_timeout_ms == 5000


def test_explicit_port_and_query_params():
    ep = parse_dingofs_url(
        "dingofs://mds-a:6701,mds-b:6701/grp?fs_id=3&num_workers=32&timeout_ms=12000"
    )
    assert ep.mds_addrs == ["mds-a:6701", "mds-b:6701"]
    assert ep.group_name == "grp"
    assert ep.fs_id == 3
    assert ep.num_workers == 32
    assert ep.request_timeout_ms == 12000


def test_trailing_slash_on_group():
    ep = parse_dingofs_url("dingofs://mds/grp/")
    assert ep.group_name == "grp"


def test_missing_scheme():
    with pytest.raises(DingoFSURLError):
        parse_dingofs_url("redis://foo/bar")


def test_missing_host():
    with pytest.raises(DingoFSURLError):
        parse_dingofs_url("dingofs:///grp")


def test_missing_group():
    with pytest.raises(DingoFSURLError):
        parse_dingofs_url("dingofs://mds:6700/")


def test_non_integer_query_param():
    with pytest.raises(DingoFSURLError):
        parse_dingofs_url("dingofs://mds/grp?num_workers=many")
