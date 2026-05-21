# SPDX-License-Identifier: Apache-2.0
"""URL parser tests — pure stdlib, no lmcache / native module needed."""

import pytest

from dingofs_connector.config import parse_dingofs_url


def test_single_mds():
    ep = parse_dingofs_url("dingofs://10.0.0.1:6700/lmcache_group")
    assert ep.mds_addrs == "10.0.0.1:6700"
    assert ep.cache_group == "lmcache_group"


def test_multi_mds():
    ep = parse_dingofs_url(
        "dingofs://mds1:6700,mds2:6700,mds3:6700/lmcache_group"
    )
    assert ep.mds_addrs == "mds1:6700,mds2:6700,mds3:6700"
    assert ep.cache_group == "lmcache_group"


@pytest.mark.parametrize(
    "url",
    [
        "redis://mds:6700/grp",          # wrong scheme
        "dingofs://mds:6700",            # no cache_group
        "dingofs:///grp",                # no mds_addrs
        "dingofs://mds:6700/",           # empty cache_group
    ],
)
def test_rejects_malformed(url):
    with pytest.raises(ValueError):
        parse_dingofs_url(url)


def test_rejects_query_string():
    # Any URL with `?` is rejected: connector knobs and dingofs gflags
    # now live in extra_config / conf file, not the URL.
    with pytest.raises(ValueError, match="no longer accepts query string"):
        parse_dingofs_url("dingofs://mds:6700/grp?cache_rpc_timeout_ms=5000")
    with pytest.raises(ValueError, match="no longer accepts query string"):
        parse_dingofs_url("dingofs://mds:6700/grp?max_chunk_mib=16")
