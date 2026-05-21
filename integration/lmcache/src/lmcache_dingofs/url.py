# SPDX-License-Identifier: Apache-2.0
"""Parser for the ``dingofs://`` connector URL.

Format::

    dingofs://host1[:port1][,host2[:port2]...]/group_name[?option=val&...]

Examples::

    dingofs://10.0.0.1:6700/kvcache
    dingofs://mds-a:6700,mds-b:6700/kvcache?fs_id=1&num_workers=16
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List
from urllib.parse import parse_qs, urlsplit

from .errors import DingoFSURLError

_DEFAULT_MDS_PORT = 6700
_DEFAULT_NUM_WORKERS = 16
_DEFAULT_TIMEOUT_MS = 5000
_DEFAULT_FS_ID = 0


@dataclass(frozen=True)
class DingoFSEndpoint:
    """Resolved connection parameters for a single dingofs cache cluster."""

    mds_addrs: List[str]
    group_name: str
    fs_id: int = _DEFAULT_FS_ID
    num_workers: int = _DEFAULT_NUM_WORKERS
    request_timeout_ms: int = _DEFAULT_TIMEOUT_MS


def _split_hosts(netloc: str) -> List[str]:
    if not netloc:
        raise DingoFSURLError("dingofs URL must contain at least one MDS host")
    hosts: List[str] = []
    for raw in netloc.split(","):
        host = raw.strip()
        if not host:
            continue
        if ":" not in host:
            host = f"{host}:{_DEFAULT_MDS_PORT}"
        hosts.append(host)
    if not hosts:
        raise DingoFSURLError(f"no valid MDS host in: {netloc!r}")
    return hosts


def _int_param(qs: dict, name: str, default: int) -> int:
    if name not in qs:
        return default
    raw = qs[name][0]
    try:
        return int(raw)
    except ValueError as e:
        raise DingoFSURLError(
            f"query param {name!r} must be an integer, got {raw!r}"
        ) from e


def parse_dingofs_url(url: str) -> DingoFSEndpoint:
    """Parse a ``dingofs://...`` URL into a :class:`DingoFSEndpoint`.

    The path component is the cache-group name. Query params override the
    defaults for ``fs_id``, ``num_workers`` and ``timeout_ms``.
    """
    if not url.startswith("dingofs://"):
        raise DingoFSURLError(f"not a dingofs URL: {url!r}")

    parts = urlsplit(url)
    hosts = _split_hosts(parts.netloc)

    group = parts.path.lstrip("/").rstrip("/")
    if not group:
        raise DingoFSURLError(
            "dingofs URL must include /<group_name> after the MDS list"
        )

    qs = parse_qs(parts.query)
    return DingoFSEndpoint(
        mds_addrs=hosts,
        group_name=group,
        fs_id=_int_param(qs, "fs_id", _DEFAULT_FS_ID),
        num_workers=_int_param(qs, "num_workers", _DEFAULT_NUM_WORKERS),
        request_timeout_ms=_int_param(qs, "timeout_ms", _DEFAULT_TIMEOUT_MS),
    )
