# SPDX-License-Identifier: Apache-2.0
"""L2 adapter config for the dingofs cache backend."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from .errors import DingoFSConfigError

# The L2 adapter framework only exists in LMCache main (post-0.3.6). Provide a
# tiny stub base when it's missing so this module still imports cleanly on
# RemoteConnector-only deploys; users hitting the L2 path will get a clear
# error at registration time instead of an import failure on startup.
try:
    from lmcache.v1.distributed.l2_adapters.config import (
        L2AdapterConfigBase,
        register_l2_adapter_type,
    )
    _L2_FRAMEWORK_AVAILABLE = True
except ImportError:  # pragma: no cover — older LMCache
    _L2_FRAMEWORK_AVAILABLE = False

    class L2AdapterConfigBase:  # type: ignore[no-redef]
        @classmethod
        def from_dict(cls, d):  # pragma: no cover
            raise DingoFSConfigError(
                "LMCache L2 adapter framework not available; "
                "RemoteConnector path is unaffected"
            )

        @classmethod
        def help(cls) -> str:  # pragma: no cover
            return "L2 adapter framework unavailable"

    def register_l2_adapter_type(name, config_cls):  # type: ignore[no-redef]
        pass


@dataclass
class DingoFSL2AdapterConfig(L2AdapterConfigBase):
    """Config for the ``dingofs`` L2 adapter.

    Required fields:
      * ``mds_addrs``: list of "host:port" strings (the dingo MDS endpoints)
      * ``group_name``: cache group name to join on MDS

    Optional tuning knobs are documented in :meth:`help`.
    """

    mds_addrs: List[str] = field(default_factory=list)
    group_name: str = ""
    fs_id: int = 0
    num_workers: int = 16
    request_timeout_ms: int = 5000

    @classmethod
    def from_dict(cls, d: dict) -> "DingoFSL2AdapterConfig":
        try:
            mds = d["mds_addrs"]
            if isinstance(mds, str):
                mds = [s.strip() for s in mds.split(",") if s.strip()]
            if not isinstance(mds, list) or not mds:
                raise DingoFSConfigError(
                    "'mds_addrs' must be a non-empty list (or comma-separated string)"
                )
            group = d.get("group_name") or d.get("group")
            if not group:
                raise DingoFSConfigError("'group_name' is required")
            return cls(
                mds_addrs=[str(x) for x in mds],
                group_name=str(group),
                fs_id=int(d.get("fs_id", 0)),
                num_workers=int(d.get("num_workers", 16)),
                request_timeout_ms=int(d.get("request_timeout_ms", 5000)),
            )
        except (KeyError, TypeError, ValueError) as e:
            raise DingoFSConfigError(f"invalid dingofs L2 adapter config: {e}") from e

    @classmethod
    def help(cls) -> str:
        return (
            "DingoFS L2 adapter — talks to a dingo-cache cluster via the\n"
            "TensorKey-aware BlockCacheService RPC.\n"
            "\n"
            'Example JSON:\n'
            '  --l2-adapter \'{"type":"dingofs",\n'
            '                  "mds_addrs":["10.0.0.1:6700","10.0.0.2:6700"],\n'
            '                  "group_name":"kvcache",\n'
            '                  "fs_id":1,\n'
            '                  "num_workers":16,\n'
            '                  "request_timeout_ms":5000}\'\n'
            "\n"
            "Fields:\n"
            "  mds_addrs           List[str] | comma-separated str — MDS endpoints.\n"
            "  group_name          str — cache-group name to query for peers.\n"
            "  fs_id               uint32 — routing id stamped on each block.\n"
            "  num_workers         int — submission fan-out (default 16).\n"
            "  request_timeout_ms  uint32 — per-RPC timeout (default 5000).\n"
        )


register_l2_adapter_type("dingofs", DingoFSL2AdapterConfig)
