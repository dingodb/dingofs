# SPDX-License-Identifier: Apache-2.0
"""Register the dingofs L2 adapter type at import time.

Once this module is imported (which happens automatically via
dingofs_connector.__init__), users can write in their LMCache config:

    --l2-adapter '{"type":"dingofs","mds_addrs":"...","cache_group":"..."}'
"""

from __future__ import annotations

from typing import Optional, TYPE_CHECKING

from lmcache.logging import init_logger
from lmcache.v1.distributed.l2_adapters.base import L2AdapterInterface
from lmcache.v1.distributed.l2_adapters.config import (
    L2AdapterConfigBase,
    register_l2_adapter_type,
)
from lmcache.v1.distributed.l2_adapters.factory import (
    register_l2_adapter_factory,
)

from .access_log import access_log
from .config import DingoFSL2AdapterConfig
from .l2_adapter import DingoFSL2Adapter

if TYPE_CHECKING:
    from lmcache.v1.distributed.internal_api import L1MemoryDesc

logger = init_logger(__name__)


def _create_dingofs_l2_adapter(
    config: L2AdapterConfigBase,
    l1_memory_desc: "Optional[L1MemoryDesc]" = None,
) -> L2AdapterInterface:
    _ = l1_memory_desc  # unused
    with access_log(
        "l2.factory",
        lambda: (
            f"mds={config.mds_addrs} cache_group={config.cache_group}"
            if isinstance(config, DingoFSL2AdapterConfig)
            else f"<bad-config:{type(config).__name__}>"
        ),
    ) as r:
        if not isinstance(config, DingoFSL2AdapterConfig):
            raise TypeError(
                f"expected DingoFSL2AdapterConfig, got {type(config).__name__}"
            )

        # Import the native module lazily so we can fail with a clear message
        # if the extension wasn't built / installed.
        try:
            from . import _dingofs_native  # type: ignore[attr-defined]
        except ImportError as e:
            raise RuntimeError(
                "dingofs_connector native extension is missing — build dingofs "
                "with -DBUILD_LMCACHE_CONNECTOR=ON, then pip install this package."
            ) from e

        native = _dingofs_native.RemoteCache(
            mds_addrs=config.mds_addrs,
            cache_group=config.cache_group,
            extra=config.extra,
        )
        logger.info(
            "Created DingoFS L2 adapter: mds=%s cache_group=%s",
            config.mds_addrs,
            config.cache_group,
        )
        adapter = DingoFSL2Adapter(native)
        r.result = "ok"
        return adapter


register_l2_adapter_type("dingofs", DingoFSL2AdapterConfig)
register_l2_adapter_factory("dingofs", _create_dingofs_l2_adapter)
