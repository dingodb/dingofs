# SPDX-License-Identifier: Apache-2.0
"""Wire the dingofs L2 adapter into LMCache's factory registry.

Two pieces register:

1. ``lmcache_dingofs.config.DingoFSL2AdapterConfig`` registers itself as the
   config class for type name ``"dingofs"``.
2. The ``_create_dingofs_l2_adapter`` factory below builds a native engine
   and wraps it in LMCache's :class:`NativeConnectorL2Adapter`, which
   provides the three-eventfd / lookup-and-lock surface.

The L2 framework is only available in LMCache versions newer than 0.3.6.
Importing this module is a no-op on older versions: the import inside the
``try`` block fails and we log a warning instead of crashing.
"""

from __future__ import annotations

from typing import Optional

from lmcache.logging import init_logger

from ._native import NativeCacheEngine

logger = init_logger(__name__)

try:
    from lmcache.v1.distributed.l2_adapters.factory import (
        register_l2_adapter_factory,
    )
    from lmcache.v1.distributed.l2_adapters.native_connector_l2_adapter import (
        NativeConnectorL2Adapter,
    )

    # Importing config also runs register_l2_adapter_type("dingofs", ...).
    from lmcache_dingofs.config import DingoFSL2AdapterConfig  # noqa: F401

    _l2_available = True
except ImportError as e:  # pragma: no cover — older LMCache
    logger.info(
        "LMCache L2 adapter framework not present (%s); dingofs L2 adapter "
        "wiring skipped. RemoteConnector usage is unaffected.",
        e,
    )
    _l2_available = False


def _create_dingofs_l2_adapter(config, l1_memory_desc: Optional[object] = None):
    """Factory: ``DingoFSL2AdapterConfig`` → ``L2AdapterInterface`` instance."""
    engine = NativeCacheEngine(
        mds_addrs=config.mds_addrs,
        group_name=config.group_name,
        fs_id=config.fs_id,
        num_workers=config.num_workers,
        request_timeout_ms=config.request_timeout_ms,
    )
    logger.info(
        "DingoFS L2 adapter ready: mds=%s group=%s fs_id=%d service_version=%d",
        config.mds_addrs,
        config.group_name,
        config.fs_id,
        engine.service_version,
    )
    return NativeConnectorL2Adapter(engine)


if _l2_available:
    register_l2_adapter_factory("dingofs", _create_dingofs_l2_adapter)
