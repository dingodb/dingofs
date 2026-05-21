# SPDX-License-Identifier: Apache-2.0
"""LMCache ``ConnectorAdapter`` that creates a :class:`DingoFSRemoteConnector`.

Loaded by LMCache via the ``external://`` scheme. Example URL::

    external://_/dingofs_connector.connector/?connector_name=DingoFSRemoteConnector

The dingofs-specific connection parameters live in
``LMCacheEngineConfig.extra_config['dingofs_url']`` (or in the URL query if
LMCache forwarded it).
"""

from __future__ import annotations

from lmcache.logging import init_logger
from lmcache.v1.storage_backend.connector import (
    ConnectorAdapter,
    ConnectorContext,
)
from lmcache.v1.storage_backend.connector.base_connector import RemoteConnector

from .connector import DingoFSRemoteConnector

logger = init_logger(__name__)


class DingoFSConnectorAdapter(ConnectorAdapter):
    """Adapter for the ``dingofs://`` scheme.

    The companion :class:`DingoFSRemoteConnector` reads the URL itself, so
    the adapter just forwards.
    """

    def __init__(self) -> None:
        super().__init__("dingofs://")

    def create_connector(self, context: ConnectorContext) -> RemoteConnector:
        logger.info("Creating DingoFS connector for URL: %s", context.url)
        return DingoFSRemoteConnector.from_url(
            url=context.url,
            loop=context.loop,
            local_cpu_backend=context.local_cpu_backend,
            config=context.config,
        )
