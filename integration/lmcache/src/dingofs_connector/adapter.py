# SPDX-License-Identifier: Apache-2.0

# First Party
from lmcache.logging import init_logger
from lmcache.v1.storage_backend.connector import ConnectorAdapter, ConnectorContext
from lmcache.v1.storage_backend.connector import parse_remote_url
from lmcache.v1.storage_backend.connector.base_connector import RemoteConnector

# Local
from .native_engine import SYNC_ALWAYS, SYNC_NONE

logger = init_logger(__name__)

_SYNC_MODE_MAP = {
    "none": SYNC_NONE,
    "always": SYNC_ALWAYS,
}


class DingoFSConnectorAdapter(ConnectorAdapter):
    """Adapter for the DingoFS connector.

    Handles URLs of the form: dingofs://host:port/mount/path
    - host:port is ignored (kept for URL compatibility)
    - /mount/path is the DingoFS mount point used as the storage directory

    Extra config options (via extra_config):
        dingofs_num_workers (int): Number of I/O worker threads. Default: 8.
        dingofs_use_odirect (bool): Use O_DIRECT for file I/O. Default: False.
        dingofs_sync_mode (str): Controls fdatasync behavior.
            "always" (default): fdatasync after every write.
            "none": No fdatasync (rely on filesystem guarantees).
    """

    def __init__(self) -> None:
        super().__init__("dingofs://")

    def create_connector(self, context: ConnectorContext) -> RemoteConnector:
        """Create a DingoFSConnector from the given context.

        Args:
            context: Connector context containing URL, config, etc.

        Returns:
            A DingoFSConnector instance.
        """
        # Local import to avoid circular dependencies and allow lazy loading
        from .connector import DingoFSConnector

        logger.info(f"Creating DingoFS connector for URL: {context.url}")

        parsed_url = parse_remote_url(context.url)
        base_path = parsed_url.path

        if not base_path:
            raise ValueError(
                f"DingoFS URL must include a path: {context.url}. "
                "Example: dingofs://host:0/mnt/dingofs/cache"
            )

        # Read extra config
        config = context.config
        extra = config.extra_config if config and config.extra_config else {}
        num_workers = int(extra.get("dingofs_num_workers", 8))
        use_odirect = bool(extra.get("dingofs_use_odirect", False))

        sync_mode_str = str(extra.get("dingofs_sync_mode", "always")).lower()
        if sync_mode_str not in _SYNC_MODE_MAP:
            raise ValueError(
                f"Invalid dingofs_sync_mode: {sync_mode_str!r}. "
                f"Must be one of: {list(_SYNC_MODE_MAP.keys())}"
            )
        sync_mode = _SYNC_MODE_MAP[sync_mode_str]

        return DingoFSConnector(
            base_path=base_path,
            loop=context.loop,
            local_cpu_backend=context.local_cpu_backend,
            num_workers=num_workers,
            use_odirect=use_odirect,
            sync_mode=sync_mode,
        )
