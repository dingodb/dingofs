# SPDX-License-Identifier: Apache-2.0
"""DingoFS connector for LMCache.

Exports the URL adapter (so users can register it as a remote_storage_plugin)
and the L2 adapter factory (auto-registered on import).
"""

from .adapter import DingoFSConnectorAdapter
from .remote_connector import DingoFSConnector

# Import for side effect: registers the "dingofs" L2 adapter type and factory.
# L2 adapter API only exists in lmcache >= 0.4; on older lmcache this import
# fails harmlessly and we just don't register the L2 type. The RemoteConnector
# path stays fully usable.
try:
    from . import l2_factory  # noqa: F401
except ImportError:
    pass

__all__ = ["DingoFSConnector", "DingoFSConnectorAdapter"]
