# SPDX-License-Identifier: Apache-2.0
"""LMCache RemoteConnector adapter for dingofs.

Wire-up in LMCache config (plugin mode, LMCache >= 0.4.5, recommended):

    remote_storage_plugins: ["dingofs"]
    extra_config:
      remote_storage_plugin.dingofs.module_path: dingofs_connector.adapter
      remote_storage_plugin.dingofs.class_name:  DingoFSConnectorAdapter
      remote_storage_plugin.dingofs.url:         dingofs://mds1:6700/grp

      # Optional: path to a gflags conf file. Same format as dingofs-mds /
      # dingo-client / cache-bench accept (one ``--flag=value`` per line).
      remote_storage_plugin.dingofs.conf:        /etc/dingofs/lmcache-client.conf

      # Optional: connector-side single-block cap (MiB). Also overridable
      # via env DINGOFS_MAX_CHUNK_MIB (env wins over yaml).
      remote_storage_plugin.dingofs.max_chunk_mib: 16

Legacy fallback: top-level ``remote_url: "dingofs://..."`` still works for
older LMCache versions; connector-side knobs and conf path are not available
on that path.

URLs MUST be ``dingofs://<mds_addrs>/<cache_group>`` exactly — no query
string. Everything else moves into extra_config above.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from lmcache.logging import init_logger
from lmcache.v1.storage_backend.connector import (
    ConnectorAdapter,
    ConnectorContext,
)
from lmcache.v1.storage_backend.connector.base_connector import RemoteConnector

from .access_log import access_log
from .remote_connector import DingoFSConnector

logger = init_logger(__name__)

_PLUGIN_TYPE = "dingofs"
_PLUGIN_URL_PREFIX = "plugin://"
_LEGACY_URL_PREFIX = "dingofs://"


def _extract_plugin_type(plugin_name: str) -> str:
    # Mirror of lmcache.v1.storage_backend.connector.extract_plugin_type
    # (only present in lmcache >= 0.4.5). Inlined for back-compat.
    return plugin_name.split(".", 1)[0]


@dataclass(frozen=True)
class _PluginOverrides:
    target_url: str
    conf_path: Optional[str]
    max_chunk_mib: Optional[int]


class DingoFSConnectorAdapter(ConnectorAdapter):
    """URL adapter for dingofs.

    Matches both ``dingofs://...`` (legacy ``remote_url`` path) and
    ``plugin://dingofs[.instance]`` (LMCache plugin path).
    """

    def __init__(self) -> None:
        with access_log("adapter.__init__", lambda: ""):
            super().__init__(_LEGACY_URL_PREFIX)

    def can_parse(self, url: str) -> bool:
        if url.startswith(_LEGACY_URL_PREFIX):
            return True
        if url.startswith(_PLUGIN_URL_PREFIX):
            pname = url[len(_PLUGIN_URL_PREFIX):]
            return _extract_plugin_type(pname) == _PLUGIN_TYPE
        return False

    def create_connector(self, context: ConnectorContext) -> RemoteConnector:
        with access_log("adapter.create_connector",
                        lambda: f"url={context.url}"):
            overrides = self._resolve_overrides(context)
            logger.info(
                "Creating DingoFSConnector: context_url=%s target_url=%s "
                "conf=%s max_chunk_mib=%s",
                context.url, overrides.target_url,
                overrides.conf_path or "-",
                overrides.max_chunk_mib if overrides.max_chunk_mib else "-",
            )
            return DingoFSConnector(
                url=overrides.target_url,
                loop=context.loop,
                local_cpu_backend=context.local_cpu_backend,
                gflag_conf_path=overrides.conf_path,
                yaml_max_chunk_mib=overrides.max_chunk_mib,
            )

    @staticmethod
    def _resolve_overrides(context: ConnectorContext) -> _PluginOverrides:
        """Resolve the dingofs:// URL and per-plugin overrides.

        - Legacy ``dingofs://...`` URL: passed straight through; no extra_config
          lookups (the legacy path doesn't know which plugin name to key under).
        - Plugin mode (``plugin://dingofs[.instance]``): read everything from
          ``extra_config[remote_storage_plugin.<plugin>.{url,conf,max_chunk_mib}]``.
        """
        url = context.url

        if url.startswith(_LEGACY_URL_PREFIX):
            return _PluginOverrides(target_url=url, conf_path=None,
                                    max_chunk_mib=None)

        plugin_name = getattr(context, "plugin_name", None) or url[
            len(_PLUGIN_URL_PREFIX):
        ]
        prefix = f"remote_storage_plugin.{plugin_name}"

        extra_config = (
            getattr(context.config, "extra_config", None)
            if context.config is not None else None
        ) or {}

        target_url = extra_config.get(f"{prefix}.url")
        if not target_url:
            raise ValueError(
                f"Plugin mode requires extra_config['{prefix}.url'] to be a "
                "dingofs:// URL (e.g. 'dingofs://mds1:6700/cache_group')"
            )
        if not isinstance(target_url, str) or not target_url.startswith(
            _LEGACY_URL_PREFIX
        ):
            raise ValueError(
                f"extra_config['{prefix}.url']={target_url!r} must be a "
                f"string starting with {_LEGACY_URL_PREFIX!r}"
            )

        conf_path = extra_config.get(f"{prefix}.conf")
        if conf_path is not None and not isinstance(conf_path, str):
            raise ValueError(
                f"extra_config['{prefix}.conf']={conf_path!r} must be a "
                "filesystem path string"
            )

        raw = extra_config.get(f"{prefix}.max_chunk_mib")
        max_chunk_mib: Optional[int] = None
        if raw is not None:
            try:
                max_chunk_mib = int(raw)
            except (TypeError, ValueError) as e:
                raise ValueError(
                    f"extra_config['{prefix}.max_chunk_mib']={raw!r} must be "
                    "a positive integer"
                ) from e
            if max_chunk_mib <= 0:
                raise ValueError(
                    f"extra_config['{prefix}.max_chunk_mib']={max_chunk_mib} "
                    "must be positive"
                )

        return _PluginOverrides(
            target_url=target_url,
            conf_path=conf_path or None,
            max_chunk_mib=max_chunk_mib,
        )
