# SPDX-License-Identifier: Apache-2.0
"""LMCache RemoteConnector backed by a dingofs cache cluster.

Hot-path design:
  - Each batched op submits one native RPC fan-in and awaits its asyncio.Future.
    The native eventfd is registered with the asyncio selector via
    `loop.add_reader`, so completions wake the main asyncio thread directly —
    no demux thread, no cross-thread Future bridge, no GIL hop.
  - ExistsLRU short-circuits exists / batched_async_contains so the common
    case (just-put key, immediate prefetch check) skips the network entirely.

This wrapping pattern mirrors LMCache's own ConnectorClientBase used by the
upstream Redis connector — see native_client.py for the eventfd plumbing.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any, List, Optional, Tuple

from lmcache.logging import init_logger
from lmcache.utils import CacheEngineKey
from lmcache.v1.memory_management import MemoryObj
from lmcache.v1.storage_backend.connector.base_connector import RemoteConnector
from lmcache.v1.storage_backend.local_cpu_backend import LocalCPUBackend

from .access_log import access_log
from .config import parse_dingofs_url
from .exists_cache import ExistsLRU
from .key_mapper import cache_engine_key_to_native_str
from .native_client import DingoFSNativeClient


def _fmt_bytes(n: int) -> str:
    """Pretty-print byte count for access log args."""
    if n < 1024:
        return f"{n}B"
    if n < 1024 * 1024:
        return f"{n / 1024:.2f}KiB"
    return f"{n / 1024 / 1024:.2f}MiB"


logger = init_logger(__name__)


class DingoFSConnector(RemoteConnector):
    """RemoteConnector talking to a dingofs cache group via the native bridge."""

    # Hard cap: dingofs cache node's LocalFileSystem registers io_uring fixed
    # buffers of this size at construction (see
    # src/cache/blockcache/local_filesystem.cc:56-59). With FLAGS_fix_buffer=true
    # (default), a single AioWrite/AioRead must fit in one slot, so a > 4 MiB
    # block silently fails / OOMs at the cache node. Fail fast here instead of
    # letting that surface as a confusing remote error.
    #
    # Override precedence (highest → lowest):
    #   1) env var ``DINGOFS_MAX_CHUNK_MIB``
    #   2) yaml ``remote_storage_plugin.dingofs.max_chunk_mib``
    #   3) default below
    _DEFAULT_MAX_CHUNK_MIB = 4
    _ENV_MAX_CHUNK_MIB = "DINGOFS_MAX_CHUNK_MIB"

    def __init__(
        self,
        url: str,
        loop: asyncio.AbstractEventLoop,
        local_cpu_backend: LocalCPUBackend,
        exists_cache_capacity: int = 1_000_000,
        gflag_conf_path: Optional[str] = None,
        yaml_max_chunk_mib: Optional[int] = None,
    ) -> None:
        with access_log("__init__", lambda: f"url={url}") as r:
            super().__init__(local_cpu_backend.config, local_cpu_backend.metadata)

            endpoint = parse_dingofs_url(url)
            max_chunk_bytes = self._resolve_max_chunk_bytes(yaml_max_chunk_mib)
            self._check_chunk_size_within_cap(
                local_cpu_backend.config, max_chunk_bytes
            )

            rdma_pools = self._collect_rdma_pools(local_cpu_backend)

            self._local_cpu_backend = local_cpu_backend
            self._exists_lru = ExistsLRU(capacity=exists_cache_capacity)
            self._client = DingoFSNativeClient(
                mds_addrs=endpoint.mds_addrs,
                cache_group=endpoint.cache_group,
                conf_path=gflag_conf_path,
                rdma_pools=rdma_pools,
                loop=loop,
            )

            total_pool_mib = sum(length for _, length in rdma_pools) / (1024 * 1024)
            logger.info(
                "DingoFSConnector ready: mds=%s cache_group=%s "
                "max_chunk_bytes=%d (%.0f MiB) conf=%s "
                "rdma_pools=%d region(s) (%.1f MiB)",
                endpoint.mds_addrs,
                endpoint.cache_group,
                max_chunk_bytes,
                max_chunk_bytes / (1024 * 1024),
                gflag_conf_path or "-",
                len(rdma_pools),
                total_pool_mib,
            )
            r.result = f"mds={endpoint.mds_addrs} cache_group={endpoint.cache_group}"

    @staticmethod
    def _collect_rdma_pools(local_cpu_backend: Any) -> List[Tuple[int, int]]:
        """Extract ``(addr, length)`` pairs covering the CPU memory arena
        underlying LMCache MemoryObj allocations.

        Both the put-side ``compressed_memory_obj`` and the get-side dst
        buffers we hand to native are slices into this arena, so registering
        it once with the RDMA NIC will cover every byte of dingofs Put/Get
        traffic.

        Returns an empty list when the arena isn't reachable:
        - ``local_cpu_backend`` is a ``SafeLocalCPUBackend`` stub (scheduler
          role), so there's no allocator attribute;
        - the allocator is a paged variant (only ``enable_p2p=true`` triggers
          this), which we don't yet enumerate — a warning is logged so it's
          obvious the RDMA path is off.
        """
        alloc = getattr(local_cpu_backend, "memory_allocator", None)
        if alloc is None:
            return []

        # MixedMemoryAllocator (the default, including PD-disaggregation runs):
        # one contiguous pinned torch tensor.
        buf = getattr(alloc, "buffer", None)
        if buf is not None:
            addr = int(buf.data_ptr())
            length = int(buf.numel() * buf.element_size())
            return [(addr, length)]

        # TODO(rdma): PagedCpuGpuMemoryAllocator (enable_p2p=true) holds a
        # list of per-page tensors. When P2P support is needed, enumerate
        # the pages here and emit one MemoryRegion per page. Until then,
        # skip silently-but-loudly so the operator notices.
        logger.warning(
            "DingoFSConnector: local_cpu_backend allocator %s has no single "
            "'buffer' attribute; RDMA pool not registered (P2P / paged "
            "allocators not yet supported — RDMA path will be disabled).",
            type(alloc).__name__,
        )
        return []

    @classmethod
    def _resolve_max_chunk_bytes(cls, yaml_override_mib: Optional[int]) -> int:
        env_raw = os.environ.get(cls._ENV_MAX_CHUNK_MIB)
        if env_raw:
            try:
                mib = int(env_raw)
            except ValueError as e:
                raise ValueError(
                    f"{cls._ENV_MAX_CHUNK_MIB} must be an integer, "
                    f"got {env_raw!r}"
                ) from e
            if mib <= 0:
                raise ValueError(
                    f"{cls._ENV_MAX_CHUNK_MIB} must be positive, got {mib}"
                )
            return mib * 1024 * 1024
        if yaml_override_mib is not None:
            return yaml_override_mib * 1024 * 1024
        return cls._DEFAULT_MAX_CHUNK_MIB * 1024 * 1024

    def _check_chunk_size_within_cap(self, config, max_chunk_bytes: int) -> None:
        chunk_bytes = self.full_chunk_size_bytes
        if chunk_bytes <= max_chunk_bytes:
            return
        cap_mib = max_chunk_bytes // (1024 * 1024)
        chunk_mib = chunk_bytes / (1024 * 1024)
        token_size = self.single_token_size
        max_tokens = max_chunk_bytes // token_size if token_size else 0
        chunk_size_cfg = getattr(config, "chunk_size", None)
        raise ValueError(
            f"DingoFSConnector: chunk_size={chunk_size_cfg} yields "
            f"{chunk_mib:.2f} MiB per chunk (single_token_size={token_size} B), "
            f"which exceeds the configured cap of {cap_mib} MiB "
            f"(see dingofs cache node LocalFileSystem io_uring fixed-buffer "
            f"slot at src/cache/blockcache/local_filesystem.cc:56-59). "
            f"Either reduce LMCache chunk_size to <= {max_tokens} for this "
            f"model, or — if the cache cluster has been deployed with larger "
            f"buffer slots — raise this client-side cap via "
            f"remote_storage_plugin.dingofs.max_chunk_mib in lmcache.yaml or "
            f"env var {self._ENV_MAX_CHUNK_MIB}=N."
        )

    # ------------------------------------------------------------------
    # exists / batched_async_contains
    # ------------------------------------------------------------------

    async def exists(self, key: CacheEngineKey) -> bool:
        key_str = cache_engine_key_to_native_str(key)
        with access_log("exists", lambda: key_str) as r:
            if self._exists_lru.has(key_str):
                r.result = "lru_hit"
                return True
            per_key = await self._client.batch_exists([key_str])
            found = bool(per_key) and bool(per_key[0])
            r.result = "found" if found else "not_found"
            if found:
                self._exists_lru.add(key_str)
            return found

    def exists_sync(self, key: CacheEngineKey) -> bool:
        key_str = cache_engine_key_to_native_str(key)
        with access_log("exists_sync", lambda: key_str) as r:
            if self._exists_lru.has(key_str):
                r.result = "lru_hit"
                return True
            try:
                found = self._client.exists_sync(key_str)
            except Exception as e:
                logger.warning("exists_sync failed for %s: %s", key_str, e)
                r.result = f"error: {e}"
                return False
            r.result = "found" if found else "not_found"
            if found:
                self._exists_lru.add(key_str)
            return found

    def support_batched_async_contains(self) -> bool:
        return True

    async def batched_async_contains(
        self,
        lookup_id: str,
        keys: List[CacheEngineKey],
        pin: bool = False,
    ) -> int:
        _ = (lookup_id, pin)  # unused by dingofs
        n = len(keys)
        with access_log("batched_async_contains",
                        lambda: f"{n} keys") as r:
            if not keys:
                r.result = "empty"
                return 0
            key_strs = [cache_engine_key_to_native_str(k) for k in keys]
            # Find the first key not in the LRU; everything before it is a hit.
            for i, ks in enumerate(key_strs):
                if not self._exists_lru.has(ks):
                    remaining = key_strs[i:]
                    per_key = await self._client.batch_exists(remaining)
                    if not per_key:
                        r.result = f"prefix={i} (lru) +0 (no resp)"
                        return i
                    for j, found in enumerate(per_key):
                        if not found:
                            r.result = f"prefix={i + j} (lru={i}, remote={j})"
                            return i + j
                        self._exists_lru.add(remaining[j])
                    r.result = f"prefix={n} (all hit; lru={i})"
                    return n
            r.result = f"prefix={n} (all lru hit)"
            return n

    # ------------------------------------------------------------------
    # get / put
    # ------------------------------------------------------------------

    async def get(self, key: CacheEngineKey) -> Optional[MemoryObj]:
        key_str = cache_engine_key_to_native_str(key)
        with access_log("get", lambda: key_str) as r:
            memory_obj = self._allocate_chunk()
            if memory_obj is None:
                r.result = "alloc_failed"
                return None

            handed_off = False
            try:
                _ok, per_key = await self._client.batch_get(
                    [key_str], [memory_obj.byte_array]
                )
                if per_key and per_key[0]:
                    self._exists_lru.add(key_str)
                    handed_off = True
                    r.result = f"ok {_fmt_bytes(len(memory_obj.byte_array))}"
                    return memory_obj
                r.result = "not_found"
                return None  # NotFound — caller treats as cache miss
            finally:
                if not handed_off:
                    memory_obj.ref_count_down()

    async def put(self, key: CacheEngineKey, memory_obj: MemoryObj) -> None:
        # NOTE: we do NOT ref_count_down memory_obj here. The caller
        # (RemoteBackend.submit_put_task) hands us a serialized
        # ``compressed_memory_obj`` whose ref count it never up'd — its
        # lifetime is managed by the serializer. Mirrors RedisConnector.put.
        key_str = cache_engine_key_to_native_str(key)
        size = len(memory_obj.byte_array)
        with access_log("put",
                        lambda: f"{key_str}, {_fmt_bytes(size)}") as r:
            ok, _ = await self._client.batch_set(
                [key_str], [memory_obj.byte_array]
            )
            if ok:
                self._exists_lru.add(key_str)
            r.result = "ok" if ok else "partial_fail"

    # ------------------------------------------------------------------
    # batched_put / batched_get
    # ------------------------------------------------------------------

    def support_batched_put(self) -> bool:
        return True

    async def batched_put(
        self,
        keys: List[CacheEngineKey],
        memory_objs: List[MemoryObj],
    ) -> None:
        # NOTE: we do NOT ref_count_down memory_objs here. See put() above —
        # these are the serializer's compressed_memory_objs whose ref counts
        # the RemoteBackend caller never up'd. Mirrors RedisConnector.batched_put.
        n = len(keys)
        size = (len(memory_objs[0].byte_array) * n) if memory_objs else 0
        with access_log("batched_put",
                        lambda: f"{n} keys, {_fmt_bytes(size)}") as r:
            if not keys:
                r.result = "empty"
                return
            if len(keys) != len(memory_objs):
                r.result = "FAIL length_mismatch"
                raise ValueError("keys and memory_objs length mismatch")
            key_strs = [cache_engine_key_to_native_str(k) for k in keys]
            views = [obj.byte_array for obj in memory_objs]
            ok, _ = await self._client.batch_set(key_strs, views)
            if ok:
                self._exists_lru.add_many(key_strs)
            r.result = "ok" if ok else "partial_fail"

    def support_batched_get(self) -> bool:
        return True

    async def batched_get(
        self,
        keys: List[CacheEngineKey],
    ) -> List[Optional[MemoryObj]]:
        n = len(keys)
        with access_log("batched_get", lambda: f"{n} keys") as r:
            if not keys:
                r.result = "empty"
                return []
            key_strs = [cache_engine_key_to_native_str(k) for k in keys]

            # Single-pass allocate; on any failure release what we've taken
            # so far and abort the whole batch.
            objs: List[MemoryObj] = []
            for _ in keys:
                obj = self._allocate_chunk()
                if obj is None:
                    for o in objs:
                        o.ref_count_down()
                    r.result = "alloc_failed"
                    return [None] * n
                objs.append(obj)

            views = [o.byte_array for o in objs]
            try:
                _ok, per_key = await self._client.batch_get(key_strs, views)
            except Exception:
                for o in objs:
                    o.ref_count_down()
                raise

            per_key_list = list(per_key or [])
            out: List[Optional[MemoryObj]] = [None] * n
            hit_keys: List[str] = []
            for i, obj in enumerate(objs):
                if i < len(per_key_list) and per_key_list[i]:
                    out[i] = obj
                    hit_keys.append(key_strs[i])
                else:
                    obj.ref_count_down()
            if hit_keys:
                self._exists_lru.add_many(hit_keys)
            r.result = f"hits={len(hit_keys)}/{n}"
            return out

    # ------------------------------------------------------------------
    # ping / list / close
    # ------------------------------------------------------------------

    def support_ping(self) -> bool:
        return True

    async def ping(self) -> int:
        # ping_sync just checks the engine's running flag — connectivity
        # was verified at construction (MDS ListMembers). Cheap; no
        # off-thread offload needed.
        with access_log("ping", lambda: "") as r:
            try:
                self._client.ping_sync()
                r.result = "ok"
                return 0
            except Exception as e:
                logger.warning("ping failed: %s", e)
                r.result = f"FAIL {e}"
                return 1

    async def list(self) -> List[str]:
        with access_log("list", lambda: "") as r:
            # dingofs has no enumeration RPC. LMCache uses list() mostly for
            # diagnostics; returning empty is safe and documented.
            r.result = "unsupported (returning [])"
            return []

    async def close(self) -> None:
        with access_log("close", lambda: ""):
            self._client.close()

    # ------------------------------------------------------------------
    # lifecycle / utility hooks (post_init, reshape_partial_chunk)
    # ------------------------------------------------------------------

    def post_init(self) -> None:
        # One-shot setup hook fired by LMCache right after __init__.
        # Logging this captures the bootstrap timeline.
        with access_log("post_init", lambda: ""):
            super().post_init()

    def reshape_partial_chunk(self, memory_obj, bytes_read):
        # Called by LMCache's storage_manager on a get whose payload was
        # shorter than full_chunk_size_bytes. dingofs currently always
        # returns full chunks (no bytes_read plumbing), so this should
        # not fire in practice — but if it ever does, the log line will
        # tell us.
        full = self.full_chunk_size_bytes
        with access_log("reshape_partial_chunk",
                        lambda: f"{bytes_read}/{full} bytes"):
            return super().reshape_partial_chunk(memory_obj, bytes_read)

    # ------------------------------------------------------------------
    # batched_get_non_blocking / remove_sync / batched_contains
    # ------------------------------------------------------------------
    #
    # These three exist on RemoteConnector but we didn't customise them.
    # Wrap-and-delegate so the access log shows whether (and how) LMCache
    # actually calls them in practice — research target, not perf path.

    def support_batched_get_non_blocking(self) -> bool:
        # Base default is True; we keep it.
        return True

    async def batched_get_non_blocking(
        self,
        lookup_id: str,
        keys: List[CacheEngineKey],
    ) -> List[MemoryObj]:
        # Base impl in base_connector.py does asyncio.gather(self.get for k in
        # keys) and trims to the longest consecutive prefix. Each per-key get
        # still emits its own access_log entry; this outer line just confirms
        # the entry point itself was hit and what prefix LMCache got.
        n = len(keys)
        with access_log("batched_get_non_blocking",
                        lambda: f"{n} keys lookup_id={lookup_id}") as r:
            result = await super().batched_get_non_blocking(lookup_id, keys)
            r.result = f"prefix={len(result)}/{n}"
            return result

    def remove_sync(self, key: CacheEngineKey) -> bool:
        # Base raises NotImplementedError. Log the call so we know whether
        # LMCache ever invokes it (eviction path), then propagate the same
        # behaviour. Returning False would silently lie about success.
        key_str = cache_engine_key_to_native_str(key)
        with access_log("remove_sync", lambda: key_str) as r:
            r.result = "FAIL NotImplementedError"
            raise NotImplementedError("dingofs connector has no remove path yet")

    def support_batched_contains(self) -> bool:
        # Base default is False; we keep it.
        return False

    def batched_contains(self, keys: List[CacheEngineKey]) -> int:
        # support_batched_contains() returns False above — LMCache shouldn't
        # call this. Wrap anyway to catch the case if it ever does.
        n = len(keys)
        with access_log("batched_contains", lambda: f"{n} keys") as r:
            r.result = "FAIL NotImplementedError"
            raise NotImplementedError(
                "dingofs connector does not support sync batched_contains"
            )

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    def _allocate_chunk(self) -> Optional[MemoryObj]:
        # Full-chunk-sized buffer; partial reads write a prefix and the rest
        # stays uninitialized (LMCache reshapes by bytes_read at the
        # storage_manager layer).
        return self._local_cpu_backend.allocate(
            self.meta_shapes[0],
            self.meta_dtypes[0],
            self.meta_fmt,
        )
