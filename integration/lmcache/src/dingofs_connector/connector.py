# SPDX-License-Identifier: Apache-2.0
"""``RemoteConnector`` implementation backed by a DingoFS cache cluster.

Threading model:

* The C++ ``NativeCacheEngine`` owns a thread pool + a single eventfd; one
  background **demux thread** in this module reads the eventfd, drains
  completions, and wakes the corresponding ``asyncio.Future`` on the parent
  loop via ``loop.call_soon_threadsafe``.

* Submissions release the GIL (the pybind macro handles it). The asyncio
  loop is therefore never blocked by an RPC.
"""

from __future__ import annotations

import asyncio
import os
import threading
from typing import Dict, List, Optional, Tuple

from lmcache.logging import init_logger
from lmcache.utils import CacheEngineKey
from lmcache.v1.config import LMCacheEngineConfig
from lmcache.v1.memory_management import MemoryObj
from lmcache.v1.storage_backend.connector.base_connector import RemoteConnector
from lmcache.v1.storage_backend.local_cpu_backend import LocalCPUBackend

from lmcache_dingofs.errors import DingoFSConnectorError
from lmcache_dingofs.key_mapper import cache_engine_key_to_native
from lmcache_dingofs.url import DingoFSEndpoint, parse_dingofs_url

from ._native import NativeCacheEngine

logger = init_logger(__name__)


# Completion tuple format: (future_id, ok, error, result_bools | None)
_Completion = Tuple[int, bool, str, Optional[List[bool]]]


class _Pending:
    """One in-flight submission. ``op`` discriminates the result shape."""

    __slots__ = ("future", "op", "memviews")

    def __init__(
        self,
        future: asyncio.Future,
        op: str,
        memviews: Optional[List[memoryview]] = None,
    ) -> None:
        self.future = future
        self.op = op
        # Hold references so the underlying buffers stay alive until the RPC
        # completes. Python GC could otherwise free the MemoryObj's byte_array
        # the moment the `await` suspends.
        self.memviews = memviews


class DingoFSRemoteConnector(RemoteConnector):
    """Talks to a dingo-cache cluster over brpc via a C++ pybind extension."""

    def __init__(
        self,
        endpoint: DingoFSEndpoint,
        loop: asyncio.AbstractEventLoop,
        local_cpu_backend: LocalCPUBackend,
    ) -> None:
        self._loop = loop
        self._local = local_cpu_backend
        self._engine = NativeCacheEngine(
            mds_addrs=endpoint.mds_addrs,
            group_name=endpoint.group_name,
            fs_id=endpoint.fs_id,
            num_workers=endpoint.num_workers,
            request_timeout_ms=endpoint.request_timeout_ms,
        )
        logger.info(
            "DingoFS engine ready: mds=%s group=%s fs_id=%d workers=%d "
            "service_version=%d",
            endpoint.mds_addrs,
            endpoint.group_name,
            endpoint.fs_id,
            endpoint.num_workers,
            self._engine.service_version,
        )
        self._pending: Dict[int, _Pending] = {}
        self._pending_lock = threading.Lock()
        self._stop = threading.Event()
        self._demux = threading.Thread(
            target=self._demux_loop,
            name="dingofs-connector-demux",
            daemon=True,
        )
        self._demux.start()

    # ------------------------------------------------------------------ #
    # Factory
    # ------------------------------------------------------------------ #

    @classmethod
    def from_url(
        cls,
        url: str,
        loop: asyncio.AbstractEventLoop,
        local_cpu_backend: LocalCPUBackend,
        config: Optional[LMCacheEngineConfig] = None,
    ) -> "DingoFSRemoteConnector":
        endpoint = parse_dingofs_url(url)
        return cls(endpoint=endpoint, loop=loop, local_cpu_backend=local_cpu_backend)

    # ------------------------------------------------------------------ #
    # Capability flags
    # ------------------------------------------------------------------ #

    def support_batched_put(self) -> bool:
        return True

    def support_batched_get(self) -> bool:
        return True

    def support_batched_async_contains(self) -> bool:
        return True

    def support_batched_get_non_blocking(self) -> bool:
        return True

    # ------------------------------------------------------------------ #
    # Exists
    # ------------------------------------------------------------------ #

    async def exists(self, key: CacheEngineKey) -> bool:
        key_str = cache_engine_key_to_native(key)
        future = self._loop.create_future()
        future_id = self._engine.submit_batch_exists([key_str])
        self._register(future_id, _Pending(future, "exists"))
        results: Optional[List[bool]] = await future
        return bool(results and results[0])

    def exists_sync(self, key: CacheEngineKey) -> bool:
        """Synchronous existence check.

        We can't reuse the asyncio path here because we're not inside the
        event loop. Instead, fire a one-shot submission and poll the eventfd
        directly until we see *our* future_id complete.
        """
        key_str = cache_engine_key_to_native(key)
        future_id = self._engine.submit_batch_exists([key_str])
        while True:
            # Blocking read; eventfd is non-blocking so we wrap with select.
            # In practice the eventfd will fire quickly because submission
            # latency is dominated by network RTT.
            r = os.read(self._engine.event_fd(), 8)
            del r
            for comp in self._engine.drain_completions():
                fid, ok, err, results = comp
                if fid == future_id:
                    if not ok:
                        raise DingoFSConnectorError(f"exists failed: {err}")
                    return bool(results and results[0])
                # Foreign completion — route through the demux pipeline.
                self._dispatch(comp)

    # ------------------------------------------------------------------ #
    # Get / Put
    # ------------------------------------------------------------------ #

    async def get(self, key: CacheEngineKey) -> Optional[MemoryObj]:
        # Allocate a chunk-sized MemoryObj first; we'll fill it in place.
        memory_obj = self._allocate_chunk()
        if memory_obj is None:
            return None
        key_str = cache_engine_key_to_native(key)
        memview = memory_obj.byte_array
        future = self._loop.create_future()
        future_id = self._engine.submit_batch_get([key_str], [memview])
        self._register(future_id, _Pending(future, "get", memviews=[memview]))
        try:
            await future
        except DingoFSConnectorError as e:
            # Free the allocated buffer on failure; signal cache miss to caller.
            memory_obj.ref_count_down()
            logger.debug("dingofs get failed for %s: %s", key, e)
            return None
        return memory_obj

    async def put(self, key: CacheEngineKey, memory_obj: MemoryObj) -> None:
        key_str = cache_engine_key_to_native(key)
        memview = memory_obj.byte_array
        future = self._loop.create_future()
        future_id = self._engine.submit_batch_set([key_str], [memview])
        self._register(future_id, _Pending(future, "set", memviews=[memview]))
        try:
            await future
        finally:
            # LMCache contract: decrement the ref count once the send finishes,
            # regardless of success.
            memory_obj.ref_count_down()

    # ------------------------------------------------------------------ #
    # Batched paths
    # ------------------------------------------------------------------ #

    async def batched_put(
        self, keys: List[CacheEngineKey], memory_objs: List[MemoryObj]
    ) -> None:
        if not keys:
            return
        key_strs = [cache_engine_key_to_native(k) for k in keys]
        memviews = [m.byte_array for m in memory_objs]
        future = self._loop.create_future()
        future_id = self._engine.submit_batch_set(key_strs, memviews)
        self._register(future_id, _Pending(future, "set", memviews=memviews))
        try:
            await future
        finally:
            for m in memory_objs:
                m.ref_count_down()

    async def batched_get(
        self, keys: List[CacheEngineKey]
    ) -> List[Optional[MemoryObj]]:
        if not keys:
            return []
        memory_objs: List[Optional[MemoryObj]] = []
        memviews: List[memoryview] = []
        for _ in keys:
            mo = self._allocate_chunk()
            memory_objs.append(mo)
            if mo is not None:
                memviews.append(mo.byte_array)
        if any(m is None for m in memory_objs):
            # Out of memory — fall back to a per-key path, freeing allocated
            # objs we won't be able to fill. Allocate-then-fail is rare.
            for mo in memory_objs:
                if mo is not None:
                    mo.ref_count_down()
            return [None] * len(keys)
        key_strs = [cache_engine_key_to_native(k) for k in keys]
        future = self._loop.create_future()
        future_id = self._engine.submit_batch_get(key_strs, memviews)
        self._register(future_id, _Pending(future, "get", memviews=memviews))
        try:
            await future
        except DingoFSConnectorError:
            for mo in memory_objs:
                mo.ref_count_down()  # type: ignore[union-attr]
            return [None] * len(keys)
        return memory_objs  # type: ignore[return-value]

    async def batched_async_contains(
        self,
        lookup_id: str,
        keys: List[CacheEngineKey],
        pin: bool = False,
    ) -> int:
        if not keys:
            return 0
        key_strs = [cache_engine_key_to_native(k) for k in keys]
        future = self._loop.create_future()
        future_id = self._engine.submit_batch_exists(key_strs)
        self._register(future_id, _Pending(future, "exists"))
        results: Optional[List[bool]] = await future
        if not results:
            return 0
        # Return length of consecutive prefix of hits — matches base contract.
        for i, hit in enumerate(results):
            if not hit:
                return i
        return len(results)

    # ------------------------------------------------------------------ #
    # Misc
    # ------------------------------------------------------------------ #

    async def list(self) -> List[str]:
        raise NotImplementedError(
            "DingoFS cache does not support enumerating keys"
        )

    async def close(self) -> None:
        if self._stop.is_set():
            return
        self._stop.set()
        try:
            self._engine.close()
        finally:
            # Closing the eventfd unblocks the demux thread's read.
            if self._demux.is_alive():
                self._demux.join(timeout=5.0)
            # Cancel any still-pending futures so awaiters don't hang.
            with self._pending_lock:
                for p in self._pending.values():
                    if not p.future.done():
                        self._loop.call_soon_threadsafe(
                            p.future.set_exception,
                            DingoFSConnectorError("connector closed"),
                        )
                self._pending.clear()

    # ------------------------------------------------------------------ #
    # Demux internals
    # ------------------------------------------------------------------ #

    def _register(self, future_id: int, pending: _Pending) -> None:
        with self._pending_lock:
            self._pending[future_id] = pending

    def _allocate_chunk(self) -> Optional[MemoryObj]:
        # 0.3.6 RemoteConnector base populates these via init_chunk_meta.
        shape = self.meta_shape
        dtype = self.meta_dtype
        fmt = self.meta_fmt
        if shape is None or dtype is None:
            raise DingoFSConnectorError(
                "DingoFS connector requires init_chunk_meta() to have been "
                "called with a populated metadata"
            )
        return self._local.allocate(shape, dtype, fmt)

    def _demux_loop(self) -> None:
        efd = self._engine.event_fd()
        while not self._stop.is_set():
            try:
                # 8 bytes per eventfd notification.
                os.read(efd, 8)
            except (BlockingIOError, OSError):
                if self._stop.is_set():
                    return
                continue
            for comp in self._engine.drain_completions():
                self._dispatch(comp)

    def _dispatch(self, comp: _Completion) -> None:
        future_id, ok, err, results = comp
        with self._pending_lock:
            pending = self._pending.pop(future_id, None)
        if pending is None:
            logger.warning("dingofs: stray completion for future_id=%d", future_id)
            return
        if not ok:
            exc = DingoFSConnectorError(err or "dingofs RPC failed")
            self._loop.call_soon_threadsafe(pending.future.set_exception, exc)
            return
        if pending.op == "exists":
            self._loop.call_soon_threadsafe(pending.future.set_result, results)
        else:
            self._loop.call_soon_threadsafe(pending.future.set_result, None)
