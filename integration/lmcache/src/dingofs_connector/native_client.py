# SPDX-License-Identifier: Apache-2.0
"""Asyncio wrapper around _dingofs_native.RemoteCache.

Inspired by LMCache's upstream ConnectorClientBase
(lmcache/v1/storage_backend/native_clients/connector_client_base.py), which
uses asyncio's selector (`loop.add_reader`) to watch the native eventfd
directly — no demux thread, no concurrent.futures.Future, no
asyncio.wrap_future. Completions are dispatched in the asyncio loop's own
context.

We diverge from upstream in one place: per-key bitmaps are passed through
to callers. Upstream collapses `ok=false` into RuntimeError, which loses
the difference between "cache miss" (a normal NotFound outcome that should
become `None`) and "transport error" (which should propagate). dingofs
reports NotFound at per-key granularity, so we need that resolution.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .access_log import access_log

__all__ = ["DingoFSNativeClient"]

logger = logging.getLogger(__name__)


def _total_size(bufs: Sequence[memoryview]) -> int:
    return sum(mv.nbytes for mv in bufs)


class DingoFSNativeClient:
    """Awaitable interface over _dingofs_native.RemoteCache."""

    def __init__(
        self,
        mds_addrs: str,
        cache_group: str,
        conf_path: Optional[str] = None,
        rdma_pools: Optional[Sequence[Tuple[int, int]]] = None,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> None:
        with access_log("native.__init__",
                        lambda: f"mds={mds_addrs} cache_group={cache_group} "
                                f"conf={conf_path or '-'} "
                                f"rdma_pools={len(rdma_pools or [])}") as r:
            # Lazy import so unit tests can mock the native module out.
            from . import _dingofs_native  # type: ignore[attr-defined]

            self._native = _dingofs_native.RemoteCache(
                mds_addrs=mds_addrs,
                cache_group=cache_group,
                conf_file=conf_path or "",
                rdma_pools=list(rdma_pools or []),
            )
            self._loop = loop or asyncio.get_running_loop()
            self._fd = int(self._native.event_fd())
            # fid → (future, keepalive_refs). keepalive_refs pins the Python
            # buffer objects until the native side stops touching them — this
            # is the only thing keeping them alive (the C++ side no longer
            # holds its own ref).
            self._pending: Dict[int, Tuple[asyncio.Future, Tuple[Any, ...]]] = {}
            self._closed = False
            self._loop.add_reader(self._fd, self._on_ready)
            r.result = f"fd={self._fd}"

    # ------------------------------------------------------------------
    # eventfd readiness handler (runs in main asyncio loop, no GIL hop)
    # ------------------------------------------------------------------

    def _on_ready(self) -> None:
        if self._closed:
            return
        # access_log here measures the native→asyncio handoff cost: kernel
        # wakes our add_reader, we drain the C++ completion queue and resolve
        # the in-flight asyncio.Futures. The interesting numbers are how many
        # completions arrive per wake (kernel batching efficiency) and the
        # drain duration itself (Python dispatch overhead).
        with access_log("native.drain_completions", lambda: "") as r:
            try:
                items = self._native.drain_completions()
            except Exception as exc:  # pragma: no cover
                logger.exception("drain_completions failed")
                self._fail_all(RuntimeError(f"drain_completions failed: {exc}"))
                r.result = f"FAIL {exc!s}"
                return

            resolved = 0
            orphans = 0
            errors = 0
            for future_id, ok, error, per_key in items:
                fid = int(future_id)
                entry = self._pending.pop(fid, None)
                if entry is None:
                    logger.warning("orphan completion future_id=%d", fid)
                    orphans += 1
                    continue
                fut, _keepalive = entry
                if fut.done():
                    continue
                # Transport/server errors → exception. Cache misses (NotFound)
                # come through with empty error string and ok=False; callers
                # inspect per_key to decide what to do.
                if error:
                    fut.set_exception(RuntimeError(error))
                    errors += 1
                else:
                    fut.set_result((bool(ok), per_key))
                resolved += 1
            parts = [f"resolved={resolved}"]
            if orphans:
                parts.append(f"orphans={orphans}")
            if errors:
                parts.append(f"errors={errors}")
            r.result = " ".join(parts)

    def _fail_all(self, exc: Exception) -> None:
        for fid, (fut, _) in list(self._pending.items()):
            if not fut.done():
                fut.set_exception(exc)
        self._pending.clear()

    def _submit(self, native_call, keepalive: Tuple[Any, ...], *args) -> asyncio.Future:
        """Submit a native batched op and register its asyncio.Future.

        Native submit returns a future_id; we register the pending entry
        before the call so a completion landing on a fast path can't lose
        the entry. The native call itself is GIL-released inside pybind.
        """
        fut: asyncio.Future = self._loop.create_future()
        fid = int(native_call(*args))
        # Race-safe: _on_ready can't fire until we yield to the event loop.
        self._pending[fid] = (fut, keepalive)
        return fut

    # ------------------------------------------------------------------
    # public API
    # ------------------------------------------------------------------

    async def batch_set(
        self, keys: Sequence[str], bufs: Sequence[memoryview]
    ) -> Tuple[bool, Optional[List[bool]]]:
        n = len(keys)
        total = _total_size(bufs)
        with access_log("native.batch_set",
                        lambda: f"{n} keys, {total} bytes") as r:
            fut = self._submit(
                self._native.submit_batch_set,
                (tuple(keys), tuple(bufs)),
                keys, bufs,
            )
            ok, per_key = await fut
            r.result = "ok" if ok else "partial_fail"
            return ok, per_key

    async def batch_get(
        self, keys: Sequence[str], bufs: Sequence[memoryview]
    ) -> Tuple[bool, Optional[List[bool]]]:
        n = len(keys)
        total = _total_size(bufs)
        with access_log("native.batch_get",
                        lambda: f"{n} keys, {total} bytes") as r:
            fut = self._submit(
                self._native.submit_batch_get,
                (tuple(keys), tuple(bufs)),
                keys, bufs,
            )
            ok, per_key = await fut
            hits = sum(1 for b in (per_key or []) if b)
            r.result = f"hits={hits}/{n}"
            return ok, per_key

    async def batch_exists(
        self, keys: Sequence[str]
    ) -> Optional[List[bool]]:
        n = len(keys)
        with access_log("native.batch_exists",
                        lambda: f"{n} keys") as r:
            fut = self._submit(
                self._native.submit_batch_exists,
                (tuple(keys),),
                keys,
            )
            _ok, per_key = await fut
            hits = sum(1 for b in (per_key or []) if b)
            r.result = f"hits={hits}/{n}"
            return per_key

    def exists_sync(self, key: str) -> bool:
        with access_log("native.exists_sync", lambda: key) as r:
            found = bool(self._native.exists_sync(key))
            r.result = "found" if found else "not_found"
            return found

    def ping_sync(self) -> None:
        """Cheap liveness check — does not issue an RPC.

        See NativeEngine::Ping: connectivity is established at construction
        time (MDS ListMembers must succeed), so this just verifies the
        engine is still running.
        """
        with access_log("native.ping_sync", lambda: ""):
            self._native.ping()

    def close(self) -> None:
        if self._closed:
            return
        with access_log("native.close", lambda: ""):
            self._closed = True
            try:
                self._loop.remove_reader(self._fd)
            except Exception:  # pragma: no cover
                pass
            self._fail_all(RuntimeError("DingoFSNativeClient closed"))
            try:
                self._native.close()
            except Exception as exc:  # pragma: no cover
                logger.warning("native.close failed: %s", exc)
