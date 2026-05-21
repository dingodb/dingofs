# SPDX-License-Identifier: Apache-2.0
"""DingoFS L2 adapter.

Inherits the upstream NativeConnectorL2Adapter for the heavy lifting (1
native eventfd demuxed into 3 user-facing eventfds, per-key result bitmaps,
race-safe pending-op table). We only override the public entry points to
attach access-log tracing — the underlying dispatch logic stays upstream.

Note about timings: submit_* calls are fire-and-forget — they enqueue work
on the native side and return a task_id immediately, so the durations
captured here are the *submit* costs (microseconds), NOT end-to-end RPC
latency. Use the `native.batch_*` lines emitted from inside the native
client to see actual server-side completion times.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List

from lmcache.v1.distributed.l2_adapters.native_connector_l2_adapter import (
    NativeConnectorL2Adapter,
)

from .access_log import access_log

if TYPE_CHECKING:
    from lmcache.native_storage_ops import Bitmap
    from lmcache.v1.distributed.api import ObjectKey
    from lmcache.v1.distributed.l2_adapters.base import L2TaskId
    from lmcache.v1.memory_management import MemoryObj

__all__ = ["DingoFSL2Adapter"]


class DingoFSL2Adapter(NativeConnectorL2Adapter):
    """Marker subclass with access-log instrumentation."""

    def __init__(self, native_client) -> None:
        with access_log("l2.__init__",
                        lambda: f"native={type(native_client).__name__}"):
            super().__init__(native_client)

    # ---- bootstrap: event fd getters + listener registration ----
    # Each fires once at L2 init; after that LMCache polls the fds directly
    # via epoll, so these wrappers do NOT sit on the hot path.

    def get_store_event_fd(self) -> int:
        with access_log("l2.get_store_event_fd", lambda: "") as r:
            fd = super().get_store_event_fd()
            r.result = f"fd={fd}"
            return fd

    def get_lookup_and_lock_event_fd(self) -> int:
        with access_log("l2.get_lookup_and_lock_event_fd", lambda: "") as r:
            fd = super().get_lookup_and_lock_event_fd()
            r.result = f"fd={fd}"
            return fd

    def get_load_event_fd(self) -> int:
        with access_log("l2.get_load_event_fd", lambda: "") as r:
            fd = super().get_load_event_fd()
            r.result = f"fd={fd}"
            return fd

    def register_listener(self, listener) -> None:
        with access_log("l2.register_listener",
                        lambda: f"{type(listener).__name__}"):
            super().register_listener(listener)

    # ---- store ----

    def submit_store_task(
        self,
        keys: "List[ObjectKey]",
        objects: "List[MemoryObj]",
    ) -> "L2TaskId":
        n = len(keys)
        with access_log("l2.submit_store_task", lambda: f"{n} keys") as r:
            task_id = super().submit_store_task(keys, objects)
            r.result = f"task_id={task_id}"
            return task_id

    def pop_completed_store_tasks(self) -> "Dict[L2TaskId, bool]":
        with access_log("l2.pop_completed_store_tasks", lambda: "") as r:
            results = super().pop_completed_store_tasks()
            r.result = f"completed={len(results)}"
            return results

    # ---- lookup_and_lock ----

    def submit_lookup_and_lock_task(
        self, keys: "List[ObjectKey]"
    ) -> "L2TaskId":
        n = len(keys)
        with access_log("l2.submit_lookup_and_lock_task",
                        lambda: f"{n} keys") as r:
            task_id = super().submit_lookup_and_lock_task(keys)
            r.result = f"task_id={task_id}"
            return task_id

    def query_lookup_and_lock_result(
        self, task_id: "L2TaskId"
    ) -> "Bitmap | None":
        with access_log("l2.query_lookup_and_lock_result",
                        lambda: f"task_id={task_id}") as r:
            bitmap = super().query_lookup_and_lock_result(task_id)
            r.result = "pending" if bitmap is None else "ready"
            return bitmap

    def submit_unlock(self, keys: "List[ObjectKey]") -> None:
        with access_log("l2.submit_unlock", lambda: f"{len(keys)} keys"):
            super().submit_unlock(keys)

    # ---- load ----

    def submit_load_task(
        self,
        keys: "List[ObjectKey]",
        objects: "List[MemoryObj]",
    ) -> "L2TaskId":
        n = len(keys)
        with access_log("l2.submit_load_task", lambda: f"{n} keys") as r:
            task_id = super().submit_load_task(keys, objects)
            r.result = f"task_id={task_id}"
            return task_id

    def query_load_result(self, task_id: "L2TaskId") -> "Bitmap | None":
        with access_log("l2.query_load_result",
                        lambda: f"task_id={task_id}") as r:
            bitmap = super().query_load_result(task_id)
            r.result = "pending" if bitmap is None else "ready"
            return bitmap

    # ---- eviction / status ----

    @property
    def supports_global_eviction(self) -> bool:
        # @property on the base class; storage manager probes it at startup
        # to decide whether to attach a global eviction policy. True iff
        # max_capacity_bytes > 0 was passed at construction.
        with access_log("l2.supports_global_eviction", lambda: "") as r:
            result = super().supports_global_eviction
            r.result = str(result)
            return result

    def delete(self, keys: "List[ObjectKey]") -> None:
        # Upstream NativeConnectorL2Adapter.delete is a SYNCHRONOUS,
        # blocking call that submits a batch_delete RPC and waits up to
        # 30 s for the demux thread to signal completion — definitely a
        # hot-path entry worth tracing.
        n = len(keys)
        with access_log("l2.delete", lambda: f"{n} keys"):
            super().delete(keys)

    def pop_completed_store_task_bytes(self) -> "Dict[L2TaskId, int]":
        # LMCache's throughput subscriber polls this between
        # pop_completed_store_tasks() calls to attribute real bytes-written
        # per task. Cheap; logging it confirms the cadence.
        with access_log("l2.pop_completed_store_task_bytes",
                        lambda: "") as r:
            results = super().pop_completed_store_task_bytes()
            r.result = f"tasks={len(results)}"
            return results

    def report_status(self) -> "Dict[str, Any]":
        with access_log("l2.report_status", lambda: "") as r:
            status = super().report_status()
            healthy = status.get("is_healthy")
            r.result = f"healthy={healthy}"
            return status

    def get_usage(self):
        # Base class default returns AdapterUsage computed from byte
        # counters maintained by _notify_keys_stored/_deleted. LMCache's
        # eviction controller polls this; logging the cadence is the
        # whole point of this exercise.
        with access_log("l2.get_usage", lambda: "") as r:
            usage = super().get_usage()
            r.result = (
                f"used={usage.total_bytes_used} "
                f"cap={usage.total_capacity_bytes} "
                f"frac={usage.usage_fraction:.4f}"
            )
            return usage

    # ---- lifecycle ----

    def close(self) -> None:
        with access_log("l2.close", lambda: ""):
            super().close()
