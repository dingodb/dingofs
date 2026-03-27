# SPDX-License-Identifier: Apache-2.0

# Standard
from typing import List, Optional
import asyncio

# First Party
from lmcache.logging import init_logger
from lmcache.utils import CacheEngineKey
from lmcache.v1.memory_management import MemoryObj
from lmcache.v1.storage_backend.connector.base_connector import RemoteConnector
from lmcache.v1.storage_backend.local_cpu_backend import LocalCPUBackend

# Local
from .native_engine import SYNC_ALWAYS, NativeIOEngine

logger = init_logger(__name__)


class DingoFSConnector(RemoteConnector):
    """High-performance DingoFS connector using a native C++ I/O engine.

    Stores KV cache chunks as files on a DingoFS mount point. Uses a
    multi-threaded C++ backend with eventfd-based async completion for
    maximum throughput on DingoFS's large-block, high-concurrency I/O path.

    Args:
        base_path: DingoFS mount point / directory for cache files.
        loop: Asyncio event loop.
        local_cpu_backend: Memory allocator interface.
        num_workers: Number of I/O worker threads.
        use_odirect: Whether to use O_DIRECT for file I/O.
        sync_mode: Controls fdatasync behavior (SYNC_NONE or SYNC_ALWAYS).
    """

    def __init__(
        self,
        base_path: str,
        loop: asyncio.AbstractEventLoop,
        local_cpu_backend: LocalCPUBackend,
        num_workers: int = 8,
        use_odirect: bool = False,
        sync_mode: int = SYNC_ALWAYS,
    ) -> None:
        super().__init__(local_cpu_backend.config, local_cpu_backend.metadata)

        self.base_path = base_path
        self.loop = loop
        self.local_cpu_backend = local_cpu_backend

        self._engine = NativeIOEngine(
            base_path=base_path,
            num_workers=num_workers,
            use_odirect=use_odirect,
            sync_mode=sync_mode,
            loop=loop,
        )

    @staticmethod
    def _as_memoryview(buf) -> memoryview:
        return buf if isinstance(buf, memoryview) else memoryview(buf)

    # ------------------------------------------------------------------
    # EXISTS
    # ------------------------------------------------------------------

    async def exists(self, key: CacheEngineKey) -> bool:
        """Check if key exists in DingoFS."""
        return await self._engine.exists(key.to_string())

    def exists_sync(self, key: CacheEngineKey) -> bool:
        """Synchronous key existence check."""
        return self._engine.exists_sync(key.to_string())

    # ------------------------------------------------------------------
    # GET
    # ------------------------------------------------------------------

    async def get(self, key: CacheEngineKey) -> Optional[MemoryObj]:
        """Retrieve data from DingoFS."""
        key_str = key.to_string()
        memory_obj = self.local_cpu_backend.allocate(
            self.meta_shapes, self.meta_dtypes, self.meta_fmt
        )
        if memory_obj is None:
            logger.warning("Failed to allocate memory during DingoFS get")
            return None

        await self._engine.get(key_str, self._as_memoryview(memory_obj.byte_array))
        return memory_obj

    # ------------------------------------------------------------------
    # PUT
    # ------------------------------------------------------------------

    async def put(self, key: CacheEngineKey, memory_obj: MemoryObj) -> None:
        """Store data to DingoFS."""
        key_str = key.to_string()
        await self._engine.set(key_str, self._as_memoryview(memory_obj.byte_array))

    # ------------------------------------------------------------------
    # Batched PUT
    # ------------------------------------------------------------------

    def support_batched_put(self) -> bool:
        return True

    async def batched_put(
        self, keys: List[CacheEngineKey], memory_objs: List[MemoryObj]
    ) -> None:
        """Batch store data to DingoFS."""
        key_strs = [key.to_string() for key in keys]
        bufs = [self._as_memoryview(m.byte_array) for m in memory_objs]
        await self._engine.batch_set(key_strs, bufs)

    # ------------------------------------------------------------------
    # Batched GET
    # ------------------------------------------------------------------

    def support_batched_get(self) -> bool:
        return True

    async def batched_get(
        self, keys: List[CacheEngineKey]
    ) -> List[Optional[MemoryObj]]:
        """Batch retrieve data from DingoFS."""
        key_strs = [key.to_string() for key in keys]
        memory_objs = self.local_cpu_backend.batched_allocate(
            [self.meta_shapes] * len(keys),
            [self.meta_dtypes] * len(keys),
            [self.meta_fmt] * len(keys),
        )
        bufs = [self._as_memoryview(m.byte_array) for m in memory_objs]
        await self._engine.batch_get(key_strs, bufs)
        return memory_objs

    # ------------------------------------------------------------------
    # Batched Contains
    # ------------------------------------------------------------------

    def support_batched_contains(self) -> bool:
        return True

    def batched_contains(self, keys: List[CacheEngineKey]) -> int:
        """Synchronous batched contains - checks consecutive prefix existence."""
        key_strs = [key.to_string() for key in keys]
        results = self._engine.batch_exists_sync(key_strs)
        count = 0
        for result in results:
            if not result:
                return count
            count += 1
        return count

    # ------------------------------------------------------------------
    # Batched Async Contains
    # ------------------------------------------------------------------

    def support_batched_async_contains(self) -> bool:
        return True

    async def batched_async_contains(
        self,
        lookup_id: str,
        keys: List[CacheEngineKey],
        pin: bool = False,
    ) -> int:
        """Check how many consecutive keys exist."""
        key_strs = [key.to_string() for key in keys]
        results = await self._engine.batch_exists(key_strs)
        count = 0
        for result in results:
            if not result:
                return count
            count += 1
        return count

    # ------------------------------------------------------------------
    # Batched Non-Blocking GET
    # ------------------------------------------------------------------

    def support_batched_get_non_blocking(self) -> bool:
        return True

    async def batched_get_non_blocking(
        self,
        lookup_id: str,
        keys: List[CacheEngineKey],
    ) -> List[MemoryObj]:
        """Non-blocking batched get (prefetch)."""
        key_strs = [key.to_string() for key in keys]
        memory_objs = self.local_cpu_backend.batched_allocate(
            [self.meta_shapes] * len(keys),
            [self.meta_dtypes] * len(keys),
            [self.meta_fmt] * len(keys),
        )
        bufs = [self._as_memoryview(m.byte_array) for m in memory_objs]
        await self._engine.batch_get(key_strs, bufs)
        return memory_objs

    # ------------------------------------------------------------------
    # List / Close
    # ------------------------------------------------------------------

    async def list(self) -> List[str]:
        """List all keys (not implemented for native engine)."""
        return []

    async def close(self) -> None:
        """Shut down the connector."""
        self._engine.close()
        logger.info("Closed the DingoFS connector")
