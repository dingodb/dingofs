# SPDX-License-Identifier: Apache-2.0
"""Minimal LMCache shims sufficient to drive DingoFSConnector end-to-end.

Real LMCacheEngine wires together a heavy stack (config + metadata +
allocator + workers + ...) just to feed a RemoteConnector. For a smoke
client all DingoFSConnector touches on its backend is:

  - local_cpu_backend.config         (.extra_config, .use_layerwise)
  - local_cpu_backend.metadata       (.get_shapes(), .get_dtypes(), .use_mla,
                                      .chunk_size, .get_num_groups())
  - local_cpu_backend.allocate(shape, dtype, fmt) -> MemoryObj
                                     (with .byte_array, .ref_count_down())

So this module exposes exactly that surface, no more. Keeps the smoke client
self-contained — no LMCacheEngine bring-up needed.

Buffers go through a size-keyed pool (mirrors real LMCache's pinned-memory
allocator): the first round pays the malloc cost, subsequent rounds reuse
the same chunks via ref_count_down. Without this, each batched_get round
would re-malloc N x chunk_size bytearrays (~30 ms / 20 x 4 MiB on stock
glibc) and dominate the wall time.
"""

from __future__ import annotations

import math
import threading
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import torch


@dataclass
class FakeConfig:
    """Subset of LMCacheEngineConfig consulted by RemoteConnector.__init__."""
    extra_config: Optional[dict] = None
    use_layerwise: bool = False


@dataclass
class FakeMetadata:
    """Subset of LMCacheMetadata consulted by RemoteConnector.__init__.

    Defaults model one chunk of fp16 KV: shape [2, 1, 256, 64], 65536 bytes.
    full_chunk_size_bytes (computed inside RemoteConnector) must be divisible
    by chunk_size — the defaults satisfy 65536 % 256 == 0.

    For benchmark scenarios, use `FakeMetadata.for_size(N)` to produce a
    metadata whose full chunk is exactly N bytes (so connector-allocated GET
    buffers match the producer's payload size).
    """
    use_mla: bool = False
    chunk_size: int = 256
    _shapes: List[torch.Size] = field(
        default_factory=lambda: [torch.Size([2, 1, 256, 64])]
    )
    _dtypes: List[torch.dtype] = field(
        default_factory=lambda: [torch.float16]
    )

    def get_shapes(self) -> List[torch.Size]:
        return self._shapes

    def get_dtypes(self) -> List[torch.dtype]:
        return self._dtypes

    def get_num_groups(self) -> int:
        return 1

    @classmethod
    def for_size(cls, size_bytes: int, *,
                 dtype: torch.dtype = torch.float16,
                 chunk_size: int = 256) -> "FakeMetadata":
        """Construct metadata whose full_chunk_size_bytes == size_bytes.

        Required: size_bytes divisible by both `chunk_size` and the dtype
        element width (LMCache asserts this in RemoteConnector.__init__).
        """
        elem_bytes = torch.empty((), dtype=dtype).element_size()
        if size_bytes % elem_bytes != 0:
            raise ValueError(
                f"size_bytes={size_bytes} not divisible by elem_size={elem_bytes}"
            )
        if size_bytes % chunk_size != 0:
            raise ValueError(
                f"size_bytes={size_bytes} not divisible by chunk_size={chunk_size}"
            )
        elements = size_bytes // elem_bytes
        # LMCache expects 4D KV shape; we use a flat [1, 1, 1, N] layout since
        # only the total byte count matters for our pure-bytes connector path.
        return cls(
            chunk_size=chunk_size,
            _shapes=[torch.Size([1, 1, 1, elements])],
            _dtypes=[dtype],
        )


class _BufferPool:
    """Size-keyed bytearray pool. Lazy growth; thread-safe.

    Real LMCache LocalCPUBackend pre-allocates a giant slab and slices it.
    For a smoke client we don't need that complexity — just bucket free
    buffers by size and hand them back on demand. After the first round
    enough buffers are pooled to satisfy steady-state allocate / release.
    """

    def __init__(self) -> None:
        self._free: Dict[int, List[bytearray]] = {}
        self._lock = threading.Lock()

    def acquire(self, size: int) -> bytearray:
        with self._lock:
            bucket = self._free.get(size)
            if bucket:
                return bucket.pop()
        return bytearray(size)

    def release(self, buf: bytearray) -> None:
        with self._lock:
            self._free.setdefault(len(buf), []).append(buf)

    def stats(self) -> Dict[int, int]:
        with self._lock:
            return {k: len(v) for k, v in self._free.items()}


class FakeMemoryObj:
    """Pool-backed MemoryObj. ref_count_down returns the buffer to the pool."""

    __slots__ = ("_buf", "_pool", "_released")

    def __init__(self, buf: bytearray, pool: "_BufferPool"):
        self._buf = buf
        self._pool = pool
        self._released = False

    @property
    def byte_array(self) -> memoryview:
        return memoryview(self._buf)

    def ref_count_down(self) -> None:
        # Idempotent — DingoFSConnector calls this once on success path and
        # again on some error paths; safe to invoke either way.
        if self._released:
            return
        self._released = True
        self._pool.release(self._buf)


class FakeLocalCPUBackend:
    """Just enough of LocalCPUBackend for DingoFSConnector to call allocate().

    Backed by a buffer pool so successive batched_get / batched_put rounds
    reuse the same bytearrays. Without the pool, every round re-mallocs
    N x chunk_size bytes; for 20 x 4 MiB that's ~30 ms/round overhead.

    A second cache memoizes the byte-size for each (shape, dtype) pair —
    `torch.empty((), dtype=...).element_size()` is ~10 µs per call (allocates
    a throwaway tensor); doing that 20× per batched_get blew ~200 µs into the
    Python wrapper overhead. With the cache it's a dict lookup.
    """

    def __init__(
        self,
        config: Optional[FakeConfig] = None,
        metadata: Optional[FakeMetadata] = None,
    ):
        self.config = config or FakeConfig()
        self.metadata = metadata or FakeMetadata()
        self._pool = _BufferPool()
        self._size_cache: Dict[Tuple[Tuple[int, ...], torch.dtype], int] = {}

    def allocate(self, shape, dtype, fmt) -> FakeMemoryObj:
        # fmt is unused: LMCache uses it for compressed layouts, dingofs is
        # opaque about the payload.
        del fmt
        cache_key = (tuple(shape), dtype)
        size = self._size_cache.get(cache_key)
        if size is None:
            size = math.prod(shape) * torch.empty((), dtype=dtype).element_size()
            self._size_cache[cache_key] = size
        return FakeMemoryObj(self._pool.acquire(size), self._pool)

    def get_full_chunk_size_bytes(self) -> int:
        shapes = self.metadata.get_shapes()
        dtypes = self.metadata.get_dtypes()
        total = 0
        for shape, dtype in zip(shapes, dtypes):
            total += math.prod(shape) * torch.empty((), dtype=dtype).element_size()
        return total

    def pool_stats(self) -> Dict[int, int]:
        return self._pool.stats()
