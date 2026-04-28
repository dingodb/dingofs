# SPDX-License-Identifier: Apache-2.0
"""Arena-backed LocalCPUBackend shim for RDMA benchmarking.

DingoFSConnector activates its zero-copy RDMA path only when the backend
exposes a single contiguous pinned arena (``memory_allocator.buffer``), which
``_collect_rdma_pools`` registers with the NIC; every allocated chunk is then a
slice of that arena. ``FakeLocalCPUBackend`` (independent bytearrays) can't do
this, so this shim wraps the *real* LMCache ``MixedMemoryAllocator`` — the same
allocator a production ``LocalCPUBackend`` uses — exposing exactly the surface
DingoFSConnector touches: ``config`` / ``metadata`` / ``memory_allocator`` /
``allocate``.

Arena size is taken from ``DINGOFS_ARENA_MIB`` (default 4096 MiB).
"""

from __future__ import annotations

import math
import os
from typing import Optional

import torch
from lmcache.v1.memory_management import MixedMemoryAllocator, MemoryFormat

from _fake_lmcache import FakeConfig, FakeMetadata


class ArenaLocalCPUBackend:
    """LocalCPUBackend surface backed by one contiguous pinned arena."""

    def __init__(
        self,
        config: Optional[FakeConfig] = None,
        metadata: Optional[FakeMetadata] = None,
        arena_mib: Optional[int] = None,
    ):
        self.config = config or FakeConfig()
        self.metadata = metadata or FakeMetadata()
        mib = arena_mib or int(os.environ.get("DINGOFS_ARENA_MIB", "4096"))
        # The real allocator pins one contiguous tensor; .buffer is what
        # DingoFSConnector._collect_rdma_pools registers with the RDMA NIC.
        self.memory_allocator = MixedMemoryAllocator(size=mib * 1024 * 1024)

    def allocate(self, shape, dtype, fmt):
        # The connector passes meta_fmt (often None for our pure-bytes path);
        # the real allocator needs a concrete MemoryFormat. Only the byte count
        # matters here, so KV_2LTD is fine.
        del fmt
        return self.memory_allocator.allocate([shape], [dtype],
                                              MemoryFormat.KV_2LTD)

    def get_full_chunk_size_bytes(self) -> int:
        total = 0
        for shape, dtype in zip(self.metadata.get_shapes(),
                                self.metadata.get_dtypes()):
            total += math.prod(shape) * torch.empty((), dtype=dtype).element_size()
        return total
