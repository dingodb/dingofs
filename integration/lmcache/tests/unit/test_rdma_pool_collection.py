# SPDX-License-Identifier: Apache-2.0
"""Tests for ``DingoFSConnector._collect_rdma_pools``.

Pure Python: we hand-roll a tiny fake of LMCache's LocalCPUBackend with the
single attribute path the collector actually walks
(``backend.memory_allocator.buffer.{data_ptr, numel, element_size}``).
No native module, no lmcache runtime needed.
"""

from types import SimpleNamespace

from dingofs_connector.remote_connector import DingoFSConnector


collect = DingoFSConnector._collect_rdma_pools


# A torch.Tensor stand-in: just the three methods the collector calls.
class _FakeTensor:
    def __init__(self, addr: int, n_elements: int, element_size: int):
        self._addr = addr
        self._numel = n_elements
        self._element_size = element_size

    def data_ptr(self):
        return self._addr

    def numel(self):
        return self._numel

    def element_size(self):
        return self._element_size


def _backend_with_buffer(addr: int, length: int):
    """LocalCPUBackend → MixedMemoryAllocator → single contiguous torch tensor."""
    return SimpleNamespace(
        memory_allocator=SimpleNamespace(
            buffer=_FakeTensor(addr, length, element_size=1),
        ),
    )


# --- happy path: MixedMemoryAllocator emits one region ---------------------


def test_mixed_allocator_emits_single_region():
    addr, length = 0xCAFEB000, 50 * 1024 * 1024 * 1024  # 50 GiB arena
    out = collect(_backend_with_buffer(addr, length))
    assert out == [(addr, length)]


def test_length_is_numel_times_element_size():
    # uint8 tensor: numel==bytes; but the collector must still multiply
    # explicitly so non-uint8 tensors (hypothetical) come out correct.
    fake = SimpleNamespace(
        memory_allocator=SimpleNamespace(
            buffer=_FakeTensor(addr=0x1000, n_elements=1024, element_size=4),
        ),
    )
    [(addr, length)] = collect(fake)
    assert addr == 0x1000
    assert length == 4096


# --- degenerate inputs return empty list, no exception ---------------------


def test_safe_stub_returns_empty():
    # SafeLocalCPUBackend (scheduler role) has no memory_allocator attribute.
    backend = SimpleNamespace()
    assert collect(backend) == []


def test_allocator_present_but_no_buffer_returns_empty(caplog):
    # PagedCpuGpuMemoryAllocator: has a .memory_allocator but no .buffer.
    # Expect graceful skip + warning, not exception.
    backend = SimpleNamespace(
        memory_allocator=SimpleNamespace(  # no `buffer` here
            __class__=type("PagedCpuGpuMemoryAllocator", (), {}),
        ),
    )

    with caplog.at_level("WARNING"):
        out = collect(backend)

    assert out == []
    assert any(
        "RDMA pool not registered" in rec.getMessage()
        for rec in caplog.records
    )


def test_none_backend_returns_empty():
    # Defensive: even if somehow the connector is built with None.
    assert collect(None) == []
