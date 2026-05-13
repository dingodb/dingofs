# Copyright 2026 DingoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Regression tests for writes at chunk and block boundaries.

DingoFS splits files into chunks (default 64MB) and blocks (default 4MB).
A single write() call that crosses a boundary must be split internally and
reassembled correctly on read. These boundary offsets are invisible to
standard POSIX test suites like pjdfstest.

Covers:
  - Single write straddling the 64MB chunk boundary
  - Single write straddling the 4MB block boundary
  - Write starting at exact chunk/block boundary
  - Write ending at exact chunk/block boundary
  - Tiny write at boundary ± 1 byte
"""

import hashlib
import os

import pytest

pytestmark = pytest.mark.standard

MB = 1024 * 1024
CHUNK_SIZE = 64 * MB   # DingoFS default
BLOCK_SIZE = 4 * MB    # DingoFS default


def md5(data):
    return hashlib.md5(data).hexdigest()


# ── Chunk boundary (64MB) ─────────────────────────────────────────────

def test_write_straddling_chunk_boundary(test_dir):
    """Single write that starts before 64MB and ends after 64MB."""
    path = os.path.join(test_dir, "straddle_chunk")
    offset = CHUNK_SIZE - 4096  # 4KB before boundary
    data = os.urandom(8192)     # crosses into next chunk

    with open(path, "wb") as f:
        f.truncate(CHUNK_SIZE + MB)
    with open(path, "r+b") as f:
        f.seek(offset)
        f.write(data)

    with open(path, "rb") as f:
        f.seek(offset)
        actual = f.read(len(data))
    assert actual == data, "data corrupted across chunk boundary"


def test_write_at_exact_chunk_start(test_dir):
    """Write starting at exactly offset 64MB."""
    path = os.path.join(test_dir, "chunk_start")
    data = os.urandom(MB)

    with open(path, "wb") as f:
        f.truncate(CHUNK_SIZE + 2 * MB)
    with open(path, "r+b") as f:
        f.seek(CHUNK_SIZE)
        f.write(data)

    with open(path, "rb") as f:
        f.seek(CHUNK_SIZE)
        assert f.read(MB) == data


def test_write_ending_at_chunk_boundary(test_dir):
    """Write that ends exactly at the 64MB boundary."""
    path = os.path.join(test_dir, "chunk_end")
    size = 4096
    offset = CHUNK_SIZE - size
    data = os.urandom(size)

    with open(path, "wb") as f:
        f.truncate(CHUNK_SIZE + MB)
    with open(path, "r+b") as f:
        f.seek(offset)
        f.write(data)

    with open(path, "rb") as f:
        f.seek(offset)
        assert f.read(size) == data


def test_chunk_boundary_plus_minus_one(test_dir):
    """Write 1 byte at chunk_boundary-1, chunk_boundary, chunk_boundary+1."""
    path = os.path.join(test_dir, "chunk_pm1")

    with open(path, "wb") as f:
        f.truncate(CHUNK_SIZE + MB)

    offsets = [CHUNK_SIZE - 1, CHUNK_SIZE, CHUNK_SIZE + 1]
    values = [b"\xAA", b"\xBB", b"\xCC"]

    with open(path, "r+b") as f:
        for off, val in zip(offsets, values):
            f.seek(off)
            f.write(val)

    with open(path, "rb") as f:
        for off, val in zip(offsets, values):
            f.seek(off)
            assert f.read(1) == val, f"byte at offset {off} corrupted"


def test_large_write_spanning_two_chunks(test_dir):
    """8MB write starting at 60MB — crosses chunk boundary, md5 verified."""
    path = os.path.join(test_dir, "two_chunks")
    offset = 60 * MB
    data = os.urandom(8 * MB)

    with open(path, "wb") as f:
        f.truncate(CHUNK_SIZE + 8 * MB)
    with open(path, "r+b") as f:
        f.seek(offset)
        f.write(data)

    with open(path, "rb") as f:
        f.seek(offset)
        actual = f.read(len(data))
    assert md5(actual) == md5(data), "md5 mismatch across chunk boundary"


# ── Block boundary (4MB) ──────────────────────────────────────────────

def test_write_straddling_block_boundary(test_dir):
    """Single write crossing the 4MB block boundary."""
    path = os.path.join(test_dir, "straddle_block")
    offset = BLOCK_SIZE - 512
    data = os.urandom(1024)

    with open(path, "wb") as f:
        f.truncate(BLOCK_SIZE + MB)
    with open(path, "r+b") as f:
        f.seek(offset)
        f.write(data)

    with open(path, "rb") as f:
        f.seek(offset)
        assert f.read(len(data)) == data


def test_block_boundary_plus_minus_one(test_dir):
    """Write 1 byte at block_boundary-1, block_boundary, block_boundary+1."""
    path = os.path.join(test_dir, "block_pm1")

    with open(path, "wb") as f:
        f.truncate(BLOCK_SIZE + MB)

    offsets = [BLOCK_SIZE - 1, BLOCK_SIZE, BLOCK_SIZE + 1]
    values = [b"\x11", b"\x22", b"\x33"]

    with open(path, "r+b") as f:
        for off, val in zip(offsets, values):
            f.seek(off)
            f.write(val)

    with open(path, "rb") as f:
        for off, val in zip(offsets, values):
            f.seek(off)
            assert f.read(1) == val, f"byte at block offset {off} corrupted"


def test_write_spanning_multiple_blocks(test_dir):
    """Single 10MB write starting at 2MB — spans 3 blocks, md5 verified."""
    path = os.path.join(test_dir, "multi_block")
    offset = 2 * MB
    data = os.urandom(10 * MB)

    with open(path, "wb") as f:
        f.truncate(16 * MB)
    with open(path, "r+b") as f:
        f.seek(offset)
        f.write(data)

    with open(path, "rb") as f:
        f.seek(offset)
        actual = f.read(len(data))
    assert md5(actual) == md5(data), "md5 mismatch across multiple blocks"
