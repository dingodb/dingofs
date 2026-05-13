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

"""Regression tests for heavily fragmented write patterns.

DingoFS stores writes as slices within chunks. Many small pwrite() calls
at random offsets create many slices, stressing:
  - Slice merging / overlap resolution in ProcessReadRequest
  - Chunk compaction trigger logic
  - Memory usage for slice metadata
  - Read-path performance with many slices per chunk

fsx does generic random writes but doesn't verify DingoFS-specific
slice behavior or md5 correctness after hundreds of tiny writes.

Covers:
  - Many 1-byte pwrite at random offsets within 1 chunk
  - Many small pwrite across chunk boundaries
  - Overwrite same region many times, final value correct
  - Sequential 1-byte writes (worst-case slice count)
"""

import hashlib
import os
import random

import pytest

pytestmark = pytest.mark.standard

MB = 1024 * 1024


def md5(data):
    return hashlib.md5(data).hexdigest()


def test_random_1byte_pwrite_single_chunk(test_dir):
    """200 random 1-byte pwrite within a 1MB region, verify full md5."""
    path = os.path.join(test_dir, "frag_1b")
    size = MB

    expected = bytearray(size)

    with open(path, "wb") as f:
        f.truncate(size)

    rng = random.Random(42)  # deterministic for reproducibility
    fd = os.open(path, os.O_WRONLY)
    for _ in range(200):
        off = rng.randint(0, size - 1)
        val = bytes([rng.randint(0, 255)])
        os.pwrite(fd, val, off)
        expected[off] = val[0]
    os.close(fd)

    with open(path, "rb") as f:
        actual = f.read()
    assert md5(actual) == md5(bytes(expected)), "random 1-byte pwrite md5 mismatch"


def test_random_small_pwrite_cross_chunk(test_dir):
    """100 random writes (1-4KB) across a 128MB file (2 chunks)."""
    path = os.path.join(test_dir, "frag_cross")
    size = 128 * MB

    expected = bytearray(size)

    with open(path, "wb") as f:
        f.truncate(size)

    rng = random.Random(123)
    fd = os.open(path, os.O_WRONLY)
    for _ in range(100):
        write_size = rng.randint(1, 4096)
        max_off = size - write_size
        off = rng.randint(0, max_off)
        data = bytes([rng.randint(0, 255)] * write_size)
        os.pwrite(fd, data, off)
        expected[off:off + write_size] = data
    os.close(fd)

    with open(path, "rb") as f:
        actual = f.read()
    assert md5(actual) == md5(bytes(expected)), "cross-chunk fragmented write md5 mismatch"


def test_overwrite_same_region_many_times(test_dir):
    """Overwrite the same 4KB region 100 times, only last value survives."""
    path = os.path.join(test_dir, "frag_overwrite")
    offset = 1000
    size = 4096

    with open(path, "wb") as f:
        f.truncate(offset + size + 1000)

    fd = os.open(path, os.O_WRONLY)
    last_data = None
    for i in range(100):
        last_data = os.urandom(size)
        os.pwrite(fd, last_data, offset)
    os.close(fd)

    with open(path, "rb") as f:
        f.seek(offset)
        actual = f.read(size)
    assert actual == last_data, "repeated overwrite: final value wrong"


def test_sequential_1byte_writes(test_dir):
    """Write 10000 bytes one at a time (worst-case slice fragmentation)."""
    path = os.path.join(test_dir, "frag_seq")
    count = 10000
    expected = bytearray(count)

    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    for i in range(count):
        val = bytes([i & 0xFF])
        os.write(fd, val)
        expected[i] = i & 0xFF
    os.close(fd)

    with open(path, "rb") as f:
        actual = f.read()
    assert len(actual) == count
    assert md5(actual) == md5(bytes(expected)), "sequential 1-byte writes md5 mismatch"
