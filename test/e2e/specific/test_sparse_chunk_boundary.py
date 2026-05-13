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

"""Regression tests for sparse file chunk boundary consistency.

Related work: Slice-relative block index refactor (commits ccdc7eaae..924688abe)
changed how block indices are calculated relative to slice positions instead of
chunk-absolute positions. Sparse files with holes spanning chunk boundaries
exercise the slice pos/len/off arithmetic at its most complex.

These tests verify that:
- Holes across chunk boundaries read as zeros
- Data patches at chunk boundaries survive write→read round-trip
- Mixing holes and data within the same chunk is correct

DingoFS default chunk_size = 64MB, block_size = 4MB.

Verification:
  Tests should PASS on both local and MDS mode.
  Any slice-relative arithmetic regression will cause md5 mismatch.
"""

import hashlib
import os

import pytest

pytestmark = pytest.mark.standard

MB = 1024 * 1024
CHUNK_SIZE = 64 * MB  # DingoFS default


def md5(data):
    return hashlib.md5(data).hexdigest()


def test_sparse_hole_across_chunk_boundary(test_dir):
    """Truncate to 128MB (2 chunks), write only at offset 0 and 65MB.
    Region [1MB, 65MB) should be all zeros (hole spanning chunk boundary)."""
    path = os.path.join(test_dir, "sparse_boundary")
    total = 128 * MB

    patch1 = os.urandom(1 * MB)  # [0, 1MB)
    patch2 = os.urandom(1 * MB)  # [65MB, 66MB) — in chunk 1

    # Build expected content
    expected = bytearray(total)
    expected[0:1 * MB] = patch1
    expected[65 * MB:66 * MB] = patch2

    # Write sparse file
    with open(path, "wb") as f:
        f.truncate(total)
    with open(path, "r+b") as f:
        f.seek(0)
        f.write(patch1)
        f.seek(65 * MB)
        f.write(patch2)

    # Read back and verify
    with open(path, "rb") as f:
        actual = f.read()

    assert len(actual) == total
    assert md5(actual) == md5(bytes(expected))


def test_sparse_write_at_exact_chunk_boundary(test_dir):
    """Write exactly at the 64MB chunk boundary offset."""
    path = os.path.join(test_dir, "at_boundary")
    patch = os.urandom(4096)

    with open(path, "wb") as f:
        f.truncate(CHUNK_SIZE + 4096)
    with open(path, "r+b") as f:
        f.seek(CHUNK_SIZE)
        f.write(patch)

    with open(path, "rb") as f:
        f.seek(CHUNK_SIZE)
        actual = f.read(4096)

    assert actual == patch

    # Verify hole before boundary is zeros
    with open(path, "rb") as f:
        hole = f.read(4096)
    assert hole == b"\x00" * 4096


def test_sparse_straddling_chunk_boundary(test_dir):
    """Write a single buffer that straddles the 64MB chunk boundary."""
    path = os.path.join(test_dir, "straddle")
    offset = CHUNK_SIZE - 2048  # starts 2KB before boundary
    data = os.urandom(4096)  # crosses into next chunk

    with open(path, "wb") as f:
        f.truncate(CHUNK_SIZE + MB)
    with open(path, "r+b") as f:
        f.seek(offset)
        f.write(data)

    with open(path, "rb") as f:
        f.seek(offset)
        actual = f.read(4096)

    assert actual == data, "data straddling chunk boundary corrupted"


def test_sparse_multiple_chunks_md5(test_dir):
    """Sparse file spanning 3 chunks with data patches in each chunk."""
    path = os.path.join(test_dir, "multi_chunk")
    total = 3 * CHUNK_SIZE  # 192MB

    patches = [
        (10 * MB, os.urandom(MB)),          # chunk 0
        (CHUNK_SIZE + 5 * MB, os.urandom(MB)),  # chunk 1
        (2 * CHUNK_SIZE + 1 * MB, os.urandom(MB)),  # chunk 2
    ]

    expected = bytearray(total)
    for off, data in patches:
        expected[off:off + len(data)] = data

    with open(path, "wb") as f:
        f.truncate(total)
    for off, data in patches:
        with open(path, "r+b") as f:
            f.seek(off)
            f.write(data)

    with open(path, "rb") as f:
        actual = f.read()

    assert len(actual) == total
    assert md5(actual) == md5(bytes(expected)), "multi-chunk sparse md5 mismatch"
