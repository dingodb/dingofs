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

"""Regression tests for rename-overwrite inode cleanup.

Bug:
  When rename(A, B) overwrites an existing file B, the MDS must atomically
    1. Replace B's dentry with A's inode
    2. Decrement B's nlink (delete B's inode if nlink reaches 0)
    3. Update quota (subtract B's length)
    4. Clean up B's chunk data if B is fully unlinked
  Pre-fix variants of these steps leaked stale content, stale length, or
  chunk data, surfacing as wrong-content / wrong-size / md5-mismatch reads
  on B after the rename.

Verification:
  pre fix:  reading B after rename(A, B) returns old B content, wrong
            size, or md5 mismatch.
  post fix: B has exactly A's content + size; repeated rename-overwrite
            does not accumulate stale state.
  works on: Local + MDS
"""

import hashlib
import os

import pytest

pytestmark = pytest.mark.standard


def md5(data):
    return hashlib.md5(data).hexdigest()


def test_rename_overwrite_content(test_dir):
    """rename(A, B) where B exists: B should have A's content."""
    a = os.path.join(test_dir, "src")
    b = os.path.join(test_dir, "dst")

    data_a = os.urandom(1024)
    data_b = os.urandom(2048)

    with open(a, "wb") as f:
        f.write(data_a)
    with open(b, "wb") as f:
        f.write(data_b)

    os.rename(a, b)

    assert not os.path.exists(a)
    with open(b, "rb") as f:
        result = f.read()
    assert result == data_a, "rename-overwrite: dst has wrong content"


def test_rename_overwrite_size(test_dir):
    """rename small file over large file: size should shrink."""
    large = os.path.join(test_dir, "large")
    small = os.path.join(test_dir, "small")

    with open(large, "wb") as f:
        f.write(os.urandom(1024 * 1024))  # 1MB
    with open(small, "wb") as f:
        f.write(b"tiny")  # 4 bytes

    os.rename(small, large)

    assert os.path.getsize(large) == 4, "rename-overwrite: size not updated"
    with open(large, "rb") as f:
        assert f.read() == b"tiny"


def test_rename_overwrite_large_over_small(test_dir):
    """rename large file over small file: size should grow."""
    small = os.path.join(test_dir, "sm")
    large = os.path.join(test_dir, "lg")

    with open(small, "wb") as f:
        f.write(b"x")
    big_data = os.urandom(512 * 1024)
    with open(large, "wb") as f:
        f.write(big_data)

    os.rename(large, small)

    assert os.path.getsize(small) == len(big_data)
    with open(small, "rb") as f:
        assert md5(f.read()) == md5(big_data)


def test_rename_overwrite_repeated(test_dir):
    """Repeated rename-overwrite must not leak stale data."""
    target = os.path.join(test_dir, "target")

    # Initialize target with large data
    with open(target, "wb") as f:
        f.write(os.urandom(10000))

    for i in range(20):
        src = os.path.join(test_dir, f"src_{i}")
        data = os.urandom(i + 1)  # increasingly small: 1, 2, ..., 20 bytes
        with open(src, "wb") as f:
            f.write(data)
        os.rename(src, target)

        with open(target, "rb") as f:
            result = f.read()
        assert result == data, (
            f"iteration {i}: expected {len(data)} bytes, got {len(result)}"
        )


def test_rename_overwrite_md5_large(test_dir):
    """Rename-overwrite with 10MB files, md5 verification."""
    a = os.path.join(test_dir, "big_a")
    b = os.path.join(test_dir, "big_b")

    data_a = os.urandom(10 * 1024 * 1024)
    data_b = os.urandom(10 * 1024 * 1024)

    with open(a, "wb") as f:
        f.write(data_a)
    with open(b, "wb") as f:
        f.write(data_b)

    os.rename(a, b)

    with open(b, "rb") as f:
        result = f.read()
    assert md5(result) == md5(data_a), "rename-overwrite 10MB: md5 mismatch"
