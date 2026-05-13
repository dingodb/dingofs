# Copyright 2024 DingoFS Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.

"""Regression tests for read data correctness.

Bug: fuse_reply_data() with fuse_bufvec + FUSE_BUF_SPLICE_MOVE caused
data loss or corruption on reads. The bufvec-to-splice path had issues
with certain iovec layouts.

Fix: commit d4460570f — Switch from fuse_reply_data(bufvec, SPLICE_MOVE)
to fuse_reply_iov(iovec) for direct iovec-based replies. Also added
file_offset and fake (hole) fields to BlockReadReq for better tracking.

Verification:
  buggy binary (pre d4460570f):  test_read_in_small_chunks / test_pread_various_offsets should FAIL
  fixed binary (post d4460570f): all tests PASS
  works on: local mode + MDS mode (FUSE reply path shared)
"""
import os, hashlib, pytest

pytestmark = pytest.mark.standard

def md5(data): return hashlib.md5(data).hexdigest()

def test_read_exact_size(test_dir):
    """Write N bytes, read exactly N bytes, md5 must match."""
    for size in [1, 511, 4096, 4097, 65536, 65537, 1024*1024]:
        path = os.path.join(test_dir, f"exact_{size}")
        data = os.urandom(size)
        with open(path, "wb") as f:
            f.write(data)
        with open(path, "rb") as f:
            actual = f.read()
        assert md5(actual) == md5(data), f"md5 mismatch for size {size}"

def test_read_in_small_chunks(test_dir):
    """Write 1MB, read back in tiny chunks (1 byte, 7 bytes, 4K, etc.)."""
    path = os.path.join(test_dir, "small_chunks")
    data = os.urandom(1024 * 1024)
    with open(path, "wb") as f:
        f.write(data)

    chunk_sizes = [1, 7, 13, 4096, 4097, 65536]
    for cs in chunk_sizes:
        with open(path, "rb") as f:
            parts = []
            while True:
                part = f.read(cs)
                if not part:
                    break
                parts.append(part)
            actual = b"".join(parts)
        assert md5(actual) == md5(data), f"md5 mismatch for chunk_size={cs}"

def test_pread_various_offsets(test_dir):
    """Write 4MB, pread from various offsets and lengths."""
    path = os.path.join(test_dir, "pread")
    size = 4 * 1024 * 1024
    data = os.urandom(size)
    with open(path, "wb") as f:
        f.write(data)

    test_cases = [
        (0, 4096),
        (0, size),
        (4096, 4096),
        (4095, 4097),       # misaligned
        (1024*1024, 1024*1024),
        (size - 100, 100),  # tail
        (size - 1, 1),      # last byte
    ]
    fd = os.open(path, os.O_RDONLY)
    for offset, length in test_cases:
        actual = os.pread(fd, length, offset)
        expected = data[offset:offset+length]
        assert md5(actual) == md5(expected), f"pread mismatch at off={offset} len={length}"
    os.close(fd)

def test_read_after_append(test_dir):
    """Append data then read full file — total md5 must be correct."""
    path = os.path.join(test_dir, "append")
    parts = [os.urandom(100 * 1024) for _ in range(10)]  # 10 x 100KB

    with open(path, "wb") as f:
        for p in parts:
            f.write(p)

    expected = b"".join(parts)
    with open(path, "rb") as f:
        actual = f.read()
    assert md5(actual) == md5(expected)

def test_read_empty_file(test_dir):
    """Read from empty file returns empty bytes."""
    path = os.path.join(test_dir, "empty")
    open(path, "w").close()
    with open(path, "rb") as f:
        assert f.read() == b""
