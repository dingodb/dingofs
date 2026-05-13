# Copyright 2024 DingoFS Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.

"""Regression tests for multi-fd read-after-write consistency.

Bug: When multiple file descriptors are open for the same inode, writing
through fd-A and reading through fd-B returns stale data. The flush and
read-cache invalidation only operated on the writing fd's handle, not
across all handles for the same inode.

Fix: commit 645895eb8 — FlushByIno() and InvalidateByIno() that iterate
all open handles for the given inode, ensuring cross-fd consistency.

Verification:
  buggy binary (pre 645895eb8):  test_write_fd_read_fd_consistency should FAIL
  fixed binary (post 645895eb8): all tests PASS
  works on: local mode + MDS mode (VFS layer shared)
"""
import os, hashlib, pytest

pytestmark = pytest.mark.standard

def md5(data): return hashlib.md5(data).hexdigest()

def test_write_fd_read_fd_consistency(test_dir):
    """Write through fd-W, read through fd-R on the same file.
    fd-R must see fd-W's data without closing fd-W first."""
    path = os.path.join(test_dir, "multi_fd")
    data = os.urandom(1024 * 1024)  # 1MB

    fd_w = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    os.write(fd_w, data)
    os.fsync(fd_w)  # ensure data is flushed

    fd_r = os.open(path, os.O_RDONLY)
    result = b""
    while True:
        chunk = os.read(fd_r, 65536)
        if not chunk:
            break
        result += chunk

    os.close(fd_r)
    os.close(fd_w)

    assert md5(result) == md5(data), "fd-R did not see fd-W's data"

def test_write_fd_read_fd_no_fsync(test_dir):
    """Same as above but WITHOUT explicit fsync — the read path should
    implicitly flush all writers for the inode."""
    path = os.path.join(test_dir, "multi_fd_no_fsync")
    data = os.urandom(512 * 1024)  # 512KB

    fd_w = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    os.write(fd_w, data)
    # NO fsync here — rely on read-side implicit flush

    fd_r = os.open(path, os.O_RDONLY)
    result = b""
    while True:
        chunk = os.read(fd_r, 65536)
        if not chunk:
            break
        result += chunk

    os.close(fd_r)
    os.close(fd_w)

    assert md5(result) == md5(data), "fd-R did not see fd-W's data (no fsync)"

def test_interleaved_write_read_two_fds(test_dir):
    """Open two rw fds, interleave writes and reads."""
    path = os.path.join(test_dir, "interleave")

    fd1 = os.open(path, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o644)
    fd2 = os.open(path, os.O_RDWR)

    # fd1 writes first 1MB
    data1 = os.urandom(1024 * 1024)
    os.write(fd1, data1)
    os.fsync(fd1)

    # fd2 reads — should see fd1's write
    os.lseek(fd2, 0, os.SEEK_SET)
    read1 = os.read(fd2, 1024 * 1024)
    assert md5(read1) == md5(data1), "fd2 didn't see fd1's write"

    # fd2 appends 1MB
    data2 = os.urandom(1024 * 1024)
    os.lseek(fd2, 0, os.SEEK_END)
    os.write(fd2, data2)
    os.fsync(fd2)

    # fd1 reads all — should see both writes
    os.lseek(fd1, 0, os.SEEK_SET)
    all_data = b""
    while True:
        chunk = os.read(fd1, 65536)
        if not chunk:
            break
        all_data += chunk

    os.close(fd1)
    os.close(fd2)

    expected = data1 + data2
    assert md5(all_data) == md5(expected), "fd1 didn't see fd2's append"

def test_multiple_readers_after_write(test_dir):
    """One writer, multiple concurrent readers on the same file."""
    path = os.path.join(test_dir, "multi_reader")
    data = os.urandom(2 * 1024 * 1024)  # 2MB

    with open(path, "wb") as f:
        f.write(data)

    expected = md5(data)

    # Open 5 read fds simultaneously
    fds = [os.open(path, os.O_RDONLY) for _ in range(5)]

    for fd in fds:
        buf = b""
        while True:
            chunk = os.read(fd, 65536)
            if not chunk:
                break
            buf += chunk
        assert md5(buf) == expected, f"reader fd {fd} got wrong data"

    for fd in fds:
        os.close(fd)
