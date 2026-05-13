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

"""Regression tests for multi-fd consistency via hardlinks.

Bug:
  Hardlinks create multiple paths pointing to the same inode. Writing
  through one path and reading through the hardlink exercises the
  cross-fd flush + invalidate paths with DIFFERENT fds on the SAME inode
  — pre-fix the relevant ByIno helpers only operated on the current fd's
  handle, missing the sibling fds.

Fix: commit 645895eb8 — FlushByIno / InvalidateByIno iterate all handles
for a given inode, not just the current fd's handle.

Verification:
  pre fix  (pre 645895eb8):  test_write_link_a_read_link_b FAILs
  post fix (post 645895eb8): all tests PASS
  works on: Local + MDS
"""

import hashlib
import os

import pytest

pytestmark = pytest.mark.standard


def md5(data):
    return hashlib.md5(data).hexdigest()


def test_write_link_a_read_link_b(test_dir):
    """Write through path-A, read through hardlink path-B (same inode)."""
    path_a = os.path.join(test_dir, "original")
    path_b = os.path.join(test_dir, "hardlink")

    # Create file and hardlink
    open(path_a, "w").close()
    os.link(path_a, path_b)

    data = os.urandom(1024 * 1024)

    # Write through path_a
    with open(path_a, "wb") as f:
        f.write(data)

    # Read through path_b (hardlink, same inode)
    with open(path_b, "rb") as f:
        result = f.read()

    assert md5(result) == md5(data), "hardlink read didn't see write through original"


def test_write_link_b_read_link_a(test_dir):
    """Write through hardlink path-B, read through original path-A."""
    path_a = os.path.join(test_dir, "orig2")
    path_b = os.path.join(test_dir, "link2")

    open(path_a, "w").close()
    os.link(path_a, path_b)

    data = os.urandom(512 * 1024)

    with open(path_b, "wb") as f:
        f.write(data)

    with open(path_a, "rb") as f:
        result = f.read()

    assert md5(result) == md5(data), "original read didn't see write through hardlink"


def test_concurrent_fd_via_hardlinks(test_dir):
    """Open both paths simultaneously, write through one, read through other."""
    path_a = os.path.join(test_dir, "sim_a")
    path_b = os.path.join(test_dir, "sim_b")

    open(path_a, "w").close()
    os.link(path_a, path_b)

    fd_w = os.open(path_a, os.O_WRONLY | os.O_TRUNC)
    fd_r = os.open(path_b, os.O_RDONLY)

    data = os.urandom(256 * 1024)
    os.write(fd_w, data)
    os.fsync(fd_w)

    result = b""
    while True:
        chunk = os.read(fd_r, 65536)
        if not chunk:
            break
        result += chunk

    os.close(fd_w)
    os.close(fd_r)

    assert md5(result) == md5(data), "concurrent fd via hardlinks: read mismatch"


def test_overwrite_via_hardlink(test_dir):
    """O_TRUNC through hardlink should truncate the shared inode."""
    path_a = os.path.join(test_dir, "ow_a")
    path_b = os.path.join(test_dir, "ow_b")

    # Write 4KB through path_a
    with open(path_a, "wb") as f:
        f.write(os.urandom(4096))
    os.link(path_a, path_b)

    # O_TRUNC through path_b, write 10 bytes
    new_data = b"hardlink_ow"
    with open(path_b, "wb") as f:
        f.write(new_data)

    # Read through path_a
    with open(path_a, "rb") as f:
        result = f.read()

    assert result == new_data, (
        f"overwrite via hardlink: expected {len(new_data)} bytes, "
        f"got {len(result)} bytes"
    )
