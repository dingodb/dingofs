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

"""End-to-end regression for fallocate(2) on dingofs.

Bug:
  - MDS mode: fallocate(2) non-functional — MDSMetaSystem missing
    Fallocate override → base class returned NotSupport → libfuse
    cached EOPNOTSUPP for subsequent calls; even after wiring the
    override, plain `mode=0` did not extend file length, PUNCH_HOLE
    and ZERO_RANGE+KEEP_SIZE failed with "beyond slice num(0)", and
    successfully-zeroed ranges still read back as the original bytes
    because the server's chunk_cache rejected same-version updates.
  - Local / Memory modes: kSupported mask omitted FALLOC_FL_ZERO_RANGE
    (0x10) → syscall returned ENOTSUP.

Fix: PR #880 (3 commits)
  - [fix][mds]    Support fallocate(2) end-to-end on MDS mode
                  (MDSMetaSystem::Fallocate override + cache invalidation;
                   PreAlloc.set_length; slice_id pre-alloc for PUNCH_HOLE /
                   ZERO_RANGE; chunk.version() bump on slice append)
  - [feat][client] Local mode: support FALLOC_FL_ZERO_RANGE
  - [feat][client] Memory mode: support FALLOC_FL_ZERO_RANGE + gflag-ify FsInfo

Verification:
  pre PR #880  on MDS:    plain_extend fails (size unchanged) — or earlier,
                          all 4 fail with EOPNOTSUPP at the syscall.
  pre PR #880  on Local:  zero_range fails with ENOTSUP (mask reject).
  pre PR #880  on Memory: zero_range fails with ENOTSUP (mask reject).
  post PR #880 all modes: 4/4 PASS.
  works on: MDS / Local / Memory.
"""

import ctypes
import os
import subprocess

import pytest


_1MB = 1 * 1024 * 1024


# linux/falloc.h constants
FALLOC_FL_KEEP_SIZE = 0x01
FALLOC_FL_PUNCH_HOLE = 0x02
FALLOC_FL_ZERO_RANGE = 0x10


def _libc_fallocate(fd, mode, offset, length):
    """Direct libc fallocate() call (Python stdlib lacks PUNCH_HOLE/ZERO_RANGE)."""
    libc = ctypes.CDLL("libc.so.6", use_errno=True)
    libc.fallocate.argtypes = [
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_long,
        ctypes.c_long,
    ]
    libc.fallocate.restype = ctypes.c_int
    rc = libc.fallocate(fd, mode, offset, length)
    if rc != 0:
        errno = ctypes.get_errno()
        raise OSError(errno, os.strerror(errno))


@pytest.mark.smoke
def test_fallocate_plain_extends_file(test_dir):
    """fallocate(mode=0) must extend file size to offset+len."""
    path = os.path.join(test_dir, "extend.bin")
    with open(path, "wb") as f:
        f.write(b"x" * _1MB)  # 1 MB initial

    with open(path, "r+b") as f:
        _libc_fallocate(f.fileno(), 0, 0, 4 * _1MB)  # extend to 4 MB

    sz = os.path.getsize(path)
    assert sz == 4 * _1MB, f"plain fallocate did not extend: size={sz}"


@pytest.mark.smoke
def test_fallocate_punch_hole_zeros_range(test_dir):
    """fallocate(PUNCH_HOLE | KEEP_SIZE) must zero [offset, offset+len) and preserve file size."""
    path = os.path.join(test_dir, "punch.bin")
    data = b"A" * (8 * _1MB)
    with open(path, "wb") as f:
        f.write(data)

    with open(path, "r+b") as f:
        _libc_fallocate(
            f.fileno(),
            FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
            2 * _1MB,  # offset
            2 * _1MB,  # len
        )
        f.seek(2 * _1MB)
        punched = f.read(2 * _1MB)

    assert punched == b"\x00" * (2 * _1MB), (
        f"punched range not zero, first 16 bytes: {punched[:16].hex()}"
    )
    # KEEP_SIZE: file size must not change
    assert os.path.getsize(path) == 8 * _1MB


@pytest.mark.smoke
def test_fallocate_zero_range_zeros_range(test_dir):
    """fallocate(ZERO_RANGE) must zero [offset, offset+len)."""
    path = os.path.join(test_dir, "zero.bin")
    with open(path, "wb") as f:
        f.write(b"B" * (8 * _1MB))

    with open(path, "r+b") as f:
        _libc_fallocate(f.fileno(), FALLOC_FL_ZERO_RANGE, 2 * _1MB, 2 * _1MB)
        f.seek(2 * _1MB)
        zeroed = f.read(2 * _1MB)

    assert zeroed == b"\x00" * (2 * _1MB)
    assert os.path.getsize(path) == 8 * _1MB


@pytest.mark.smoke
def test_fallocate_cli_extend_via_subprocess(test_dir):
    """fallocate(1) CLI tool wraps the syscall — end-to-end UX check."""
    path = os.path.join(test_dir, "cli.bin")
    subprocess.check_call([
        "dd",
        "if=/dev/zero",
        f"of={path}",
        "bs=1M",
        "count=1",
        "status=none",
    ])
    rc = subprocess.call(["fallocate", "-l", "8M", path])
    assert rc == 0, "fallocate -l 8M CLI failed"
    assert os.path.getsize(path) == 8 * _1MB
