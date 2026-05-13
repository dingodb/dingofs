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

"""Regression tests for readahead buffer invalidation after truncate /
fallocate on the same fd.

Bug:
  VFSImpl::SetAttr and VFSImpl::Fallocate did not invoke
  handle_manager_->InvalidateByIno after a size / content change, so the
  cached ReadRequest::buffer in FileReader::requests_ kept stale bytes
  for any range that overlapped the modified region. A subsequent read
  on the SAME fd returned the stale buffer instead of the post-truncate
  zeros (or post-PUNCH_HOLE zeros, etc).

  Test shape (all 3 cases):
    1. Open + full read → readahead populated
    2. truncate / fallocate on the same fd
    3. Re-read the affected range
    4. Assert zeros (per POSIX), not stale bytes

Verification:
  pre fix:  affected range reads back stale (originally-written) bytes.
  post fix: affected range reads zeros.
  works on: Local + MDS (SetAttr fix from #879; Fallocate fix from #882).
"""

import ctypes
import hashlib
import os

import pytest

_1MB = 1 * 1024 * 1024


# fallocate flags (linux/falloc.h)
FALLOC_FL_KEEP_SIZE = 0x01
FALLOC_FL_PUNCH_HOLE = 0x02


def _libc_fallocate(fd, mode, offset, length):
    """Direct libc fallocate() call (Python stdlib lacks PUNCH_HOLE)."""
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


def _first_nonzero(buf):
    """Return offset of first non-zero byte, or -1 if all zero."""
    for i, b in enumerate(buf):
        if b != 0:
            return i
    return -1


@pytest.mark.smoke
def test_truncate_shrink_then_grow_invalidates_readahead(test_dir):
    """Scenario A: same fd, read → ftruncate(shrink) → ftruncate(grow) → re-read.

    After shrink + grow, the extended range MUST be zero. The bug exposes
    the stale ReadRequest::buffer covering the original file content.
    """
    path = os.path.join(test_dir, "shrink_grow.bin")

    data = os.urandom(4 * _1MB)
    with open(path, "wb") as f:
        f.write(data)

    with open(path, "r+b") as f:
        # Step 1: full read triggers readahead, caches buffer covering [0, 4M)
        first = f.read()
        assert len(first) == 4 * _1MB
        assert first == data

        # Step 2: same fd, shrink to 1MB then grow back to 4MB
        os.ftruncate(f.fileno(), 1 * _1MB)
        os.ftruncate(f.fileno(), 4 * _1MB)

        # Step 3: re-read the extended range [1M, 4M)
        f.seek(1 * _1MB)
        tail = f.read()

    assert len(tail) == 3 * _1MB, f"unexpected read length {len(tail)}"
    nz = _first_nonzero(tail)
    if nz >= 0:
        pytest.fail(
            f"readahead invalidate bug: extended range [1M, 4M) read non-zero "
            f"at offset +{nz} (absolute {1 * _1MB + nz}); "
            f"expected zeros after shrink+grow. "
            f"first 16 bytes of tail: {tail[:16].hex()}"
        )


@pytest.mark.smoke
def test_fallocate_punch_hole_invalidates_readahead(test_dir):
    """Scenario C: same fd, read → fallocate(PUNCH_HOLE) → re-read.

    After punch hole, the punched range MUST read as zero. The bug exposes
    stale ReadRequest::buffer covering that range.
    """
    path = os.path.join(test_dir, "punch.bin")

    data = os.urandom(8 * _1MB)
    with open(path, "wb") as f:
        f.write(data)

    with open(path, "r+b") as f:
        # Step 1: full read triggers readahead, buffer covers [0, 8M)
        first = f.read()
        assert len(first) == 8 * _1MB

        # Step 2: same fd, punch hole [2M, 4M)
        try:
            _libc_fallocate(
                f.fileno(),
                FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                2 * _1MB,
                2 * _1MB,
            )
        except OSError as e:
            pytest.skip(f"fallocate(PUNCH_HOLE) not supported: {e}")

        # Step 3: same fd, re-read the punched range
        f.seek(2 * _1MB)
        punched = f.read(2 * _1MB)

    assert len(punched) == 2 * _1MB, f"unexpected read length {len(punched)}"
    nz = _first_nonzero(punched)
    if nz >= 0:
        pytest.fail(
            f"readahead invalidate bug: punched range [2M, 4M) read non-zero "
            f"at offset +{nz} (absolute {2 * _1MB + nz}); "
            f"expected zeros after PUNCH_HOLE. "
            f"first 16 bytes of punched range: {punched[:16].hex()}"
        )


@pytest.mark.smoke
def test_truncate_shrink_in_place_extends_zeros_on_pwrite(test_dir):
    """Scenario A variant: read → ftruncate(small) → pwrite beyond → read whole.

    After truncate(1M) then pwrite at offset 3M, the file becomes 3M+len.
    The range [1M, 3M) is a hole (file shrunk past it then re-extended via
    the implicit hole before pwrite offset) and MUST read as zero. The bug
    keeps stale ReadRequest::buffer for [0, 4M) and surfaces non-zero data.
    """
    path = os.path.join(test_dir, "shrink_pwrite.bin")

    data = os.urandom(4 * _1MB)
    with open(path, "wb") as f:
        f.write(data)

    marker = b"DINGOFS_MARKER_FRESH_WRITE_AFTER_TRUNCATE"

    with open(path, "r+b") as f:
        # Step 1: read triggers readahead
        first = f.read()
        assert len(first) == 4 * _1MB

        # Step 2: shrink + pwrite beyond original length (after the shrink)
        os.ftruncate(f.fileno(), 1 * _1MB)
        os.pwrite(f.fileno(), marker, 3 * _1MB)

        # Step 3: read hole region [1M, 3M)
        f.seek(1 * _1MB)
        hole = f.read(2 * _1MB)

        # And the marker should be readable at [3M, 3M+len(marker))
        f.seek(3 * _1MB)
        readback_marker = f.read(len(marker))

    assert readback_marker == marker, "pwrite marker not readable; meta bug?"

    nz = _first_nonzero(hole)
    if nz >= 0:
        pytest.fail(
            f"readahead invalidate bug: hole [1M, 3M) read stale non-zero at "
            f"offset +{nz} (absolute {1 * _1MB + nz}); "
            f"expected zeros for hole region. "
            f"first 16 bytes of hole: {hole[:16].hex()}"
        )
