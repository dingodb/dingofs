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

"""Regression tests for O_TRUNC overwrite race condition.

Bug: When close(fd1) and open(fd2, O_TRUNC) race, fd2 inherits fd1's
chunk_set with a stale last_write_length_. fd2's write computes
max(stale_len, new_len) and FlushFile sends the stale (larger) length
to MDS, making the file appear bigger than it should be.

Example: write 4 bytes → close → open(O_TRUNC) → write 2 bytes → close
         → read returns b'BB\\x00\\x00' (4 bytes) instead of b'BB' (2 bytes)

Root cause: MDSMetaSystem::Open did not call chunk_set->ResetLastWriteLength()
on O_TRUNC. The shared file_session/chunk_set retained the old value.

Fix: commit e5cf76916 — add chunk_set->ResetLastWriteLength() in the O_TRUNC
branch of MDSMetaSystem::DoOpen.

Verification:
  buggy binary:  test_overwrite_basic should FAIL ~30% on MDS mode
  fixed binary:  all tests PASS
  works on: MDS mode (race window depends on RPC latency; local mode rarely triggers)
"""

import hashlib
import os

import pytest

pytestmark = pytest.mark.standard

def md5(data):
    return hashlib.md5(data).hexdigest()

def test_overwrite_basic(test_dir):
    """Write 4 bytes, O_TRUNC overwrite with 2 bytes, read must be 2 bytes."""
    p = os.path.join(test_dir, "ow")
    with open(p, "wb") as f:
        f.write(b"AAAA")
    with open(p, "wb") as f:
        f.write(b"BB")
    with open(p, "rb") as f:
        assert f.read() == b"BB"

def test_overwrite_repeated(test_dir):
    """Repeat overwrite 20 times — must never see stale length."""
    p = os.path.join(test_dir, "ow_repeat")
    for i in range(20):
        long_data = os.urandom(1024)
        short_data = os.urandom(7)
        with open(p, "wb") as f:
            f.write(long_data)
        with open(p, "wb") as f:
            f.write(short_data)
        with open(p, "rb") as f:
            result = f.read()
        assert result == short_data, (
            f"iteration {i}: expected {len(short_data)} bytes, "
            f"got {len(result)} bytes"
        )

def test_overwrite_large_to_small(test_dir):
    """Overwrite 1MB file with 1 byte — size must be 1."""
    p = os.path.join(test_dir, "ow_large")
    with open(p, "wb") as f:
        f.write(os.urandom(1024 * 1024))
    with open(p, "wb") as f:
        f.write(b"X")
    with open(p, "rb") as f:
        data = f.read()
    assert data == b"X"
    assert os.path.getsize(p) == 1

def test_overwrite_md5_consistency(test_dir):
    """Overwrite and verify md5 matches the new content exactly."""
    p = os.path.join(test_dir, "ow_md5")
    with open(p, "wb") as f:
        f.write(os.urandom(4096))
    new_data = os.urandom(100)
    with open(p, "wb") as f:
        f.write(new_data)
    with open(p, "rb") as f:
        assert md5(f.read()) == md5(new_data)
