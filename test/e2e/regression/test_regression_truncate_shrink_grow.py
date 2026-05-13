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

"""Regression test for truncate shrink-grow stale data leak.

Bug: truncate to a smaller size then grow back to the original size would
let the old data beyond the truncation point reappear instead of being
filled with zeros. Root cause was MDSMetaSystem::SetAttr only invalidating
chunk_memo_ but not chunk_cache_ on shrink_file, so the ReadSlice cache-hit
path returned pre-shrink slices.

Fix: commit 924688abe — add chunk_cache_.Delete(ino) alongside
chunk_memo_.Forget(ino) in the SetAttr shrink path.

Verification:
  buggy binary (pre 924688abe):  test_truncate_shrink_grow_tail_must_be_zero should FAIL
  fixed binary (post 924688abe): all tests PASS
  works on: MDS mode only (LocalMetaSystem has no chunk_cache_)
"""

import hashlib
import os

import pytest

pytestmark = pytest.mark.standard

_1MB = 1024 * 1024
_5MB = 5 * _1MB
_10MB = 10 * _1MB


def md5(data):
    return hashlib.md5(data).hexdigest()


def test_truncate_shrink_grow_tail_must_be_zero(test_dir):
    """Shrink then grow: the extended region must be all zeros."""
    path = os.path.join(test_dir, "shrink_grow.bin")

    data = os.urandom(_10MB)
    with open(path, "wb") as f:
        f.write(data)

    os.truncate(path, _5MB)
    os.truncate(path, _10MB)

    with open(path, "rb") as f:
        content = f.read()

    assert len(content) == _10MB
    assert content[_5MB:] == b"\x00" * _5MB, "stale data reappeared after shrink-grow"


def test_truncate_shrink_grow_head_preserved(test_dir):
    """Shrink then grow: the head portion must be unchanged."""
    path = os.path.join(test_dir, "shrink_grow_head.bin")

    data = os.urandom(_10MB)
    with open(path, "wb") as f:
        f.write(data)

    expected_head_md5 = md5(data[:_5MB])

    os.truncate(path, _5MB)
    os.truncate(path, _10MB)

    with open(path, "rb") as f:
        head = f.read(_5MB)

    assert md5(head) == expected_head_md5, "head data corrupted after shrink-grow"


def test_truncate_to_zero_and_grow(test_dir):
    """Truncate to 0 then grow: entire file must be zeros."""
    path = os.path.join(test_dir, "zero_grow.bin")

    with open(path, "wb") as f:
        f.write(os.urandom(_1MB))

    os.truncate(path, 0)
    os.truncate(path, _1MB)

    with open(path, "rb") as f:
        content = f.read()

    assert content == b"\x00" * _1MB, "old data leaked after truncate-to-zero then grow"
