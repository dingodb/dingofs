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

"""Tests for truncate consistency."""

import hashlib
import os

import pytest

_1MB = 1 * 1024 * 1024
_5MB = 5 * 1024 * 1024
_10MB = 10 * 1024 * 1024


def md5(data):
    return hashlib.md5(data).hexdigest()


@pytest.mark.standard
def test_truncate_shrink_preserves_head(test_dir):
    """Truncate to smaller size preserves the head portion."""
    path = os.path.join(test_dir, "shrink.bin")

    data = os.urandom(_10MB)
    with open(path, "wb") as f:
        f.write(data)

    expected_md5 = md5(data[:_5MB])

    os.truncate(path, _5MB)

    with open(path, "rb") as f:
        head = f.read()

    assert len(head) == _5MB
    assert md5(head) == expected_md5


@pytest.mark.standard
def test_truncate_grow_fills_zero(test_dir):
    """Truncate to larger size fills the extension with zeros."""
    path = os.path.join(test_dir, "grow.bin")

    with open(path, "wb") as f:
        f.write(os.urandom(_5MB))

    os.truncate(path, _10MB)

    with open(path, "rb") as f:
        f.seek(_5MB)
        tail = f.read()

    assert len(tail) == _5MB
    assert tail == b"\x00" * _5MB


@pytest.mark.standard
def test_truncate_grow_head_unchanged(test_dir):
    """Truncate to larger size does not alter the original head."""
    path = os.path.join(test_dir, "grow_head.bin")

    data = os.urandom(_5MB)
    with open(path, "wb") as f:
        f.write(data)

    expected_md5 = md5(data)

    os.truncate(path, _10MB)

    with open(path, "rb") as f:
        head = f.read(_5MB)

    assert md5(head) == expected_md5


@pytest.mark.standard
def test_truncate_to_zero(test_dir):
    """Truncate to zero results in an empty file."""
    path = os.path.join(test_dir, "zero.bin")

    with open(path, "wb") as f:
        f.write(os.urandom(_1MB))

    os.truncate(path, 0)

    assert os.path.getsize(path) == 0


