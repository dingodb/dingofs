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

"""Tests for extended attribute (xattr) operations."""

import os

import pytest


def _check_xattr_support(path):
    """Try a trivial xattr operation; skip if not supported."""
    try:
        os.setxattr(path, b"user.test_probe", b"1")
        os.removexattr(path, b"user.test_probe")
    except OSError:
        pytest.skip("xattr not supported")


@pytest.mark.standard
def test_setxattr_getxattr(test_dir):
    """Set an xattr and read it back."""
    path = os.path.join(test_dir, "file.txt")
    with open(path, "w") as f:
        f.write("")

    _check_xattr_support(path)

    os.setxattr(path, b"user.key1", b"value1")
    assert os.getxattr(path, b"user.key1") == b"value1"


@pytest.mark.standard
def test_listxattr(test_dir):
    """List xattrs returns all set attributes."""
    path = os.path.join(test_dir, "file.txt")
    with open(path, "w") as f:
        f.write("")

    _check_xattr_support(path)

    os.setxattr(path, b"user.k1", b"v1")
    os.setxattr(path, b"user.k2", b"v2")

    attrs = os.listxattr(path)
    assert "user.k1" in attrs
    assert "user.k2" in attrs


@pytest.mark.standard
def test_removexattr(test_dir):
    """Remove an xattr so that get raises OSError."""
    path = os.path.join(test_dir, "file.txt")
    with open(path, "w") as f:
        f.write("")

    _check_xattr_support(path)

    os.setxattr(path, b"user.gone", b"bye")
    os.removexattr(path, b"user.gone")

    with pytest.raises(OSError):
        os.getxattr(path, b"user.gone")


@pytest.mark.standard
def test_xattr_on_directory(test_dir):
    """Xattr operations work on directories."""
    d = os.path.join(test_dir, "subdir")
    os.makedirs(d)

    _check_xattr_support(d)

    os.setxattr(d, b"user.dirattr", b"dirvalue")
    assert os.getxattr(d, b"user.dirattr") == b"dirvalue"
