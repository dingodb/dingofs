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

"""Tests for rename operations."""

import os

import pytest


@pytest.mark.smoke
def test_rename_same_dir(test_dir):
    """Rename within the same directory moves content correctly."""
    old = os.path.join(test_dir, "old.txt")
    new = os.path.join(test_dir, "new.txt")

    with open(old, "w") as f:
        f.write("rename me")

    os.rename(old, new)

    assert os.path.exists(new)
    assert not os.path.exists(old)

    with open(new, "r") as f:
        assert f.read() == "rename me"


@pytest.mark.smoke
def test_rename_cross_dir(test_dir):
    """Rename across directories moves the file correctly."""
    dir1 = os.path.join(test_dir, "dir1")
    dir2 = os.path.join(test_dir, "dir2")
    os.makedirs(dir1)
    os.makedirs(dir2)

    src = os.path.join(dir1, "file.txt")
    dst = os.path.join(dir2, "file.txt")

    with open(src, "w") as f:
        f.write("cross dir")

    os.rename(src, dst)

    assert os.path.exists(dst)
    assert not os.path.exists(src)

    with open(dst, "r") as f:
        assert f.read() == "cross dir"


@pytest.mark.smoke
def test_rename_directory(test_dir):
    """Rename a directory moves all its contents."""
    d1 = os.path.join(test_dir, "d1")
    d2 = os.path.join(test_dir, "d2")
    os.makedirs(d1)

    # Create a file inside d1
    child = os.path.join(d1, "f.txt")
    with open(child, "w") as f:
        f.write("inside")

    os.rename(d1, d2)

    assert os.path.exists(os.path.join(d2, "f.txt"))
    assert not os.path.exists(d1)
