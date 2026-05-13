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

"""Tests for hard link and symbolic link operations."""

import os

import pytest


@pytest.mark.smoke
def test_hardlink(test_dir):
    """Hard link shares inode, nlink=2, and content is identical."""
    src = os.path.join(test_dir, "src.txt")
    lnk = os.path.join(test_dir, "lnk.txt")

    with open(src, "w") as f:
        f.write("hello hardlink")

    os.link(src, lnk)

    src_stat = os.stat(src)
    lnk_stat = os.stat(lnk)

    assert src_stat.st_ino == lnk_stat.st_ino
    assert src_stat.st_nlink == 2
    assert lnk_stat.st_nlink == 2

    with open(lnk, "r") as f:
        assert f.read() == "hello hardlink"


@pytest.mark.smoke
def test_hardlink_unlink_preserves_source(test_dir):
    """Removing hard link leaves source with nlink=1 and content intact."""
    src = os.path.join(test_dir, "src.txt")
    lnk = os.path.join(test_dir, "lnk.txt")

    with open(src, "w") as f:
        f.write("preserve me")

    os.link(src, lnk)
    os.unlink(lnk)

    stat = os.stat(src)
    assert stat.st_nlink == 1

    with open(src, "r") as f:
        assert f.read() == "preserve me"


@pytest.mark.smoke
def test_symlink_readlink(test_dir):
    """Symlink target is correct and content is readable through symlink."""
    target = os.path.join(test_dir, "target.txt")
    link = os.path.join(test_dir, "sym.txt")

    with open(target, "w") as f:
        f.write("symlink content")

    os.symlink(target, link)

    assert os.readlink(link) == target

    with open(link, "r") as f:
        assert f.read() == "symlink content"


@pytest.mark.smoke
def test_symlink_dangling(test_dir):
    """Reading a dangling symlink raises FileNotFoundError."""
    target = os.path.join(test_dir, "target.txt")
    link = os.path.join(test_dir, "sym.txt")

    with open(target, "w") as f:
        f.write("soon gone")

    os.symlink(target, link)
    os.unlink(target)

    with pytest.raises(FileNotFoundError):
        with open(link, "r") as f:
            f.read()
