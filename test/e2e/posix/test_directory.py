# Copyright 2024 DingoFS Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.

import os
import shutil

import pytest

pytestmark = pytest.mark.smoke


def test_mkdir(test_dir):
    d = os.path.join(test_dir, "subdir")
    os.mkdir(d)
    assert os.path.isdir(d)


def test_mkdir_nested(test_dir):
    d = os.path.join(test_dir, "a", "b", "c", "d")
    os.makedirs(d)
    assert os.path.isdir(d)


def test_rmdir_empty(test_dir):
    d = os.path.join(test_dir, "empty_dir")
    os.mkdir(d)
    os.rmdir(d)
    assert not os.path.exists(d)


def test_rmdir_nonempty_fails(test_dir):
    d = os.path.join(test_dir, "nonempty")
    os.mkdir(d)
    open(os.path.join(d, "f"), "w").close()
    with pytest.raises(OSError):
        os.rmdir(d)


def test_readdir(test_dir):
    names = {"f1", "f2", "f3", "sub"}
    for n in ["f1", "f2", "f3"]:
        open(os.path.join(test_dir, n), "w").close()
    os.mkdir(os.path.join(test_dir, "sub"))
    listed = set(os.listdir(test_dir))
    assert listed == names


def test_recursive_delete(test_dir):
    base = os.path.join(test_dir, "tree")
    os.makedirs(os.path.join(base, "a", "b"))
    open(os.path.join(base, "a", "f"), "w").close()
    open(os.path.join(base, "a", "b", "g"), "w").close()
    shutil.rmtree(base)
    assert not os.path.exists(base)
