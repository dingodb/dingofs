# Copyright 2024 DingoFS Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.

import hashlib
import os

import pytest

pytestmark = pytest.mark.smoke


def md5(data):
    return hashlib.md5(data).hexdigest()


def test_create_file(test_dir):
    """touch creates an empty file."""
    p = os.path.join(test_dir, "empty")
    open(p, "w").close()
    assert os.path.isfile(p)
    assert os.path.getsize(p) == 0


def test_write_read_small(test_dir):
    """Small file round-trip."""
    p = os.path.join(test_dir, "small.txt")
    data = b"hello dingofs\n"
    with open(p, "wb") as f:
        f.write(data)
    with open(p, "rb") as f:
        assert f.read() == data


def test_write_read_1mb(test_dir):
    """1MB random data md5 round-trip."""
    p = os.path.join(test_dir, "1mb")
    data = os.urandom(1024 * 1024)
    with open(p, "wb") as f:
        f.write(data)
    with open(p, "rb") as f:
        assert md5(f.read()) == md5(data)


def test_delete_file(test_dir):
    """File does not exist after deletion."""
    p = os.path.join(test_dir, "todel")
    open(p, "w").close()
    os.remove(p)
    assert not os.path.exists(p)


def test_stat_size(test_dir):
    """stat returns correct size."""
    p = os.path.join(test_dir, "sized")
    data = os.urandom(12345)
    with open(p, "wb") as f:
        f.write(data)
    assert os.path.getsize(p) == 12345
