# Copyright 2024 DingoFS Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.

import os
import stat

import pytest

pytestmark = pytest.mark.smoke


def test_chmod(test_dir):
    p = os.path.join(test_dir, "ch")
    open(p, "w").close()
    os.chmod(p, 0o600)
    assert oct(os.stat(p).st_mode)[-3:] == "600"
    os.chmod(p, 0o644)
    assert oct(os.stat(p).st_mode)[-3:] == "644"


def test_utime(test_dir):
    p = os.path.join(test_dir, "ut")
    open(p, "w").close()
    target_time = 1750000000.0
    os.utime(p, (target_time, target_time))
    st = os.stat(p)
    assert abs(st.st_mtime - target_time) < 2


def test_stat_type_file(test_dir):
    p = os.path.join(test_dir, "reg")
    open(p, "w").close()
    assert stat.S_ISREG(os.stat(p).st_mode)


def test_stat_type_dir(test_dir):
    d = os.path.join(test_dir, "dir")
    os.mkdir(d)
    assert stat.S_ISDIR(os.stat(d).st_mode)


def test_unlink(test_dir):
    p = os.path.join(test_dir, "ul")
    open(p, "w").close()
    assert os.path.exists(p)
    os.unlink(p)
    assert not os.path.exists(p)
