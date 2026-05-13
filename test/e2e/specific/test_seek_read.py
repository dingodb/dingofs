# Copyright 2026 DingoDB. All rights reserved.

import os
import hashlib
import uuid
import shutil

import pytest

pytestmark = pytest.mark.standard


def md5(data):
    return hashlib.md5(data).hexdigest()


# Module-scope test directory (the conftest may not provide this).
@pytest.fixture(scope="module")
def test_dir_module(mount_point):
    d = os.path.join(mount_point, f"test_mod_{uuid.uuid4().hex[:8]}")
    os.makedirs(d)
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture(scope="module")
def large_file_data():
    """Module-level fixture: generate 130MB of random data."""
    return os.urandom(130 * 1024 * 1024)


@pytest.fixture(scope="module")
def large_file(test_dir_module, large_file_data):
    """Module-level fixture: write the 130MB file once."""
    path = os.path.join(test_dir_module, "seekfile")
    with open(path, "wb") as f:
        f.write(large_file_data)
    return path


_SEEK_PARAMS = [
    (0, 1), (1, 1), (63, 1), (64, 1), (65, 1),
    (100, 1), (127, 1), (128, 1), (129, 1),
    (0, 10), (100, 30), (50, 64),
]


@pytest.mark.parametrize(
    "offset_mb,count_mb",
    _SEEK_PARAMS,
    ids=[f"skip{o}M_cnt{c}M" for o, c in _SEEK_PARAMS],
)
def test_seek_read_md5(large_file, large_file_data, offset_mb, count_mb):
    """Seek to offset and read count bytes, compare md5 with source data."""
    MB = 1024 * 1024
    offset = offset_mb * MB
    count = count_mb * MB

    with open(large_file, "rb") as f:
        f.seek(offset)
        actual = f.read(count)

    expected = large_file_data[offset:offset + count]
    assert md5(actual) == md5(expected)
