# Copyright 2026 DingoDB. All rights reserved.

import os
import hashlib

import pytest

pytestmark = pytest.mark.standard


def md5(data):
    return hashlib.md5(data).hexdigest()


@pytest.mark.parametrize("size", [
    1,                    # 1B
    1024,                 # 1KB
    4 * 1024,             # 4KB
    1024 * 1024,          # 1MB
    4 * 1024 * 1024,      # 4MB
    10 * 1024 * 1024,     # 10MB
    64 * 1024 * 1024,     # 64MB
    130 * 1024 * 1024,    # 130MB (cross multi chunk)
], ids=["1B", "1KB", "4KB", "1MB", "4MB", "10MB", "64MB", "130MB"])
def test_write_read_md5(test_dir, size):
    """Write random data of the given size, read back and compare md5."""
    path = os.path.join(test_dir, f"file_{size}")
    data = os.urandom(size)
    expected = md5(data)

    with open(path, "wb") as f:
        f.write(data)

    with open(path, "rb") as f:
        actual = md5(f.read())

    assert actual == expected, f"md5 mismatch for size {size}"
