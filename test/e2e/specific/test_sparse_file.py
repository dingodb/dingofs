# Copyright 2026 DingoDB. All rights reserved.

import os
import hashlib

import pytest

pytestmark = pytest.mark.standard


def md5(data):
    return hashlib.md5(data).hexdigest()


def test_sparse_write_read(test_dir):
    """Truncate to 100MB, write 3 patches at different offsets, verify md5."""
    path = os.path.join(test_dir, "sparse")
    MB = 1024 * 1024

    # Build expected content
    expected = bytearray(100 * MB)
    patches = [
        (5, os.urandom(2 * MB)),
        (50, os.urandom(2 * MB)),
        (97, os.urandom(2 * MB)),
    ]
    for off_mb, data in patches:
        expected[off_mb * MB: off_mb * MB + len(data)] = data

    # Create sparse file via truncate
    with open(path, "wb") as f:
        f.truncate(100 * MB)

    # Write each patch
    for off_mb, data in patches:
        with open(path, "r+b") as f:
            f.seek(off_mb * MB)
            f.write(data)

    # Read back and verify
    with open(path, "rb") as f:
        actual = f.read()

    assert md5(actual) == md5(bytes(expected))
