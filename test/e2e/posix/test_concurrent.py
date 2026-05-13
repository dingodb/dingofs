# Copyright 2026 DingoDB. All rights reserved.

import os
import hashlib
import concurrent.futures

import pytest

pytestmark = pytest.mark.standard


def md5(data):
    return hashlib.md5(data).hexdigest()


def test_concurrent_write_read(test_dir):
    """8 threads write 10MB files in parallel, then verify each md5."""
    MB = 1024 * 1024
    num_files = 8
    file_size = 10 * MB

    # Prepare datasets
    datasets = [(f"cf_{i}", os.urandom(file_size)) for i in range(num_files)]

    def write_file(name_data):
        name, data = name_data
        path = os.path.join(test_dir, name)
        with open(path, "wb") as f:
            f.write(data)
        return name, md5(data)

    # Parallel write
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_files) as pool:
        results = list(pool.map(write_file, datasets))

    # Read back and verify each file
    for name, expected_md5 in results:
        path = os.path.join(test_dir, name)
        with open(path, "rb") as f:
            assert md5(f.read()) == expected_md5, f"{name} md5 mismatch"
