# Copyright 2024 DingoFS Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.

"""Regression tests for concurrent write+flush correctness.

Bug: When write and flush happen concurrently, the single mutex in
ChunkWriter caused slice ordering/commit sequence errors, leading to
data corruption.

Fix: commit e2fe490db — Split writer_mutex_ (for write batching) from
write_flush_mutex_ (for slice write + flush ordering). Write batch is
sorted while holding writer_mutex_, then slice writes happen under
write_flush_mutex_ to prevent interleaving with flush.

Verification:
  buggy binary (pre e2fe490db):  test_concurrent_write_and_fsync should FAIL (data corruption)
  fixed binary (post e2fe490db): all tests PASS
  works on: local mode + MDS mode (ChunkWriter shared)
"""
import os, hashlib, threading, pytest

pytestmark = pytest.mark.standard

def md5(data): return hashlib.md5(data).hexdigest()

def test_concurrent_write_and_fsync(test_dir):
    """Multiple threads: some write, some fsync, verify final data."""
    path = os.path.join(test_dir, "concurrent_flush")
    total_size = 4 * 1024 * 1024  # 4MB
    chunk_size = 64 * 1024  # 64KB per write
    num_chunks = total_size // chunk_size

    # Pre-generate ordered data
    chunks = [os.urandom(chunk_size) for _ in range(num_chunks)]
    expected = b"".join(chunks)

    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    errors = []

    write_idx = [0]
    lock = threading.Lock()

    def writer():
        while True:
            with lock:
                idx = write_idx[0]
                if idx >= num_chunks:
                    return
                write_idx[0] += 1
            try:
                os.pwrite(fd, chunks[idx], idx * chunk_size)
            except Exception as e:
                errors.append(e)

    def fsyncer():
        """Periodically fsync while writes are in progress."""
        import time
        for _ in range(20):
            try:
                os.fsync(fd)
            except Exception as e:
                errors.append(e)
            time.sleep(0.01)

    threads = [threading.Thread(target=writer) for _ in range(4)]
    threads.append(threading.Thread(target=fsyncer))

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    os.fsync(fd)  # final sync
    os.close(fd)

    assert not errors, f"Errors during concurrent write/flush: {errors}"

    with open(path, "rb") as f:
        actual = f.read()

    assert len(actual) == total_size
    assert md5(actual) == md5(expected), "Data corrupted after concurrent write+flush"

def test_sequential_write_with_interleaved_flush(test_dir):
    """Write sequentially but fsync after every N writes."""
    path = os.path.join(test_dir, "seq_flush")
    chunk_size = 128 * 1024
    num_chunks = 32
    flush_interval = 4

    data_parts = [os.urandom(chunk_size) for _ in range(num_chunks)]

    with open(path, "wb") as f:
        for i, part in enumerate(data_parts):
            f.write(part)
            if (i + 1) % flush_interval == 0:
                os.fsync(f.fileno())

    expected = b"".join(data_parts)

    with open(path, "rb") as f:
        actual = f.read()

    assert md5(actual) == md5(expected)

def test_overwrite_same_region_concurrent(test_dir):
    """Multiple threads overwrite the same region, last writer wins,
    but final content must be valid (one of the written values, not garbage)."""
    path = os.path.join(test_dir, "overwrite_region")
    size = 1024 * 1024  # 1MB region

    # Create initial file
    initial = b"\x00" * size
    with open(path, "wb") as f:
        f.write(initial)

    # Each thread writes a distinct byte pattern
    patterns = [bytes([i]) * size for i in range(4)]
    threads = []

    def overwriter(pattern):
        fd = os.open(path, os.O_WRONLY)
        os.pwrite(fd, pattern, 0)
        os.fsync(fd)
        os.close(fd)

    for p in patterns:
        t = threading.Thread(target=overwriter, args=(p,))
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    with open(path, "rb") as f:
        actual = f.read(size)

    # The content should be exactly one of the patterns (not a mix)
    assert actual in patterns, "Data is garbage mix of concurrent overwrites"
