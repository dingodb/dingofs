# SPDX-License-Identifier: Apache-2.0
#
# Performance benchmarks for DingoFS connector.
# Run with: pytest test_performance.py -xvs

# Standard
import os
import shutil
import tempfile
import time

# Third Party
import pytest

# Local
from dingofs_connector.native_engine import NativeIOEngine


def _format_throughput(bytes_total: int, elapsed: float) -> str:
    """Format throughput as human-readable string."""
    if elapsed <= 0:
        return "inf"
    mb = bytes_total / (1024 * 1024)
    return f"{mb / elapsed:.1f} MB/s"


def _format_size(size: int) -> str:
    """Format byte size as human-readable."""
    if size >= 1024 * 1024:
        return f"{size // (1024 * 1024)}MB"
    elif size >= 1024:
        return f"{size // 1024}KB"
    return f"{size}B"


class TestWriteThroughput:
    """Benchmark write (SET) throughput."""

    @pytest.mark.parametrize(
        "chunk_size",
        [256 * 1024, 1024 * 1024, 4 * 1024 * 1024],
        ids=["256KB", "1MB", "4MB"],
    )
    def test_write_throughput(self, tmp_dir, chunk_size):
        """Measure write throughput for different chunk sizes."""
        num_workers = 8
        num_chunks = 32
        client = NativeIOEngine(tmp_dir, num_workers=num_workers)

        # Prepare data
        keys = [f"write_bench_{i}" for i in range(num_chunks)]
        bufs = [bytearray(os.urandom(chunk_size)) for _ in range(num_chunks)]

        # Warm up
        client.set_sync("warmup", memoryview(bytearray(chunk_size)))

        # Benchmark
        start = time.perf_counter()
        client.batch_set_sync(keys, [memoryview(b) for b in bufs])
        elapsed = time.perf_counter() - start

        total_bytes = num_chunks * chunk_size
        throughput = _format_throughput(total_bytes, elapsed)
        print(
            f"\n  WRITE {_format_size(chunk_size)} x {num_chunks} chunks, "
            f"{num_workers} workers: {throughput} ({elapsed:.3f}s)"
        )

        client.close()


class TestReadThroughput:
    """Benchmark read (GET) throughput."""

    @pytest.mark.parametrize(
        "chunk_size",
        [256 * 1024, 1024 * 1024, 4 * 1024 * 1024],
        ids=["256KB", "1MB", "4MB"],
    )
    def test_read_throughput(self, tmp_dir, chunk_size):
        """Measure read throughput for different chunk sizes."""
        num_workers = 8
        num_chunks = 32
        client = NativeIOEngine(tmp_dir, num_workers=num_workers)

        # Write data first
        keys = [f"read_bench_{i}" for i in range(num_chunks)]
        bufs = [bytearray(os.urandom(chunk_size)) for _ in range(num_chunks)]
        client.batch_set_sync(keys, [memoryview(b) for b in bufs])

        # Prepare read buffers
        read_bufs = [bytearray(chunk_size) for _ in range(num_chunks)]

        # Drop page cache (best effort)
        try:
            os.sync()
        except Exception:
            pass

        # Benchmark
        start = time.perf_counter()
        client.batch_get_sync(keys, [memoryview(b) for b in read_bufs])
        elapsed = time.perf_counter() - start

        total_bytes = num_chunks * chunk_size
        throughput = _format_throughput(total_bytes, elapsed)
        print(
            f"\n  READ {_format_size(chunk_size)} x {num_chunks} chunks, "
            f"{num_workers} workers: {throughput} ({elapsed:.3f}s)"
        )

        # Verify data integrity
        for i in range(num_chunks):
            assert bufs[i] == read_bufs[i], f"Data mismatch at chunk {i}"

        client.close()


class TestConcurrencyScaling:
    """Benchmark throughput scaling with different worker counts."""

    @pytest.mark.parametrize("num_workers", [1, 4, 8, 16], ids=["1w", "4w", "8w", "16w"])
    def test_write_scaling(self, num_workers):
        """Measure write throughput scaling with worker count."""
        tmp_dir = tempfile.mkdtemp(prefix="dingofs_bench_")
        try:
            chunk_size = 1024 * 1024  # 1 MB
            num_chunks = 32
            client = NativeIOEngine(tmp_dir, num_workers=num_workers)

            keys = [f"scale_w_{i}" for i in range(num_chunks)]
            bufs = [bytearray(os.urandom(chunk_size)) for _ in range(num_chunks)]

            start = time.perf_counter()
            client.batch_set_sync(keys, [memoryview(b) for b in bufs])
            elapsed = time.perf_counter() - start

            total_bytes = num_chunks * chunk_size
            throughput = _format_throughput(total_bytes, elapsed)
            print(
                f"\n  WRITE 1MB x {num_chunks}, {num_workers} workers: "
                f"{throughput} ({elapsed:.3f}s)"
            )

            client.close()
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    @pytest.mark.parametrize("num_workers", [1, 4, 8, 16], ids=["1w", "4w", "8w", "16w"])
    def test_read_scaling(self, num_workers):
        """Measure read throughput scaling with worker count."""
        tmp_dir = tempfile.mkdtemp(prefix="dingofs_bench_")
        try:
            chunk_size = 1024 * 1024  # 1 MB
            num_chunks = 32
            client = NativeIOEngine(tmp_dir, num_workers=num_workers)

            keys = [f"scale_r_{i}" for i in range(num_chunks)]
            bufs = [bytearray(os.urandom(chunk_size)) for _ in range(num_chunks)]
            client.batch_set_sync(keys, [memoryview(b) for b in bufs])

            read_bufs = [bytearray(chunk_size) for _ in range(num_chunks)]

            start = time.perf_counter()
            client.batch_get_sync(keys, [memoryview(b) for b in read_bufs])
            elapsed = time.perf_counter() - start

            total_bytes = num_chunks * chunk_size
            throughput = _format_throughput(total_bytes, elapsed)
            print(
                f"\n  READ 1MB x {num_chunks}, {num_workers} workers: "
                f"{throughput} ({elapsed:.3f}s)"
            )

            client.close()
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)


class TestExistsThroughput:
    """Benchmark EXISTS operation throughput."""

    def test_exists_throughput(self, tmp_dir):
        """Measure EXISTS throughput."""
        num_workers = 8
        num_keys = 100
        client = NativeIOEngine(tmp_dir, num_workers=num_workers)

        # Write keys
        keys = [f"exists_bench_{i}" for i in range(num_keys)]
        chunk_size = 4096
        bufs = [bytearray(chunk_size) for _ in range(num_keys)]
        client.batch_set_sync(keys, [memoryview(b) for b in bufs])

        # Benchmark EXISTS
        start = time.perf_counter()
        results = client.batch_exists_sync(keys)
        elapsed = time.perf_counter() - start

        assert all(results)
        ops_per_sec = num_keys / elapsed if elapsed > 0 else float("inf")
        print(
            f"\n  EXISTS {num_keys} keys, {num_workers} workers: "
            f"{ops_per_sec:.0f} ops/s ({elapsed:.3f}s)"
        )

        client.close()
