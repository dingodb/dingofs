# SPDX-License-Identifier: Apache-2.0
"""Performance benchmark for NativeIOEngine.

Compares throughput under different configurations:
  - Worker count scaling (1, 4, 8, 16)
  - Chunk sizes (256KB, 1MB, 4MB)
  - O_DIRECT on/off
  - Sync mode (always vs none)

Usage:
    python examples/benchmark/benchmark.py [--base-path PATH]
                                           [--write-only] [--read-only]
"""

# Standard
import argparse
import shutil
import tempfile
import time

# First Party
from dingofs_connector import NativeIOEngine
from dingofs_connector.native_engine import SYNC_ALWAYS, SYNC_NONE

KB = 1024
MB = 1024 * KB


def _write_chunks(
    engine: NativeIOEngine,
    num_chunks: int,
    chunk_size: int,
    prefix: str = "k",
) -> float:
    """Write chunks and return elapsed seconds."""
    keys = [f"{prefix}_{i}" for i in range(num_chunks)]
    bufs = [memoryview(bytearray(chunk_size)) for _ in keys]
    t0 = time.perf_counter()
    engine.batch_set_sync(keys, bufs)
    return time.perf_counter() - t0


def _read_chunks(
    engine: NativeIOEngine,
    num_chunks: int,
    chunk_size: int,
    prefix: str = "k",
) -> float:
    """Read chunks and return elapsed seconds."""
    keys = [f"{prefix}_{i}" for i in range(num_chunks)]
    bufs = [memoryview(bytearray(chunk_size)) for _ in keys]
    t0 = time.perf_counter()
    engine.batch_get_sync(keys, bufs)
    return time.perf_counter() - t0


def _throughput(total_bytes: int, elapsed: float) -> float:
    """Return throughput in MB/s."""
    if elapsed == 0:
        return float("inf")
    return total_bytes / MB / elapsed


def bench_worker_scaling(
    base_path: str,
    do_write: bool,
    do_read: bool,
) -> None:
    """Benchmark throughput with different worker counts."""
    print("\n--- Worker Scaling (1MB x 32 chunks) ---")
    chunk_size = 1 * MB
    num_chunks = 32
    total = num_chunks * chunk_size

    for nw in [1, 4, 8, 16]:
        engine = NativeIOEngine(base_path=base_path, num_workers=nw)

        parts = [f"  workers={nw:2d} |"]
        if do_write:
            wt = _write_chunks(engine, num_chunks, chunk_size, prefix=f"ws_{nw}")
            parts.append(f" WRITE {_throughput(total, wt):5.0f} MB/s |")
        if do_read:
            # Ensure data exists for read
            if not do_write:
                _write_chunks(engine, num_chunks, chunk_size, prefix=f"ws_{nw}")
            rt = _read_chunks(engine, num_chunks, chunk_size, prefix=f"ws_{nw}")
            parts.append(f" READ {_throughput(total, rt):5.0f} MB/s")

        print("".join(parts))
        engine.close()


def bench_chunk_sizes(
    base_path: str,
    do_write: bool,
    do_read: bool,
) -> None:
    """Benchmark throughput with different chunk sizes."""
    print("\n--- Chunk Size (8 workers) ---")

    for label, chunk_size in [("256KB", 256 * KB), ("  1MB", 1 * MB), ("  4MB", 4 * MB)]:
        num_chunks = 32
        total = num_chunks * chunk_size
        engine = NativeIOEngine(base_path=base_path, num_workers=8)

        parts = [f"  chunk={label} |"]
        prefix = f"cs_{chunk_size}"
        if do_write:
            wt = _write_chunks(engine, num_chunks, chunk_size, prefix=prefix)
            parts.append(f" WRITE {_throughput(total, wt):5.0f} MB/s |")
        if do_read:
            if not do_write:
                _write_chunks(engine, num_chunks, chunk_size, prefix=prefix)
            rt = _read_chunks(engine, num_chunks, chunk_size, prefix=prefix)
            parts.append(f" READ {_throughput(total, rt):5.0f} MB/s")

        print("".join(parts))
        engine.close()


def bench_odirect(
    base_path: str,
    do_write: bool,
    do_read: bool,
) -> None:
    """Benchmark O_DIRECT on vs off."""
    print("\n--- O_DIRECT (1MB x 32 chunks, 8 workers) ---")
    chunk_size = 1 * MB
    num_chunks = 32
    total = num_chunks * chunk_size

    for label, odirect in [("off", False), ("on ", True)]:
        engine = NativeIOEngine(
            base_path=base_path, num_workers=8, use_odirect=odirect
        )

        parts = [f"  odirect={label} |"]
        prefix = f"od_{label.strip()}"
        try:
            if do_write:
                wt = _write_chunks(engine, num_chunks, chunk_size, prefix=prefix)
                parts.append(f" WRITE {_throughput(total, wt):5.0f} MB/s |")
            if do_read:
                if not do_write:
                    _write_chunks(engine, num_chunks, chunk_size, prefix=prefix)
                rt = _read_chunks(engine, num_chunks, chunk_size, prefix=prefix)
                parts.append(f" READ {_throughput(total, rt):5.0f} MB/s")
            print("".join(parts))
        except RuntimeError:
            # O_DIRECT requires aligned buffers and filesystem support;
            # tmpfs / some filesystems do not support it.
            print(f"  odirect={label} | SKIPPED (not supported on this filesystem)")
        finally:
            engine.close()


def bench_sync_mode(base_path: str) -> None:
    """Benchmark sync mode impact on write throughput."""
    print("\n--- Sync Mode (1MB x 32 chunks, 8 workers) ---")
    chunk_size = 1 * MB
    num_chunks = 32
    total = num_chunks * chunk_size

    for label, mode in [("always", SYNC_ALWAYS), ("none  ", SYNC_NONE)]:
        engine = NativeIOEngine(
            base_path=base_path, num_workers=8, sync_mode=mode
        )
        wt = _write_chunks(engine, num_chunks, chunk_size, prefix=f"sm_{label.strip()}")
        print(f"  sync={label} | WRITE {_throughput(total, wt):5.0f} MB/s")
        engine.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="DingoFS Connector Benchmark")
    parser.add_argument(
        "--base-path",
        default=None,
        help="Storage directory (default: auto-created temp dir)",
    )
    parser.add_argument("--write-only", action="store_true", help="Only run write benchmarks")
    parser.add_argument("--read-only", action="store_true", help="Only run read benchmarks")
    args = parser.parse_args()

    do_write = not args.read_only
    do_read = not args.write_only

    tmp_dir = None
    base_path = args.base_path
    if base_path is None:
        tmp_dir = tempfile.mkdtemp(prefix="dingofs_bench_")
        base_path = tmp_dir

    print("=" * 60)
    print("DingoFS Connector Benchmark")
    print(f"Base path: {base_path}")
    print("=" * 60)

    try:
        bench_worker_scaling(base_path, do_write, do_read)
        bench_chunk_sizes(base_path, do_write, do_read)
        bench_odirect(base_path, do_write, do_read)
        if do_write:
            bench_sync_mode(base_path)
    finally:
        if tmp_dir:
            shutil.rmtree(tmp_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
