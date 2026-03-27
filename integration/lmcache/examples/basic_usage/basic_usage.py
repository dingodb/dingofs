# SPDX-License-Identifier: Apache-2.0
"""Basic usage of NativeIOEngine — standalone, no LMCache dependency.

Usage:
    python examples/basic_usage/basic_usage.py
"""

# Standard
import asyncio
import shutil
import tempfile

# First Party
from dingofs_connector import NativeIOEngine


def demo_sync(base_path: str) -> None:
    """Synchronous single-key and batch operations."""
    engine = NativeIOEngine(base_path=base_path, num_workers=4)

    # --- Single key write / read ---
    data = bytearray(b"Hello DingoFS! " * 100)
    engine.set_sync("greeting", memoryview(data))

    buf = bytearray(len(data))
    engine.get_sync("greeting", memoryview(buf))
    assert buf == data
    print("[sync]  Single key read/write ... OK")

    # --- Exists check ---
    assert engine.exists_sync("greeting") is True
    assert engine.exists_sync("nonexistent") is False
    print("[sync]  Exists check ........... OK")

    # --- Batch write / read / exists ---
    chunk_size = 256 * 1024  # 256 KB
    keys = [f"chunk_{i}" for i in range(8)]
    write_bufs = [
        memoryview(bytearray(i.to_bytes(1, "big") * chunk_size))
        for i in range(8)
    ]
    engine.batch_set_sync(keys, write_bufs)

    read_bufs = [memoryview(bytearray(chunk_size)) for _ in keys]
    engine.batch_get_sync(keys, read_bufs)
    for i, (w, r) in enumerate(zip(write_bufs, read_bufs)):
        assert bytes(w) == bytes(r), f"Batch mismatch at chunk_{i}"

    results = engine.batch_exists_sync(keys)
    assert all(results)
    print("[sync]  Batch operations ....... OK")

    engine.close()


async def demo_async(base_path: str) -> None:
    """Async operations with asyncio event loop integration."""
    loop = asyncio.get_event_loop()
    engine = NativeIOEngine(base_path=base_path, num_workers=4, loop=loop)

    # --- Single key ---
    data = bytearray(b"async data " * 200)
    await engine.set("async_key", memoryview(data))

    buf = bytearray(len(data))
    await engine.get("async_key", memoryview(buf))
    assert buf == data
    print("[async] Single key read/write .. OK")

    # --- Concurrent writes ---
    chunk_size = 512 * 1024  # 512 KB
    tasks = []
    for i in range(16):
        key = f"concurrent_{i}"
        payload = memoryview(bytearray(i.to_bytes(1, "big") * chunk_size))
        tasks.append(engine.set(key, payload))
    await asyncio.gather(*tasks)
    print("[async] Concurrent writes ...... OK")

    # --- Batch read ---
    keys = [f"concurrent_{i}" for i in range(16)]
    read_bufs = [memoryview(bytearray(chunk_size)) for _ in keys]
    await engine.batch_get(keys, read_bufs)
    for i, buf in enumerate(read_bufs):
        expected = i.to_bytes(1, "big") * chunk_size
        assert bytes(buf) == expected
    print("[async] Batch read ............. OK")

    engine.close()


def main() -> None:
    tmp_dir = tempfile.mkdtemp(prefix="dingofs_example_")
    try:
        demo_sync(tmp_dir)
        asyncio.run(demo_async(tmp_dir))
        print("\nAll examples passed!")
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
