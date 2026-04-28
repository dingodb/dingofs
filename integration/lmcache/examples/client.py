#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Smoke + benchmark client driving DingoFSConnector against a real cluster.

Mirrors LMCache's actual call shape: CacheEngineKey + MemoryObj through the
RemoteConnector interface (async get/put/exists, batched_get/batched_put).
Requires lmcache to be importable (i.e. Python 3.10+).

For a no-lmcache, Python-3.9-friendly version that talks straight to the
native module, see client_raw.py — same commands, same wire format.

Examples:

    # liveness
    python examples/client.py --url dingofs://mds:6700/grp ping

    # one-shot self-test
    python examples/client.py --url dingofs://mds:6700/grp roundtrip --hash 0xcafebabe

    # latency benchmark (20 x 4 MiB, serial)
    python examples/client.py --url dingofs://mds:6700/grp \
        bench --count 20 --size 4194304 --mode seq

    # throughput benchmark (20 x 4 MiB batched, 30 rounds for steady state)
    python examples/client.py --url dingofs://mds:6700/grp \
        bench --count 20 --size 4194304 --mode batch --rounds 30

    # live bandwidth meter (dstat-like, Ctrl-C to stop)
    python examples/client.py --url dingofs://mds:6700/grp \
        watch --op get --count 20 --size 4194304 --duration 60
"""

from __future__ import annotations

import argparse
import asyncio
import collections
import logging
import os
import random
import signal
import struct
import sys
import time
from pathlib import Path
from typing import Optional

# Allow `python examples/client.py ...` from the package root without install.
_HERE = Path(__file__).resolve().parent
_PKG_ROOT = _HERE.parent / "src"
if _PKG_ROOT.exists() and str(_PKG_ROOT) not in sys.path:
    sys.path.insert(0, str(_PKG_ROOT))
sys.path.insert(0, str(_HERE))  # for _fake_lmcache

import torch
from lmcache.utils import CacheEngineKey

from _fake_lmcache import FakeLocalCPUBackend, FakeMemoryObj, FakeMetadata
from dingofs_connector.remote_connector import DingoFSConnector

# Set by main() from the global --arena-mib / --conf flags. arena_mib > 0 swaps
# the bytearray-pool fake backend for a real contiguous-arena backend so the
# connector's zero-copy RDMA path activates; conf_path toggles --use_rdma for a
# clean TCP-vs-RDMA A/B (arena stays registered in both runs).
_ARENA_MIB = 0
_CONF_PATH: Optional[str] = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


# ---------------------------------------------------------------------------
# constants & helpers
# ---------------------------------------------------------------------------

_PATTERN_LEN = 8
DEFAULT_CHUNK_BYTES = 4 * 1024 * 1024   # 4 MiB matches bench/watch default


def _hex_int(s: str) -> int:
    return int(s, 0)


def _make_key(args: argparse.Namespace,
              chunk_hash: Optional[int] = None) -> CacheEngineKey:
    return CacheEngineKey(
        model_name=args.model,
        world_size=args.world_size,
        worker_id=args.worker_id,
        chunk_hash=chunk_hash if chunk_hash is not None else args.hash,
        dtype=torch.float16,
    )


def _fill_pattern(mv: memoryview, seed: int) -> None:
    """Bulk-fill (fast): ~30x quicker than per-8-byte loop on 4 MiB."""
    # Real LMCache MemoryObj hands back a '<B'-format memoryview, which rejects
    # slice assignment; the plain-byte view does not.
    if mv.format != "B":
        mv = mv.cast("B")
    pattern = struct.pack("<Q", seed & 0xFFFFFFFFFFFFFFFF)
    full = len(mv) // _PATTERN_LEN
    if full:
        mv[: full * _PATTERN_LEN] = pattern * full
    tail = len(mv) % _PATTERN_LEN
    if tail:
        mv[-tail:] = pattern[:tail]


def _verify_pattern(mv: memoryview, seed: int) -> bool:
    """Bulk-compare (fast)."""
    if mv.format != "B":
        mv = mv.cast("B")
    pattern = struct.pack("<Q", seed & 0xFFFFFFFFFFFFFFFF)
    full = len(mv) // _PATTERN_LEN
    if full and bytes(mv[: full * _PATTERN_LEN]) != pattern * full:
        return False
    tail = len(mv) % _PATTERN_LEN
    if tail and bytes(mv[-tail:]) != pattern[:tail]:
        return False
    return True


def _allocate(backend: FakeLocalCPUBackend) -> FakeMemoryObj:
    shape = backend.metadata.get_shapes()[0]
    dtype = backend.metadata.get_dtypes()[0]
    return backend.allocate(shape, dtype, fmt=None)


async def _with_connector(url: str, fn, *, size: Optional[int] = None):
    """Spin up DingoFSConnector with a backend sized to match args.size.

    The connector's internal _allocate_chunk uses metadata.get_shapes()[0] to
    decide GET buffer size; we shape FakeMetadata around `size` so reads come
    back as full-sized chunks.
    """
    metadata = FakeMetadata.for_size(size) if size else FakeMetadata()
    if _ARENA_MIB > 0:
        from _arena_lmcache import ArenaLocalCPUBackend
        backend = ArenaLocalCPUBackend(metadata=metadata, arena_mib=_ARENA_MIB)
    else:
        backend = FakeLocalCPUBackend(metadata=metadata)
    loop = asyncio.get_running_loop()
    conn = DingoFSConnector(url=url, loop=loop, local_cpu_backend=backend,
                            gflag_conf_path=_CONF_PATH)
    # Surface the actual transport so an A/B run can't silently fall back.
    native = conn._client._native
    from dingofs_connector import _dingofs_native
    logging.info("transport: use_rdma=%s rdma_regions=%d",
                 _dingofs_native.get_flag("use_rdma"),
                 len(native.registered_rdma_regions()))
    try:
        return await fn(conn, backend)
    finally:
        await conn.close()


# ---------------------------------------------------------------------------
# basic commands (put / get / exists / roundtrip / ping)
# ---------------------------------------------------------------------------

async def cmd_ping(args: argparse.Namespace) -> int:
    async def go(conn: DingoFSConnector, _backend):
        ret = await conn.ping()
        print(f"ping: ret={ret}")
        return ret
    return await _with_connector(args.url, go)


async def cmd_put(args: argparse.Namespace) -> int:
    async def go(conn: DingoFSConnector, backend: FakeLocalCPUBackend):
        key = _make_key(args)
        obj = _allocate(backend)
        _fill_pattern(obj.byte_array, args.seed)
        print(
            f"put: key={key.to_string()} "
            f"size={len(obj.byte_array)} seed=0x{args.seed:x}"
        )
        await conn.put(key, obj)
        obj.ref_count_down()  # return the arena slot (connector.put doesn't)
        print("put: ok")
        return 0
    return await _with_connector(args.url, go, size=args.size)


async def cmd_get(args: argparse.Namespace) -> int:
    async def go(conn: DingoFSConnector, _backend):
        key = _make_key(args)
        obj: Optional[FakeMemoryObj] = await conn.get(key)  # type: ignore[assignment]
        if obj is None:
            print(f"get: NOT FOUND key={key.to_string()}")
            return 1
        print(f"get: ok key={key.to_string()} size={len(obj.byte_array)}")
        if args.verify:
            ok = _verify_pattern(obj.byte_array, args.seed)
            print(f"get: verify={'PASS' if ok else 'FAIL'} seed=0x{args.seed:x}")
            return 0 if ok else 1
        return 0
    return await _with_connector(args.url, go, size=args.size)


async def cmd_exists(args: argparse.Namespace) -> int:
    async def go(conn: DingoFSConnector, _backend):
        key = _make_key(args)
        present = await conn.exists(key)
        print(f"exists: key={key.to_string()} -> {present}")
        return 0 if present else 1
    return await _with_connector(args.url, go)


async def cmd_roundtrip(args: argparse.Namespace) -> int:
    async def go(conn: DingoFSConnector, backend: FakeLocalCPUBackend):
        key = _make_key(args)

        src = _allocate(backend)
        _fill_pattern(src.byte_array, args.seed)
        print(f"[1/3] put key={key.to_string()} size={len(src.byte_array)} "
              f"seed=0x{args.seed:x}")
        await conn.put(key, src)
        src.ref_count_down()  # return the arena slot (connector.put doesn't)

        # Server-side AsyncCache is fire-and-forget: the put RPC ACKs as
        # soon as the request is enqueued, but data hits the cache-node disk
        # asynchronously. Retry the GET a few times to give that thread room
        # to land before we conclude the cluster lost the data.
        print(f"[2/3] get key={key.to_string()}")
        got = None
        attempts = 0
        for delay_ms in (0, 20, 50, 100, 250, 500, 1000):
            if delay_ms:
                await asyncio.sleep(delay_ms / 1000)
            try:
                got = await conn.get(key)
            except RuntimeError as e:
                # Transient failures (InvalidParam from storage fallback
                # before AsyncCache lands) — keep retrying.
                got = None
                err_msg = str(e)
            else:
                err_msg = ""
            attempts += 1
            if got is not None:
                if delay_ms:
                    print(f"      (got after {delay_ms} ms delay, "
                          f"attempt #{attempts})")
                break
        if got is None:
            print(f"FAIL: get returned None / errored after {attempts} attempts; "
                  f"last={err_msg!r}")
            return 1

        print("[3/3] verify byte pattern")
        ok = _verify_pattern(got.byte_array, args.seed)
        got.ref_count_down()
        if not ok:
            print("FAIL: byte pattern mismatch")
            return 1
        print("PASS")
        return 0
    return await _with_connector(args.url, go, size=args.size)


# ---------------------------------------------------------------------------
# bench (seq / batch with rounds)
# ---------------------------------------------------------------------------

async def cmd_bench(args: argparse.Namespace) -> int:
    """Put + get benchmark.

    --mode seq   : strict serial via conn.put / conn.get; per-op latency
    --mode batch : conn.batched_put / conn.batched_get; aggregate throughput
                   (use --rounds N for sustained measurement, fio-style)
    """
    rng = (random.Random(args.rand_seed)
           if args.rand_seed is not None else random.Random())
    hashes = [rng.randint(0, 0xFFFFFFFF) for _ in range(args.count)]
    if args.rand_seed is None:
        print("  (random hashes; pass --rand-seed=<int> to reproduce)")
    else:
        print(f"  (deterministic hashes from --rand-seed=0x{args.rand_seed:x})")

    async def go(conn: DingoFSConnector, backend: FakeLocalCPUBackend):
        keys = [_make_key(args, h) for h in hashes]
        size_mib = args.size / (1024 * 1024)
        total_mib_per_round = size_mib * args.count
        print(f"=== bench: mode={args.mode} count={args.count} "
              f"size={args.size} bytes ({size_mib:.2f} MiB each, "
              f"{total_mib_per_round:.2f} MiB per round) ===")
        rounds = max(1, args.rounds) if args.mode == "batch" else 1

        # --- PUT ---
        if args.mode == "seq":
            put_lat, put_wall, put_failed = await _bench_seq_put(
                conn, backend, keys, hashes, args.size, args.seed, size_mib)
        else:
            put_lat, put_wall, put_failed = await _bench_batch_put(
                conn, backend, keys, args.size, args.seed, rounds)
        _print_stats("put", put_lat, args.size, put_wall,
                     put_failed, args.count * rounds, args.mode)

        # AsyncCache is fire-and-forget; let the writes land on the cache node
        # before reading, else the GET phase races and sees misses. Larger
        # blocks take longer to flush, so scale the wait a little with size.
        await asyncio.sleep(3.0)

        # --- GET ---
        if args.mode == "seq":
            get_lat, get_wall, get_failed, get_mismatch = await _bench_seq_get(
                conn, keys, hashes, args.seed, size_mib)
        else:
            get_lat, get_wall, get_failed, get_mismatch = await _bench_batch_get(
                conn, keys, args.seed, rounds)
        _print_stats("get", get_lat, args.size, get_wall,
                     get_failed + get_mismatch,
                     args.count * rounds, args.mode)

        return 0 if (put_failed == 0 and get_failed == 0
                     and get_mismatch == 0) else 1

    return await _with_connector(args.url, go, size=args.size)


async def _bench_seq_put(conn, backend, keys, hashes, size, seed, size_mib):
    del size  # backend's metadata fixes chunk size
    lat_ms, failed = [], 0
    wall_start = time.perf_counter()
    for i, (key, h) in enumerate(zip(keys, hashes)):
        obj = _allocate(backend)
        _fill_pattern(obj.byte_array, seed)
        t0 = time.perf_counter()
        try:
            await conn.put(key, obj)
            elapsed = (time.perf_counter() - t0) * 1000
            lat_ms.append(elapsed)
            print(f"  put #{i:02d} hash=0x{h:08x}: ok   {elapsed:7.2f} ms  "
                  f"({size_mib / (elapsed / 1000):.1f} MiB/s)")
        except Exception as e:
            elapsed = (time.perf_counter() - t0) * 1000
            lat_ms.append(elapsed)
            failed += 1
            print(f"  put #{i:02d} hash=0x{h:08x}: FAIL {elapsed:7.2f} ms  err={e!r}")
        finally:
            obj.ref_count_down()  # return the arena slot
    return lat_ms, (time.perf_counter() - wall_start) * 1000, failed


async def _bench_seq_get(conn, keys, hashes, seed, size_mib):
    lat_ms, failed, mismatch = [], 0, 0
    wall_start = time.perf_counter()
    for i, (key, h) in enumerate(zip(keys, hashes)):
        t0 = time.perf_counter()
        try:
            obj = await conn.get(key)
            elapsed = (time.perf_counter() - t0) * 1000
            lat_ms.append(elapsed)
            if obj is None:
                failed += 1
                print(f"  get #{i:02d} hash=0x{h:08x}: NOT_FOUND {elapsed:7.2f} ms")
            else:
                ok = _verify_pattern(obj.byte_array, seed)
                obj.ref_count_down()
                if not ok:
                    mismatch += 1
                    tag = "MISMATCH"
                else:
                    tag = "ok      "
                print(f"  get #{i:02d} hash=0x{h:08x}: {tag} {elapsed:7.2f} ms  "
                      f"({size_mib / (elapsed / 1000):.1f} MiB/s)")
        except Exception as e:
            elapsed = (time.perf_counter() - t0) * 1000
            lat_ms.append(elapsed)
            failed += 1
            print(f"  get #{i:02d} hash=0x{h:08x}: FAIL {elapsed:7.2f} ms  err={e!r}")
    return lat_ms, (time.perf_counter() - wall_start) * 1000, failed, mismatch


async def _bench_batch_put(conn, backend, keys, size, seed, rounds):
    del size  # backend's metadata fixes chunk size
    per_round_ms, failed = [], 0
    overall_start = time.perf_counter()
    for r in range(rounds):
        # Pool-backed: first round mallocs N chunks, later rounds reuse them
        # via ref_count_down — same as real LMCache LocalCPUBackend.
        objs = [_allocate(backend) for _ in keys]
        for obj in objs:
            _fill_pattern(obj.byte_array, seed)
        t0 = time.perf_counter()
        try:
            await conn.batched_put(keys, objs)
            elapsed = (time.perf_counter() - t0) * 1000
            per_round_ms.append(elapsed)
            print(f"  put round#{r:02d}: ok={len(keys)} fail=0  wall={elapsed:.2f} ms")
        except Exception as e:
            elapsed = (time.perf_counter() - t0) * 1000
            per_round_ms.append(elapsed)
            failed += len(keys)
            print(f"  put round#{r:02d}: FAIL {elapsed:.2f} ms  err={e!r}")
        finally:
            for o in objs:
                o.ref_count_down()  # return arena slots before next round
    return per_round_ms, (time.perf_counter() - overall_start) * 1000, failed


async def _bench_batch_get(conn, keys, seed, rounds):
    per_round_ms, failed, mismatch = [], 0, 0
    overall_start = time.perf_counter()
    for r in range(rounds):
        t0 = time.perf_counter()
        try:
            objs = await conn.batched_get(keys)
            elapsed = (time.perf_counter() - t0) * 1000
            per_round_ms.append(elapsed)
            ok_n = sum(1 for o in objs if o is not None)
            round_failed = len(keys) - ok_n
            failed += round_failed
            round_mismatch = 0
            for o in objs:
                if o is not None:
                    if not _verify_pattern(o.byte_array, seed):
                        round_mismatch += 1
                    o.ref_count_down()
            mismatch += round_mismatch
            print(f"  get round#{r:02d}: ok={ok_n} fail={round_failed} "
                  f"mismatch={round_mismatch}  wall={elapsed:.2f} ms")
        except Exception as e:
            elapsed = (time.perf_counter() - t0) * 1000
            per_round_ms.append(elapsed)
            failed += len(keys)
            print(f"  get round#{r:02d}: FAIL {elapsed:.2f} ms  err={e!r}")
    return per_round_ms, (time.perf_counter() - overall_start) * 1000, failed, mismatch


def _print_stats(op, lat_ms, size_bytes, wall_ms, failed, count, mode):
    if not lat_ms:
        print(f"  {op}: no samples")
        return
    size_mib = size_bytes / (1024 * 1024)
    total_mib = size_mib * count

    if mode == "seq":
        sorted_lat = sorted(lat_ms)
        n = len(sorted_lat)
        avg = sum(sorted_lat) / n
        p50 = sorted_lat[n // 2]
        p99 = sorted_lat[min(n - 1, int(n * 0.99))]
        mn, mx = sorted_lat[0], sorted_lat[-1]
        print(f"  {op} summary: n={n} failed={failed}  min={mn:.2f}  "
              f"p50={p50:.2f}  avg={avg:.2f}  p99={p99:.2f}  max={mx:.2f}  (ms per op)")
        print(f"  {op} throughput: per-op={size_mib / (avg / 1000):.1f} MiB/s  "
              f"aggregate={total_mib / (wall_ms / 1000):.1f} MiB/s  "
              f"wall={wall_ms:.1f} ms")
    else:
        print(f"  {op} summary: count={count} failed={failed}  "
              f"batch_wall={wall_ms:.2f} ms")
        print(f"  {op} throughput: aggregate={total_mib / (wall_ms / 1000):.1f} MiB/s")


# ---------------------------------------------------------------------------
# watch (continuous live bandwidth)
# ---------------------------------------------------------------------------

async def cmd_watch(args: argparse.Namespace) -> int:
    """Continuously fire batches and print live bandwidth (dstat-like).

    --op get : prime a fixed set of keys, then loop GETting them
               (purest sustained read throughput)
    --op put : every round writes a fresh batch of random hashes
               (sustained ingest)

    Ctrl-C stops; the final summary excludes --warmup rounds.
    """
    rng = (random.Random(args.rand_seed)
           if args.rand_seed is not None else random.Random())

    # First Ctrl-C sets a flag; we leave the loop after the current batch.
    # Second Ctrl-C force-exits — covers the case where the current batch
    # never returns (e.g. all cache-group peers down → brpc connect refused
    # → outstanding future never resolves).
    stop = {"flag": False, "presses": 0}
    def _on_sigint(_signo, _frame):
        stop["presses"] += 1
        if stop["presses"] >= 2:
            print("\n  (second Ctrl-C; force exit)", flush=True)
            os._exit(130)
        stop["flag"] = True
        print("\n  (Ctrl-C; will exit after current batch — press again to "
              "force-exit if it's stuck)", flush=True)
    signal.signal(signal.SIGINT, _on_sigint)

    async def go(conn: DingoFSConnector, backend: FakeLocalCPUBackend):
        size_mib = args.size / (1024 * 1024)
        per_call_mib = size_mib * args.count
        round_mib = per_call_mib * args.concurrency
        op = args.op
        conc = max(1, args.concurrency)

        primed_keys = None
        if op == "get":
            # Prime args.count distinct keys; every concurrent call refetches
            # the same primed set (realistic: parallel reads of hot KV chunks).
            primed_hashes = [rng.randint(0, 0xFFFFFFFF) for _ in range(args.count)]
            primed_keys = [_make_key(args, h) for h in primed_hashes]
            print(f"  priming {args.count} keys with {args.size}-byte payload...",
                  flush=True)
            primer_objs = [_allocate(backend) for _ in range(args.count)]
            for o in primer_objs:
                _fill_pattern(o.byte_array, args.seed)
            try:
                await conn.batched_put(primed_keys, primer_objs)
            except Exception as e:
                print(f"  prime FAIL: {e!r}")
                return 1
            for o in primer_objs:
                o.ref_count_down()  # priming done; return arena slots
            # Let the async Cache land before the read loop starts.
            await asyncio.sleep(2.5)

        print(f"  watching: op={op} count={args.count} concurrency={conc} "
              f"size={args.size} ({size_mib:.2f} MiB each, "
              f"{per_call_mib:.2f} MiB per call × {conc} = "
              f"{round_mib:.2f} MiB per round). Ctrl-C to stop.")
        print(f"  {'round':>6}  {'wall':>9}  {'inst':>12}  "
              f"{'window':>14}  {'cumulative':>14}")

        # One coroutine = one batched_put / batched_get RPC. asyncio.gather
        # fires `conc` of these per round; they ride the same connector loop
        # and translate to `conc` independent native RPCs in flight.

        async def _one_put() -> int:
            hashes = [rng.randint(0, 0xFFFFFFFF) for _ in range(args.count)]
            keys = [_make_key(args, h) for h in hashes]
            objs = [_allocate(backend) for _ in range(args.count)]
            for o in objs:
                _fill_pattern(o.byte_array, args.seed)
            await conn.batched_put(keys, objs)
            for o in objs:
                o.ref_count_down()  # return arena slots
            return 0

        async def _one_get() -> int:
            objs = await conn.batched_get(primed_keys)
            fail = sum(1 for o in objs if o is None)
            if args.verify:
                for o in objs:
                    if o is not None and not _verify_pattern(
                            o.byte_array, args.seed):
                        fail += 1
            for o in objs:
                if o is not None:
                    o.ref_count_down()
            return fail

        window: collections.deque = collections.deque(maxlen=args.window)
        per_round_ms: list = []
        total_bytes = 0
        cum_start = time.perf_counter()
        deadline = (time.perf_counter() + args.duration) if args.duration > 0 else None
        round_no = 0

        while not stop["flag"]:
            if deadline is not None and time.perf_counter() >= deadline:
                break

            one = _one_put if op == "put" else _one_get
            t0 = time.perf_counter()
            results = await asyncio.gather(
                *(one() for _ in range(conc)),
                return_exceptions=True,
            )
            wall_ms = (time.perf_counter() - t0) * 1000

            failed = 0
            for r in results:
                if isinstance(r, Exception):
                    failed += args.count
                    if round_no < 3 or round_no % 100 == 0:
                        print(f"  err round#{round_no}: {r!r}")
                else:
                    failed += r

            inst_mibs = round_mib / (wall_ms / 1000)
            window.append((wall_ms, round_mib))

            warm = round_no >= args.warmup
            if warm:
                per_round_ms.append(wall_ms)
                total_bytes += int(round_mib * 1024 * 1024)

            win_wall = sum(w for w, _ in window)
            win_data = sum(d for _, d in window)
            win_mibs = win_data / (win_wall / 1000) if win_wall > 0 else 0
            cum_elapsed = time.perf_counter() - cum_start
            cum_mibs = (total_bytes / (1024 * 1024)) / cum_elapsed if (
                warm and cum_elapsed > 0) else 0

            warm_tag = " " if warm else "W"
            fail_tag = "" if failed == 0 else f"  fail={failed}"
            print(f"{warm_tag} #{round_no:05d}  "
                  f"{wall_ms:7.2f}ms  "
                  f"{inst_mibs:8.1f} MiB/s  "
                  f"win{len(window):02d}: {win_mibs:6.1f} MiB/s  "
                  f"cum: {cum_mibs:6.1f} MiB/s{fail_tag}",
                  flush=True)
            round_no += 1

        print()
        if per_round_ms:
            lat = sorted(per_round_ms)
            n = len(lat)
            avg = sum(lat) / n
            p50 = lat[n // 2]
            p99 = lat[min(n - 1, int(n * 0.99))]
            total_mib = total_bytes / (1024 * 1024)
            wall_s = sum(per_round_ms) / 1000
            print(f"=== summary (excluding {args.warmup} warmup rounds) ===")
            print(f"  rounds counted: {n}  total: {total_mib:.1f} MiB  "
                  f"wall: {wall_s:.2f} s")
            print(f"  per-batch latency: min={lat[0]:.2f}  p50={p50:.2f}  "
                  f"avg={avg:.2f}  p99={p99:.2f}  max={lat[-1]:.2f}  (ms)")
            print(f"  sustained throughput: {total_mib / wall_s:.1f} MiB/s "
                  f"({total_mib / wall_s / 1024:.2f} GiB/s)")
        else:
            print("  no warm rounds recorded — increase --duration or "
                  "decrease --warmup")
        return 0

    return await _with_connector(args.url, go, size=args.size)


# ---------------------------------------------------------------------------
# argparse
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--url", required=True,
                   help="dingofs URL, e.g. dingofs://mds1:6700,mds2:6700/cache_group")
    p.add_argument("--model", default="smoke-client",
                   help="model_name field of the CacheEngineKey")
    p.add_argument("--world-size", type=int, default=1)
    p.add_argument("--worker-id", type=int, default=0)
    p.add_argument("--arena-mib", type=int, default=0,
                   help="if >0, back the connector with a real contiguous "
                        "pinned arena of this size (enables the zero-copy RDMA "
                        "path); 0 uses the bytearray-pool fake backend")
    p.add_argument("--conf", default=None,
                   help="gflags conf file passed to the connector "
                        "(e.g. a file with --use_rdma=true|false)")
    sub = p.add_subparsers(dest="cmd", required=True)

    for name in ("put", "get", "exists", "roundtrip"):
        sp = sub.add_parser(name)
        sp.add_argument("--hash", required=True, type=_hex_int,
                        help="chunk_hash (decimal or 0x-prefixed hex)")
        sp.add_argument("--seed", type=_hex_int, default=0xCAFEBABE,
                        help="seed for deterministic payload (default 0xcafebabe)")
        if name != "exists":
            sp.add_argument("--size", type=int, default=DEFAULT_CHUNK_BYTES,
                            help=f"payload size in bytes (default {DEFAULT_CHUNK_BYTES})")
        if name == "get":
            sp.add_argument("--verify", action="store_true",
                            help="after get, byte-compare against --seed pattern")

    sub.add_parser("ping")

    bench_sp = sub.add_parser(
        "bench",
        help="put + get benchmark; reports latency (seq) or throughput (batch)")
    bench_sp.add_argument("--count", type=int, default=20,
                          help="number of chunks (default 20)")
    bench_sp.add_argument("--size", type=int, default=DEFAULT_CHUNK_BYTES,
                          help="bytes per chunk (default 4 MiB)")
    bench_sp.add_argument("--mode", choices=("seq", "batch"), default="seq",
                          help="seq: serial via conn.put/get (per-op latency); "
                               "batch: conn.batched_put/get (throughput)")
    bench_sp.add_argument("--rounds", type=int, default=1,
                          help="batch-only: back-to-back batches for sustained "
                               "measurement (default 1)")
    bench_sp.add_argument("--rand-seed", type=_hex_int, default=None,
                          help="PRNG seed for hash sampling (omit for fresh)")
    bench_sp.add_argument("--seed", type=_hex_int, default=0xCAFEBABE,
                          help="seed for deterministic payload pattern")

    watch_sp = sub.add_parser(
        "watch",
        help="continuous batch loop with live bandwidth output (dstat-like)")
    watch_sp.add_argument("--op", choices=("put", "get"), default="get")
    watch_sp.add_argument("--count", type=int, default=20,
                          help="keys per single batched_* call")
    watch_sp.add_argument("--concurrency", type=int, default=1,
                          help="number of parallel batched_* coroutines per round "
                               "(asyncio.gather; mimics LMCache fire-and-forget "
                               "where the worker fires N independent put/get "
                               "futures into the same connector loop)")
    watch_sp.add_argument("--size", type=int, default=DEFAULT_CHUNK_BYTES)
    watch_sp.add_argument("--duration", type=float, default=0,
                          help="seconds to run; 0 = until Ctrl-C")
    watch_sp.add_argument("--window", type=int, default=10,
                          help="rolling-avg window size")
    watch_sp.add_argument("--warmup", type=int, default=2,
                          help="rounds excluded from final summary")
    watch_sp.add_argument("--rand-seed", type=_hex_int, default=None)
    watch_sp.add_argument("--seed", type=_hex_int, default=0xCAFEBABE)
    watch_sp.add_argument("--verify", action="store_true",
                          help="byte-verify each GET response (op=get only); "
                               "off by default since it costs ~4ms/4MiB")

    return p


def main() -> None:
    args = build_parser().parse_args()
    global _ARENA_MIB, _CONF_PATH
    _ARENA_MIB = args.arena_mib
    _CONF_PATH = args.conf
    cmd = {
        "put": cmd_put,
        "get": cmd_get,
        "exists": cmd_exists,
        "roundtrip": cmd_roundtrip,
        "ping": cmd_ping,
        "bench": cmd_bench,
        "watch": cmd_watch,
    }[args.cmd]
    rc = asyncio.run(cmd(args))
    sys.exit(rc or 0)


if __name__ == "__main__":
    main()
