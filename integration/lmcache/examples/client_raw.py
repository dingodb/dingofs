#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Raw smoke client — talks to dingofs via the pybind11 module directly.

Skips the full DingoFSConnector / LMCache import chain, so it runs on any
Python the .so was built for (no lmcache install, no Python 3.10+ match
requirement). The wire format (key string + payload bytes) matches what
DingoFSConnector emits, so data written by this client is readable by a
real LMCache-driven DingoFSConnector, and vice versa.

Usage (matches client.py):

    python examples/client_raw.py --url dingofs://mds:6700/grp ping
    python examples/client_raw.py --url dingofs://mds:6700/grp roundtrip --hash 0xcafebabe
    python examples/client_raw.py --url dingofs://mds:6700/grp put       --hash 0xdeadbeef
    python examples/client_raw.py --url dingofs://mds:6700/grp get       --hash 0xdeadbeef --verify
    python examples/client_raw.py --url dingofs://mds:6700/grp exists    --hash 0xdeadbeef
"""

from __future__ import annotations

import argparse
import ctypes
import importlib.util
import logging
import os
import select
import struct
import sys
from pathlib import Path

# --------------------------------------------------------------------------
# Load only the native .so. Bypasses dingofs_connector/__init__.py — which
# would pull in lmcache and fail on Python 3.9 (lmcache uses 3.10 match).
# --------------------------------------------------------------------------

_HERE = Path(__file__).resolve().parent
_PKG_DIR = _HERE.parent / "src" / "dingofs_connector"

_so_candidates = list(_PKG_DIR.glob("_dingofs_native*.so"))
if not _so_candidates:
    sys.exit(
        f"FATAL: no _dingofs_native*.so found under {_PKG_DIR}.\n"
        f"Build it first:\n"
        f"  cd dingofs/build && cmake -DBUILD_LMCACHE_CONNECTOR=ON .. \\\n"
        f"      && make -j _dingofs_native"
    )

_spec = importlib.util.spec_from_file_location("_dingofs_native", _so_candidates[0])
_native = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_native)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

_RDMA_POOL_KEEPALIVE = []
_RDMA_POOL_OFFSET = 0


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------

# Default chunk: 64 KiB (matches FakeMetadata in _fake_lmcache.py — fp16,
# shape [2, 1, 256, 64], 2*1*256*64*2 = 65536 bytes).
DEFAULT_CHUNK_BYTES = 65536
_PATTERN_LEN = 8


def _hex_int(s: str) -> int:
    return int(s, 0)


def _make_key(args: argparse.Namespace) -> str:
    """Render the wire-format key string. Must match exactly what
    DingoFSConnector emits via key_mapper.cache_engine_key_to_native_str."""
    kv_rank = (args.world_size << 24) | (args.worker_id << 16)
    chunk_hash_low = args.hash & 0xFFFFFFFF
    return f"{args.model}@{kv_rank:08x}@{chunk_hash_low:08x}"


def _fill_pattern(mv: memoryview, seed: int) -> None:
    """Bulk-fill — avoid the per-8-byte Python loop (~30x faster on 4 MiB)."""
    pattern = struct.pack("<Q", seed & 0xFFFFFFFFFFFFFFFF)
    full = len(mv) // _PATTERN_LEN
    if full:
        mv[: full * _PATTERN_LEN] = pattern * full
    tail = len(mv) % _PATTERN_LEN
    if tail:
        mv[-tail:] = pattern[:tail]


def _verify_pattern(mv: memoryview, seed: int) -> bool:
    """Bulk-compare — ~30x faster than the per-8-byte Python loop on 4 MiB."""
    pattern = struct.pack("<Q", seed & 0xFFFFFFFFFFFFFFFF)
    full = len(mv) // _PATTERN_LEN
    if full and bytes(mv[: full * _PATTERN_LEN]) != pattern * full:
        return False
    tail = len(mv) % _PATTERN_LEN
    if tail and bytes(mv[-tail:]) != pattern[:tail]:
        return False
    return True


def _parse_url(url: str) -> tuple[str, str]:
    """dingofs://<mds-addrs>/<cache-group>[?gflag=value&...]"""
    scheme = "dingofs://"
    if not url.startswith(scheme):
        raise SystemExit(f"FATAL: not a dingofs:// URL: {url}")
    body = url[len(scheme):]
    if "?" in body:
        body, _ = body.split("?", 1)
    if "/" not in body:
        raise SystemExit(f"FATAL: missing /<cache_group> in {url}")
    mds, grp = body.split("/", 1)
    if not mds or not grp:
        raise SystemExit(f"FATAL: empty mds_addrs or cache_group in {url}")
    return mds, grp


def _open_native(args_or_url):
    global _RDMA_POOL_OFFSET
    url = args_or_url if isinstance(args_or_url, str) else args_or_url.url
    rdma_pool_mib = 0 if isinstance(args_or_url, str) else args_or_url.rdma_pool_mib
    mds, grp = _parse_url(url)
    rdma_pools = []
    if rdma_pool_mib:
        pool = bytearray(rdma_pool_mib * 1024 * 1024)
        addr = ctypes.addressof(ctypes.c_char.from_buffer(pool))
        rdma_pools.append((addr, len(pool)))
        _RDMA_POOL_KEEPALIVE.append(pool)
        _RDMA_POOL_OFFSET = 0
    logging.info("connecting: mds=%s cache_group=%s rdma_pools=%d",
                 mds, grp, len(rdma_pools))
    rc = _native.RemoteCache(
        mds_addrs=mds,
        cache_group=grp,
        conf_file="",
        rdma_pools=rdma_pools,
    )
    if rdma_pools:
        logging.info("rdma: device=%s regions=%s",
                     rc.rdma_device_name(), rc.registered_rdma_regions())
    return rc


def _alloc_payload(size: int):
    global _RDMA_POOL_OFFSET
    if not _RDMA_POOL_KEEPALIVE:
        return bytearray(size)

    pool = _RDMA_POOL_KEEPALIVE[-1]
    end = _RDMA_POOL_OFFSET + size
    if end > len(pool):
        raise SystemExit(
            "FATAL: --rdma-pool-mib is too small for this command "
            f"(need at least {end} bytes)"
        )
    out = memoryview(pool)[_RDMA_POOL_OFFSET:end]
    _RDMA_POOL_OFFSET = end
    return out


def _wait_one_completion(rc, expected_future_id: int, timeout_ms: int = 30_000):
    """Block until one completion for `expected_future_id` arrives.

    Returns (ok: bool, error: str, per_key: list[bool] | None).
    Polls the native eventfd, drains, matches by future_id. Other completions
    (shouldn't happen here since we issue one at a time) are discarded with a
    warning.
    """
    poller = select.poll()
    poller.register(rc.event_fd(), select.POLLIN)
    deadline_left = timeout_ms
    while deadline_left > 0:
        events = poller.poll(min(deadline_left, 500))
        if not events:
            deadline_left -= 500
            continue
        for fid, ok, err, per_key in rc.drain_completions():
            if int(fid) == expected_future_id:
                return bool(ok), str(err), per_key
            logging.warning("ignoring stray completion future_id=%d", fid)
        deadline_left = timeout_ms  # reset on activity
    raise TimeoutError(f"no completion for future_id={expected_future_id} after {timeout_ms} ms")


# --------------------------------------------------------------------------
# commands
# --------------------------------------------------------------------------

def cmd_ping(args: argparse.Namespace) -> int:
    rc = _open_native(args)
    try:
        rc.ping()
        print("ping: ok")
        return 0
    except Exception as e:
        print(f"ping: FAIL {e}")
        return 1
    finally:
        rc.close()


def cmd_put(args: argparse.Namespace) -> int:
    rc = _open_native(args)
    try:
        key = _make_key(args)
        buf = _alloc_payload(args.size)
        _fill_pattern(memoryview(buf), args.seed)
        print(f"put: key={key} size={len(buf)} seed=0x{args.seed:x}")
        fid = rc.submit_batch_set([key], [memoryview(buf)])
        ok, err, per_key = _wait_one_completion(rc, fid)
        if err:
            print(f"put: FAIL {err}")
            return 1
        if not ok or not per_key or not per_key[0]:
            print("put: FAIL (per-key not ok)")
            return 1
        print("put: ok")
        return 0
    finally:
        rc.close()


def cmd_get(args: argparse.Namespace) -> int:
    rc = _open_native(args)
    try:
        key = _make_key(args)
        buf = _alloc_payload(args.size)
        print(f"get: key={key} size={len(buf)}")
        fid = rc.submit_batch_get([key], [memoryview(buf)])
        ok, err, per_key = _wait_one_completion(rc, fid)
        if err:
            print(f"get: FAIL {err}")
            return 1
        if not ok or not per_key or not per_key[0]:
            print("get: NOT FOUND")
            return 1
        print(f"get: ok ({len(buf)} bytes)")
        if args.verify:
            passed = _verify_pattern(memoryview(buf), args.seed)
            print(f"get: verify={'PASS' if passed else 'FAIL'} seed=0x{args.seed:x}")
            return 0 if passed else 1
        return 0
    finally:
        rc.close()


def cmd_exists(args: argparse.Namespace) -> int:
    rc = _open_native(args)
    try:
        key = _make_key(args)
        present = rc.exists_sync(key)
        print(f"exists: key={key} -> {present}")
        return 0 if present else 1
    finally:
        rc.close()


def cmd_bench(args: argparse.Namespace) -> int:
    """Put + get benchmark.

    --mode seq   : strict serial — submit one, wait one, repeat.
                   Measures per-op end-to-end latency.
    --mode batch : submit all N at once, wait for the fan-in completion.
                   Measures aggregate throughput when the client can
                   pipeline many concurrent requests to the cache group.
    """
    import random
    import time

    # Hashes are sampled randomly per run (so we don't keep hitting the same
    # server-side paths across runs and accidentally measure "overwrite a
    # warm file" instead of "fresh put + fresh get"). Pass --rand-seed for
    # a deterministic, reproducible sample.
    rng = random.Random(args.rand_seed) if args.rand_seed is not None else random.Random()
    hashes = [rng.randint(0, 0xFFFFFFFF) for _ in range(args.count)]
    if args.rand_seed is None:
        print(f"  (random hashes; pass --rand-seed=<int> to reproduce)")
    else:
        print(f"  (deterministic hashes from --rand-seed=0x{args.rand_seed:x})")

    rc = _open_native(args)
    try:
        kv_rank = (args.world_size << 24) | (args.worker_id << 16)
        keys = [f"{args.model}@{kv_rank:08x}@{h:08x}" for h in hashes]

        size_mib = args.size / (1024 * 1024)
        total_mib = size_mib * args.count
        print(f"=== bench: mode={args.mode} count={args.count} "
              f"size={args.size} bytes ({size_mib:.2f} MiB each, "
              f"{total_mib:.2f} MiB total) ===")

        # One source buffer reused across N keys. Native zero-copy holds a
        # ref until brpc releases the IOBuf block; the buffer object stays
        # alive for the whole loop scope, so reuse is safe.
        src = _alloc_payload(args.size)
        _fill_pattern(memoryview(src), args.seed)

        rounds = max(1, args.rounds)

        # ---- PUT ----
        if args.mode == "seq":
            put_lat_ms, put_wall_ms, put_failed = _bench_seq(
                rc, "put", keys, hashes,
                [memoryview(src)] * args.count,
                rc.submit_batch_set, size_mib)
        else:
            put_lat_ms, put_wall_ms, put_failed = _bench_batch(
                rc, "put", keys, [memoryview(src)] * args.count,
                rc.submit_batch_set, rounds=rounds)
        _print_stats("put", put_lat_ms, args.size, put_wall_ms,
                     put_failed, args.count * (rounds if args.mode == "batch" else 1),
                     args.mode)

        # ---- GET ----
        dsts = [_alloc_payload(args.size) for _ in range(args.count)]
        mvs = [memoryview(d) for d in dsts]
        if args.mode == "seq":
            get_lat_ms, get_wall_ms, get_failed = _bench_seq(
                rc, "get", keys, hashes, mvs,
                rc.submit_batch_get, size_mib)
        else:
            get_lat_ms, get_wall_ms, get_failed = _bench_batch(
                rc, "get", keys, mvs, rc.submit_batch_get, rounds=rounds)

        # verify (does not count against op timing)
        get_mismatch = sum(
            1 for d in dsts if not _verify_pattern(memoryview(d), args.seed))
        if get_mismatch:
            print(f"  get verify: {get_mismatch} chunks MISMATCH")
        _print_stats("get", get_lat_ms, args.size, get_wall_ms,
                     get_failed + get_mismatch,
                     args.count * (rounds if args.mode == "batch" else 1),
                     args.mode)

        return 0 if (put_failed == 0 and get_failed == 0
                     and get_mismatch == 0) else 1
    finally:
        rc.close()


def _bench_seq(rc, op_name, keys, hashes, bufs, submit_fn, size_mib):
    """Strict serial: submit one, wait one. Each entry timed independently."""
    import time
    lat_ms = []
    failed = 0
    wall_start = time.perf_counter()
    for i, (key, h, buf) in enumerate(zip(keys, hashes, bufs)):
        t0 = time.perf_counter()
        fid = submit_fn([key], [buf])
        ok, err, per_key = _wait_one_completion(rc, fid)
        elapsed_ms = (time.perf_counter() - t0) * 1000
        lat_ms.append(elapsed_ms)
        success = (not err) and ok and per_key and per_key[0]
        if not success:
            failed += 1
            print(f"  {op_name} #{i:02d} hash=0x{h:08x}: "
                  f"FAIL {elapsed_ms:7.2f} ms  err={err!r}")
        else:
            print(f"  {op_name} #{i:02d} hash=0x{h:08x}: "
                  f"ok   {elapsed_ms:7.2f} ms  "
                  f"({size_mib / (elapsed_ms / 1000):.1f} MiB/s)")
    return lat_ms, (time.perf_counter() - wall_start) * 1000, failed


def _bench_batch(rc, op_name, keys, bufs, submit_fn, rounds: int = 1):
    """Submit all N in one call per round; let native fan out concurrently.

    With rounds>1 we measure sustained throughput (like fio): each round is
    one fan-in batch of N ops, executed back-to-back. Reports each round
    individually and an aggregate summary so you can see warm-up vs steady.
    """
    import time
    per_round_ms = []
    total_failed = 0
    overall_start = time.perf_counter()
    for r in range(rounds):
        t0 = time.perf_counter()
        fid = submit_fn(keys, bufs)
        ok, err, per_key = _wait_one_completion(rc, fid, timeout_ms=120_000)
        elapsed_ms = (time.perf_counter() - t0) * 1000
        per_round_ms.append(elapsed_ms)
        if err:
            print(f"  {op_name} round#{r:02d}: FAIL {elapsed_ms:.2f} ms  err={err!r}")
            total_failed += len(keys)
            continue
        failed = sum(1 for b in (per_key or []) if not b)
        total_failed += failed
        print(f"  {op_name} round#{r:02d}: ok={len(keys) - failed} fail={failed}  "
              f"wall={elapsed_ms:.2f} ms")
    overall_wall_ms = (time.perf_counter() - overall_start) * 1000
    return per_round_ms, overall_wall_ms, total_failed


def _print_stats(op: str, lat_ms: list, size_bytes: int, wall_ms: float,
                 failed: int, count: int, mode: str) -> None:
    """Pretty-print latency / throughput summary."""
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
        mn = sorted_lat[0]
        mx = sorted_lat[-1]
        print(
            f"  {op} summary: n={n} failed={failed}  "
            f"min={mn:.2f}  p50={p50:.2f}  avg={avg:.2f}  "
            f"p99={p99:.2f}  max={mx:.2f}  (ms per op)"
        )
        print(
            f"  {op} throughput: per-op={size_mib / (avg / 1000):.1f} MiB/s  "
            f"aggregate={total_mib / (wall_ms / 1000):.1f} MiB/s  "
            f"wall={wall_ms:.1f} ms"
        )
    else:  # batch
        print(
            f"  {op} summary: count={count} failed={failed}  "
            f"batch_wall={wall_ms:.2f} ms"
        )
        print(
            f"  {op} throughput: aggregate={total_mib / (wall_ms / 1000):.1f} MiB/s  "
            f"({count} concurrent ops, {size_mib:.2f} MiB each)"
        )


def cmd_watch(args: argparse.Namespace) -> int:
    """Continuously fire batches and print live bandwidth (like dstat/iostat).

    --op get : prime a set of keys once, then loop forever issuing batched
               GETs over the same keys (purest throughput measurement).
    --op put : loop forever, each round writes a fresh batch of random
               hashes (measures sustained ingest rate).

    Stops on Ctrl-C; prints a final summary that excludes the first --warmup
    rounds so cold-start RTTs don't skew the numbers.
    """
    import collections
    import random
    import signal
    import time

    rng = random.Random(args.rand_seed) if args.rand_seed is not None else random.Random()
    kv_rank = (args.world_size << 24) | (args.worker_id << 16)
    rc = _open_native(args)

    # First Ctrl-C: drop out of the loop after the current batch.
    # Second Ctrl-C: force-exit. Needed when the current batch is wedged
    # (all peers down → outstanding RPC future never resolves).
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

    try:
        size_mib = args.size / (1024 * 1024)
        batch_mib = size_mib * args.count
        op = args.op

        # In get mode: prime a fixed key set once. Subsequent rounds reuse it
        # so every iteration is a real cache-group read (no cache miss noise).
        if op == "get":
            primed_hashes = [rng.randint(0, 0xFFFFFFFF) for _ in range(args.count)]
            primed_keys = [f"{args.model}@{kv_rank:08x}@{h:08x}"
                           for h in primed_hashes]
            primer = _alloc_payload(args.size)
            _fill_pattern(memoryview(primer), args.seed)
            print(f"  priming {args.count} keys with {args.size}-byte payload...",
                  flush=True)
            fid = rc.submit_batch_set(primed_keys, [memoryview(primer)] * args.count)
            ok, err, per_key = _wait_one_completion(rc, fid, timeout_ms=60_000)
            if err or not ok:
                print(f"  prime FAIL: err={err!r}  per_key={per_key}")
                return 1

        # Headers
        print(f"  watching: op={op} count={args.count} size={args.size} "
              f"({size_mib:.2f} MiB each, {batch_mib:.2f} MiB per batch). "
              f"Ctrl-C to stop.")
        print(f"  {'round':>6}  {'wall':>9}  {'inst':>12}  "
              f"{'window':>14}  {'cumulative':>14}")

        window = collections.deque(maxlen=args.window)
        per_round_ms = []   # excludes warmup for final summary
        total_bytes = 0
        cum_start = time.perf_counter()
        deadline = (time.perf_counter() + args.duration) if args.duration > 0 else None
        round_no = 0
        src = _alloc_payload(args.size)
        _fill_pattern(memoryview(src), args.seed)
        dsts = [_alloc_payload(args.size) for _ in range(args.count)]
        mvs = [memoryview(d) for d in dsts]

        while not stop["flag"]:
            if deadline is not None and time.perf_counter() >= deadline:
                break

            if op == "put":
                hashes = [rng.randint(0, 0xFFFFFFFF) for _ in range(args.count)]
                keys = [f"{args.model}@{kv_rank:08x}@{h:08x}" for h in hashes]
                t0 = time.perf_counter()
                fid = rc.submit_batch_set(keys, [memoryview(src)] * args.count)
            else:  # get
                t0 = time.perf_counter()
                fid = rc.submit_batch_get(primed_keys, mvs)

            ok, err, per_key = _wait_one_completion(rc, fid, timeout_ms=120_000)
            wall_ms = (time.perf_counter() - t0) * 1000
            failed = 0 if (ok and not err) else (
                sum(1 for b in (per_key or []) if not b)
                if per_key else args.count)
            # Optional byte-level verify so an all-zero server response doesn't
            # masquerade as success in the throughput numbers.
            if op == "get" and args.verify and ok and not err:
                for dst_buf in dsts:
                    if not _verify_pattern(memoryview(dst_buf), args.seed):
                        failed += 1

            inst_mibs = batch_mib / (wall_ms / 1000)
            window.append((wall_ms, batch_mib))

            warm = round_no >= args.warmup
            if warm:
                per_round_ms.append(wall_ms)
                total_bytes += int(batch_mib * 1024 * 1024)

            win_wall = sum(w for w, _ in window)
            win_data = sum(d for _, d in window)
            win_mibs = win_data / (win_wall / 1000) if win_wall > 0 else 0
            cum_elapsed = time.perf_counter() - cum_start
            cum_mibs = (total_bytes / (1024 * 1024)) / cum_elapsed if (
                warm and cum_elapsed > 0) else 0

            warm_tag = " " if warm else "W"  # "W" marks warm-up rounds
            fail_tag = "" if failed == 0 else f"  fail={failed}"
            print(f"{warm_tag} #{round_no:05d}  "
                  f"{wall_ms:7.2f}ms  "
                  f"{inst_mibs:8.1f} MiB/s  "
                  f"win{len(window):02d}: {win_mibs:6.1f} MiB/s  "
                  f"cum: {cum_mibs:6.1f} MiB/s{fail_tag}",
                  flush=True)
            round_no += 1

        # Final summary (excludes warmup)
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
            print("  no warm rounds recorded — increase duration or decrease --warmup")
        return 0
    finally:
        rc.close()


def cmd_roundtrip(args: argparse.Namespace) -> int:
    rc = _open_native(args)
    try:
        key = _make_key(args)

        # [1/3] put
        src = _alloc_payload(args.size)
        _fill_pattern(memoryview(src), args.seed)
        print(f"[1/3] put key={key} size={len(src)} seed=0x{args.seed:x}")
        fid = rc.submit_batch_set([key], [memoryview(src)])
        ok, err, per_key = _wait_one_completion(rc, fid)
        if err or not ok or not per_key or not per_key[0]:
            print(f"FAIL: put failed: ok={ok} err={err!r} per_key={per_key}")
            return 1

        # [2/3] get
        print(f"[2/3] get key={key}")
        dst = _alloc_payload(args.size)
        fid = rc.submit_batch_get([key], [memoryview(dst)])
        ok, err, per_key = _wait_one_completion(rc, fid)
        if err:
            print(f"FAIL: get error: {err}")
            return 1
        if not ok or not per_key or not per_key[0]:
            print("FAIL: get returned not-found after put — cluster did not persist")
            return 1

        # [3/3] verify
        print("[3/3] verify byte pattern")
        if not _verify_pattern(memoryview(dst), args.seed):
            print("FAIL: byte pattern mismatch")
            return 1
        print("PASS")
        return 0
    finally:
        rc.close()


# --------------------------------------------------------------------------
# argparse
# --------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Raw smoke client for dingofs (no lmcache dependency)."
    )
    p.add_argument("--url", required=True,
                   help="dingofs URL: dingofs://mds1:6700[,mds2:6700,...]/cache_group")
    p.add_argument("--model", default="smoke-client",
                   help="model_name field (default: smoke-client)")
    p.add_argument("--world-size", type=int, default=1)
    p.add_argument("--worker-id", type=int, default=0)
    p.add_argument("--rdma-pool-mib", type=int, default=0,
                   help="allocate and register an RDMA pool before connecting")
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
    bench_sp.add_argument("--size", type=int, default=4 * 1024 * 1024,
                          help="bytes per chunk (default 4 MiB)")
    bench_sp.add_argument("--mode", choices=("seq", "batch"), default="seq",
                          help="seq: submit/wait one-by-one (per-op latency); "
                               "batch: submit all at once (aggregate throughput)")
    bench_sp.add_argument("--rounds", type=int, default=1,
                          help="batch-only: number of back-to-back batches "
                               "to measure sustained throughput (default 1)")

    watch_sp = sub.add_parser(
        "watch",
        help="continuous batch loop with live bandwidth output (dstat-like)")
    watch_sp.add_argument("--op", choices=("put", "get"), default="get",
                          help="put: fresh hashes each round (ingest);  "
                               "get: prime once, then loop reading same keys")
    watch_sp.add_argument("--count", type=int, default=20,
                          help="ops per batch (default 20)")
    watch_sp.add_argument("--size", type=int, default=4 * 1024 * 1024,
                          help="bytes per op (default 4 MiB)")
    watch_sp.add_argument("--duration", type=float, default=0,
                          help="seconds to run; 0 = until Ctrl-C (default 0)")
    watch_sp.add_argument("--window", type=int, default=10,
                          help="rolling window size for moving-avg bandwidth")
    watch_sp.add_argument("--warmup", type=int, default=2,
                          help="rounds to exclude from final summary (default 2)")
    watch_sp.add_argument("--rand-seed", type=_hex_int, default=None,
                          help="PRNG seed (omit for fresh randomness)")
    watch_sp.add_argument("--seed", type=_hex_int, default=0xCAFEBABE,
                          help="payload-pattern seed (default 0xcafebabe)")
    watch_sp.add_argument("--verify", action="store_true",
                          help="byte-verify each GET response (op=get only); "
                               "off by default to focus on throughput")
    bench_sp.add_argument("--rand-seed", type=_hex_int, default=None,
                          help="PRNG seed for hash sampling; omit for a fresh "
                               "random sample each run, pass an int to reproduce")
    bench_sp.add_argument("--seed", type=_hex_int, default=0xCAFEBABE,
                          help="seed for deterministic payload pattern")

    return p


def main() -> None:
    args = build_parser().parse_args()
    cmd = {
        "put": cmd_put,
        "get": cmd_get,
        "exists": cmd_exists,
        "roundtrip": cmd_roundtrip,
        "ping": cmd_ping,
        "bench": cmd_bench,
        "watch": cmd_watch,
    }[args.cmd]
    sys.exit(cmd(args))


if __name__ == "__main__":
    main()
