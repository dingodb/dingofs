# Copyright 2026 DingoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Slow stress workloads retained from the legacy suite."""

import hashlib
import multiprocessing as mp
import os
import random

import pytest

pytestmark = pytest.mark.slow


def _data(size, seed):
    """Match the legacy suite's deterministic data pattern."""
    generator = random.Random(seed)
    chunk = bytes(generator.getrandbits(8) for _ in range(min(size, 65536)))
    return (chunk * (size // len(chunk) + 1))[:size] if chunk else b""


def _big_writer(job):
    directory, writer, size_mb = job
    path = os.path.join(directory, f"big{writer}")
    digest = hashlib.md5()
    with open(path, "wb") as stream:
        for index in range(size_mb):
            data = _data(1024 * 1024, writer * 1000 + index)
            stream.write(data)
            digest.update(data)
    return path, digest.hexdigest()


def _small_worker(job):
    directory, writer = job
    errors = 0
    subdir = os.path.join(directory, f"small{writer}")
    os.makedirs(subdir, exist_ok=True)
    for index in range(300):
        path = os.path.join(subdir, f"f{index}")
        data = _data(1 + (index * 131) % 20000, writer * 500 + index)
        with open(path, "wb") as stream:
            stream.write(data)
        if index % 3 == 0:
            renamed = path + ".renamed"
            os.rename(path, renamed)
            path = renamed
        if index % 5 == 0:
            os.truncate(path, len(data) // 2)
            data = data[:len(data) // 2]
        with open(path, "rb") as stream:
            errors += stream.read() != data
        if index % 4 == 0:
            os.unlink(path)
    return errors


def _otrunc_rewrite(job):
    path, rounds = job
    head = b"H" * 106496
    tail = b"T" * 5531
    errors = 0
    for _ in range(rounds):
        try:
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
            os.write(fd, head)
            os.write(fd, tail)
            os.close(fd)
        except OSError:
            errors += 1
    return errors


def test_chaos_file_operations(test_dir):
    operations = int(os.environ.get("DINGOFS_CHAOS_OPS", "2000"))
    generator = random.Random(int(os.environ.get("DINGOFS_CHAOS_SEED", "51")))
    expected = {}
    mismatches = []
    for step in range(operations):
        operation = generator.choice(
            ("create", "write", "read", "truncate", "rename", "delete", "stat")
        )
        if not expected and operation != "create":
            operation = "create"
        if operation == "create":
            name = f"f{generator.randrange(10000)}"
            data = _data(generator.randrange(65536), step)
            with open(os.path.join(test_dir, name), "wb") as stream:
                stream.write(data)
            expected[name] = bytearray(data)
            continue

        name = generator.choice(list(expected))
        path = os.path.join(test_dir, name)
        data = expected[name]
        if operation == "write":
            offset = generator.randrange(len(data) + 1) if data else 0
            patch = _data(generator.randrange(1, 8192), step)
            with open(path, "r+b") as stream:
                stream.seek(offset)
                stream.write(patch)
            if offset + len(patch) > len(data):
                data.extend(b"\x00" * (offset + len(patch) - len(data)))
            data[offset:offset + len(patch)] = patch
        elif operation == "read":
            with open(path, "rb") as stream:
                if stream.read() != data:
                    mismatches.append((step, name, "read"))
        elif operation == "truncate":
            size = generator.randrange(len(data) + 4096)
            os.truncate(path, size)
            if size <= len(data):
                del data[size:]
            else:
                data.extend(b"\x00" * (size - len(data)))
        elif operation == "rename":
            new_name = f"f{generator.randrange(10000)}"
            if new_name != name:
                os.rename(path, os.path.join(test_dir, new_name))
                expected[new_name] = expected.pop(name)
        elif operation == "delete":
            os.unlink(path)
            del expected[name]
        else:
            if os.path.getsize(path) != len(data):
                mismatches.append((step, name, "stat"))

    assert not mismatches, f"data mismatches: {mismatches[:5]}"
    assert set(os.listdir(test_dir)) == set(expected)
    for name, data in expected.items():
        with open(os.path.join(test_dir, name), "rb") as stream:
            assert stream.read() == data


def test_mixed_big_small_file_stress(test_dir):
    workers = int(os.environ.get("DINGOFS_CONC", "4"))
    size_mb = int(os.environ.get("DINGOFS_STRESS_MB", "128"))
    with mp.Pool(workers * 2) as pool:
        big = pool.map_async(_big_writer, [(test_dir, index, size_mb)
                                           for index in range(workers)])
        small = pool.map_async(_small_worker, [(test_dir, index)
                                               for index in range(workers)])
        big_files = big.get()
        small_errors = small.get()

    assert sum(small_errors) == 0
    for path, expected in big_files:
        actual = hashlib.md5()
        with open(path, "rb") as stream:
            for data in iter(lambda: stream.read(1024 * 1024), b""):
                actual.update(data)
        assert actual.hexdigest() == expected


def test_processes_rewrite_one_file_with_otrunc(test_dir):
    path = os.path.join(test_dir, "shared")
    workers = int(os.environ.get("DINGOFS_CONC", "8"))
    rounds = int(os.environ.get("DINGOFS_OTRUNC_ROUNDS", "3000"))
    with mp.Pool(workers) as pool:
        errors = pool.map(_otrunc_rewrite, [(path, rounds)] * workers)

    assert sum(errors) == 0
    assert os.path.isfile(path)
    size = os.path.getsize(path)
    expected = b"H" * 106496 + b"T" * 5531
    assert 0 <= size <= len(expected)
    with open(path, "rb") as stream:
        content = stream.read()
    assert len(content) == size
    assert content == expected[:size]
