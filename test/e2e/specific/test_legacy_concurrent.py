# Copyright 2026 DingoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Slow concurrent filesystem workloads retained from the legacy suite."""

from collections import Counter
import errno
import hashlib
import multiprocessing as mp
import os
import random
import threading
import time

import pytest

from _linux import (FALLOC_FL_COLLAPSE_RANGE, FALLOC_FL_INSERT_RANGE,
                    FALLOC_FL_KEEP_SIZE, FALLOC_FL_PUNCH_HOLE,
                    FALLOC_FL_ZERO_RANGE, fallocate)

pytestmark = pytest.mark.slow

_CHUNK = 1024 * 1024


def _data(size, seed):
    """Match the legacy suite's deterministic data pattern."""
    generator = random.Random(seed)
    chunk = bytes(generator.getrandbits(8) for _ in range(min(size, 65536)))
    return (chunk * (size // len(chunk) + 1))[:size] if chunk else b""


def _write_file(job):
    path, seed, size = job
    data = _data(size, seed)
    with open(path, "wb") as stream:
        stream.write(data)
    return hashlib.md5(data).hexdigest()


def _append_records(job):
    path, writer = job
    fd = os.open(path, os.O_WRONLY | os.O_APPEND)
    try:
        for _ in range(100):
            os.write(fd, bytes([writer]) * 4096)
    finally:
        os.close(fd)


def _stream_writer(path):
    with open(path, "wb") as stream:
        for index in range(32):
            stream.write(_data(_CHUNK, index))
            stream.flush()
            os.fsync(stream.fileno())


def _stream_reader(job):
    path, result = job
    seen = bad = 0
    deadline = time.monotonic() + 120
    while seen < 32 and time.monotonic() < deadline:
        try:
            available = os.path.getsize(path) // _CHUNK
        except OSError:
            time.sleep(0.05)
            continue
        if available == seen:
            time.sleep(0.05)
            continue
        with open(path, "rb") as stream:
            stream.seek(seen * _CHUNK)
            for index in range(seen, available):
                bad += stream.read(_CHUNK) != _data(_CHUNK, index)
        seen = available
    result.put((seen, bad))


def _create_delete(job):
    directory, writer = job
    errors = 0
    for index in range(200):
        path = os.path.join(directory, f"w{writer}_f{index}")
        try:
            with open(path, "wb") as stream:
                stream.write(b"x" * 100)
            if os.path.getsize(path) != 100:
                errors += 1
            if index % 2 == 0:
                os.unlink(path)
        except OSError:
            errors += 1
    return errors


def _rename(job):
    source, target = job
    try:
        os.rename(source, target)
        return None
    except OSError as error:
        return error.errno


def _mkdir_rmdir(job):
    path, seed = job
    errors = []
    generator = random.Random(seed)
    for _ in range(100):
        try:
            (os.mkdir if generator.random() < 0.5 else os.rmdir)(path)
        except OSError as error:
            if error.errno not in {errno.EEXIST, errno.ENOENT, errno.ENOTEMPTY, errno.EBUSY}:
                errors.append(error.errno)
    return errors


def _truncate(job):
    path, seed = job
    generator = random.Random(seed)
    for _ in range(100):
        os.truncate(path, generator.randrange(4 * _CHUNK))


def _pwrite(job):
    path, seed = job
    generator = random.Random(seed)
    fd = os.open(path, os.O_WRONLY)
    try:
        for _ in range(100):
            os.pwrite(fd, b"W" * 4096, generator.randrange(4 * _CHUNK))
    finally:
        os.close(fd)


def test_processes_write_separate_files(test_dir):
    workers = int(os.environ.get("DINGOFS_CONC", "8"))
    size = 4 * _CHUNK
    jobs = [(os.path.join(test_dir, f"f{index}"), index, size)
            for index in range(workers)]
    with mp.Pool(workers) as pool:
        digests = pool.map(_write_file, jobs)

    for (path, _, _), digest in zip(jobs, digests):
        with open(path, "rb") as stream:
            assert hashlib.md5(stream.read()).hexdigest() == digest
        assert os.path.getsize(path) == size


def test_threads_write_disjoint_regions(test_dir):
    import threading

    workers = int(os.environ.get("DINGOFS_CONC", "8"))
    region = 2 * _CHUNK
    path = os.path.join(test_dir, "shared")
    with open(path, "wb") as stream:
        stream.truncate(workers * region)
    fd = os.open(path, os.O_WRONLY)
    data = [_data(region, index) for index in range(workers)]
    errors = []

    def write_region(index):
        try:
            offset = index * region
            for start in range(0, region, 256 * 1024):
                os.pwrite(fd, data[index][start:start + 256 * 1024], offset + start)
        except OSError as error:
            errors.append(error)

    threads = [threading.Thread(target=write_region, args=(index,))
               for index in range(workers)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    os.close(fd)

    assert errors == []
    with open(path, "rb") as stream:
        for index in range(workers):
            stream.seek(index * region)
            assert stream.read(region) == data[index]


def test_processes_append_complete_records(test_dir):
    path = os.path.join(test_dir, "append")
    open(path, "wb").close()
    workers = int(os.environ.get("DINGOFS_CONC", "6"))
    with mp.Pool(workers) as pool:
        pool.map(_append_records, [(path, index + 1) for index in range(workers)])

    assert os.path.getsize(path) == workers * 100 * 4096
    with open(path, "rb") as stream:
        records = [stream.read(4096) for _ in range(workers * 100)]
    assert all(len(set(record)) == 1 for record in records)
    assert Counter(record[0] for record in records) == Counter(
        {index: 100 for index in range(1, workers + 1)}
    )


def test_process_reader_observes_complete_written_chunks(test_dir):
    path = os.path.join(test_dir, "stream")
    result = mp.Queue()
    reader = mp.Process(target=_stream_reader, args=((path, result),))
    writer = mp.Process(target=_stream_writer, args=(path,))
    reader.start()
    writer.start()
    writer.join(130)
    reader.join(130)
    assert writer.exitcode == 0
    assert reader.exitcode == 0
    assert result.get(timeout=1) == (32, 0)

    expected = hashlib.md5()
    for index in range(32):
        expected.update(_data(_CHUNK, index))
    with open(path, "rb") as stream:
        assert hashlib.md5(stream.read()).hexdigest() == expected.hexdigest()


def test_processes_create_delete_files(test_dir):
    workers = int(os.environ.get("DINGOFS_CONC", "8"))
    with mp.Pool(workers) as pool:
        errors = pool.map(_create_delete, [(test_dir, index) for index in range(workers)])

    assert sum(errors) == 0
    remaining = os.listdir(test_dir)
    assert len(remaining) == workers * 100
    assert all(os.path.getsize(os.path.join(test_dir, name)) == 100
               for name in remaining)


def test_processes_race_to_rename_one_source(test_dir):
    source = os.path.join(test_dir, "source")
    with open(source, "wb") as stream:
        stream.write(b"race")
    jobs = [(source, os.path.join(test_dir, f"target{index}")) for index in range(8)]
    with mp.Pool(8) as pool:
        results = pool.map(_rename, jobs)

    assert results.count(None) == 1
    assert all(result in (None, errno.ENOENT) for result in results)
    targets = [target for _, target in jobs if os.path.exists(target)]
    assert len(targets) == 1
    with open(targets[0], "rb") as stream:
        assert stream.read() == b"race"


def test_processes_race_mkdir_rmdir(test_dir):
    path = os.path.join(test_dir, "contended")
    with mp.Pool(8) as pool:
        errors = pool.map(_mkdir_rmdir, [(path, index) for index in range(8)])
    assert errors == [[], [], [], [], [], [], [], []]
    assert not os.path.exists(path) or os.path.isdir(path)


def test_processes_race_truncate_and_write(test_dir):
    path = os.path.join(test_dir, "mixed")
    with open(path, "wb") as stream:
        stream.truncate(4 * _CHUNK)
    with mp.Pool(6) as pool:
        truncate = pool.map_async(_truncate, [(path, index) for index in range(3)])
        write = pool.map_async(_pwrite, [(path, 100 + index) for index in range(3)])
        truncate.get()
        write.get()

    size = os.path.getsize(path)
    assert 0 <= size <= 4 * _CHUNK + 4096
    with open(path, "rb") as stream:
        data = stream.read()
    assert len(data) == size
    assert set(data) <= {0, ord("W")}


def test_threads_mix_read_write_and_truncate(test_dir):
    path = os.path.join(test_dir, "threaded")
    maximum = _CHUNK
    with open(path, "wb") as stream:
        stream.write(_data(maximum, 61))
    errors = []
    stop = threading.Event()

    def reader():
        try:
            while not stop.is_set():
                with open(path, "rb") as stream:
                    stream.read()
                time.sleep(0.0001)
        except OSError as error:
            errors.append(error)

    def writer():
        try:
            for index in range(200):
                with open(path, "ab" if index % 2 == 0 else "wb") as stream:
                    stream.write(_data(4096, index))
        except OSError as error:
            errors.append(error)

    def truncator():
        try:
            fd = os.open(path, os.O_RDWR)
            try:
                for index in range(400):
                    os.ftruncate(fd, (index * 131071) % (maximum + 1))
            finally:
                os.close(fd)
        except OSError as error:
            errors.append(error)

    readers = [threading.Thread(target=reader) for _ in range(2)]
    workers = readers + [threading.Thread(target=writer),
                         threading.Thread(target=truncator)]
    for worker in workers:
        worker.start()
    for worker in workers[2:]:
        worker.join()
    stop.set()
    for worker in readers:
        worker.join()

    assert errors == []
    size = os.path.getsize(path)
    assert 0 <= size <= maximum + 100 * 4096
    with open(path, "rb") as stream:
        assert len(stream.read()) == size


def test_threads_mix_read_write_and_fallocate(test_dir):
    path = os.path.join(test_dir, "fallocate")
    maximum = _CHUNK
    with open(path, "wb") as stream:
        stream.write(_data(maximum, 62))
    errors = []
    stop = threading.Event()
    modes = (0, FALLOC_FL_KEEP_SIZE, FALLOC_FL_ZERO_RANGE,
             FALLOC_FL_PUNCH_HOLE, FALLOC_FL_COLLAPSE_RANGE,
             FALLOC_FL_INSERT_RANGE)

    def reader():
        try:
            while not stop.is_set():
                with open(path, "rb") as stream:
                    stream.read()
                time.sleep(0.0001)
        except OSError as error:
            errors.append(error)

    def writer():
        try:
            fd = os.open(path, os.O_RDWR)
            try:
                for index in range(300):
                    os.pwrite(fd, _data(4096, index),
                              (index * 4093) % (maximum - 4096))
            finally:
                os.close(fd)
        except OSError as error:
            errors.append(error)

    def fallocator():
        try:
            fd = os.open(path, os.O_RDWR)
            try:
                for index in range(300):
                    fallocate(fd, modes[index % len(modes)],
                              (index * 4093) % (maximum - 4096), 4096)
            finally:
                os.close(fd)
        except OSError as error:
            errors.append(error)

    readers = [threading.Thread(target=reader) for _ in range(2)]
    workers = readers + [threading.Thread(target=writer),
                         threading.Thread(target=fallocator)]
    for worker in workers:
        worker.start()
    for worker in workers[2:]:
        worker.join()
    stop.set()
    for worker in readers:
        worker.join()

    assert errors == []
    with open(path, "rb") as stream:
        assert len(stream.read()) == os.path.getsize(path)
