#!/usr/bin/env python3
"""Case 40: concurrent reader + writer; writer writes sequentially, reader polls;
final content consistent."""
import multiprocessing as mp
import os
import time
from common import run_case, rand_data, md5, md5_file

CHUNK = 1 << 20
NCHUNK = 32


def writer(path):
    with open(path, "wb") as f:
        for i in range(NCHUNK):
            f.write(rand_data(CHUNK, seed=i))
            f.flush()
            os.fsync(f.fileno())


def reader(path, q):
    """Poll the growing file; verify every fully-written chunk seen so far."""
    bad = 0
    seen = 0
    deadline = time.time() + 120
    while seen < NCHUNK and time.time() < deadline:
        try:
            size = os.path.getsize(path)
        except OSError:
            time.sleep(0.05)
            continue
        avail = size // CHUNK
        if avail > seen:
            with open(path, "rb") as f:
                f.seek(seen * CHUNK)
                for i in range(seen, avail):
                    if f.read(CHUNK) != rand_data(CHUNK, seed=i):
                        bad += 1
            seen = avail
        else:
            time.sleep(0.05)
    q.put((seen, bad))


def case(c, d):
    p = os.path.join(d, "stream")
    q = mp.Queue()
    pr = mp.Process(target=reader, args=(p, q))
    pw = mp.Process(target=writer, args=(p,))
    pr.start()
    pw.start()
    pw.join()
    pr.join(130)
    seen, bad = q.get()
    c.check_eq(seen, NCHUNK, "reader saw all chunks")
    c.check_eq(bad, 0, "no corrupted chunks observed during concurrent read")
    expect = b"".join(rand_data(CHUNK, seed=i) for i in range(NCHUNK))
    c.check_eq(md5_file(p), md5(expect), "final content md5")


if __name__ == "__main__":
    run_case("40_reader_writer_concurrent", case)
