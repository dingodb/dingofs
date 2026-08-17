#!/usr/bin/env python3
"""Case 53 (slow): concurrent O_TRUNC rewrite of one shared file from N processes.

Reproduces the dingo-client FATAL in chunk.cc Reset() during the
Open(O_TRUNC) vs. in-flight WriteSlice race. Each process repeatedly opens the
same file with O_WRONLY|O_CREAT|O_TRUNC, writes head then tail, and closes.
Pass criteria: no OS errors, filesystem stays usable, final content is a prefix
of H*HEAD_SZ + T*TAIL_SZ.

Env: DINGOFS_CONC (default 8), DINGOFS_OTRUNC_ROUNDS (default 3000)."""
import multiprocessing as mp
import os
from common import run_case

HEAD_SZ = 106496
TAIL_SZ = 5531


def worker(args):
    path, head_sz, tail_sz, rounds = args
    head = b"H" * head_sz
    tail = b"T" * tail_sz
    errs = 0
    for _ in range(rounds):
        try:
            # O_TRUNC 是关键：内核 atomic_o_trunc 随 OPEN 下发，
            # 客户端 DoOpen 同步执行 chunk_set->Reset()。
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
            os.write(fd, head)
            os.write(fd, tail)
            os.close(fd)
        except OSError:
            errs += 1
    return errs


def case(c, d):
    p = os.path.join(d, "shared")
    n = int(os.environ.get("DINGOFS_CONC", "8"))
    rounds = int(os.environ.get("DINGOFS_OTRUNC_ROUNDS", "3000"))
    with mp.Pool(n) as pool:
        errs = pool.map(worker, [(p, HEAD_SZ, TAIL_SZ, rounds)] * n)
    c.check_eq(sum(errs), 0, "no OS errors during concurrent O_TRUNC rewrite")
    c.check(os.path.isfile(p), "target file exists and mount point usable after race")
    size = os.path.getsize(p)
    c.check(0 <= size <= HEAD_SZ + TAIL_SZ, "final size sane: %d" % size)
    with open(p, "rb") as f:
        content = f.read()
    c.check_eq(len(content), size, "read length matches stat size")
    expected = b"H" * HEAD_SZ + b"T" * TAIL_SZ
    c.check(content == expected[:len(content)],
            "content is prefix of H*%d+T*%d" % (HEAD_SZ, TAIL_SZ))


if __name__ == "__main__":
    run_case("53_concurrent_otrunc_rewrite", case)
