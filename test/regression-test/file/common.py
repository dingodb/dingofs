#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Common helpers for DingoFS file operation tests.

Test root directory resolution order:
  1. First command line argument
  2. Env var DINGOFS_TEST_DIR
  3. ./dingofs_test (cwd)
Each test case runs inside its own temporary subdirectory.
"""
import hashlib
import os
import random
import shutil
import string
import sys
import time
import traceback


def get_test_root():
    if len(sys.argv) > 1 and not sys.argv[1].startswith("-"):
        root = sys.argv[1]
    else:
        root = os.environ.get("DINGOFS_TEST_DIR", os.path.join(os.getcwd(), "dingofs_test"))
    os.makedirs(root, exist_ok=True)
    return root


def make_case_dir(name):
    d = os.path.join(get_test_root(), "%s_%d_%d" % (name, os.getpid(), int(time.time())))
    if os.path.exists(d):
        shutil.rmtree(d)
    os.makedirs(d)
    return d


def rand_bytes(n, seed=None):
    rnd = random.Random(seed)
    return bytes(rnd.getrandbits(8) for _ in range(min(n, 1 << 20))) * (1 if n <= (1 << 20) else 1)


def rand_data(n, seed=None):
    """Random bytes of length n (efficient for large n)."""
    rnd = random.Random(seed)
    chunk = bytes(rnd.getrandbits(8) for _ in range(min(n, 65536)))
    if n <= 65536:
        return chunk[:n]
    reps = n // len(chunk) + 1
    return (chunk * reps)[:n]


def md5(data):
    return hashlib.md5(data).hexdigest()


def md5_file(path, bufsize=1 << 20):
    h = hashlib.md5()
    with open(path, "rb") as f:
        while True:
            b = f.read(bufsize)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def rand_name(n=8):
    return "".join(random.choice(string.ascii_lowercase + string.digits) for _ in range(n))


class Checker(object):
    def __init__(self, case_name):
        self.case = case_name
        self.failures = []
        self.checks = 0

    def check(self, cond, msg):
        self.checks += 1
        if cond:
            print("  [OK]   %s" % msg)
        else:
            print("  [FAIL] %s" % msg)
            self.failures.append(msg)
        return cond

    def check_eq(self, actual, expect, msg):
        return self.check(actual == expect,
                          "%s (expect=%r actual=%r)" % (msg, _short(expect), _short(actual)))

    def check_raises(self, errnos, fn, msg):
        self.checks += 1
        try:
            fn()
        except OSError as e:
            if not errnos or e.errno in errnos:
                print("  [OK]   %s (errno=%d %s)" % (msg, e.errno, os.strerror(e.errno)))
                return True
            print("  [FAIL] %s (unexpected errno=%d %s)" % (msg, e.errno, os.strerror(e.errno)))
            self.failures.append(msg)
            return False
        print("  [FAIL] %s (no exception raised)" % msg)
        self.failures.append(msg)
        return False


def _short(v):
    s = repr(v)
    return s if len(s) <= 64 else s[:61] + "..."


def run_case(name, fn, cleanup=True):
    """Run a test case function fn(checker, case_dir). Exit 0 on pass, 1 on fail."""
    print("=" * 60)
    print("CASE: %s" % name)
    print("=" * 60)
    d = make_case_dir(name)
    c = Checker(name)
    ok = True
    try:
        fn(c, d)
    except Exception:
        traceback.print_exc()
        ok = False
    if c.failures:
        ok = False
    print("-" * 60)
    if ok:
        print("RESULT: PASS (%d checks)" % c.checks)
    else:
        print("RESULT: FAIL (%d/%d checks failed)" % (len(c.failures), c.checks))
        for m in c.failures:
            print("  failed: %s" % m)
    if cleanup and ok:
        shutil.rmtree(d, ignore_errors=True)
    else:
        print("case dir kept: %s" % d)
    sys.exit(0 if ok else 1)
