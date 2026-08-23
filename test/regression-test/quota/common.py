#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Helpers for quota regression tests.

The tests operate on a mounted DingoFS directory and query the MDS HTTP JSON
service after each filesystem operation.  Override the defaults when testing a
different deployment:
  DINGOFS_MDS_ADDR   (default 10.220.69.5:7801)
  DINGOFS_FS_ID      (default 10000)
  DINGOFS_ROOT_INO   (default 10000000)
"""
import errno
import json
import os
import shutil
import subprocess
import sys
import time
import traceback


MDS_ADDR = os.environ.get("DINGOFS_MDS_ADDR", "10.220.69.5:7801")
FS_ID = int(os.environ.get("DINGOFS_FS_ID", "10000"))
ROOT_INO = int(os.environ.get("DINGOFS_ROOT_INO", "10000000"))
BASE_URL = "http://%s/MDSService" % MDS_ADDR


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


def _short(value):
    text = repr(value)
    return text if len(text) <= 96 else text[:93] + "..."


def _request(path, body):
    cmd = [
        "curl", "-sS", "--max-time", os.environ.get("DINGOFS_CURL_TIMEOUT", "10"),
        BASE_URL + path, "-d", json.dumps(body, separators=(",", ":")),
    ]
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            text=True, check=False)
    if result.returncode != 0:
        raise RuntimeError("curl %s failed: %s" % (path, result.stderr.strip()))
    try:
        response = json.loads(result.stdout)
    except ValueError as e:
        raise RuntimeError("invalid JSON from %s: %r" % (path, result.stdout)) from e
    if not isinstance(response, dict):
        raise RuntimeError("unexpected response from %s: %r" % (path, response))
    return response


def get_fs_quota(fs_id=FS_ID):
    response = _request("/GetFsQuota", {"fs_id": fs_id})
    if "error" in response:
        raise RuntimeError("GetFsQuota(%s) returned %s" % (fs_id, response["error"]))
    return response.get("quota", {})


def get_dir_quota(ino, fs_id=FS_ID):
    response = _request("/GetDirQuota", {"fs_id": fs_id, "ino": int(ino)})
    if "error" in response:
        raise RuntimeError("GetDirQuota(%s) returned %s" % (ino, response["error"]))
    return response.get("quota", {})


def get_error(path, body):
    response = _request(path, body)
    return response.get("error")


def set_dir_quota(ino, max_bytes, max_inodes, fs_id=FS_ID):
    response = _request("/SetDirQuota", {
        "fs_id": fs_id,
        "ino": int(ino),
        "quota": {"max_bytes": int(max_bytes), "max_inodes": int(max_inodes)},
    })
    if "error" in response:
        raise RuntimeError("SetDirQuota(%s) returned %s" % (ino, response["error"]))


def delete_dir_quota(ino, fs_id=FS_ID):
    response = _request("/DeleteDirQuota", {"fs_id": fs_id, "ino": int(ino)})
    if "error" in response and response["error"].get("errcode") != "ENOT_FOUND":
        raise RuntimeError("DeleteDirQuota(%s) returned %s" % (ino, response["error"]))


def quota_field(quota, name):
    """Accept both protobuf JSON spellings used by different brpc versions."""
    if name in quota:
        return quota[name]
    camel = name.split("_")[0] + "".join(part.title() for part in name.split("_")[1:])
    # Proto3 JSON omits scalar fields whose value is zero.
    return quota.get(camel, 0)


def quota_tuple(quota):
    return tuple(quota_field(quota, name) for name in
                 ("max_bytes", "max_inodes", "used_bytes", "used_inodes"))


def quota_version(quota):
    return quota_field(quota, "version")


def wait_for_quota(getter, expected, timeout=None):
    """Wait for asynchronous quota accounting to become visible."""
    timeout = float(timeout or os.environ.get("DINGOFS_QUOTA_WAIT", "20"))
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = getter()
        if expected(last):
            return last
        time.sleep(0.2)
    raise RuntimeError("quota did not converge before timeout; last=%r" % (quota_tuple(last or {}),))


def wait_for_values(getter, values, timeout=None):
    return wait_for_quota(getter, lambda quota: quota_tuple(quota) == tuple(values), timeout)


def fsync_write(path, data, mode="wb"):
    with open(path, mode) as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())


def run_case(name, fn, cleanup=True):
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
        for message in c.failures:
            print("  failed: %s" % message)
    if cleanup and ok:
        shutil.rmtree(d, ignore_errors=True)
    else:
        print("case dir kept: %s" % d)
    sys.exit(0 if ok else 1)


# Keep errno use in this module so callers can import the same test constants
# without depending on platform-specific numeric values.
EDQUOT = getattr(errno, "EDQUOT", 122)
# Some FUSE clients translate the MDS quota error to ENOSPC.
QUOTA_ERRNOS = {EDQUOT, errno.ENOSPC}
