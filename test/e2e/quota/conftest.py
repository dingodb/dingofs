# Copyright 2026 DingoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Quota control-plane helpers for FUSE end-to-end tests."""

import json
import os
import shutil
import subprocess
import time
import uuid

import pytest


class QuotaApi:
    def __init__(self, config):
        self.fs_id = config["fs_id"]
        self.root_ino = config["root_ino"]
        self.base_url = f"http://{config['mds_addr']}/MDSService"

    def request(self, method, body, allow_error=False):
        result = subprocess.run(
            [
                "curl", "-sS", "--max-time",
                os.environ.get("DINGOFS_CURL_TIMEOUT", "10"),
                f"{self.base_url}/{method}", "-d",
                json.dumps(body, separators=(",", ":")),
            ],
            capture_output=True,
            check=False,
            text=True,
        )
        if result.returncode:
            raise RuntimeError(f"curl {method} failed: {result.stderr.strip()}")
        response = json.loads(result.stdout)
        if not isinstance(response, dict):
            raise RuntimeError(f"unexpected response from {method}: {response!r}")
        if "error" in response and not allow_error:
            raise RuntimeError(f"{method} returned {response['error']}")
        return response

    def get_fs_quota(self):
        return self.request("GetFsQuota", {"fs_id": self.fs_id}).get("quota", {})

    def get_dir_quota(self, ino):
        return self.request(
            "GetDirQuota", {"fs_id": self.fs_id, "ino": int(ino)}
        ).get("quota", {})

    def get_error(self, method, body):
        return self.request(method, body, allow_error=True).get("error")

    def set_dir_quota(self, ino, max_bytes, max_inodes):
        self.request("SetDirQuota", {
            "fs_id": self.fs_id,
            "ino": int(ino),
            "quota": {"max_bytes": max_bytes, "max_inodes": max_inodes},
        })

    def delete_dir_quota(self, ino):
        error = self.get_error(
            "DeleteDirQuota", {"fs_id": self.fs_id, "ino": int(ino)}
        )
        if error and error.get("errcode") != "ENOT_FOUND":
            raise RuntimeError(f"DeleteDirQuota({ino}) returned {error}")

    @staticmethod
    def values(quota):
        def get(name):
            if name in quota:
                return quota[name]
            camel = name.split("_")[0] + "".join(
                part.title() for part in name.split("_")[1:]
            )
            return quota.get(camel, 0)

        return tuple(get(name) for name in (
            "max_bytes", "max_inodes", "used_bytes", "used_inodes"
        ))

    def wait_for(self, getter, predicate):
        deadline = time.monotonic() + float(
            os.environ.get("DINGOFS_QUOTA_WAIT", "20")
        )
        last = None
        while time.monotonic() < deadline:
            last = getter()
            if predicate(last):
                return last
            time.sleep(0.2)
        raise AssertionError(
            f"quota did not converge; actual={self.values(last or {})!r}"
        )

    def wait_for_values(self, getter, expected):
        deadline = time.monotonic() + float(
            os.environ.get("DINGOFS_QUOTA_WAIT", "20")
        )
        last = None
        while time.monotonic() < deadline:
            last = getter()
            if self.values(last) == tuple(expected):
                return last
            time.sleep(0.2)
        raise AssertionError(
            f"quota did not converge; expected={expected!r}, "
            f"actual={self.values(last or {})!r}"
        )

    def wait_for_stable_values(self, getter):
        """Wait for five unchanged seconds before using a quota baseline."""
        deadline = time.monotonic() + float(
            os.environ.get("DINGOFS_QUOTA_WAIT", "20")
        )
        previous = self.values(getter())
        unchanged_since = time.monotonic()
        while time.monotonic() < deadline:
            time.sleep(0.2)
            current = self.values(getter())
            if current != previous:
                previous = current
                unchanged_since = time.monotonic()
            elif time.monotonic() - unchanged_since >= 5:
                return current
        raise AssertionError(f"quota did not stabilize; actual={previous!r}")


@pytest.fixture
def quota_api(quota_config):
    return QuotaApi(quota_config)


@pytest.fixture
def quota_dir(request, mount_point, quota_api):
    """An isolated directory whose inode charge has reached quota accounting."""
    before = quota_api.wait_for_stable_values(quota_api.get_fs_quota)
    directory = os.path.join(mount_point, f"test_{uuid.uuid4().hex[:8]}")
    os.makedirs(directory)
    expected = before[:2] + (before[2], before[3] + 1)
    try:
        quota_api.wait_for_values(quota_api.get_fs_quota, expected)
    except Exception:
        print(f"case dir kept: {directory}")
        raise

    yield directory

    reports = (getattr(request.node, f"rep_{phase}", None)
               for phase in ("setup", "call"))
    if any(report and report.failed for report in reports):
        print(f"case dir kept: {directory}")
    else:
        shutil.rmtree(directory, ignore_errors=True)
        quota_api.wait_for_values(quota_api.get_fs_quota, before)
