# Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""gflags_demo.py — demonstrate runtime SDK option API (set/get/list)."""

import os
import sys
import dingofs
from dingofs import DingofsError
from common import MDS_ADDRS, FS_NAME, MOUNT_POINT

LOG_DIR = "/tmp/dingofs-options-demo"
os.makedirs(LOG_DIR, exist_ok=True)

# ── Discover options ─────────────────────────────────────────────────────────

print("=== print_options ===")
dingofs.Client.print_options()
print()

opts = dingofs.Client.list_options()
print(f"list_options: {len(opts)} options available\n")

# ── Read defaults ────────────────────────────────────────────────────────────

print("=== get_option (defaults) ===")
for key in ("log_dir", "log_level", "vfs_meta_rpc_timeout_ms",
            "vfs_access_logging", "logtostderr", "minloglevel"):
    try:
        val = dingofs.Client.get_option(key)
        print(f"  {key:<32} = {val}")
    except DingofsError as e:
        print(f"  {key:<32} ! {e}")
print()

# ── Tune parameters ──────────────────────────────────────────────────────────

print("=== set_option ===")
tuning = [
    ("log_dir",                  LOG_DIR),
    ("log_level",                "WARNING"),
    ("vfs_meta_rpc_timeout_ms",  "3000"),
    ("vfs_meta_rpc_retry_times", "5"),
    ("vfs_access_logging",       "false"),
    ("logtostderr",              "false"),
    ("minloglevel",              "1"),
]
for key, val in tuning:
    dingofs.Client.set_option(key, val)
    print(f"[ OK ] set_option  {key} = {val}")

# Unknown key must raise DingofsError
try:
    dingofs.Client.set_option("not_exist_flag", "1")
    print("[FAIL] unknown key should have raised", file=sys.stderr)
    sys.exit(1)
except DingofsError as e:
    print(f"[ OK ] unknown key correctly rejected: {e}\n")

# ── Start client and run a quick smoke test ───────────────────────────────────

client = dingofs.Client.build(
    mds_addrs=MDS_ADDRS,
    fs_name=FS_NAME,
    mount_point=MOUNT_POINT,
    log_dir=LOG_DIR,
)
print("[ OK ] Start")

try:
    client.mkdir("/options_demo", 0o755)
    print("[ OK ] MkDir")

    with client.open("/options_demo/hello.txt", "wb") as f:
        n = f.write(b"hello options demo")
        print(f"[ OK ] Write  {n} bytes")

    client.unlink("/options_demo/hello.txt")
    client.rmdir("/options_demo")
    print("[ OK ] Cleanup")

except DingofsError as e:
    print(f"[FAIL] {e}", file=sys.stderr)
    sys.exit(1)
finally:
    client.stop()
    print("[ OK ] Stop")

# ── Verify logs landed in the configured directory ───────────────────────────

log_files = list(os.scandir(LOG_DIR))
if not log_files:
    print(f"[FAIL] no log files in {LOG_DIR}", file=sys.stderr)
    sys.exit(1)
print(f"\n[ OK ] {len(log_files)} log file(s) in {LOG_DIR}:")
for e in log_files:
    print(f"         {e.name}  ({e.stat().st_size} bytes)")
