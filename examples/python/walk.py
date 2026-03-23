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

"""walk.py — demonstrate client.walk() with topdown=True and topdown=False."""

import sys
from dingofs import DingofsError
from common import make_client

DEMO_ROOT = "/walk_demo"

client = make_client()
try:
    # Build a two-level directory tree:
    #   /walk_demo/
    #     a.txt  b.txt
    #     sub/
    #       c.txt
    client.mkdir(DEMO_ROOT, 0o755)
    client.mkdir(f"{DEMO_ROOT}/sub", 0o755)
    for name in ("a.txt", "b.txt"):
        with client.open(f"{DEMO_ROOT}/{name}", "wb") as f:
            f.write(b"data")
    with client.open(f"{DEMO_ROOT}/sub/c.txt", "wb") as f:
        f.write(b"data")
    print(f"[ OK ] Created tree under {DEMO_ROOT}")

    # --- topdown=True (default) ---
    print("\n-- walk topdown=True --")
    for dirpath, dirnames, filenames in client.walk(DEMO_ROOT):
        indent = "  " * dirpath.count("/")
        print(f"{indent}{dirpath}/")
        for fname in filenames:
            print(f"{indent}  {fname}")

    # --- topdown=False ---
    print("\n-- walk topdown=False --")
    for dirpath, dirnames, filenames in client.walk(DEMO_ROOT, topdown=False):
        indent = "  " * dirpath.count("/")
        print(f"{indent}{dirpath}/")
        for fname in filenames:
            print(f"{indent}  {fname}")

    # Cleanup
    client.unlink(f"{DEMO_ROOT}/sub/c.txt")
    client.rmdir(f"{DEMO_ROOT}/sub")
    client.unlink(f"{DEMO_ROOT}/a.txt")
    client.unlink(f"{DEMO_ROOT}/b.txt")
    client.rmdir(DEMO_ROOT)
    print(f"\n[ OK ] Cleaned up {DEMO_ROOT}")

except DingofsError as e:
    print(f"[FAIL] {e}", file=sys.stderr)
    sys.exit(1)
finally:
    client.stop()
    print("[ OK ] Stop")
