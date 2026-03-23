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

"""scandir.py — demonstrate client.scandir() and its context-manager form."""

import sys
from dingofs import DingofsError
from common import make_client

DEMO_DIR = "/scandir_demo"

client = make_client()
try:
    # Create demo directory and some children
    client.mkdir(DEMO_DIR, 0o755)
    client.mkdir(f"{DEMO_DIR}/subdir", 0o755)
    with client.open(f"{DEMO_DIR}/file1.txt", "wb") as f:
        f.write(b"hello")
    with client.open(f"{DEMO_DIR}/file2.txt", "wb") as f:
        f.write(b"world")
    print(f"[ OK ] Created {DEMO_DIR} with 1 sub-directory and 2 files")

    # --- Basic for-loop iteration ---
    print("\n-- for-loop iteration --")
    for entry in client.scandir(DEMO_DIR):
        print(f"  {entry.name}  ino={entry.ino}")

    # --- Context-manager form: early break, handle released immediately ---
    print("\n-- context-manager form (break on first sub-directory) --")
    with client.scandir(DEMO_DIR) as it:
        for entry in it:
            print(f"  {entry.name}  ino={entry.ino}")
            if entry.name == "subdir":
                print("  (found subdir, stop early — remaining entries never fetched)")
                break
        # directory handle is released here by __exit__, even though
        # iteration was not finished

    # Cleanup
    client.unlink(f"{DEMO_DIR}/file1.txt")
    client.unlink(f"{DEMO_DIR}/file2.txt")
    client.rmdir(f"{DEMO_DIR}/subdir")
    client.rmdir(DEMO_DIR)
    print(f"\n[ OK ] Cleaned up {DEMO_DIR}")

except DingofsError as e:
    print(f"[FAIL] {e}", file=sys.stderr)
    sys.exit(1)
finally:
    client.stop()
    print("[ OK ] Stop")
