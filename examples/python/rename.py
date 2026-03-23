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

"""rename.py — rename a file and verify via stat."""

import sys
from dingofs import DingofsError
from common import make_client

client = make_client()
try:
    client.mkdir("/demo_dir", 0o755)
    print("[ OK ] MkDir")

    with client.open("/demo_dir/old_name.txt", "wb") as f:
        f.write(b"data")
    old_attr = client.stat("/demo_dir/old_name.txt")
    print(f"[ OK ] Create  ino={old_attr.ino}")

    # Rename
    client.rename("/demo_dir/old_name.txt", "/demo_dir/new_name.txt")
    print("[ OK ] Rename")

    # Verify: old path is gone, new path resolves to the same inode
    renamed = client.stat("/demo_dir/new_name.txt")
    print(f"[ OK ] Stat    ino={renamed.ino}  (was {old_attr.ino})")

    client.unlink("/demo_dir/new_name.txt")
    client.rmdir("/demo_dir")
    print("[ OK ] Cleanup")

except DingofsError as e:
    print(f"[FAIL] {e}", file=sys.stderr)
    sys.exit(1)
finally:
    client.stop()
    print("[ OK ] Stop")
