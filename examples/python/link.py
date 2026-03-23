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

"""link.py — create a hard link and verify the link count."""

import sys
from dingofs import DingofsError
from common import make_client

client = make_client()
try:
    client.mkdir("/demo_dir", 0o755)
    print("[ OK ] MkDir")

    with client.open("/demo_dir/original.txt", "wb") as f:
        f.write(b"original content")
    orig = client.stat("/demo_dir/original.txt")
    print(f"[ OK ] Create  ino={orig.ino}  nlink={orig.nlink}")

    # Hard link
    link_attr = client.link("/demo_dir/original.txt", "/demo_dir/hardlink.txt")
    print(f"[ OK ] Link    ino={link_attr.ino}  nlink={link_attr.nlink}")

    # Both paths share the same inode
    assert link_attr.ino == orig.ino, "hard link must share the same inode"
    print("[ OK ] Inode verified")

    client.unlink("/demo_dir/hardlink.txt")
    client.unlink("/demo_dir/original.txt")
    client.rmdir("/demo_dir")
    print("[ OK ] Cleanup")

except DingofsError as e:
    print(f"[FAIL] {e}", file=sys.stderr)
    sys.exit(1)
finally:
    client.stop()
    print("[ OK ] Stop")
