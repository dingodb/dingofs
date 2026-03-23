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

"""symlink.py — create a symbolic link and read back its target."""

import sys
from dingofs import DingofsError
from common import make_client, MOUNT_POINT

client = make_client()
try:
    client.mkdir("/demo_dir", 0o755)
    print("[ OK ] MkDir")

    with client.open("/demo_dir/target.txt", "wb") as f:
        f.write(b"target content")
    print("[ OK ] Create target.txt")

    # symlink(target_path, link_path)
    # target is the absolute path as seen through the DingoFS mount point
    target = f"{MOUNT_POINT}/demo_dir/target.txt"
    sym_attr = client.symlink(target, "/demo_dir/link.txt")
    print(f"[ OK ] Symlink  ino={sym_attr.ino}")

    # readlink
    resolved = client.readlink("/demo_dir/link.txt")
    print(f"[ OK ] ReadLink  target={resolved}")

    client.unlink("/demo_dir/link.txt")
    client.unlink("/demo_dir/target.txt")
    client.rmdir("/demo_dir")
    print("[ OK ] Cleanup")

except DingofsError as e:
    print(f"[FAIL] {e}", file=sys.stderr)
    sys.exit(1)
finally:
    client.stop()
    print("[ OK ] Stop")
