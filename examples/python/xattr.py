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

"""xattr.py — set, get, list, and remove extended attributes."""

import sys
from dingofs import DingofsError
from common import make_client

client = make_client()
try:
    client.mkdir("/demo_dir", 0o755)
    print("[ OK ] MkDir")

    with client.open("/demo_dir/hello.txt", "wb") as f:
        f.write(b"hello")
    print("[ OK ] Create hello.txt")

    # setxattr
    client.setxattr("/demo_dir/hello.txt", "user.author",  "dingodb")
    client.setxattr("/demo_dir/hello.txt", "user.version", "1.0")
    print("[ OK ] SetXattr  user.author  user.version")

    # getxattr
    val = client.getxattr("/demo_dir/hello.txt", "user.author")
    print(f"[ OK ] GetXattr  user.author = {val!r}")

    # listxattr
    keys = client.listxattr("/demo_dir/hello.txt")
    print(f"[ OK ] ListXattr  {len(keys)} keys:")
    for k in keys:
        print(f"         {k}")

    # removexattr
    client.removexattr("/demo_dir/hello.txt", "user.author")
    client.removexattr("/demo_dir/hello.txt", "user.version")
    print("[ OK ] RemoveXattr")

    client.unlink("/demo_dir/hello.txt")
    client.rmdir("/demo_dir")
    print("[ OK ] Cleanup")

except DingofsError as e:
    print(f"[FAIL] {e}", file=sys.stderr)
    sys.exit(1)
finally:
    client.stop()
    print("[ OK ] Stop")
