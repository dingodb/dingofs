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

"""stat.py — query and modify inode attributes (stat / chmod / chown)."""

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

    # stat
    attr = client.stat("/demo_dir/hello.txt")
    print(f"[ OK ] Stat   ino={attr.ino}  length={attr.length}  mode={oct(attr.mode)}")

    # chmod
    out = client.chmod("/demo_dir/hello.txt", 0o600)
    print(f"[ OK ] Chmod  new mode={oct(out.mode)}")

    # chown (pass -1 to leave uid/gid unchanged)
    out2 = client.chown("/demo_dir/hello.txt", -1, -1)
    print(f"[ OK ] Chown  uid={out2.uid}  gid={out2.gid}")

    client.unlink("/demo_dir/hello.txt")
    client.rmdir("/demo_dir")
    print("[ OK ] Cleanup")

except DingofsError as e:
    print(f"[FAIL] {e}", file=sys.stderr)
    sys.exit(1)
finally:
    client.stop()
    print("[ OK ] Stop")
