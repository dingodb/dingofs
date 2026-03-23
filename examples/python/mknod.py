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

"""mknod.py — create a file node with mknod."""

import stat
import sys
from dingofs import DingofsError
from common import make_client

client = make_client()
try:
    client.mkdir("/demo_dir", 0o755)
    print("[ OK ] MkDir")

    node_attr = client.mknod("/demo_dir/demo_node", stat.S_IFREG | 0o644)
    print(f"[ OK ] MkNod  ino={node_attr.ino}  mode={oct(node_attr.mode)}")

    client.unlink("/demo_dir/demo_node")
    client.rmdir("/demo_dir")
    print("[ OK ] Cleanup")

except DingofsError as e:
    print(f"[FAIL] {e}", file=sys.stderr)
    sys.exit(1)
finally:
    client.stop()
    print("[ OK ] Stop")
