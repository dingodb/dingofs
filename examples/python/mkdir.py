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

"""mkdir.py — create a directory, list it, then remove it."""

import sys
from dingofs import DingofsError
from common import make_client

client = make_client()
try:
    # mkdir
    attr = client.mkdir("/demo_dir", 0o755)
    print(f"[ OK ] MkDir  ino={attr.ino}")

    # stat
    looked = client.stat("/demo_dir")
    print(f"[ OK ] Stat   ino={looked.ino}  mode={oct(looked.mode)}")

    # listdir
    entries = client.listdir("/demo_dir")
    print(f"[ OK ] ListDir  {len(entries)} entries")
    for e in entries:
        print(f"         {e.name}  ino={e.ino}")

    # rmdir
    client.rmdir("/demo_dir")
    print("[ OK ] RmDir")

except DingofsError as e:
    print(f"[FAIL] {e}", file=sys.stderr)
    sys.exit(1)
finally:
    client.stop()
    print("[ OK ] Stop")
