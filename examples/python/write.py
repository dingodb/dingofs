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

"""write.py — create a file and write data to it."""

import sys
from dingofs import DingofsError
from common import make_client

DATA = b"Hello DingoFS SDK!"

client = make_client()
try:
    client.mkdir("/demo_dir", 0o755)
    print("[ OK ] MkDir")

    # open() returns a DingoFile; the with-block calls flush + release on exit.
    with client.open("/demo_dir/hello.txt", "wb") as f:
        n = f.write(DATA)
        print(f"[ OK ] Write  {n} bytes  (mode={f.mode!r}  path={f.name!r})")
        f.flush()
        print("[ OK ] Flush")
        f.fsync()
        print("[ OK ] Fsync")
    print("[ OK ] Close  (flush + release)")

    client.unlink("/demo_dir/hello.txt")
    client.rmdir("/demo_dir")
    print("[ OK ] Cleanup")

except DingofsError as e:
    print(f"[FAIL] {e}", file=sys.stderr)
    sys.exit(1)
finally:
    client.stop()
    print("[ OK ] Stop")
