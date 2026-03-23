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

"""read.py — write a file then read it back, demonstrating seek/tell."""

import sys
from dingofs import DingofsError
from common import make_client

DATA = b"Hello DingoFS SDK!"

client = make_client()
try:
    client.mkdir("/demo_dir", 0o755)
    print("[ OK ] MkDir")

    # Write
    with client.open("/demo_dir/hello.txt", "wb") as f:
        f.write(DATA)
    print(f"[ OK ] Write  {len(DATA)} bytes")

    # Read back with seek/tell demo
    with client.open("/demo_dir/hello.txt", "rb") as f:
        # read all at once (size=-1 reads to EOF)
        content = f.read()
        print(f"[ OK ] Read   {len(content)} bytes")
        print(f"         content : [{content.decode()}]")
        print(f"         tell()  : {f.tell()}")

        # seek back to the beginning and read a partial chunk
        f.seek(0)
        print(f"[ OK ] Seek(0)   tell={f.tell()}")
        chunk = f.read(5)
        print(f"[ OK ] Read(5)   [{chunk.decode()}]  tell={f.tell()}")

        # seek from current position
        f.seek(1, 1)   # SEEK_CUR: skip 1 byte
        rest = f.read()
        print(f"[ OK ] Read rest [{rest.decode()}]")

    # Verify
    if content != DATA:
        print("[FAIL] content mismatch", file=sys.stderr)
        sys.exit(1)
    print("[ OK ] Content verified")

    client.unlink("/demo_dir/hello.txt")
    client.rmdir("/demo_dir")
    print("[ OK ] Cleanup")

except DingofsError as e:
    print(f"[FAIL] {e}", file=sys.stderr)
    sys.exit(1)
finally:
    client.stop()
    print("[ OK ] Stop")
