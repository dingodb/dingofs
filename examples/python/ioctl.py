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

"""ioctl.py — issue FS_IOC_GETFLAGS via ioctl."""

import struct
import sys
from dingofs import DingofsError
from common import make_client

# FS_IOC_GETFLAGS = _IOR('f', 1, long) = 0x80086601 on x86-64
FS_IOC_GETFLAGS = 0x80086601

client = make_client()
try:
    client.mkdir("/demo_dir", 0o755)
    print("[ OK ] MkDir")

    with client.open("/demo_dir/hello.txt", "wb") as f:
        f.write(b"hello")
    print("[ OK ] Create hello.txt")

    # ioctl: returns bytes on success, raises DingofsError on failure
    try:
        out_buf = client.ioctl("/demo_dir/hello.txt", FS_IOC_GETFLAGS, out_size=8)
        flags_val = struct.unpack_from("<I", out_buf)[0]
        print(f"[ OK ] Ioctl FS_IOC_GETFLAGS: 0x{flags_val:08x}")
    except DingofsError as e:
        # Some filesystem backends do not implement ioctl — that is expected.
        print(f"[ -- ] Ioctl not supported by this backend: {e}")

    client.unlink("/demo_dir/hello.txt")
    client.rmdir("/demo_dir")
    print("[ OK ] Cleanup")

except DingofsError as e:
    print(f"[FAIL] {e}", file=sys.stderr)
    sys.exit(1)
finally:
    client.stop()
    print("[ OK ] Stop")
