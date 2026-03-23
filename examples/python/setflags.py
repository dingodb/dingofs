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

"""setflags.py — set and verify inode flags via set_attr(SET_ATTR_FLAGS)."""

import sys
import dingofs
from dingofs import DingofsError, Attr
from common import make_client

# Linux inode flags (linux/fs.h)
FS_NODUMP_FL    = 0x00000040
FS_IMMUTABLE_FL = 0x00000010
FS_APPEND_FL    = 0x00000020


def flags_str(flags: int) -> str:
    if flags == 0:
        return "0x0 (none)"
    parts = [f"0x{flags:x}"]
    if flags & FS_NODUMP_FL:    parts.append("FS_NODUMP_FL")
    if flags & FS_IMMUTABLE_FL: parts.append("FS_IMMUTABLE_FL")
    if flags & FS_APPEND_FL:    parts.append("FS_APPEND_FL")
    return "  ".join(parts)


def assert_flags(client, path: str, expected: int, step: str) -> None:
    attr = client.stat(path)
    print(f"  flags: {flags_str(attr.flags)}")
    if attr.flags != expected:
        raise AssertionError(
            f"{step}: expected 0x{expected:x}, got 0x{attr.flags:x}"
        )
    print(f"[ OK ] {step}")


client = make_client()
try:
    client.mkdir("/flags_demo", 0o755)
    print("[ OK ] MkDir")

    with client.open("/flags_demo/test.txt", "wb") as f:
        f.write(b"test")
    print("[ OK ] Create test.txt")

    # step 1: initial flags should be 0
    print("\n── step 1: initial flags ─────────────────────────────────────")
    assert_flags(client, "/flags_demo/test.txt", 0, "initial flags == 0")

    # step 2: set FS_NODUMP_FL
    print("\n── step 2: set FS_NODUMP_FL ──────────────────────────────────")
    in_attr = Attr()
    in_attr.flags = FS_NODUMP_FL
    out = client.set_attr("/flags_demo/test.txt", dingofs.SET_ATTR_FLAGS, in_attr)
    print(f"[ OK ] SetAttr  out.flags={flags_str(out.flags)}")

    # step 3: verify
    print("\n── step 3: verify FS_NODUMP_FL ───────────────────────────────")
    assert_flags(client, "/flags_demo/test.txt", FS_NODUMP_FL, "flags == FS_NODUMP_FL")

    # step 4: combine flags
    print("\n── step 4: set FS_NODUMP_FL | 0x01 ──────────────────────────")
    in_attr2 = Attr()
    in_attr2.flags = FS_NODUMP_FL | 0x01
    out2 = client.set_attr("/flags_demo/test.txt", dingofs.SET_ATTR_FLAGS, in_attr2)
    print(f"[ OK ] SetAttr  out.flags={flags_str(out2.flags)}")

    # step 5: verify combined
    print("\n── step 5: verify combined flags ─────────────────────────────")
    assert_flags(client, "/flags_demo/test.txt", FS_NODUMP_FL | 0x01,
                 "flags == FS_NODUMP_FL | 0x01")

    # step 6: clear all flags
    print("\n── step 6: clear flags → 0 ───────────────────────────────────")
    in_attr3 = Attr()
    in_attr3.flags = 0
    out3 = client.set_attr("/flags_demo/test.txt", dingofs.SET_ATTR_FLAGS, in_attr3)
    print(f"[ OK ] SetAttr  out.flags={flags_str(out3.flags)}")

    # step 7: verify cleared
    print("\n── step 7: verify cleared ────────────────────────────────────")
    assert_flags(client, "/flags_demo/test.txt", 0, "flags == 0 (cleared)")

    client.unlink("/flags_demo/test.txt")
    client.rmdir("/flags_demo")
    print("\n[ OK ] Cleanup")

except (DingofsError, AssertionError) as e:
    print(f"[FAIL] {e}", file=sys.stderr)
    sys.exit(1)
finally:
    client.stop()
    print("[ OK ] Stop")
