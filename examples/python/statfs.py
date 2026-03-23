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

"""statfs.py — query filesystem-level usage statistics."""

import sys
from dingofs import DingofsError
from common import make_client

client = make_client()
try:
    stat = client.statfs()
    print("[ OK ] StatFs")
    print(f"  max_bytes:   {stat.max_bytes:>20,}")
    print(f"  used_bytes:  {stat.used_bytes:>20,}")
    print(f"  max_inodes:  {stat.max_inodes:>20,}")
    print(f"  used_inodes: {stat.used_inodes:>20,}")
except DingofsError as e:
    print(f"[FAIL] {e}", file=sys.stderr)
    sys.exit(1)
finally:
    client.stop()
    print("[ OK ] Stop")
