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

"""Shared helpers for DingoFS Python SDK demos.

Edit MDS_ADDRS / FS_NAME / MOUNT_POINT to match your cluster before
running any example.
"""

import dingofs

MDS_ADDRS   = "10.232.10.5:8801"
FS_NAME     = "test03"
MOUNT_POINT = "/dingofs/client/mnt/test03"
LOG_DIR     = "/tmp/dingofs-demo-log"


def make_client() -> dingofs.Client:
    """Configure SDK options, build a Client, and return it.

    Raises :class:`~dingofs.DingofsError` if the cluster is unreachable.
    """
    dingofs.Client.set_option("log_dir",   LOG_DIR)
    dingofs.Client.set_option("log_level", "WARNING")

    client = dingofs.Client.build(
        mds_addrs=MDS_ADDRS,
        fs_name=FS_NAME,
        mount_point=MOUNT_POINT,
        log_dir=LOG_DIR,
    )
    print("[ OK ] Start")
    return client
