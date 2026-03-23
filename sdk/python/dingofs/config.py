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

"""Configuration for the DingoFS Python client.

Maps 1-to-1 to the C++ PyDingofsConfig struct defined in
src/client/shim/py_client.h.
"""

from dataclasses import dataclass, field


@dataclass
class Config:
    """Client startup configuration.

    Attributes:
        mds_addrs:   Comma-separated list of MDS addresses, e.g.
                     "10.232.10.5:8801,10.232.10.6:8801".
        fs_name:     Name of the filesystem to mount, e.g. "test03".
        mount_point: Logical mount-point path registered with MDS, e.g.
                     "/dingofs/client/mnt/test03".  Does not have to exist
                     as a real directory on the host.
        conf_file:   Optional path to a client.conf file (gflags format).
                     Each line contains one ``--flag=value`` entry and
                     overrides internal gflag defaults.  Leave empty to
                     rely purely on the fields below.
        log_dir:     Directory for access/meta logs.  Defaults to /tmp when
                     empty.
        log_level:   Logging verbosity: one of DEBUG, INFO, WARNING, ERROR,
                     FATAL.  Defaults to INFO.
        log_v:       Verbose log level (glog -v).  Defaults to 0.
    """

    mds_addrs: str = ""
    fs_name: str = ""
    mount_point: str = ""
    conf_file: str = ""
    log_dir: str = ""
    log_level: str = "INFO"
    log_v: int = 0
