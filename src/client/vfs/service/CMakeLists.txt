# Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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

add_library(vfs_service
    inode_blocks_service.cc
    fuse_stat_service.cc
)

target_link_libraries(vfs_service
    PROTO_OBJS
    vfs_data
    vfs_meta
    vfs_hub
    mds_common
    client_options
    dingofs_status
    brpc::brpc
    fmt::fmt
    glog::glog
    gflags::gflags

)
