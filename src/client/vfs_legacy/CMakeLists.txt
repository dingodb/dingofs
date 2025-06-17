# Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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

add_subdirectory(common)
add_subdirectory(datastream)
add_subdirectory(kvclient)
add_subdirectory(lease)

file(GLOB VFS_OLD_SRC_LIB
    "filesystem/*.cpp"
    "s3/*.cpp"
    "service/*.cpp"
    "warmup/*.cpp"
    "*.cpp"
)

add_library(vfs_legacy_lib ${VFS_OLD_SRC_LIB})

target_link_libraries(vfs_legacy_lib
    PROTO_OBJS
    vfs_legacy_common
    dingofs_executor
    rpcclient
    dingofs_common
    client_lease
    client_options
    client_memcached_client
    dynamic_vlog
    vfs_meta
    vfs_legacy_datastream
    cache_lib
    dingofs_utils
    block_accesser
    dingofs_metrics
    stub_common
    gflags::gflags
    glog::glog
    fmt::fmt
    spdlog::spdlog
    brpc::brpc
    absl::memory
    absl::strings
    absl::synchronization
    absl::str_format
    absl::type_traits
    absl::optional
    absl::btree
    ${FUSE3_LIBRARY}
)
