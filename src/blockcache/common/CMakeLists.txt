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

file(GLOB BLOCKCACHE_COMMON_SRCS CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")
list(REMOVE_ITEM BLOCKCACHE_COMMON_SRCS "${CMAKE_CURRENT_SOURCE_DIR}/status.cc")

add_library(blockcache_status status.cc)
target_compile_features(blockcache_status PRIVATE cxx_std_20)
target_link_libraries(blockcache_status
    ibverbs::ibverbs

    dingofs_common
    glog::glog
)
blockcache_tune(blockcache_status)

add_library(blockcache_common ${BLOCKCACHE_COMMON_SRCS})
target_compile_features(blockcache_common PRIVATE cxx_std_20)
target_link_libraries(blockcache_common
    blockcache_core
    vfs_meta_mds_rpc_lib
    dingofs_common
    PROTO_OBJS
    brpc::brpc
    gflags::gflags
    glog::glog
)
blockcache_tune(blockcache_common)
