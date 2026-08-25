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

file(GLOB BLOCKCACHE_NODE_SRCS CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")
list(REMOVE_ITEM BLOCKCACHE_NODE_SRCS "${CMAKE_CURRENT_SOURCE_DIR}/main.cc")

add_library(blockcache_node ${BLOCKCACHE_NODE_SRCS})
target_compile_features(blockcache_node PRIVATE cxx_std_20)
target_link_libraries(blockcache_node
    blockcache_net
    brpc::brpc
    blockcache_infiniband
    blockcache_block
    blockcache_common
    blockcache_object
    blockcache_store
    PROTO_OBJS
    gflags::gflags
    glog::glog
)
blockcache_tune(blockcache_node)

add_executable(blockcache-node main.cc)
target_compile_features(blockcache-node PRIVATE cxx_std_20)
target_link_libraries(blockcache-node
    ibverbs::ibverbs
    blockcache_node
    blockcache_flags
)
set_target_properties(blockcache-node PROPERTIES
    OUTPUT_NAME dingo-cache
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin/blockcache
)
blockcache_tune(blockcache-node)
