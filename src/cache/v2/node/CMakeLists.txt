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

file(GLOB CACHE_V2_NODE_SRCS "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")
list(REMOVE_ITEM CACHE_V2_NODE_SRCS "${CMAKE_CURRENT_SOURCE_DIR}/main.cc")

add_library(cache_v2_node ${CACHE_V2_NODE_SRCS})
target_compile_features(cache_v2_node PRIVATE cxx_std_20)
target_link_libraries(cache_v2_node
    cache_v2_server
    cache_v2_brpc
    cache_v2_block
    cache_v2_common
    cache_v2_object
    cache_v2_store
    PROTO_OBJS
    gflags::gflags
    glog::glog
)
cache_tune(cache_v2_node)

add_executable(dingo-cache-v2 main.cc)
target_compile_features(dingo-cache-v2 PRIVATE cxx_std_20)
target_link_libraries(dingo-cache-v2
    ibverbs::ibverbs
    cache_v2_node
    cache_v2_flags
)
set_target_properties(dingo-cache-v2 PROPERTIES
    OUTPUT_NAME dingo-cache
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin/v2
)
cache_tune(dingo-cache-v2)
