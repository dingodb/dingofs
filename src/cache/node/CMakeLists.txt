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
# Define the BASE_FLAGS and DINGO_DEFAULT_COPTS variables

# The cache server node. dingo_cache.cc (daemon bootstrap) is built into
# cache_lib, and main.cc is the dingo-cache executable entrypoint; both are
# excluded from the cache_node library.
file(GLOB NODE_LIB_SRCS
    "*.cc"
)
list(REMOVE_ITEM NODE_LIB_SRCS
    "${CMAKE_CURRENT_SOURCE_DIR}/main.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/dingo_cache.cc"
)

add_library(cache_node ${NODE_LIB_SRCS})
target_link_libraries(cache_node
    cache_common
    cache_infiniband
    cache_iutil
    cache_local
)
