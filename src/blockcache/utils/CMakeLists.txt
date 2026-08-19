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

file(GLOB_RECURSE BLOCKCACHE_UTILS_SRCS "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")
list(REMOVE_ITEM BLOCKCACHE_UTILS_SRCS "${CMAKE_CURRENT_SOURCE_DIR}/flags.cc")

add_library(blockcache_utils ${BLOCKCACHE_UTILS_SRCS})
target_compile_features(blockcache_utils PRIVATE cxx_std_20)
target_link_libraries(blockcache_utils
    blockcache_status
    dingofs_common
    glog::glog
    ${NUMA_LIBRARY}
)
blockcache_tune(blockcache_utils)

add_library(blockcache_flags flags.cc)
target_compile_features(blockcache_flags PRIVATE cxx_std_20)
target_link_libraries(blockcache_flags
    dingofs_common
    dingofs_utils
    gflags::gflags
    glog::glog
)
