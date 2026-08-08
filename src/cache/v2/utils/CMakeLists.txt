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

file(GLOB_RECURSE CACHE_V2_UTILS_SRCS "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")
list(REMOVE_ITEM CACHE_V2_UTILS_SRCS "${CMAKE_CURRENT_SOURCE_DIR}/flags.cc")

add_library(cache_v2_utils ${CACHE_V2_UTILS_SRCS})
target_compile_features(cache_v2_utils PRIVATE cxx_std_20)
target_link_libraries(cache_v2_utils
    cache_v2_status
    dingofs_common
    glog::glog
    ${NUMA_LIBRARY}
)
cache_tune(cache_v2_utils)

add_library(cache_v2_flags flags.cc)
target_compile_features(cache_v2_flags PRIVATE cxx_std_20)
target_link_libraries(cache_v2_flags
    dingofs_common
    dingofs_utils
    gflags::gflags
    glog::glog
)
