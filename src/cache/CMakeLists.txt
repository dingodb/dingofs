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

add_subdirectory(common)
add_subdirectory(utils)
add_subdirectory(metric)
add_subdirectory(storage)
add_subdirectory(blockcache)
add_subdirectory(cachegroup)
add_subdirectory(remotecache)
add_subdirectory(tiercache)
add_subdirectory(benchmark)

file(GLOB CACHE_LIB_SRCS
    "dingo_cache.cc"
)
add_library(cache_lib ${CACHE_LIB_SRCS})
target_link_libraries(cache_lib
    cache_utils
    cache_common
    cache_metric
    cache_storage
    cache_blockcache
    cache_cachegroup
    cache_remotecache
    cache_tiercache
    dingofs_options

    ${CPU_PROFILER_LIB}
)

add_executable(dingo-cache main.cc)
target_link_libraries(dingo-cache
    cache_lib

    ${CPU_PROFILER_LIB}
)
