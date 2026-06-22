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
add_subdirectory(iutil)
add_subdirectory(infiniband)
add_subdirectory(local)
add_subdirectory(cachegroup)
add_subdirectory(remotecache)
add_subdirectory(tiercache)
#add_subdirectory(benchmark)

file(GLOB CACHE_LIB_SRCS
    "dingo_cache.cc"
)
add_library(cache_lib ${CACHE_LIB_SRCS})
target_link_libraries(cache_lib
    cache_common
    cache_iutil
    cache_infiniband
    cache_local
    cache_cachegroup
    cache_remotecache
    cache_tiercache
    dingofs_options
)

add_executable(dingo-cache main.cc)
# NOTE: cache_lib transitively drags in librados, which links the STATIC
# libibverbs.a. Those static rdma-core symbols cannot dlopen the mlx5 provider
# plugin, so ibv_get_device_list() returns no devices and the RDMA slab pool
# (required by the on-disk cache) fails to initialize. List the SHARED
# libibverbs first so its (dynamic) verbs symbols win, and allow the duplicate
# definitions from the static copy.
target_link_libraries(dingo-cache
    ibverbs::ibverbs

    cache_lib
)
target_link_options(dingo-cache PRIVATE
    "-Wl,--allow-multiple-definition"
)
