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

file(GLOB_RECURSE BLOCKCACHE_RDMA_SRCS "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")

add_library(blockcache_rdma ${BLOCKCACHE_RDMA_SRCS})

target_compile_features(blockcache_rdma PRIVATE cxx_std_20)

# ibverbs FIRST, and it matters at RUNTIME, not at link time. cache_lib
# transitively drags in librados, which links the STATIC libibverbs.a; those
# symbols cannot dlopen the mlx5 provider, so ibv_get_device_list() quietly
# returns nothing and every rdma path looks like "this box has no HCA". Any
# executable that links both this target and cache_lib needs the same order
# plus -Wl,--allow-multiple-definition (see src/cache/CMakeLists.txt).
target_link_libraries(blockcache_rdma
    ibverbs::ibverbs

    blockcache_infiniband
    blockcache_net
    absl::cleanup
)

blockcache_tune(blockcache_rdma)
