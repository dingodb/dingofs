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

add_library(dingofs_write_mempool
    cpu_local.cc
    write_page_pool.cc
    write_mem_pool.cc
)
target_link_libraries(dingofs_write_mempool
    glog::glog
    gflags::gflags
    brpc::brpc
    fmt::fmt
)

add_executable(write_mem_pool_bench EXCLUDE_FROM_ALL
    bench/write_mem_pool_bench.cc
)
target_link_libraries(write_mem_pool_bench
    dingofs_write_mempool
    cache_common
    glog::glog
    gflags::gflags
)
