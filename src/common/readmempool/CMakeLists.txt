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

# Self-contained read memory pool: buddy over arena (+ slab/TLS). No dependency on common/writemempool/memory_pool.
# Links brpc for bvar::Adder metrics counters (per-thread sharded, lock-free).
add_library(dingofs_read_mempool
    arena.cc
    buddy_allocator.cc
    slab_allocator.cc
    tls_cache.cc
    read_buf.cc
    read_mem_pool.cc
)
target_link_libraries(dingofs_read_mempool
    glog::glog
    gflags::gflags
    brpc::brpc
)
