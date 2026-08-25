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

# The share-nothing runtime: one reactor, one io_uring and one arena per
# pinned core, plus the WorkerPool -- the one way OFF a reactor. It lives here
# rather than a layer up because every layer needs it: the transports have
# bulk copies to get off the shard just as much as the block cache has S3
# calls, and a facility they all reach cannot sit above any of them.
# Deliberately has no ibverbs and no brpc on its link line, so "the runtime
# builds and tests without a network" stays checkable.

file(GLOB_RECURSE BLOCKCACHE_CORE_SRCS CONFIGURE_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/reactor/*.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/fs/*.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/runtime/*.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/memory/*.cc"
)
add_library(blockcache_core ${BLOCKCACHE_CORE_SRCS})
target_compile_features(blockcache_core PRIVATE cxx_std_20)
target_link_libraries(blockcache_core
    blockcache_utils
    uring::uring
)
blockcache_tune(blockcache_core)
