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

# Real end-to-end cache integration tests: a gtest process spawns real dingo-mds
# and dingo-cache binaries (posix_spawn) and drives the cache client over real
# RPC. Requires an active RDMA device and pre-built dingo-mds / dingo-cache.
#
#   deploy/      - shared harness (process spawning, MDS, cache node)
#   node/        - cache node lifecycle: startup, registration, deregister
#   local/       - client-side local on-disk cache: Put/Range
#   distributed/ - client -> remote cache node: Put/Range/Cache/Prefetch

# So that #include "test/integration/cache/deploy/..." resolves.
include_directories(${CMAKE_SOURCE_DIR})

add_subdirectory(deploy)
add_subdirectory(node)
add_subdirectory(local)
add_subdirectory(distributed)

# Single entry point, mirroring test/unit/cache: main.cc plus all suites compile
# into one binary, selected at run time with --gtest_filter (e.g. 'NodeTest.*',
# 'LocalCacheTest.*', 'DistributedCacheTest.*').
add_executable(integration_test_cache
    main.cc
    $<TARGET_OBJECTS:integration_cache_node_tests>
    $<TARGET_OBJECTS:integration_cache_local_tests>
    $<TARGET_OBJECTS:integration_cache_distributed_tests>
)

# ibverbs::ibverbs must precede the libs that transitively drag in the STATIC
# libibverbs.a (via librados), so the dynamic verbs symbols win and the RDMA
# slab pool can dlopen the mlx5 provider. --allow-multiple-definition tolerates
# the duplicate verbs definitions.
target_link_libraries(integration_test_cache
    ibverbs::ibverbs
    integration_cache_deploy
    ${TEST_INTEGRATION_DEPS_WITHOUT_MAIN}
)

target_link_options(integration_test_cache PRIVATE
    "-Wl,--allow-multiple-definition"
)

set_target_properties(integration_test_cache PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${TEST_EXECUTABLE_OUTPUT_PATH}
)
