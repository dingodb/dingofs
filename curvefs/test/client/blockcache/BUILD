# Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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

load("//:copts.bzl", "CURVE_TEST_COPTS")

BLOCKCACHE_TEST_DEPS = [
    "//curvefs/src/client:fuse_client_lib",
    "//curvefs/src/client/blockcache:blockcache_lib",
    "//curvefs/test/client/blockcache/helper:blockcache_helper_lib",
    "//curvefs/test/client/blockcache/mock:blockcache_mock_lib",
    "@gtest//:gtest",
]

cc_test(
    name = "test_block_cache",
    srcs = [ "test_block_cache.cpp" ],
    deps = BLOCKCACHE_TEST_DEPS,
    copts = CURVE_TEST_COPTS,
    visibility = ["//visibility:public"],
    linkstatic=1,
)

cc_test(
    name = "test_countdown",
    srcs = [ "test_countdown.cpp" ],
    deps = BLOCKCACHE_TEST_DEPS,
    copts = CURVE_TEST_COPTS,
    visibility = ["//visibility:public"],
    linkstatic=1,
)

cc_test(
    name = "test_disk_cache_layout",
    srcs = [ "test_disk_cache_layout.cpp" ],
    deps = BLOCKCACHE_TEST_DEPS,
    copts = CURVE_TEST_COPTS,
    visibility = ["//visibility:public"],
    linkstatic=1,
)

cc_test(
    name = "test_disk_cache_loader",
    srcs = [ "test_disk_cache_loader.cpp" ],
    deps = BLOCKCACHE_TEST_DEPS,
    copts = CURVE_TEST_COPTS,
    visibility = ["//visibility:public"],
    linkstatic=1,
)

cc_test(
    name = "test_disk_cache_manager",
    srcs = [ "test_disk_cache_manager.cpp" ],
    deps = BLOCKCACHE_TEST_DEPS,
    copts = CURVE_TEST_COPTS,
    visibility = ["//visibility:public"],
    linkstatic=1,
)

cc_test(
    name = "test_disk_cache",
    srcs = [ "test_disk_cache.cpp" ],
    deps = BLOCKCACHE_TEST_DEPS,
    copts = CURVE_TEST_COPTS,
    visibility = ["//visibility:public"],
    linkstatic=1,
)

cc_test(
    name = "test_disk_state_machine",
    srcs = [ "test_disk_state_machine.cpp" ],
    deps = BLOCKCACHE_TEST_DEPS,
    copts = CURVE_TEST_COPTS,
    visibility = ["//visibility:public"],
    linkstatic=1,
)

cc_test(
    name = "test_error",
    srcs = [ "test_error.cpp" ],
    deps = BLOCKCACHE_TEST_DEPS,
    copts = CURVE_TEST_COPTS,
    visibility = ["//visibility:public"],
    linkstatic=1,
)

cc_test(
    name = "test_local_filesystem",
    srcs = [ "test_local_filesystem.cpp" ],
    deps = BLOCKCACHE_TEST_DEPS,
    copts = CURVE_TEST_COPTS,
    visibility = ["//visibility:public"],
    linkstatic=1,
)

cc_test(
    name = "test_log",
    srcs = [ "test_log.cpp" ],
    deps = BLOCKCACHE_TEST_DEPS,
    copts = CURVE_TEST_COPTS,
    visibility = ["//visibility:public"],
    linkstatic=1,
)

cc_test(
    name = "test_lru_cache",
    srcs = [ "test_lru_cache.cpp" ],
    deps = BLOCKCACHE_TEST_DEPS,
    copts = CURVE_TEST_COPTS,
    visibility = ["//visibility:public"],
    linkstatic=1,
)

cc_test(
    name = "test_mem_cache",
    srcs = [ "test_mem_cache.cpp" ],
    deps = BLOCKCACHE_TEST_DEPS,
    copts = CURVE_TEST_COPTS,
    visibility = ["//visibility:public"],
    linkstatic=1,
)
