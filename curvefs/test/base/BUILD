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

TEST_BASE_DPES = [
    "//curvefs/src/base:curvefs_base_lib",
    "@gtest//:gtest",
]

cc_test(
    name = "test_cache",
    deps = TEST_BASE_DPES,
    srcs = [ "test_cache.cpp" ],
    copts = CURVE_TEST_COPTS,
    visibility = ["//visibility:public"],
    linkstatic=1,
)

cc_test(
    name = "test_filepath",
    srcs = [ "test_filepath.cpp" ],
    deps = TEST_BASE_DPES,
    copts = CURVE_TEST_COPTS,
    visibility = ["//visibility:public"],
    linkstatic=1,
)

cc_test(
    name = "test_ketama_con_hash",
    srcs = [ "test_ketama_con_hash.cpp" ],
    deps = TEST_BASE_DPES,
    copts = CURVE_TEST_COPTS,
    visibility = ["//visibility:public"],
    linkstatic=1,
)

cc_test(
    name = "test_math",
    srcs = [ "test_math.cpp" ],
    deps = TEST_BASE_DPES,
    copts = CURVE_TEST_COPTS,
    visibility = ["//visibility:public"],
    linkstatic=1,
)

cc_test(
    name = "test_message_queue",
    srcs = [ "test_message_queue.cpp" ],
    deps = TEST_BASE_DPES,
    copts = CURVE_TEST_COPTS,
    visibility = ["//visibility:public"],
    linkstatic=1,
)

cc_test(
    name = "test_string",
    srcs = [ "test_string.cpp" ],
    deps = TEST_BASE_DPES,
    copts = CURVE_TEST_COPTS,
    visibility = ["//visibility:public"],
    linkstatic=1,
)

cc_test(
    name = "test_time",
    srcs = [ "test_time.cpp" ],
    deps = TEST_BASE_DPES,
    copts = CURVE_TEST_COPTS,
    visibility = ["//visibility:public"],
    linkstatic=1,
)

cc_test(
    name = "test_timer_impl",
    srcs = [ "test_timer_impl.cpp" ],
    deps = TEST_BASE_DPES,
    copts = CURVE_TEST_COPTS,
    visibility = ["//visibility:public"],
    linkstatic=1,
)
