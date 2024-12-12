#
#  Copyright (c) 2021 NetEase Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

load("//:copts.bzl", "CURVE_TEST_COPTS")

cc_library(
    name = "curvefs_test_utils",
    srcs = ["protobuf_message_utils.cpp"],
    hdrs = ["protobuf_message_utils.h"],
    copts = CURVE_TEST_COPTS + ["-DUNIT_TEST"],
    visibility = ["//visibility:public"],
    deps = [
         "@protobuf//:protobuf",       
    ],
)

cc_test(
    name = "common-test",
    srcs = glob([
        "*.cpp",
        ],
        exclude = ["protobuf_message_utils.cpp"],
    ),
    deps = [
        "//curvefs/src/utils:curve_common",
        "//curvefs/src/utils:curve_auth",
        "//curvefs/src/aws:aws_s3_adapter",
        "//curvefs/src/utils/concurrent:curve_concurrent",
        "//curvefs/src/mds/kvstorageclient:fs_mds_kvstorage_client",
        "@gtest//:gtest",
    ],
    copts = CURVE_TEST_COPTS,
)
