# Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


load("//:copts.bzl", "CURVE_DEFAULT_COPTS")

cc_library(
    name = "mdsv2_lib",
    srcs = glob(
        ["*.cc"],
        exclude = [
            "main.cc",
        ],
    ) + glob(
        ["common/*.cc"],
    ) + glob(
        ["storage/*.cc"],
    ) + glob(
        ["cache/*.cc"],
    )+ glob(
        ["service/*.cc"],
    ),
    hdrs = glob(
        ["*.h"],
    ) + glob(
        ["common/*.h"],
    ) + glob(
        ["storage/*.h"],
    ) + glob(
        ["cache/*.h"],
    ) + glob(
        ["service/*.h"],
    ),
    copts = CURVE_DEFAULT_COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "//curvefs/proto:error_cc_proto",
        "//curvefs/proto:mdsv2_cc_proto",
        "//curvefs/src/utils:curve_common",
        "@fmt//:fmt",
        "@gflags//:gflags",
        "@glog//:glog",
        "@gtest//:gtest",
        "@brpc//:brpc",
        "@rocksdb//:rocksdb",
        "@protobuf//:protobuf",
        "@sdk//:sdk",
        "@com_google_absl//absl/cleanup",
        "@com_google_absl//absl/container:btree",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/utility",
        "@com_google_absl//absl/strings",
    ],
)


cc_binary(
    name = "mdsv2",
    srcs = ["main.cc"],
    copts = CURVE_DEFAULT_COPTS,
    visibility = ["//visibility:public"],
    deps = [
        ":mdsv2_lib",
    ],
)