#
#  Copyright (c) 2020 NetEase Inc.
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
    name = "integration_cluster_common",
    srcs = glob([
        "cluster.h",
        "cluster.cpp",
    ]),
    copts = CURVE_TEST_COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "//src/common:curve_common",
        "//src/client:curve_client",
        "//src/snapshotcloneserver:snapshot_server_lib",
        "//test/util:test_util",
        "@gflags//:gflags",
        "@glog//:glog",
        "@gtest//:gtest",
    ],
    linkopts = [
    ]
)

cc_test(
  name = "cluster_basic_test",
  srcs = glob(["cluster_basic_test.cpp"]),
  copts = CURVE_TEST_COPTS,
  deps = [
        "//test/integration/cluster_common:integration_cluster_common",
        "@gtest//:gtest",
  ]
)
