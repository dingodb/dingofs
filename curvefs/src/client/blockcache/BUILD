#
#  Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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

load("//:copts.bzl", "BASE_FLAGS", "CURVE_DEFAULT_COPTS")

cc_library(
    name = "cachestore_lib",
    hdrs = [
        "cache_store.h",
        "error.h"
    ],
    copts = CURVE_DEFAULT_COPTS,
    visibility = ["//visibility:public"],
)

cc_library(
    name = "blockcache_lib",
    srcs = glob(["*.cpp"]),
    hdrs = glob(["*.h"]),
    copts = CURVE_DEFAULT_COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "//src/common:curve_common",
        "//src/common:curve_s3_adapter",
        "//curvefs/src/base:curvefs_base_lib",
        "//curvefs/src/common:curvefs_common",
        "//curvefs/src/client/common:common",
        "//curvefs/src/client/metric:client_metric",
        "@glog//:glog",
        "@gflags//:gflags",
        "@brpc//:brpc",
        "@spdlog//:spdlog",
        "@com_google_absl//absl/cleanup",
    ],
)
