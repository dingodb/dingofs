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

load("//:copts.bzl", "CURVE_DEFAULT_COPTS")

cc_library(
    name = "curvefs_common",
    srcs = glob(
        ["*.cpp"],
        exclude = ["dynamic_vlog.cpp", "threading.cpp"],
    ),
    hdrs = glob(
        ["*.h"],
        exclude = [
            "metric_utils.h",
            "dynamic_vlog.h",
            "threading.h",
        ],
    ),
    copts = CURVE_DEFAULT_COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "//src/common:curve_common",
        "//curvefs/src/client/blockcache:cachestore_lib",
        "@gflags//:gflags",
        "@glog//:glog",
    ],
)

cc_library(
    name = "metric_utils",
    hdrs = [
        "metric_utils.h",
    ],
    copts = CURVE_DEFAULT_COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "@brpc//:brpc"
    ],
)

cc_library(
    name = "dynamic_vlog",
    srcs = [
        "dynamic_vlog.cpp",
    ],
    hdrs = [
        "dynamic_vlog.h",
    ],
    copts = CURVE_DEFAULT_COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "@brpc//:brpc",
        "@gflags//:gflags",
        "@glog//:glog",
    ],
)

cc_library(
    name = "threading",
    srcs = [
        "threading.cpp",
    ],
    hdrs = [
        "threading.h",
    ],
    copts = CURVE_DEFAULT_COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "@glog//:glog",
    ],
)
