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

load("//:copts.bzl", "CURVE_DEFAULT_COPTS")

cc_library(
    name = "nebdclient",
    srcs = glob(
        [
            "*.h",
            "*.cpp",
        ],
    ),
    copts = CURVE_DEFAULT_COPTS,
    linkopts = ["-Wl,-rpath=/usr/lib/nebd"],
    visibility = ["//visibility:public"],
    deps = [
        "@brpc//:brpc",
        "@glog//:glog",
        "//include:include-common",
        "//nebd/proto:client_cc_proto",
        "//nebd/src/common:nebd_common",
    ],
)
