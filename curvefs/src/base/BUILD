#
# Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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
    name = "curvefs_base_lib",
    srcs = glob([
      "cache/*.cpp",
      "file/*.cpp",
      "filepath/*.cpp",
      "hash/*.cpp",
      "math/*.cpp",
      "queue/*.cpp",
      "string/*.cpp",
      "time/*.cpp",
      "timer/*.cpp",
    ]),
    hdrs = glob([
      "cache/*.h",
      "file/*.h",
      "filepath/*.h",
      "hash/*.h",
      "math/*.h",
      "queue/*.h",
      "string/*.h",
      "time/*.h",
      "timer/*.h",
    ]),
    copts = CURVE_DEFAULT_COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "//src/common:curve_common",
        "//src/common/concurrent:curve_concurrent",
        "//curvefs/src/common:threading",
        "@glog//:glog",
        "@gflags//:gflags",
        "@brpc//:brpc",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
    ],
)
