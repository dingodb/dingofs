#
#  Copyright (c) 2022 NetEase Inc.
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
    name = "memcached_client_lib",
    srcs = glob(["*.cpp"]),
    hdrs = glob(["*.h"]),
    copts = CURVE_DEFAULT_COPTS,
    linkopts = [
        "-L/usr/local/lib",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//curvefs/proto:curvefs_topology_cc_proto",
        "//curvefs/src/client/common:common",
        "//curvefs/src/client/metric:client_metric",
        "//src/common:curve_common",
        "//src/common:curve_s3_adapter",
        "@com_google_absl//absl/strings",
        "@glog//:glog",
        "@brpc//:brpc",
        "@libmemcached//:memcache"
    ],
)
