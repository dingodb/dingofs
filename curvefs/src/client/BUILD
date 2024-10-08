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

load("//:copts.bzl", "BASE_FLAGS", "CURVE_DEFAULT_COPTS")

cc_binary(
    name = "curve-fuse",
    srcs = ["main.cpp"],
    copts = BASE_FLAGS + [
        "-I/usr/local/include/fuse3",
    ],
    linkopts = [
        "-lfuse3",
        "-L/usr/local/lib/x86_64-linux-gnu",
    ],
    deps = [
        ":fuse_client_lib",
        "@com_google_absl//absl/memory",
    ],
)

cc_library(
    name = "filesystem_xattr",
    hdrs = ["filesystem/xattr.h"],
    copts = CURVE_DEFAULT_COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "@brpc//:brpc",
    ],
)

cc_library(
    name = "fuse_client_lib",
    srcs = glob(
        [
            "*.cpp",
            "*.c",
            "*.h",
            "s3/*.cpp",
            "s3/*.h",
            "volume/*.cpp",
            "volume/*.h",
            "filesystem/*.cpp",
            "filesystem/*.h",
            "warmup/*.cpp",
            "warmup/*.h",
            "service/*.cpp",
            "service/*.h",
        ],
        exclude = ["main.cpp"],
    ),
    copts = CURVE_DEFAULT_COPTS + ["-I/usr/local/include/fuse3"],
    linkopts = [
        "-lfuse3",
        "-L/usr/local/lib/x86_64-linux-gnu",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//curvefs/src/base:curvefs_base_lib",
        "//curvefs/proto:mds_cc_proto",
        "//curvefs/proto:metaserver_cc_proto",
        "//curvefs/proto:space_cc_proto",
        "//curvefs/proto:curvefs_client_h2_cc_proto",
        "//curvefs/src/client/rpcclient",
        "//curvefs/src/common:curvefs_common",
        "//curvefs/src/client/lease:curvefs_lease",
        "//curvefs/src/client/kvclient:memcached_client_lib",
        "//curvefs/src/client/datastream:datastream_lib",
        "//curvefs/src/client/blockcache:blockcache_lib",
        ":filesystem_xattr",
        "//src/client:curve_client",
        "//src/common:curve_common",
        "//src/common:curve_s3_adapter",
        "//curvefs/src/volume",
        "//curvefs/src/common:metric_utils",
        "//curvefs/src/common:dynamic_vlog",
        "//curvefs/src/common:threading",
        "@brpc//:brpc",
        "@gflags//:gflags",
        "@glog//:glog",
        "@gtest//:gtest",
        "@fmt//:fmt",
        "@spdlog//:spdlog",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/meta:type_traits",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/container:btree",
    ],
)
