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
    name = "snapshot_server_lib",
    srcs = glob([
        "clone/*.cpp",
        "clone/*.h",
        "snapshot/*.cpp",
        "snapshot/*.h",
        "common/*.cpp",
        "common/*.h",
        "*.cpp",
        "*.h"
    ],
    exclude = [
        "main.cpp",
    ]),
    copts = CURVE_DEFAULT_COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "//proto:snapshotcloneserver_cc_proto",
        "//src/common:curve_common",
        "//src/common/concurrent:curve_dlock",
        "//src/common:curve_s3_adapter",
        "//src/common/snapshotclone:curve_snapshotclone",
        "//proto:nameserver2_cc_proto",
        "//proto:chunkserver-cc-protos",
        "//src/client:curve_client",
        "//src/leader_election:leader_election",
        "@brpc//:brpc",
        "@gflags//:gflags",
        "@leveldb//:leveldb",
        "@protobuf//:protobuf",
        "@jsoncpp//:jsoncpp",
    ],
)

cc_binary(
    name = "snapshotcloneserver",
    srcs = glob([
        "clone/*.cpp",
        "clone/*.h",
        "snapshot/*.cpp",
        "snapshot/*.h",
        "common/*.cpp",
        "common/*.h",
        "*.cpp",
        "*.h"
    ]),
    copts = CURVE_DEFAULT_COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "//proto:snapshotcloneserver_cc_proto",
        "//src/common:curve_common",
        "//src/common/concurrent:curve_dlock",
        "//src/common:curve_s3_adapter",
        "//src/common/snapshotclone:curve_snapshotclone",
        "//proto:nameserver2_cc_proto",
        "//proto:chunkserver-cc-protos",
        "//src/client:curve_client",
        "//src/leader_election:leader_election",
        "@brpc//:brpc",
        "@gflags//:gflags",
        "@leveldb//:leveldb",
        "@protobuf//:protobuf",
        "@jsoncpp//:jsoncpp",
    ],
)
