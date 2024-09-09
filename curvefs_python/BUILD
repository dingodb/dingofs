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

# https://docs.bazel.build/versions/master/be/c-cpp.html#cc_library

COPTS = [
    "-DGFLAGS=gflags",
    "-DOS_LINUX",
    "-DSNAPPY",
    "-DHAVE_SSE42",
    "-DNDEBUG",
    "-fno-omit-frame-pointer",
    "-momit-leaf-frame-pointer",
    "-msse4.2",
    "-pthread",
    "-Wsign-compare",
    "-Wno-unused-parameter",
    "-Wno-unused-variable",
    "-Woverloaded-virtual",
    "-Wnon-virtual-dtor",
    "-Wno-missing-field-initializers",
    "-I /usr/include/python3.9",
]

cc_library(
    name = "curvefs",
    srcs = glob(["*.cxx", "*.cpp"]),
    hdrs = glob(["*h"]),
    deps = [
        "@gflags//:gflags",
        "@glog//:glog",
        "@leveldb//:leveldb",
        "@brpc//:brpc",
        "@braft//:braft",
        "@protobuf//:protobuf",
        "@openssl//:openssl",
        "//src/common:curve_common",
        "//src/common:curve_auth",
        "//include/client:include_client",
        "//include:include-common",
        "//proto:nameserver2_cc_proto",
        "//proto:common_cc_proto",
        "//proto:topology_cc_proto",
        "//proto:scan_cc_proto",
        "//proto:chunkserver-cc-protos",
        "//src/client:curve_client"
    ],
    copts = COPTS,
    linkopts = [
        "-lcbd",
        "-lcurve_client",
        "-lcurve_common",
        "-lcurve_auth",
        "-lcurve_concurrent",
        "-lnameserver2_proto",
        "-ltopology_proto",
        "-lcommon_proto",
        "-lscan_proto",
        "-lchunkserver-protos",
        "-lrt",
        "-ldl",
        "-lz",
        "-lpthread",
        "-lunwind",
        "-lstdc++",
        "-lm",
        "-Wl,-rpath=$$ORIGIN",
    ],
    visibility = ["//visibility:public"],
)
