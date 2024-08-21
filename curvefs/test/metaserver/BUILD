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

load("//:copts.bzl", "CURVE_TEST_COPTS")

cc_test(
    name = "curvefs_metaserver_test",
    srcs = glob(
        ["*.cpp", "*.h"],
        exclude = [
            "metastore_test.cpp",
            "mock_client_s3.h",
            "mock_metaserver_s3.h",
            "metaserver_s3_adaptor_test.h",
            "metaserver_s3_adaptor_test.cpp",
            "metaserver_s3_test.cpp",
            "mock_s3compact_inode.h",
            "s3compact_test.cpp",
            "mock_s3_adapter.h",
            "partition_clean_test.cpp",
            "recycle*.cpp",
        ],
    ),
    copts = CURVE_TEST_COPTS,
    deps = [
            "//curvefs/src/metaserver:curvefs_metaserver",
            "//curvefs/proto:curvefs_topology_cc_proto",
            "//curvefs/test/metaserver/copyset/mock:metaserver_copyset_test_mock",
            "//curvefs/src/metaserver:metaserver_s3_lib",
            "//curvefs/test/metaserver/storage:metaserver_storage_test_utils",
            "//curvefs/test/metaserver/mock:metaserver_test_mock",
            "//curvefs/test/client/rpcclient:rpcclient_test_mock",
            "@brpc//:brpc",
            "@gtest//:gtest",
            "@com_google_absl//absl/types:optional",
    ],
    linkstatic=1,
)

cc_test(
    name = "curvefs_partition_clean_test",
    srcs = glob([
        "partition_clean_test.cpp",
        "mock_metaserver_s3_adaptor.h",
        "mock_mds_client.h",
    ],
        exclude = [],
    ),
    copts = CURVE_TEST_COPTS,
    deps = [
            "//curvefs/src/metaserver:curvefs_metaserver",
            "//curvefs/proto:curvefs_topology_cc_proto",
            "//curvefs/test/metaserver/copyset/mock:metaserver_copyset_test_mock",
            "//curvefs/src/metaserver:metaserver_s3_lib",
            "//curvefs/test/client/rpcclient:rpcclient_test_mock",
            "//curvefs/test/metaserver/storage:metaserver_storage_test_utils",
            "@gtest//:gtest",
    ],
    linkstatic=1,
)

cc_test(
    name = "curvefs_recycle_test",
    srcs = glob([
        "recycle*.cpp",
    ],
        exclude = [],
    ),
    copts = CURVE_TEST_COPTS,
    deps = [
            "//curvefs/src/metaserver:curvefs_metaserver",
            # "//curvefs/proto:curvefs_topology_cc_proto",
            "//curvefs/test/metaserver/copyset/mock:metaserver_copyset_test_mock",
            # "//curvefs/src/metaserver:metaserver_s3_lib",
            "//curvefs/test/client/rpcclient:rpcclient_test_mock",
            "//curvefs/test/client:mock",
            "//curvefs/test/metaserver/storage:metaserver_storage_test_utils",
            "@gtest//:gtest",
    ],
    linkstatic=1,
)


# s3 adaptor
cc_test(
    name = "metaserver_s3_adaptor_test",
    srcs = glob([
        "mock_metaserver_s3.h",
        "metaserver_s3_adaptor_test.h",
        "metaserver_s3_adaptor_test.cpp",
        "curvefs/test/client/mock_client_s3.h",
        "mock_metaserver_s3_adaptor.h",
        "metaserver_s3_test.cpp"
    ]),
    copts = CURVE_TEST_COPTS,
    defines = ["UNIT_TEST"],
    visibility = ["//visibility:public"],
    deps = [
        "//include/client:include_client",
        "//src/common:curve_s3_adapter",
        "//src/client:curve_client",
        "//curvefs/src/client:fuse_client_lib",
        "//curvefs/src/metaserver:metaserver_s3_lib",
        "//curvefs/test/client:client_s3_test_lib",
        "//test/client/mock:client_mock_lib",
        "//test/common:common_mock",
        "@gtest//:gtest",
        "@brpc//:brpc",
        "@glog//:glog",
        "@gflags//:gflags",
    ],
    linkopts = [
        "-lfuse3",
        "-L/usr/local/lib/x86_64-linux-gnu",
    ],
    linkstatic=1,
)

cc_test(
    name = "curvefs_metaserver_metastore_test",
    srcs = glob(
        ["metastore*"],
    ),
    copts = CURVE_TEST_COPTS,
    deps = [
        "//curvefs/src/metaserver:curvefs_metaserver",
        "//curvefs/test/metaserver/storage:metaserver_storage_test_utils",
        "@brpc//:brpc",
        "@gtest//:gtest",
    ],
    linkstatic=1,
)
