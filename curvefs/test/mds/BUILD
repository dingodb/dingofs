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
    name = "curvefs_mds_test",
    srcs = glob(["*.cpp", "*.h"],
        exclude = [
            "chunkid_allocator_test.cpp",
            "mock_etcdclient.h",
        ]),
    copts = CURVE_TEST_COPTS,
    deps = [
            "//curvefs/src/mds:curvefs_mds_lib",
            "//src/common:curve_common",
            "//curvefs/proto:space_cc_proto",
            "//curvefs/test/mds/mock:curvefs_mds_mock",
            "//test/common:common_mock",
            "@brpc//:brpc",
            "@gtest//:gtest",
    ],
    linkstatic=1,
)

cc_test(
    name = "curvefs_chunkid_allocator_test",
    srcs = glob([
        "chunkid_allocator_test.cpp",
        "mock_etcdclient.h"
    ]),
    copts = CURVE_TEST_COPTS,
    deps = [
            "//curvefs/src/mds:curvefs_mds_lib",
            "//src/common:curve_common",
            "//curvefs/proto:space_cc_proto",
            "//curvefs/test/mds/mock:curvefs_mds_mock",
            "@brpc//:brpc",
            "@gtest//:gtest",
    ],
    linkstatic=1,
)

cc_test(
    name = "curvefs_metaserver_balance_poc_test",
    srcs = glob([
        "metaserver_balance_poc.cpp",
        "mock_etcdclient.h"
    ]),
    copts = CURVE_TEST_COPTS,
    deps = [
            "//curvefs/src/mds/topology:curvefs_topology",
            "//curvefs/test/mds/mock:curvefs_mds_mock",
            "//curvefs/src/mds/schedule:curvefs_schedule",
            "@gtest//:gtest",
    ],
    linkstatic=1,
)