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
   name = "rpcclient_test",
   srcs = glob([
       "*.cpp",
       "*.h"],
   ),
   copts = CURVE_TEST_COPTS,
   deps = [
       "//curvefs/src/client/rpcclient:rpcclient",
       "//curvefs/src/client:filesystem_xattr",
       "//curvefs/proto:curvefs_topology_cc_proto",
       "//curvefs/src/common:curvefs_common",
       "//curvefs/test/utils:curvefs_test_utils",
       "//curvefs/test/client:client_test_utils",
       "@gtest//:gtest",
   ],
   visibility = ["//visibility:public"],
    linkstatic=1,
)

cc_library(
    name = "rpcclient_test_mock",
    hdrs = glob(["*.h"]),
    copts = CURVE_TEST_COPTS + ["-DUNIT_TEST"],
    visibility = ["//visibility:public"],
    deps = [
       "//curvefs/src/client/rpcclient:rpcclient",
       "//curvefs/proto:curvefs_topology_cc_proto",
       "@gtest//:gtest",
    ],
)
