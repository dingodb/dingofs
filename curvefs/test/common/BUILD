#
#  Copyright (c) 2023 NetEase Inc.
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
    name = "curvefs-common-test",
    srcs = glob([
        "*.cpp",
    ]),
    deps = [
        "//curvefs/src/common:curvefs_common",
        "@brpc//:brpc",
        "@gtest//:gtest",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/utility",
    ],
    copts = CURVE_TEST_COPTS,
    linkstatic=1,
)
