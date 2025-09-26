# Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

set(TEST_DEPS
    GTest::gtest
    GTest::gmock
    GTest::gtest_main
    brpc::brpc
    glog::glog
    gflags::gflags
    dl
)

set(TEST_DEPS_WITHOUT_MAIN
    GTest::gtest
    GTest::gmock
    brpc::brpc
    glog::glog
    gflags::gflags
    dl
)

add_subdirectory(fs)
add_subdirectory(utils)

add_subdirectory(cache)
add_subdirectory(client)
add_subdirectory(mdsv2)

add_subdirectory(trace)