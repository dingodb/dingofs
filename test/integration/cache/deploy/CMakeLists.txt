# Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

# Shared deployment harness for the cache integration suites: spawns real
# dingo-mds / dingo-cache processes. The concrete suites (node / local /
# distributed) link this library; the gtest entry point lives in
# ../main.cc.
add_library(integration_cache_deploy STATIC
    process_guard.cc
    mds_server.cc
    cache_node.cc
)

target_link_libraries(integration_cache_deploy PUBLIC
    cache_lib
    block_accesser
    dingofs_common
    dingofs_utils
    PROTO_OBJS

    absl::strings
    absl::str_format
    protobuf::libprotobuf

    GTest::gtest
    GTest::gmock
    brpc::brpc
    glog::glog
    gflags::gflags
)
