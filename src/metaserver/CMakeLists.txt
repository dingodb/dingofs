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

add_subdirectory(common)
add_subdirectory(s3)

file(GLOB DINGOFS_METASERVER_SRCS
    "*.cpp"
    "copyset/*.cpp"
    "storage/*.cpp"
    "superpartition/*.cpp"
    "streaming/*.cpp"
    "compaction/*.cpp"
    "trash/*.cpp"
)

list(REMOVE_ITEM DINGOFS_METASERVER_SRCS
    "${CMAKE_CURRENT_SOURCE_DIR}/main.cpp"
)

add_library(metaserver_lib
    ${DINGOFS_METASERVER_SRCS}
)
target_link_libraries(metaserver_lib
    PROTO_OBJS
    dingofs_common
    metaserver_common
    dingofs_utils
    dingofs_metrics
    rpcclient
    dynamic_vlog
    local_fs
    metaserver_s3
    stub_options
    braft::braft
    brpc::brpc
    RocksDB::rocksdb
    absl::log_internal_message
    absl::cleanup
    absl::btree
    absl::memory
    absl::utility
)

add_executable(dingo-metaserver main.cpp)
target_link_libraries(dingo-metaserver
    metaserver_lib
)