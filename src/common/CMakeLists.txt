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

add_subdirectory(blockaccess)
add_subdirectory(metrics)
add_subdirectory(options)
add_subdirectory(trace)
add_subdirectory(opentrace)
add_subdirectory(readmempool)
add_subdirectory(writemempool)

add_library(dingofs_common
    version.cc
    flag.cc
    string_slice.cc
    status.cc
    logging.cc
    sync_point.cc
)

set_property(SOURCE version.cc APPEND PROPERTY COMPILE_DEFINITIONS
    GIT_VERSION="${GIT_VERSION}"
    GIT_TAG_NAME="${GIT_TAG_NAME}"
    GIT_COMMIT_USER="${GIT_COMMIT_USER}"
    GIT_COMMIT_MAIL="${GIT_COMMIT_MAIL}"
    GIT_COMMIT_TIME="${GIT_COMMIT_TIME}"
    GIT_LAST_COMMIT_ID="${GIT_LAST_COMMIT_ID}"
    GIT_BRANCH_NAME="${GIT_BRANCH_NAME}"
    PROTO_GIT_COMMIT_ID="${PROTO_GIT_COMMIT_ID}"
    DINGOFS_BUILD_TYPE="${CMAKE_BUILD_TYPE}"
)

target_link_libraries(dingofs_common
    dingofs_utils
    brpc::brpc
    fmt::fmt
    gflags::gflags
    glog::glog
    dingofs_open_trace
    dingofs_read_mempool
    dingofs_write_mempool
)
