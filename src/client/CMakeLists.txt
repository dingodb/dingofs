# Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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

add_subdirectory(memory)
add_subdirectory(vfs)

file(GLOB FUSE_CLIENT_LIB_SRCS
    "fuse/fuse_op.cc"
    "fuse/fuse_server.cc"
)

add_library(fuse_client_lib ${FUSE_CLIENT_LIB_SRCS})
target_link_libraries(fuse_client_lib
    PROTO_OBJS

    vfs_lib
    block_accesser
    dingofs_metrics
    dingofs_common
    client_options
    cache_tiercache
    jsoncpp
    ${FUSE3_LIBRARY}

    leveldb
)

add_executable(dingo-fuse main.cc)
target_link_libraries(dingo-fuse
    fuse_client_lib
)
