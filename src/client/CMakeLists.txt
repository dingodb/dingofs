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

#add_subdirectory(cachegroup)
add_subdirectory(common)
add_subdirectory(datastream)
add_subdirectory(kvclient)
add_subdirectory(lease)

file(GLOB FUSE_CLIENT_LIB_SRCS
    "*.cpp"
    "s3/*.cpp"
    "filesystem/*.cpp"
    "warmup/*.cpp"
    "service/*.cpp"
)
list(REMOVE_ITEM FUSE_CLIENT_LIB_SRCS
    "${CMAKE_CURRENT_SOURCE_DIR}/main.cpp"
)
add_library(fuse_client_lib ${FUSE_CLIENT_LIB_SRCS})
target_link_libraries(fuse_client_lib
    PROTO_OBJS
    dingofs_base_lib
    rpcclient
    dingofs_common
    client_lease
    client_memcached_client
    dynamic_vlog
    client_datastream
    client_blockcache
    #client_cachegroup
    dingofs_utils
    aws_s3_adapter
    client_common
    dingofs_threading
    gflags
    glog
    fmt
    spdlog
    brpc::brpc
    absl::memory
    absl::strings
    absl::synchronization
    absl::str_format
    absl::type_traits
    absl::optional
    absl::btree
    ${FUSE3_LIBRARY}
)

add_executable(dingo-fuse main.cpp)
target_link_libraries(dingo-fuse
    fuse_client_lib
)
