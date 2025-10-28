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


file(GLOB VFS_METASYSTEM_DUMMY_LIB_SRC
    "*.cc"
)

add_library(vfs_metasystem_dummy_lib ${VFS_METASYSTEM_DUMMY_LIB_SRC})

target_link_libraries(vfs_metasystem_dummy_lib
    PROTO_OBJS
    dingofs_common
    gflags::gflags
    glog::glog
    fmt::fmt
    spdlog::spdlog
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
