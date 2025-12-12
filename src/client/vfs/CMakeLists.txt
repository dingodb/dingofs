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

add_subdirectory(background)
add_subdirectory(blockstore)
add_subdirectory(data)
add_subdirectory(handle)
add_subdirectory(hub)
add_subdirectory(metasystem)
add_subdirectory(service)
add_subdirectory(components)

add_library(vfs_lib
    vfs_impl.cc
    vfs_wrapper.cc
    data_buffer.cc
    access_log.cc
)

target_link_libraries(vfs_lib
    PROTO_OBJS
    dingofs_common
    dingofs_trace
    vfs_metasystem
    vfs_data
    vfs_hub
    vfs_handle
    vfs_service
    vfs_components
    glog::glog
    gflags::gflags
    fmt::fmt
    absl::str_format
)
