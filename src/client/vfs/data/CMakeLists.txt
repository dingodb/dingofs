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

add_subdirectory(flat)
add_subdirectory(slice)

add_library(vfs_data 
    common.cpp
    file.cpp
    chunk.cpp
    task/file_flush_task.cpp
    task/chunk_flush_task.cpp
)

target_link_libraries(vfs_data
    PROTO_OBJS
    vfs_meta
    vfs_data_flat
    vfs_data_slice
    vfs_background
    fmt::fmt
    glog::glog
)