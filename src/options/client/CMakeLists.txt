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
# Define the BASE_FLAGS and DINGO_DEFAULT_COPTS variables

add_library(client_options
    fuse/fuse_dynamic_option.cc
    fuse/fuse_option.cc
    vfs/vfs_dynamic_option.cc
    vfs/vfs_option.cc
    vfs_legacy/vfs_legacy_dynamic_config.cc
    vfs_legacy/vfs_legacy_option.cc
    client_dynamic_option.cc
    client_option.cc
    common_option.cc
)

target_link_libraries(client_options
    stub_common
    brpc::brpc
    fmt::fmt
    glog::glog
    gflags::gflags
    cache_options
    cache_lib # TODO: remove this
)
