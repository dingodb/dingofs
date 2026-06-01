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

add_library(vfs_components
    file_suffix_watcher.cc
    prefetch_manager.cc
    warmup_manager.cc
    uid_gid_mapper.cc
    passwd_watcher.cc
)

target_link_libraries(vfs_components
    dingofs_common
    dingofs_utils

    gflags::gflags
    glog::glog
    fmt::fmt
    jsoncpp
    spdlog::spdlog
    OpenSSL::Crypto
    absl::strings
    absl::synchronization
    absl::str_format
)