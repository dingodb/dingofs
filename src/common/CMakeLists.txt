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

add_library(dingofs_common
    process.cpp
    rpc_stream.cpp
    wrap_posix.cpp
    io_buffer.cpp
)
target_link_libraries(dingofs_common
    dingofs_utils
    gflags::gflags
    glog::glog
)

add_library(dingofs_options
    options.cpp
)
target_link_libraries(dingofs_options
    glog::glog
    gflags::gflags
    toml11::toml11
)

add_library(dynamic_vlog
    dynamic_vlog.cpp
)
target_link_libraries(dynamic_vlog
    brpc::brpc
    gflags::gflags
    glog::glog
)

add_library(dingofs_threading
    threading.cpp
)
target_link_libraries(dingofs_threading
    glog::glog
)

add_library(dingofs_status
    slice.cpp
    status.cpp
)
target_link_libraries(dingofs_status
    fmt::fmt
    glog::glog
)