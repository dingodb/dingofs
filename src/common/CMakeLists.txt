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

add_library(dingofs_common
    io_buffer.cc
    version.cc
    flag.cc
    string_slice.cc
    status.cc
    logging.cc
)
target_link_libraries(dingofs_common
    dingofs_utils
    brpc::brpc
    fmt::fmt
    gflags::gflags
    glog::glog
)
