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


add_library(rpcclient 
    base_client.cpp
    cli2_client.cpp
    mds_client.cpp 
    metacache.cpp
    metaserver_client.cpp
    task_excutor.cpp
    meta_access_log.cpp
)

target_link_libraries(rpcclient
    PROTO_OBJS
    stub_common
    dingofs_metrics
    dingofs_utils
    dingofs_common
    brpc::brpc
    gflags::gflags
    glog::glog
    fmt::fmt
    spdlog::spdlog
    absl::str_format
    absl::str_format
    absl::cleanup
    absl::optional
)