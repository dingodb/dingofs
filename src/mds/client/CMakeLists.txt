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


add_library(mds_client
  mds.cc
  store.cc
  interaction.cc
  br.cc
)

target_link_libraries(mds_client
  PROTO_OBJS
  protobuf::libprotobuf
  jsoncpp

  mds_common
  mds_storage
  mds_filesystem

  dingo::sdk
)

add_executable(dingo-mds-client
  main.cc
)

target_link_libraries(dingo-mds-client
  PROTO_OBJS

  dingofs_common

  mds_client

  brpc::brpc
  fmt::fmt
  gflags::gflags
  glog::glog
  OpenSSL::SSL
)