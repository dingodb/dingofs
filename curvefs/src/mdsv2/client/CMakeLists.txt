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



add_executable(dingo-mdsv2-client
  main.cc
  mds.cc
  interaction.cc
)

target_link_libraries(dingo-mdsv2-client
  PROTO_OBJS

  mdsv2_common

  brpc
  fmt
  gflags
  glog
)