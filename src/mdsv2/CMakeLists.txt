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

add_subdirectory(common)
add_subdirectory(cache)
add_subdirectory(coordinator)
add_subdirectory(filesystem)
add_subdirectory(mds)
add_subdirectory(service)
add_subdirectory(storage)
add_subdirectory(background)
add_subdirectory(client)

add_library(mdsv2_lib
  server.cc
)

target_link_libraries(mdsv2_lib
  PROTO_OBJS

  mdsv2_common
  mdsv2_cache
  mdsv2_coordinator
  mdsv2_filesystem
  mdsv2_storage
  mdsv2_service
  mdsv2_background

  dingofs_utils

  absl::log_internal_message
  absl::cleanup
  absl::btree
  absl::memory
  absl::utility
  absl::strings

  dingo::sdk

  brpc
  fmt
  gflags
  glog
  backtrace
  libunwind
)

add_executable(dingo-mdsv2 main.cc)
target_link_libraries(dingo-mdsv2
  mdsv2_lib
)