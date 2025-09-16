# Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set(tcmalloc_lib "")
if(USE_TCMALLOC)
  message(STATUS "use tcmalloc")
  
  add_definitions(-DUSE_TCMALLOC=ON)

  set(tcmalloc_lib "-ltcmalloc")
endif()


add_compile_options(-Wall -Wno-reorder -Wno-overloaded-virtual)

add_subdirectory(common)
add_subdirectory(coordinator)
add_subdirectory(filesystem)
add_subdirectory(mds)
add_subdirectory(service)
add_subdirectory(storage)
add_subdirectory(background)
add_subdirectory(statistics)
add_subdirectory(quota)
add_subdirectory(client)
add_subdirectory(cachegroup)

add_library(mdsv2_lib
  server.cc
)

# target_compile_options(mdsv2_lib PRIVATE -Wall)

target_link_libraries(mdsv2_lib
  PROTO_OBJS

  mdsv2_common
  mdsv2_coordinator
  mdsv2_filesystem
  mdsv2_storage
  mdsv2_service
  mdsv2_background
  mdsv2_statistics
  mdsv2_cachegroup

  dingofs_options
  dingofs_utils

  absl::log_internal_message
  absl::cleanup
  absl::btree
  absl::memory
  absl::utility
  absl::strings

  dingo::sdk
  brpc::brpc
  fmt::fmt
  gflags::gflags
  glog::glog
  backtrace
  libunwind
  jsoncpp
)

add_executable(dingo-mdsv2 main.cc)

target_link_libraries(dingo-mdsv2
  mdsv2_lib

  ${tcmalloc_lib}
)
