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

add_subdirectory(buffer)
add_subdirectory(vfs)
add_subdirectory(fuse)

add_executable(dingo-client main.cc)
target_link_libraries(dingo-client
    fuse_client_lib
    glog::glog
)

if(DINGOFS_BUILD_SDK)
    include(${CMAKE_CURRENT_LIST_DIR}/cmake/bundle.cmake)
    include(${CMAKE_CURRENT_LIST_DIR}/cmake/install.cmake)
endif()
