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

option(WITH_LIBUSRBIO "Enable LIBUSRBIO feature" OFF)

file(GLOB BLOCKCACHE_LIB_SRCS
    "*.cpp"
)
add_library(client_blockcache ${BLOCKCACHE_LIB_SRCS})

target_link_libraries(client_blockcache
    dingofs_utils
    aws_s3_adapter
    dingofs_base_lib
    dingofs_common
    stub_common
    stub_metric
    glog::glog
    gflags::gflags
    brpc::brpc
    spdlog::spdlog
    uring::uring
    absl::cleanup
)

# currently only support Ubuntu
cmake_host_system_information(RESULT PRETTY_NAME QUERY DISTRIB_PRETTY_NAME)
if(WITH_LIBUSRBIO AND ${PRETTY_NAME} MATCHES "Ubuntu")
    find_package(usrbio REQUIRED)
    target_compile_definitions(client_blockcache PRIVATE WITH_LIBUSRBIO)
    message(STATUS "LIBUSRBIO feature enabled")
    target_link_libraries(client_blockcache 3fs::usrbio)
else()
    message(STATUS "LIBUSRBIO feature disabled")
endif()
