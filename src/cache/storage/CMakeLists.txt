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
# Define the BASE_FLAGS and DINGO_DEFAULT_COPTS variables

option(WITH_LIBUSRBIO "Enable LIBUSRBIO feature" OFF)

file(GLOB STORAGE_LIB_SRCS
    "*.cc"
    "aio/*.cc"
)

add_library(cache_storage ${STORAGE_LIB_SRCS})

target_link_libraries(cache_storage
    uring::uring

    cache_utils
    cache_common
)

# now we support 3fs usrbio only for ubuntu
cmake_host_system_information(RESULT PRETTY_NAME QUERY DISTRIB_PRETTY_NAME)
if(WITH_LIBUSRBIO AND ${PRETTY_NAME} MATCHES "Ubuntu")
    find_package(usrbio REQUIRED)
    target_compile_definitions(cache_storage PRIVATE WITH_LIBUSRBIO)
    message(STATUS "LIBUSRBIO feature enabled")
    target_link_libraries(cache_storage 3fs::usrbio)
else()
    message(STATUS "LIBUSRBIO feature disabled")
endif()

