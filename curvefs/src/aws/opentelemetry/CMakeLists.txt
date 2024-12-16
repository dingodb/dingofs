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

file(GLOB SOURCES "*.cpp")
file(GLOB HEADERS "*.h")

add_library(opentelemetry_lib 
    ${SOURCES} 
    ${HEADERS}
)

target_include_directories(opentelemetry_lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR} ${OPENTELEMETRY_CPP_INCLUDE_DIRS})

target_link_libraries(opentelemetry_lib
    ${OPENTELEMETRY_CPP_LIBRARIES}
    ${AWSSDK_LIBRARIES}
)
