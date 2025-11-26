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

add_subdirectory(executor)

file(GLOB UTILS_TEST_SRCS
    "*.cc"
)

add_executable(test_utils ${UTILS_TEST_SRCS})

target_link_libraries(test_utils
    mds_common

    dingofs_utils
    block_accesser

    protobuf::libprotobuf
    ${TEST_DEPS_WITHOUT_MAIN}
)

set_target_properties(test_utils PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${TEST_EXECUTABLE_OUTPUT_PATH}
)