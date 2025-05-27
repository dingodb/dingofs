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

set(TEST_BASE_DEPS
    dingofs_base_lib
    ${TEST_DEPS}
)
message("TEST_BASE_DEPS: ${TEST_BASE_DEPS}")

# Function to create a test target
function(add_base_test test_name)
    add_executable(${test_name} ${ARGN})
    target_link_libraries(${test_name} ${TEST_BASE_DEPS})
    set_target_properties(${test_name} PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${TEST_EXECUTABLE_OUTPUT_PATH}
    )
endfunction()

add_base_test(test_cache test_cache.cpp)
add_base_test(test_filepath test_filepath.cpp)
add_base_test(test_ketama_con_hash test_ketama_con_hash.cpp)
add_base_test(test_math test_math.cpp)
add_base_test(test_message_queue test_message_queue.cpp)
add_base_test(test_string test_string.cpp)
add_base_test(test_time test_time.cpp)
