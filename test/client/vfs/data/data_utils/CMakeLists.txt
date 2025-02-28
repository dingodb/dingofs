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

add_executable(test_process_read_request 
    test_process_read_request.cpp)
target_link_libraries(test_process_read_request 
    vfs_data
    ${TEST_DEPS}
)
set_target_properties(test_process_read_request PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${TEST_EXECUTABLE_OUTPUT_PATH}
)

add_executable(test_convert_slice_read_req_to_block_read_req 
    test_convert_slice_read_req_to_block_read_req.cpp)
target_link_libraries(test_convert_slice_read_req_to_block_read_req 
    vfs_data
    ${TEST_DEPS}
)
set_target_properties(test_convert_slice_read_req_to_block_read_req PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${TEST_EXECUTABLE_OUTPUT_PATH}
)
