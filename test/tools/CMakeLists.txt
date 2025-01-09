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

set(TEST_TOOLS_DEPS
    tools_lib
    ${TEST_DEPS}
)

function(add_tools_test test_name)
    add_executable(${test_name} ${ARGN})
    target_link_libraries(${test_name} PRIVATE ${TEST_TOOLS_DEPS})
    set_target_properties(${test_name} PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${TEST_EXECUTABLE_OUTPUT_PATH}
    )
endfunction()

add_tools_test(test_tools_build_topology_tool_test dingofs_build_topology_tool_test.cpp)
add_tools_test(test_tools_umount_tool dingofs_umount_fs_tool_test.cpp)
add_tools_test(test_tools_version_tool dingofs_version_tool_test.cpp)
add_tools_test(test_tools_tool_factory dingofs_tool_factory_test.cpp)
add_tools_test(test_tools_space_base_tool dingofs_space_base_tool_test.cpp)
