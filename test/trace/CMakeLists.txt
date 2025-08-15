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

set(TEST_TRACE_DEPS
    dingofs_trace
    glog::glog
    brpc::brpc
    ${TEST_DEPS}
)
message("TEST_TRACE_DEPS: ${TEST_TRACE_DEPS}")

function(add_trace_test test_name)
    add_executable(${test_name} ${ARGN})
    target_link_libraries(${test_name} ${TEST_TRACE_DEPS})
    set_target_properties(${test_name} PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${TEST_EXECUTABLE_OUTPUT_PATH}
    )
endfunction()

add_trace_test(test_utils test_utils.cc)
add_trace_test(test_tracer test_tracer.cc)
