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

set(CACHEGROUP_TEST_DEPS
    PROTO_OBJS
    client_blockcache
    ${TEST_DEPS}
)

# Function to create a test target
function(add_cachegroup_unittest test_name)
    add_executable(${test_name} ${ARGN})
    target_link_libraries(${test_name} PRIVATE ${CACHEGROUP_TEST_DEPS})
    set_target_properties(${test_name} PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${TEST_EXECUTABLE_OUTPUT_PATH}/client/cachegroup
    )
endfunction()

function(add_cachegroup_benchmark test_name)
    add_executable(${test_name} ${ARGN})
    target_link_libraries(${test_name} PRIVATE ${CACHEGROUP_TEST_DEPS})
    set_target_properties(${test_name} PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${TEST_EXECUTABLE_OUTPUT_PATH}/client/cachegroup
    )
endfunction()

add_cachegroup_unittest(test_iobuf test_iobuf.cpp)

#add_cachegroup_benchmark(zerocopy_client zerocopy/client.cpp)
#add_cachegroup_benchmark(zerocopy_server zerocopy/server.cpp)

#add_cachegroup_benchmark(memset memset/memset.cpp)

add_cachegroup_benchmark(bench_server benchmark/bandwidth/server.cpp)
add_cachegroup_benchmark(bench_client benchmark/bandwidth/client.cpp)

