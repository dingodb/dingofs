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

add_subdirectory(blockcache)
add_subdirectory(common)
add_subdirectory(filesystem)


add_executable(test_lease_test 
    lease/lease_executor_test.cpp
)
target_link_libraries(test_lease_test
    client_lease
    ${TEST_DEPS}
)
set_target_properties(test_lease_test PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${TEST_EXECUTABLE_OUTPUT_PATH}
)

add_executable(test_flat_file_test 
    service/flat_file_test.cpp)
target_link_libraries(test_flat_file_test
    fuse_client_lib
    ${TEST_DEPS}
)
set_target_properties(test_flat_file_test PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${TEST_EXECUTABLE_OUTPUT_PATH}
)

set(TEST_CLIENT_DEPS
    fuse_client_lib
    client_memcached_client
    absl::strings
    ${PROTO_OBJS}
    ${TEST_DEPS}
)

set(CLIENT_TEST_SRCS 
    chunk_cache_manager_test.cpp
    client_memcache_test.cpp
    client_operator_test.cpp
    client_s3_adaptor_Integration.cpp
    client_s3_adaptor_test.cpp
    client_s3_test.cpp
    data_cache_test.cpp
    file_cache_manager_test.cpp
    fs_cache_manager_test.cpp
    test_dentry_cache_manager.cpp
    test_fuse_s3_client.cpp
    test_inodeWrapper.cpp
    test_inode_cache_manager.cpp
)

function(add_client_test test_name)
    add_executable(${test_name} ${ARGN})
    target_link_libraries(${test_name} PRIVATE ${TEST_CLIENT_DEPS})
    set_target_properties(${test_name} PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${TEST_EXECUTABLE_OUTPUT_PATH}
    )
endfunction()

foreach(file ${CLIENT_TEST_SRCS})
    get_filename_component(test_name ${file} NAME_WE)
    set(test_target_name test_client_${test_name})
    add_client_test(${test_target_name} ${file})
endforeach()

