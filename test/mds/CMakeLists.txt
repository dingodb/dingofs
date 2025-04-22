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

add_subdirectory(codec)
add_subdirectory(heartbeat)
add_subdirectory(schedule)
add_subdirectory(topology)


add_executable(test_mds_balance_poc 
    metaserver_balance_poc.cpp
)
target_link_libraries(test_mds_balance_poc 
    PROTO_OBJS
    mds_schedule
    ${TEST_DEPS}
)
set_target_properties(test_mds_balance_poc PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${TEST_EXECUTABLE_OUTPUT_PATH}
)

add_executable(test_mds
    fake_metaserver.cpp 
    fs_info_wrapper_test.cpp
    fs_manager_test.cpp
    fs_manager_test2.cpp
    fs_storage_test.cpp
    mds_service_test.cpp 
    mds_test.cpp
    metaserver_client_test.cpp
    persist_kvstorage_test.cpp
)
target_link_libraries(test_mds
    PROTO_OBJS
    mds_lib
    dingofs_utils
    protobuf::libprotobuf
    ${TEST_DEPS}
)
set_target_properties(test_mds PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${TEST_EXECUTABLE_OUTPUT_PATH}
)
