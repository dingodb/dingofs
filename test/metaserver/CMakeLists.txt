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

add_subdirectory(copyset)
add_subdirectory(s3compact)
add_subdirectory(storage)
add_subdirectory(superpartition)


add_executable(test_metaserver 
    main.cpp
    dentry_manager_test.cpp
    dentry_storage_test.cpp
    heartbeat_task_executor_test.cpp
    heartbeat_test.cpp  
    inode_manager_test.cpp 
    inode_storage_test.cpp
    metaserver_service_test2.cpp
    metaserver_test.cpp
    partition_test.cpp
    test_helper.cpp
    transaction_test.cpp
    trash_test.cpp
)
target_link_libraries(test_metaserver
    metaserver_lib
    metaserver_s3
    absl::optional
    ${TEST_DEPS}
)
set_target_properties(test_metaserver PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${TEST_EXECUTABLE_OUTPUT_PATH}
)

add_executable(test_metaserver_partition_clean
    partition_clean_test.cpp
)
target_link_libraries(test_metaserver_partition_clean
    metaserver_lib
    metaserver_s3
    absl::optional
    ${TEST_DEPS}
)
set_target_properties(test_metaserver_partition_clean PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${TEST_EXECUTABLE_OUTPUT_PATH}
)

add_executable(test_metaserver_recycle
    recycle_cleaner_test.cpp
    recycle_manager_test.cpp
)
target_link_libraries(test_metaserver_recycle
    metaserver_lib
    metaserver_s3
    absl::optional
    ${TEST_DEPS}
)
set_target_properties(test_metaserver_recycle PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${TEST_EXECUTABLE_OUTPUT_PATH}
)

add_executable(test_metaserver_s3_adaptor
    metaserver_s3_adaptor_test.cpp
    metaserver_s3_test.cpp
)
target_link_libraries(test_metaserver_s3_adaptor
    metaserver_lib
    metaserver_s3
    aws_s3_adapter
    ${TEST_DEPS}
)
set_target_properties(test_metaserver_s3_adaptor PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${TEST_EXECUTABLE_OUTPUT_PATH}
)

add_executable(test_metaserver_metastore
    metastore_test.cpp
)
target_link_libraries(test_metaserver_metastore
    metaserver_lib
    dingofs_utils
    local_fs
    dingofs_common
    ${TEST_DEPS}
)
set_target_properties(test_metaserver_metastore PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${TEST_EXECUTABLE_OUTPUT_PATH}
)

