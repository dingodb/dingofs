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

set(BLOCKCACHE_TEST_DEPS
    fuse_client_lib
    cache_lib
    ${TEST_DEPS}
)

# Function to create a test target
function(add_blockcache_test test_name)
    add_executable(${test_name} ${ARGN})
    target_link_libraries(${test_name} PRIVATE ${BLOCKCACHE_TEST_DEPS})
    set_target_properties(${test_name} PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${TEST_EXECUTABLE_OUTPUT_PATH}
    )
endfunction()

#add_blockcache_test(test_block_cache_upload_queue test_block_cache_upload_queue.cc)
#add_blockcache_test(test_block_cache test_block_cache.cc)
#add_blockcache_test(test_countdown test_countdown.cc)
#add_blockcache_test(test_disk_cache_layout test_disk_cache_layout.cc)
#add_blockcache_test(test_disk_cache_loader test_disk_cache_loader.cc)
#add_blockcache_test(test_disk_cache_manager test_disk_cache_manager.cc)
#add_blockcache_test(test_disk_cache test_disk_cache.cc)
#add_blockcache_test(test_disk_state_machine test_disk_state_machine.cc)
#add_blockcache_test(test_local_filesystem test_local_filesystem.cc)
#add_blockcache_test(test_log test_log.cc)
#add_blockcache_test(test_lru_cache test_lru_cache.cc)
#add_blockcache_test(test_mem_cache test_mem_cache.cc)
#add_blockcache_test(test_memory_pool test_memory_pool.cc)
