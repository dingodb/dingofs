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

add_subdirectory(common)
add_subdirectory(iutil)
add_subdirectory(blockcache)
add_subdirectory(remotecache)
add_subdirectory(cachegroup)
add_subdirectory(tiercache)

add_executable(test_cache
    main.cc
)

target_link_libraries(test_cache
    $<TARGET_OBJECTS:test_cache_common>
    $<TARGET_OBJECTS:test_cache_iutil>
    $<TARGET_OBJECTS:test_cache_blockcache>
    $<TARGET_OBJECTS:test_cache_remotecache>
    $<TARGET_OBJECTS:test_cache_cachegroup>
    $<TARGET_OBJECTS:test_cache_tiercache>

    cache_lib

    ${TEST_DEPS}
)

set_target_properties(test_cache PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${TEST_EXECUTABLE_OUTPUT_PATH}
)
