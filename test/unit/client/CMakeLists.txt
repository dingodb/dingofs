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

add_subdirectory(vfs)

add_executable(test_client
  main.cc
)

target_link_libraries(test_client
  $<TARGET_OBJECTS:test_compact_utils>
  $<TARGET_OBJECTS:test_compactor>
  $<TARGET_OBJECTS:test_client_vfs_data>
  $<TARGET_OBJECTS:test_vfs_common>
  $<TARGET_OBJECTS:test_vfs_handle>
  $<TARGET_OBJECTS:test_vfs_memory>
  $<TARGET_OBJECTS:test_vfs_components>
  $<TARGET_OBJECTS:test_vfs_flat>
  $<TARGET_OBJECTS:test_vfs_impl>
  $<TARGET_OBJECTS:test_vfs_service>
  $<TARGET_OBJECTS:test_vfs_metasystem>

  vfs_lib
  vfs_data
  vfs_handle
  vfs_memory
  vfs_components
  vfs_compaction

  PROTO_OBJS
  ${TEST_DEPS_WITHOUT_MAIN}
)

set_target_properties(test_client PROPERTIES
  RUNTIME_OUTPUT_DIRECTORY ${TEST_EXECUTABLE_OUTPUT_PATH}
)
