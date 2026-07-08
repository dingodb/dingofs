# Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

# standalone runners: TEST() registration inside static libs linked into the
# aggregated test_client gets dropped by the linker, so build own binaries.

add_executable(test_chunkset_flush_race
    test_chunkset_flush_race.cc
)
target_link_libraries(test_chunkset_flush_race
  vfs_metasystem_mds_lib
  ${TEST_DEPS}
)
set_target_properties(test_chunkset_flush_race PROPERTIES
  RUNTIME_OUTPUT_DIRECTORY ${TEST_EXECUTABLE_OUTPUT_PATH}
)

add_executable(test_trunc_chunkset_split
    test_trunc_chunkset_split.cc
)
target_link_libraries(test_trunc_chunkset_split
  vfs_metasystem_mds_lib
  ${TEST_DEPS}
)
set_target_properties(test_trunc_chunkset_split PROPERTIES
  RUNTIME_OUTPUT_DIRECTORY ${TEST_EXECUTABLE_OUTPUT_PATH}
)
