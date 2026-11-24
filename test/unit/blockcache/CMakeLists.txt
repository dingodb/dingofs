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

add_subdirectory(base)
add_subdirectory(common)
add_subdirectory(core)
add_subdirectory(fs)
add_subdirectory(runtime)
add_subdirectory(infiniband)
add_subdirectory(utils)

add_executable(test_blockcache
  main.cc
)
target_compile_features(test_blockcache PRIVATE cxx_std_20)

target_link_libraries(test_blockcache
  ibverbs::ibverbs

  PROTO_OBJS

  $<TARGET_OBJECTS:test_blockcache_base>
  $<TARGET_OBJECTS:test_blockcache_common>
  $<TARGET_OBJECTS:test_blockcache_core>
  $<TARGET_OBJECTS:test_blockcache_utils>

  blockcache_api
  blockcache_block
  blockcache_node
  blockcache_object
  blockcache_remote
  blockcache_store
  blockcache_tier
  blockcache_common
  blockcache_status
  blockcache_core
  blockcache_flags

  protobuf::libprotobuf
  ${TEST_DEPS_WITHOUT_MAIN}
)

if(USE_TCMALLOC)
  target_link_libraries(test_blockcache tcmalloc unwind)
endif()

set_target_properties(test_blockcache PROPERTIES
  RUNTIME_OUTPUT_DIRECTORY ${TEST_EXECUTABLE_OUTPUT_PATH}
)
