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

file(GLOB TEST_BLOCKCACHE_BASE_SRCS "*.cc")

add_library(test_blockcache_base OBJECT
  ${TEST_BLOCKCACHE_BASE_SRCS}
)
target_compile_features(test_blockcache_base PRIVATE cxx_std_20)
# The coverage report is scoped to the real source checkout, not a path
# derived from /proc/self/exe, which may resolve outside the repo.
target_compile_definitions(test_blockcache_base PRIVATE
  DINGOFS_SOURCE_DIR="${CMAKE_SOURCE_DIR}"
)

target_link_libraries(test_blockcache_base
  ${TEST_DEPS_WITHOUT_MAIN}
)
