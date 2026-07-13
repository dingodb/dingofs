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

add_library(test_coverage STATIC coverage.cc)
target_link_libraries(test_coverage ${TEST_DEPS_WITHOUT_MAIN})
# Coverage report generation must scope gcovr's filter to the real source
# checkout, not a path derived from /proc/self/exe -- that resolves through
# the (possibly symlinked) build directory and can point outside the repo.
target_compile_definitions(test_coverage PRIVATE
  DINGOFS_SOURCE_DIR="${CMAKE_SOURCE_DIR}")

# test_coverage_helper has no custom main.cc (unlike the test binaries this
# library serves), so it needs gtest_main to supply one.
add_executable(test_coverage_helper test_coverage.cc)
target_link_libraries(test_coverage_helper test_coverage GTest::gtest_main
  ${TEST_DEPS_WITHOUT_MAIN})
set_target_properties(test_coverage_helper PROPERTIES
  RUNTIME_OUTPUT_DIRECTORY ${TEST_EXECUTABLE_OUTPUT_PATH})
