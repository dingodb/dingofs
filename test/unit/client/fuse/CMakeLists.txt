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

# Standalone test binary for the FUSE hot-upgrade module -- intentionally NOT
# folded into test_client. The handover controller is libfuse-free by design
# (it drives the session via the HandoverSession interface), so it is compiled
# directly here instead of pulling in fuse_client_lib (+ libfuse).
file(GLOB TEST_DINGOFS_CLIENT_FUSE_SRCS
  "*.cc"
)

add_executable(test_fuse_upgrade
  ${TEST_DINGOFS_CLIENT_FUSE_SRCS}
  ${CMAKE_SOURCE_DIR}/src/client/fuse/upgrade/handover_controller.cc
  ${CMAKE_SOURCE_DIR}/src/client/fuse/upgrade/handover_server.cc
)

target_link_libraries(test_fuse_upgrade
  dingofs_common

  ${TEST_DEPS}
)

set_target_properties(test_fuse_upgrade PROPERTIES
  RUNTIME_OUTPUT_DIRECTORY ${TEST_EXECUTABLE_OUTPUT_PATH}
)
