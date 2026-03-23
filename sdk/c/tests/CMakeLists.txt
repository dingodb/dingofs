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

# ============================================================================
# test_libdingofs — unit tests for the C API
#
# Links against:
#   - libdingofs.cc  (the C API implementation)
#   - mock_binding_client.cc  (in-memory BindingClient stub)
#
# This binary does NOT depend on dingofs_shim or any real filesystem,
# so all tests run without a cluster.
# ============================================================================

add_executable(test_libdingofs
    test_libdingofs.cc
    ../libdingofs.cc
    mock_binding_client.cc
)

target_include_directories(test_libdingofs PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..    # for libdingofs.h
    ${PROJECT_SOURCE_DIR}/sdk/shim    # for binding_client.h
    ${PROJECT_SOURCE_DIR}/include     # for dingofs/status.h, dingofs/meta.h, etc.
    ${PROJECT_SOURCE_DIR}/src         # for src/common/io_buffer.h
)

# Mock binary links against dingofs_common and client_buffer (for Status,
# DataBuffer, etc.) but NOT dingofs_shim (which contains the real BindingClient).
target_link_libraries(test_libdingofs PRIVATE dingofs_common client_buffer gflags)

add_test(NAME libdingofs_unit COMMAND test_libdingofs)
