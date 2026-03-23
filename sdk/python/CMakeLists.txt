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
# _dingofs_core: nanobind extension module for the DingoFS Python SDK
# ============================================================================

cmake_minimum_required(VERSION 3.17)

find_package(Python COMPONENTS Interpreter Development.Module REQUIRED)

find_package(nanobind CONFIG REQUIRED
    HINTS "${THIRD_PARTY_INSTALL_PATH}/nanobind/cmake"
)

nanobind_add_module(_dingofs_core
    src/wrapper.cc
    src/status_bindings.cc
    src/meta_bindings.cc
    src/client_bindings.cc
)

target_include_directories(_dingofs_core PRIVATE
    ${PROJECT_SOURCE_DIR}/include
    ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(_dingofs_core PRIVATE dingofs_shim)

# GNU ld >= 2.38 errors when a versioned symbol (@@VERSION) from a static
# archive (librados.a) is pulled into a shared library but no version script
# defines the version node.  Provide a minimal version script that declares
# the LIBRADOS_14.2.0 node so the linker is satisfied.
target_link_options(_dingofs_core PRIVATE
    "-Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/librados_version.map"
)

# scikit-build-core 通过 cmake install 收集产物打入 wheel。
# DESTINATION 使用 scikit-build-core 约定的 skbuild 相对路径：
#   - 安装到 dingofs/ 包目录，与 wheel.packages = ["dingofs"] 对齐。
install(TARGETS _dingofs_core DESTINATION dingofs)
