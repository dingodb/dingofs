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
# dingofs_c: C API shared library (libdingofs.so)
#
# Wraps dingofs_shim with a stable extern "C" interface suitable for use
# from any language FFI (ctypes, cgo, JNI, Rust FFI, …).
# ============================================================================

add_library(dingofs_c SHARED
    libdingofs.cc
)

set_target_properties(dingofs_c PROPERTIES
    OUTPUT_NAME  dingofs
    EXPORT_NAME  c
    VERSION      1.0.0
    SOVERSION    1
)

target_include_directories(dingofs_c
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/..>
        $<INSTALL_INTERFACE:include>
    PRIVATE
        ${PROJECT_SOURCE_DIR}/include
        ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(dingofs_c
    PRIVATE
        dingofs_shim
)

# GNU ld >= 2.38 errors when a versioned symbol (@@VERSION) from a static
# archive is pulled into a shared library but no version script defines the
# version node.  Provide the same map used by the Python extension.
target_link_options(dingofs_c PRIVATE
    "-Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/librados_version.map"
)

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
if(BUILD_TESTING)
    add_subdirectory(tests)
endif()

# ---------------------------------------------------------------------------
# Install rules
# ---------------------------------------------------------------------------
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

install(TARGETS dingofs_c
    EXPORT   dingofsTargets
    LIBRARY  DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE  DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME  DESTINATION ${CMAKE_INSTALL_BINDIR}
    INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

install(FILES libdingofs.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/dingofs
)

# CMake package config — lets consumers do find_package(dingofs REQUIRED)
install(EXPORT dingofsTargets
    FILE        dingofsTargets.cmake
    NAMESPACE   dingofs::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/dingofs
)

configure_package_config_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/dingofsConfig.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/dingofsConfig.cmake"
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/dingofs
)

write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/dingofsConfigVersion.cmake"
    VERSION       1.0.0
    COMPATIBILITY SameMajorVersion
)

install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/dingofsConfig.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/dingofsConfigVersion.cmake"
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/dingofs
)
