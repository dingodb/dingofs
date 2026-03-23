# ============================================================================
# Install rules
# Prefix and GNUInstallDirs are set in the top-level CMakeLists.txt.
# Usage:
#   cmake --install build                        # installs to ~/.local/dingofs
#   cmake --install build --prefix /opt/dingofs  # custom prefix
# ============================================================================

# Public headers
install(FILES
    ${PROJECT_SOURCE_DIR}/include/dingofs/client.h
    ${PROJECT_SOURCE_DIR}/include/dingofs/meta.h
    ${PROJECT_SOURCE_DIR}/include/dingofs/data_buffer.h
    ${PROJECT_SOURCE_DIR}/include/dingofs/status.h
    ${PROJECT_SOURCE_DIR}/include/dingofs/string_slice.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/dingofs
)

# Single fat static library (dingofs internal code only)
install(FILES "${_BUNDLE_OUTPUT}"
    DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

# CMake package config — installed to lib/cmake/dingofs/
# Downstream projects use:  find_package(dingofs REQUIRED)
include(CMakePackageConfigHelpers)

configure_package_config_file(
    "${PROJECT_SOURCE_DIR}/src/client/cmake/dingofsConfig.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/dingofsConfig.cmake"
    INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/dingofs"
)

write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/dingofsConfigVersion.cmake"
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
)

install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/dingofsConfig.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/dingofsConfigVersion.cmake"
    # Find modules for deps that lack cmake package configs in dingo-eureka
    "${PROJECT_SOURCE_DIR}/cmake/Findbrpc.cmake"
    "${PROJECT_SOURCE_DIR}/cmake/Findrados.cmake"
    "${PROJECT_SOURCE_DIR}/cmake/Finduring.cmake"
    "${PROJECT_SOURCE_DIR}/cmake/Findudev.cmake"
    "${PROJECT_SOURCE_DIR}/cmake/Findblkid.cmake"
    DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/dingofs"
)
