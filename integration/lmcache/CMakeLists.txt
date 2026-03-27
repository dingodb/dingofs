cmake_minimum_required(VERSION 3.14)
project(dingofs_connector C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Main shared library
add_library(dingofs_connector SHARED
    src/io_engine/file_io.cpp
    src/io_engine/io_engine.cpp
    src/io_engine/io_engine_capi.cpp
)

target_include_directories(dingofs_connector PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src/io_engine>
    $<INSTALL_INTERFACE:include>
)

target_compile_options(dingofs_connector PRIVATE -O3 -Wall -Wextra -Wpedantic)
target_link_libraries(dingofs_connector PRIVATE pthread)

set_target_properties(dingofs_connector PROPERTIES
    OUTPUT_NAME "dingofs_connector"
)

# Install
install(TARGETS dingofs_connector
    LIBRARY DESTINATION dingofs_connector
    ARCHIVE DESTINATION dingofs_connector
)
install(FILES src/io_engine/io_engine_capi.h DESTINATION include)

# Tests
option(BUILD_TESTS "Build C++ tests" OFF)
if(BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests/io_engine)
endif()
