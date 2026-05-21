# Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

# Builds _dingofs_native.so — the pybind11 module that bridges LMCache (Python)
# to RemoteBlockCacheImpl (dingofs C++). Expects to be add_subdirectory()'d from
# the dingofs top-level CMakeLists with all find_package() already done.

find_package(Python3 COMPONENTS Interpreter Development.Module REQUIRED)
find_package(pybind11 CONFIG REQUIRED)

set(NATIVE_SRCS
    csrc/module.cc
    csrc/native_engine.cc
    csrc/completion_queue.cc
)

pybind11_add_module(_dingofs_native MODULE ${NATIVE_SRCS})

target_include_directories(_dingofs_native PRIVATE
    ${CMAKE_SOURCE_DIR}/src
    ${PROTO_OUTPUT_DIR}
)

# cache_remotecache transitively pulls cache_blockcache / cache_common /
# cache_iutil / dingofs_executor. dingofs_options carries the FLAGS_* storage
# we mutate at init time. PROTO_OBJS supplies the generated pb code that
# block_handle_helper.h depends on.
target_link_libraries(_dingofs_native PRIVATE
    cache_remotecache
    dingofs_options
    PROTO_OBJS
)

# Place the .so straight inside the Python package source tree so
# `pip install -e .` (or a regular wheel build) picks it up via package_data.
set_target_properties(_dingofs_native PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/src/dingofs_connector
)
