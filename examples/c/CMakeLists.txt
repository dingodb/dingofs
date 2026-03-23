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

cmake_minimum_required(VERSION 3.16)
project(dingofs-c-examples C CXX)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ── dingofs SDK ───────────────────────────────────────────────────────────────
# Point to the installed SDK.  Override with:
#   cmake -DDINGOFS_INSTALL_DIR=/opt/dingofs ..
if(NOT DINGOFS_INSTALL_DIR)
    set(DINGOFS_INSTALL_DIR "$ENV{HOME}/.local/dingofs")
endif()
list(APPEND CMAKE_PREFIX_PATH "${DINGOFS_INSTALL_DIR}")

find_package(dingofs REQUIRED)

# ── examples ─────────────────────────────────────────────────────────────────
foreach(example
    hello
    statfs
    mkdir
    write
    read
    stat
    mknod
    link
    symlink
    rename
    xattr
    options_demo
)
    add_executable(${example} ${example}.c)
    target_link_libraries(${example} PRIVATE dingofs::c)
endforeach()
