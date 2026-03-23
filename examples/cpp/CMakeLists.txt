cmake_minimum_required(VERSION 3.16)
project(dingofs-demo CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ── libdingofs (C API) ────────────────────────────────────────────────────────
# Tell CMake where dingofs is installed.
# Override: cmake -DDINGOFS_INSTALL_DIR=/opt/dingofs ..
if(NOT DINGOFS_INSTALL_DIR)
    set(DINGOFS_INSTALL_DIR "$ENV{HOME}/.local/dingofs")
endif()
list(APPEND CMAKE_PREFIX_PATH "${DINGOFS_INSTALL_DIR}")

find_package(dingofs REQUIRED)

# ── helper to register one example executable ─────────────────────────────────
function(add_example name)
    add_executable(${name} ${name}.cc)
    target_link_libraries(${name} PRIVATE dingofs::c)
endfunction()

# ── examples ─────────────────────────────────────────────────────────────────
add_example(statfs)
add_example(mkdir)
add_example(write)
add_example(read)
add_example(stat)
add_example(link)
add_example(symlink)
add_example(rename)
add_example(xattr)
add_example(options_demo)
