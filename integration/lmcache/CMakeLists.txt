# DingoFS LMCache connector — pybind11 native extension.
#
# Two consumption modes:
#   1. In-tree (default): invoked by scikit-build-core as a top-level CMake
#      project; we add_subdirectory the parent dingofs tree so cache_lib and
#      its transitive deps become available as targets.
#   2. Add_subdirectory: when dingofs/CMakeLists.txt opts in to include this
#      directory, cache_lib is already a target and we just hook the extension
#      onto it.

cmake_minimum_required(VERSION 3.18)
project(dingofs_lmcache_connector LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Resolve the dingofs source root. When this file is the top-level project
# (pip install ./integration/lmcache), DINGOFS_SOURCE_DIR walks two levels up.
if(NOT DEFINED DINGOFS_SOURCE_DIR)
  get_filename_component(DINGOFS_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../.."
                         ABSOLUTE)
endif()

option(DINGOFS_LMCACHE_USE_INSTALLED
       "Link against an installed dingofs (skips re-building cache_lib)" OFF)

if(NOT TARGET cache_lib)
  if(DINGOFS_LMCACHE_USE_INSTALLED)
    message(
      FATAL_ERROR
        "DINGOFS_LMCACHE_USE_INSTALLED is set but find_package(dingofs) is "
        "not implemented yet — build dingofs in-tree or set "
        "DINGOFS_LMCACHE_USE_INSTALLED=OFF.")
  endif()
  message(
    STATUS
      "dingofs cache_lib target not found — adding ${DINGOFS_SOURCE_DIR} as a "
      "subproject (this rebuilds dingofs).")
  add_subdirectory("${DINGOFS_SOURCE_DIR}" dingofs_root EXCLUDE_FROM_ALL)
endif()

# Locate LMCache's csrc headers (IStorageConnector + LMCACHE_BIND_CONNECTOR_METHODS).
# Default lookup: site-packages contains the LMCache python tree; the csrc/
# headers live in a sibling /csrc/storage_backends/ in the source tarball.
if(NOT DEFINED LMCACHE_CSRC_DIR)
  execute_process(
    COMMAND "${PYTHON_EXECUTABLE}" -c
            "import lmcache, pathlib, sys; \
             p = pathlib.Path(lmcache.__file__).resolve().parent; \
             for cand in (p.parent/'csrc/storage_backends', \
                          p.parent.parent/'csrc/storage_backends'): \
                 if cand.exists(): print(cand); sys.exit(0)
             sys.exit(1)"
    OUTPUT_VARIABLE _lmcache_csrc
    OUTPUT_STRIP_TRAILING_WHITESPACE
    RESULT_VARIABLE _lmcache_csrc_rc)
  if(_lmcache_csrc_rc EQUAL 0)
    set(LMCACHE_CSRC_DIR "${_lmcache_csrc}")
  else()
    message(
      WARNING
        "Could not auto-detect LMCache csrc/ from python; set LMCACHE_CSRC_DIR "
        "explicitly to .../LMCache/csrc/storage_backends if the build fails.")
  endif()
endif()

if(LMCACHE_CSRC_DIR)
  message(STATUS "Using LMCache csrc headers from: ${LMCACHE_CSRC_DIR}")
endif()

find_package(pybind11 CONFIG REQUIRED)

pybind11_add_module(_native MODULE
                    src/native/module.cc
                    src/native/native_cache_engine.cc
                    src/native/key_codec.cc)

target_include_directories(_native PRIVATE
                           "${DINGOFS_SOURCE_DIR}/src"
                           "${CMAKE_BINARY_DIR}/protos"
                           "${LMCACHE_CSRC_DIR}")

# dingofs's third-party static archives live next to dingofs itself —
# propagate the link-search paths the parent CMakeLists registers via
# link_directories() so the shared module can find them at link time.
if(NOT DEFINED THIRD_PARTY_INSTALL_PATH)
  if(DEFINED ENV{THIRD_PARTY_INSTALL_PATH})
    set(THIRD_PARTY_INSTALL_PATH "$ENV{THIRD_PARTY_INSTALL_PATH}")
  else()
    set(THIRD_PARTY_INSTALL_PATH "$ENV{HOME}/.local/dingo-eureka")
  endif()
endif()
if(NOT DEFINED DINGOSDK_INSTALL_PATH)
  if(DEFINED ENV{DINGOSDK_INSTALL_PATH})
    set(DINGOSDK_INSTALL_PATH "$ENV{DINGOSDK_INSTALL_PATH}")
  else()
    set(DINGOSDK_INSTALL_PATH "$ENV{HOME}/.local/dingo-sdk")
  endif()
endif()
target_link_directories(_native PRIVATE
                        "${THIRD_PARTY_INSTALL_PATH}/lib"
                        "${THIRD_PARTY_INSTALL_PATH}/lib64"
                        "${DINGOSDK_INSTALL_PATH}/lib")

# cache_lib transitively pulls cache_remotecache + cache_cachegroup +
# brpc/bthread/protobuf, which is everything the extension needs.
target_link_libraries(_native PRIVATE cache_lib)

# Match the dingofs C++ standard.
target_compile_features(_native PRIVATE cxx_std_17)

# scikit-build-core picks this up and ships it into the wheel under
# site-packages/dingofs_connector/_native*.so.
install(TARGETS _native LIBRARY DESTINATION dingofs_connector)
