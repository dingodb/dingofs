# Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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

# Find libmemcached
#
#  LIBMEMCACHED_INCLUDE_DIR - where to find libmemcached/memcached.h, etc.
#  LIBMEMCACHED_LIBRARY     - List of libraries when using libmemcached.
#  LIBMEMCACHED_FOUND       - True if libmemcached found.

find_package(PkgConfig)
pkg_check_modules(LIBMEMCACHED QUIET libmemcached)

IF (LIBMEMCACHED_INCLUDE_DIR)
  # Already in cache, be silent
  SET(LIBMEMCACHED_FIND_QUIETLY TRUE)
ENDIF ()

FIND_PATH(LIBMEMCACHED_INCLUDE_DIR libmemcached/memcached.h)

FIND_LIBRARY(LIBMEMCACHED_LIBRARY NAMES memcached memcached-dbg)

# handle the QUIETLY and REQUIRED arguments and set Libmemcached_FOUND to TRUE 
# if all listed variables are TRUE
INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(LIBMEMCACHED DEFAULT_MSG LIBMEMCACHED_LIBRARY LIBMEMCACHED_INCLUDE_DIR)

SET(LIBMEMCACHED_VERSION 0)

IF(LIBMEMCACHED_FOUND)
  if (EXISTS "${LIBMEMCACHED_INCLUDE_DIR}/libmemcached/configure.h")
    FILE(READ "${LIBMEMCACHED_INCLUDE_DIR}/libmemcached/configure.h" _MEMCACHE_VERSION_CONTENTS)
  endif()
  if (EXISTS "${LIBMEMCACHED_INCLUDE_DIR}/libmemcached-1.0/configure.h")
    FILE(READ "${LIBMEMCACHED_INCLUDE_DIR}/libmemcached-1.0/configure.h" _MEMCACHE_VERSION_CONTENTS)
  endif()
  if (_MEMCACHE_VERSION_CONTENTS)
    STRING(REGEX REPLACE ".*#define LIBMEMCACHED_VERSION_STRING \"([0-9.]+)\".*" "\\1" LIBMEMCACHED_VERSION "${_MEMCACHE_VERSION_CONTENTS}")
  endif()
ENDIF()

SET(LIBMEMCACHED_VERSION ${LIBMEMCACHED_VERSION} CACHE STRING "Version number of libmemcached")

MARK_AS_ADVANCED(LIBMEMCACHED_LIBRARY LIBMEMCACHED_INCLUDE_DIR LIBMEMCACHED_VERSION)