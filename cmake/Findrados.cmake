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

# - Try to find librados
# This module will also define the following variables:
#
# RADOS_INCLUDE_DIRS - where to find rados headers
# RADOS_LIBRARIES - List of libraries when using rados.
# RADOS_FOUND - True if rados found.

find_path(RADOS_INCLUDE_DIRS
  NAMES rados/librados.hpp)

find_library(RADOS_LIBRARIES
  NAMES librados.a)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(rados DEFAULT_MSG RADOS_LIBRARIES RADOS_INCLUDE_DIRS)

mark_as_advanced(
  RADOS_LIBRARIES
  RADOS_INCLUDE_DIRS)

if(RADOS_FOUND AND NOT (TARGET ceph::rados))
  set(Boost_USE_STATIC_LIBS ON)
  set(Boost_USE_STATIC_RUNTIME ON)
  find_package(Boost REQUIRED CONFIG COMPONENTS iostreams thread)

  # find rdma library
  find_library(RDMACM_LIBRARIES NAMES librdmacm.a)
  find_library(IBVERBS_LIBRARIES NAMES libibverbs.a)

  set(RADOS_DEPS_LIBS
      OpenSSL::SSL
      ${Boost_LIBRARIES}
      ${RDMACM_LIBRARIES}
      ${IBVERBS_LIBRARIES}
      resolv
 )
  message("RADOS_DEPS_LIBS: ${RADOS_DEPS_LIBS}")
  add_library(ceph::rados STATIC IMPORTED)
  set_target_properties(ceph::rados
    PROPERTIES
      IMPORTED_LOCATION ${RADOS_LIBRARIES}
      INTERFACE_INCLUDE_DIRECTORIES ${RADOS_INCLUDE_DIRS}
      INTERFACE_LINK_LIBRARIES "${RADOS_DEPS_LIBS}")
endif()
