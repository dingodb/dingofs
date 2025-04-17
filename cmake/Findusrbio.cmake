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

# - Try to find usrbio
# This module will also define the following variables:
#
# USRBIO_INCLUDE_DIRS - where to find usrbio headers
# USRBIO_LIBRARIES - List of libraries when using usrbio.
# USRBIO_FOUND - True if usrbio found.

find_path(USRBIO_INCLUDE_DIRS
  NAMES hf3fs_usrbio.h)

find_library(USRBIO_LIBRARIES
  NAMES usrbio)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(usrbio DEFAULT_MSG USRBIO_LIBRARIES USRBIO_INCLUDE_DIRS)

mark_as_advanced(
  USRBIO_LIBRARIES
  USRBIO_INCLUDE_DIRS)

if(USRBIO_FOUND AND NOT (TARGET 3fs::usrbio))
  set(USRBIO_DEPS_LIBS
      folly
      numa
      double-conversion
      event
      boost_filesystem
      boost_context) 
  message("USRBIO_DEPS_LIBS: ${USRBIO_DEPS_LIBS}")
  add_library(3fs::usrbio STATIC IMPORTED)
  set_target_properties(3fs::usrbio
    PROPERTIES
      IMPORTED_LOCATION ${USRBIO_LIBRARIES}
      INTERFACE_INCLUDE_DIRECTORIES ${USRBIO_INCLUDE_DIRS}
      INTERFACE_LINK_LIBRARIES "${USRBIO_DEPS_LIBS}") 
endif()