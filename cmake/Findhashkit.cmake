# - Find libhashkit
#
# hashkit_INCLUDE_DIR - Where to find libhashkit.h
# hashkit_LIBRARIES - List of libraries when using hashkit.
# hashkit_FOUND - True if hashkit found.

find_library(hashkit_LIBRARIES
  NAMES hashkit hashkit-dbg)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(hashkit
  DEFAULT_MSG hashkit_LIBRARIES)

mark_as_advanced(
  hashkit_LIBRARIES)

if(hashkit_FOUND AND NOT TARGET hashkit::hashkit)
  add_library(hashkit::hashkit UNKNOWN IMPORTED)
  set_target_properties(hashkit::hashkit PROPERTIES
    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
    IMPORTED_LOCATION "${hashkit_LIBRARIES}")
endif()
