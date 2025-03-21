# Find libunwind
# Find the libunwind library and includes
# LIBUNWIND_INCLUDE_DIRS - where to find libunwind.h, etc.
# LIBUNWIND_LIBRARIES - List of libraries when using libunwind.
# LIBUNWIND_FOUND - True if libunwind found.

find_path(LIBUNWIND_INCLUDE_DIRS
  NAMES libunwind.h)

find_library(LIBUNWIND_LIBRARIES
  NAMES libunwind.a)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(libunwind DEFAULT_MSG LIBUNWIND_LIBRARIES LIBUNWIND_INCLUDE_DIRS)

mark_as_advanced(
  LIBUNWIND_LIBRARIES
  LIBUNWIND_INCLUDE_DIRS)

if(LIBUNWIND_FOUND AND NOT (TARGET libunwind))
  add_library (libunwind UNKNOWN IMPORTED)
  set_target_properties(libunwind
    PROPERTIES
      IMPORTED_LINK_INTERFACE_LANGUAGES "C"
      IMPORTED_LOCATION ${LIBUNWIND_LIBRARIES}
      INTERFACE_INCLUDE_DIRECTORIES ${LIBUNWIND_INCLUDE_DIRS})  
endif()