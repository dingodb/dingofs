# Find backtrace
# Find the backtrace library and includes
# BACKTRACE_INCLUDE_DIRS - where to find backtrace.h, etc.
# BACKTRACE_LIBRARIES - List of libraries when using backtrace.
# BACKTRACE_FOUND - True if backtrace found.

find_path(BACKTRACE_INCLUDE_DIRS
  NAMES backtrace.h)

find_library(BACKTRACE_LIBRARIES
  NAMES libbacktrace.a)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(backtrace DEFAULT_MSG BACKTRACE_LIBRARIES BACKTRACE_INCLUDE_DIRS)

mark_as_advanced(
  BACKTRACE_LIBRARIES
  BACKTRACE_INCLUDE_DIRS)

if(BACKTRACE_FOUND AND NOT (TARGET backtrace))
  add_library (backtrace UNKNOWN IMPORTED)
  set_target_properties(backtrace
    PROPERTIES
      IMPORTED_LINK_INTERFACE_LANGUAGES "C"
      IMPORTED_LOCATION ${BACKTRACE_LIBRARIES}
      INTERFACE_INCLUDE_DIRECTORIES ${BACKTRACE_INCLUDE_DIRS})  
endif()