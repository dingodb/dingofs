# - Find fiu 
# Find the fiu library and includes
#
# FIU_INCLUDE_DIRS - where to find fiu.h, etc.
# FIU_LIBRARIES - List of libraries when using fiu.
# FIU_FOUND - True if fiu found.

find_path(FIU_INCLUDE_DIRS
  NAMES fiu.h
  HINTS ${fiu_ROOT_DIR}/include)

find_library(FIU_LIBRARIES
  NAMES fiu
  HINTS ${fiu_ROOT_DIR}/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(fiu DEFAULT_MSG FIU_LIBRARIES FIU_INCLUDE_DIRS)

mark_as_advanced(
  FIU_LIBRARIES
  FIU_INCLUDE_DIRS)

if(FIU_FOUND AND NOT (TARGET fiu::fiu))
  add_library (fiu::fiu UNKNOWN IMPORTED)
  set_target_properties(fiu::fiu
    PROPERTIES
      IMPORTED_LOCATION ${FIU_LIBRARIES}
      INTERFACE_INCLUDE_DIRECTORIES ${FIU_INCLUDE_DIRS})
endif()
