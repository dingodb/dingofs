# - Find libibverbs
#
# ibverbs_INCLUDE_DIR - Where to find infiniband/verbs.h
# ibverbs_LIBRARIES   - List of libraries when using ibverbs.
# ibverbs_FOUND       - True if ibverbs found.

find_path(ibverbs_INCLUDE_DIR
  NAMES infiniband/verbs.h)

# Keep the raw static archive for consumers that truly need it (notably
# librados/librdmacm's cross-archive rdma-core references), but make the
# imported target prefer libibverbs.so so runtime providers such as mlx5 can
# be dlopened from /etc/libibverbs.d.
find_library(ibverbs_STATIC_LIBRARIES
  NAMES libibverbs.a)
find_library(ibverbs_SHARED_LIBRARIES
  NAMES libibverbs.so.1 libibverbs.so)

if(ibverbs_STATIC_LIBRARIES)
  set(ibverbs_LIBRARIES ${ibverbs_STATIC_LIBRARIES})
else()
  set(ibverbs_LIBRARIES ${ibverbs_SHARED_LIBRARIES})
endif()

if(ibverbs_SHARED_LIBRARIES)
  set(ibverbs_TARGET_LIBRARIES ${ibverbs_SHARED_LIBRARIES})
else()
  set(ibverbs_TARGET_LIBRARIES ${ibverbs_STATIC_LIBRARIES})
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(ibverbs
  DEFAULT_MSG ibverbs_LIBRARIES ibverbs_INCLUDE_DIR)

mark_as_advanced(
  ibverbs_INCLUDE_DIR
  ibverbs_LIBRARIES
  ibverbs_STATIC_LIBRARIES
  ibverbs_SHARED_LIBRARIES
  ibverbs_TARGET_LIBRARIES)

if(ibverbs_FOUND AND NOT TARGET ibverbs::ibverbs)
  add_library(ibverbs::ibverbs UNKNOWN IMPORTED)
  set_target_properties(ibverbs::ibverbs PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${ibverbs_INCLUDE_DIR}"
    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
    IMPORTED_LOCATION "${ibverbs_TARGET_LIBRARIES}")
endif()
