# ============================================================================
# Fat static library: bundle all dingofs *internal* libs into one .a.
# Third-party dependencies (brpc, glog, protobuf, …) are NOT bundled here —
# they are declared in the installed dingofsConfig.cmake so that downstream
# projects use their own copies and avoid symbol conflicts.
# ============================================================================

set(_DINGOFS_INTERNAL_LIBS
    # shim
    dingofs_sdk
    # VFS
    vfs_lib
    vfs_service
    vfs_metasystem
    vfs_metasystem_mds_lib
    vfs_metasystem_memory_lib
    vfs_metasystem_local_lib
    vfs_meta_mds_rpc_lib
    vfs_data
    vfs_data_slice
    vfs_data_common
    vfs_hub
    vfs_handle
    vfs_memory
    vfs_components
    vfs_block_store
    vfs_compaction
    # buffer
    client_buffer
    # cache
    cache_tiercache
    cache_remotecache
    cache_blockcache
    cache_cachegroup
    cache_iutil
    cache_common
    # mds (client-side MDS RPC stubs used by vfs_metasystem_mds_lib)
    mds_mds
    mds_common
    # common
    dingofs_common
    dingofs_trace
    dingofs_options
    dingofs_metrics
    dingofs_open_trace
    client_options
    block_accesser
    fake_accesser
    file_accesser
    s3_accesser
    aws_s3_adapter
    rados_accesser
    blockaccess_options
    # utils
    dingofs_utils
    dingofs_executor
)

set(_BUNDLE_OUTPUT "${CMAKE_BINARY_DIR}/sdk_bundle/libdingofs_client.a")

# Ensure output dir exists at configure time
file(MAKE_DIRECTORY "${CMAKE_BINARY_DIR}/sdk_bundle")

set(_BUNDLE_SH "${CMAKE_BINARY_DIR}/sdk_bundle/bundle.sh")
set(_AR_BIN "${CMAKE_AR}")
configure_file(
    "${PROJECT_SOURCE_DIR}/src/client/cmake/bundle_libs.sh.in"
    "${_BUNDLE_SH}"
    @ONLY
)

add_custom_command(
    OUTPUT  "${_BUNDLE_OUTPUT}"
    COMMAND sh "${_BUNDLE_SH}" "${_BUNDLE_OUTPUT}"
            $<TARGET_FILE:dingofs_sdk>
            $<TARGET_FILE:vfs_lib>
            $<TARGET_FILE:vfs_service>
            $<TARGET_FILE:vfs_metasystem>
            $<TARGET_FILE:vfs_metasystem_mds_lib>
            $<TARGET_FILE:vfs_metasystem_memory_lib>
            $<TARGET_FILE:vfs_metasystem_local_lib>
            $<TARGET_FILE:vfs_meta_mds_rpc_lib>
            $<TARGET_FILE:vfs_data>
            $<TARGET_FILE:vfs_data_slice>
            $<TARGET_FILE:vfs_data_common>
            $<TARGET_FILE:vfs_hub>
            $<TARGET_FILE:vfs_handle>
            $<TARGET_FILE:vfs_memory>
            $<TARGET_FILE:vfs_components>
            $<TARGET_FILE:vfs_block_store>
            $<TARGET_FILE:vfs_compaction>
            $<TARGET_FILE:client_buffer>
            $<TARGET_FILE:cache_tiercache>
            $<TARGET_FILE:cache_remotecache>
            $<TARGET_FILE:cache_blockcache>
            $<TARGET_FILE:cache_cachegroup>
            $<TARGET_FILE:cache_iutil>
            $<TARGET_FILE:cache_common>
            $<TARGET_FILE:mds_mds>
            $<TARGET_FILE:mds_common>
            $<TARGET_FILE:dingofs_common>
            $<TARGET_FILE:dingofs_trace>
            $<TARGET_FILE:dingofs_options>
            $<TARGET_FILE:dingofs_metrics>
            $<TARGET_FILE:dingofs_open_trace>
            $<TARGET_FILE:client_options>
            $<TARGET_FILE:block_accesser>
            $<TARGET_FILE:fake_accesser>
            $<TARGET_FILE:file_accesser>
            $<TARGET_FILE:s3_accesser>
            $<TARGET_FILE:aws_s3_adapter>
            $<TARGET_FILE:rados_accesser>
            $<TARGET_FILE:blockaccess_options>
            $<TARGET_FILE:dingofs_utils>
            $<TARGET_FILE:dingofs_executor>
    DEPENDS ${_DINGOFS_INTERNAL_LIBS}
    COMMENT "Bundling dingofs internal libs → libdingofs_client.a"
)

add_custom_target(dingofs_client_bundle ALL DEPENDS "${_BUNDLE_OUTPUT}")
