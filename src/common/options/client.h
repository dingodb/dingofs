/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef DINGOFS_COMMON_OPTIONS_CLIENT_OPTION_H_
#define DINGOFS_COMMON_OPTIONS_CLIENT_OPTION_H_

#include <gflags/gflags_declare.h>

namespace brpc {
DECLARE_int32(connect_timeout_as_unreachable);
DECLARE_int32(max_connection_pool_size);

}  // namespace brpc

namespace dingofs {
namespace client {

#define USING_FLAG(name) using ::dingofs::client::FLAGS_##name;

/**
 * use vlog_level to set vlog level on the fly
 * When vlog_level is set, CheckVLogLevel is called to check the validity of the
 * value. Dynamically modify the vlog level by setting FLAG_v in CheckVLogLevel.
 *
 * You can modify the vlog level to 0 using:
 * curl -s http://127.0.0.1:9000/flags/vlog_level?setvalue=0
 */
// log
DECLARE_string(client_log_dir);
DECLARE_int32(client_log_level);

// file system
DECLARE_string(fstype);

// bthread
DECLARE_int32(client_bthread_worker_num);
// access log
DECLARE_bool(client_access_logging);
DECLARE_int64(client_access_log_threshold_us);

// fuse mount option
DECLARE_string(fuse_mount_options);
DECLARE_bool(fuse_use_single_thread);
DECLARE_bool(fuse_use_clone_fd);
DECLARE_uint32(fuse_max_threads);

// fuse module
DECLARE_bool(fuse_file_info_direct_io);
DECLARE_bool(fuse_file_info_keep_cache);
DECLARE_bool(fuse_enable_readdir_cache);
DECLARE_bool(fuse_conn_info_want_splice_move);
DECLARE_bool(fuse_conn_info_want_splice_read);
DECLARE_bool(fuse_conn_info_want_splice_write);
DECLARE_bool(fuse_conn_info_want_auto_inval_data);

DECLARE_uint32(client_fuse_entry_cache_timeout_s);
DECLARE_uint32(client_fuse_attr_cache_timeout_s);

DECLARE_bool(fuse_dryrun_bench_mode);

// smooth upgrade
DECLARE_uint32(client_fuse_fd_get_max_retries);
DECLARE_uint32(client_fuse_fd_get_retry_interval_ms);
DECLARE_uint32(client_fuse_check_alive_max_retries);
DECLARE_uint32(client_fuse_check_alive_retry_interval_ms);

// vfs meta system log
DECLARE_bool(client_vfs_meta_logging);
DECLARE_int64(client_vfs_meta_log_threshold_us);
DECLARE_uint64(client_vfs_meta_memo_expired_s);
DECLARE_uint64(client_vfs_meta_inode_cache_expired_s);

// vfs read
DECLARE_int32(client_vfs_read_executor_thread);
DECLARE_int32(client_vfs_read_max_retry_block_not_found);

DECLARE_uint32(client_vfs_read_buffer_total_mb);

DECLARE_bool(client_vfs_print_readahead_stats);

// vfs flush
DECLARE_int32(client_vfs_flush_bg_thread);
DECLARE_int32(client_vfs_periodic_flush_interval_ms);
DECLARE_double(client_vfs_trigger_flush_free_page_ratio);

// vfs prefetch
DECLARE_uint32(client_vfs_prefetch_blocks);
DECLARE_uint32(client_vfs_prefetch_threads);

// vfs warmup
DECLARE_int32(client_vfs_warmup_threads);
DECLARE_bool(client_vfs_intime_warmup_enable);
DECLARE_int64(client_vfs_warmup_mtime_restart_interval_secs);
DECLARE_int64(client_vfs_warmup_trigger_restart_interval_secs);

// vfs meta
DECLARE_bool(client_vfs_inode_cache_enable);
DECLARE_uint32(client_vfs_read_dir_batch_size);
DECLARE_uint32(client_vfs_rpc_timeout_ms);
DECLARE_int32(client_vfs_rpc_retry_times);

DECLARE_uint32(client_write_slicce_operation_merge_delay_us);

// begin used in inode_blocks_service
DECLARE_uint32(format_file_offset_width);
DECLARE_uint32(format_len_width);
DECLARE_uint32(format_block_offset_width);
DECLARE_uint32(format_block_name_width);
DECLARE_uint32(format_block_len_width);
DECLARE_string(format_delimiter);
// end used in inode_blocks_service

// unix socket path
DECLARE_string(socket_path);

// vfs option
DECLARE_uint32(vfs_meta_max_name_length);
DECLARE_bool(vfs_data_writeback);
DECLARE_string(vfs_data_writeback_suffix);
DECLARE_uint32(vfs_dummy_server_port);

// memory option
DECLARE_uint32(data_stream_page_size);
DECLARE_uint64(data_stream_page_total_size_mb);
DECLARE_bool(data_stream_page_use_pool);

// trace log
DECLARE_bool(trace_logging);

// block store
DECLARE_bool(use_fake_block_store);

}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_COMMON_OPTIONS_CLIENT_OPTION_H_