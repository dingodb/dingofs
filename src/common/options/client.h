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

// bthread
DECLARE_int32(vfs_bthread_worker_num);
// access log
DECLARE_bool(vfs_access_logging);
DECLARE_int64(vfs_access_log_threshold_us);

// fuse mount option
DECLARE_string(fuse_mount_options);
DECLARE_bool(fuse_use_single_thread);
DECLARE_bool(fuse_use_clone_fd);
DECLARE_uint32(fuse_max_threads);

// fuse module
DECLARE_bool(fuse_enable_direct_io);
DECLARE_bool(fuse_enable_keep_cache);
DECLARE_bool(fuse_enable_readdir_cache);
DECLARE_bool(fuse_enable_splice_move);
DECLARE_bool(fuse_enable_splice_read);
DECLARE_bool(fuse_enable_splice_write);
DECLARE_bool(fuse_enable_auto_inval_data);

DECLARE_uint32(fuse_entry_cache_timeout_s);
DECLARE_uint32(fuse_attr_cache_timeout_s);

DECLARE_bool(fuse_dryrun_bench_mode);

// smooth upgrade
DECLARE_uint32(fuse_fd_get_max_retries);
DECLARE_uint32(fuse_fd_get_retry_interval_ms);
DECLARE_uint32(fuse_check_alive_max_retries);
DECLARE_uint32(fuse_check_alive_retry_interval_ms);

// vfs meta system log
DECLARE_bool(vfs_meta_access_logging);
DECLARE_int64(vfs_meta_access_log_threshold_us);
DECLARE_uint64(vfs_meta_memo_expired_s);
DECLARE_uint64(vfs_meta_chunk_cache_expired_s);
DECLARE_uint64(vfs_meta_inode_cache_expired_s);

// vfs read
DECLARE_int32(vfs_read_executor_thread);
DECLARE_int32(vfs_read_max_retry_block_not_found);
DECLARE_uint32(vfs_read_buffer_total_mb);
DECLARE_bool(vfs_print_readahead_stats);

// vfs write
DECLARE_uint32(vfs_write_buffer_page_size);
DECLARE_uint64(vfs_write_buffer_total_mb);

// vfs flush
DECLARE_int32(vfs_flush_thread);

// vfs prefetch
DECLARE_uint32(vfs_prefetch_blocks);
DECLARE_uint32(vfs_prefetch_threads);

// vfs warmup
DECLARE_int32(vfs_warmup_threads);
DECLARE_bool(vfs_intime_warmup_enable);
DECLARE_int64(vfs_warmup_mtime_restart_interval_secs);
DECLARE_int64(vfs_warmup_trigger_restart_interval_secs);

// vfs handle
DECLARE_int32(vfs_bg_executor_thread);
DECLARE_int32(vfs_periodic_flush_interval_ms);
DECLARE_int32(vfs_periodic_trim_mem_ms);

// vfs meta
DECLARE_bool(vfs_meta_inode_cache_enable);
DECLARE_uint32(vfs_meta_read_dir_batch_size);
DECLARE_uint32(vfs_meta_rpc_timeout_ms);
DECLARE_int32(vfs_meta_rpc_retry_times);

DECLARE_bool(vfs_meta_batch_operation_enable);
DECLARE_uint32(vfs_meta_batch_operation_merge_delay_us);
DECLARE_uint32(vfs_meta_commit_slice_max_num);

DECLARE_bool(vfs_meta_compact_chunk_enable);
DECLARE_uint32(vfs_meta_compact_chunk_threshold_num);
DECLARE_uint32(vfs_meta_compact_chunk_interval_ms);

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

// trace log
DECLARE_bool(vfs_trace_logging);

// block store
DECLARE_bool(vfs_use_fake_block_store);
DECLARE_bool(vfs_block_store_access_log_enable);
DECLARE_int64(vfs_block_store_access_log_threshold_us);

}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_COMMON_OPTIONS_CLIENT_OPTION_H_