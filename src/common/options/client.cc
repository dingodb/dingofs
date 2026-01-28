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

#include "common/options/client.h"

#include <brpc/reloadable_flags.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

namespace dingofs {
namespace client {

DEFINE_int32(vfs_bthread_worker_num, 0, "bthread worker num");

// access log
DEFINE_bool(vfs_access_logging, true, "enable access log");
DEFINE_validator(vfs_access_logging, brpc::PassValidate);

DEFINE_int64(vfs_access_log_threshold_us, 0, "access log threshold");
DEFINE_validator(vfs_access_log_threshold_us, brpc::PassValidate);

DEFINE_bool(fuse_enable_readdir_cache, true, "enable readdir cache");
DEFINE_validator(fuse_enable_readdir_cache, brpc::PassValidate);

DEFINE_uint32(fuse_entry_cache_timeout_s, 3600,
              "fuse entry cache timeout in seconds");
DEFINE_validator(fuse_entry_cache_timeout_s, brpc::PassValidate);

DEFINE_uint32(fuse_attr_cache_timeout_s, 3600,
              "fuse attr cache timeout in seconds");
DEFINE_validator(fuse_attr_cache_timeout_s, brpc::PassValidate);

DEFINE_bool(fuse_dryrun_bench_mode, false, "enable fuse dryrun bench mode");

// smooth upgrade
DEFINE_uint32(fuse_fd_get_max_retries, 100,
              "the max retries that get fuse fd from old dingo-client during "
              "smooth upgrade");
DEFINE_validator(fuse_fd_get_max_retries, brpc::PassValidate);
DEFINE_uint32(
    fuse_fd_get_retry_interval_ms, 100,
    "the interval in millseconds that get fuse fd from old dingo-client "
    "during smooth upgrade");
DEFINE_validator(fuse_fd_get_retry_interval_ms, brpc::PassValidate);
DEFINE_uint32(fuse_check_alive_max_retries, 600,
              "the max retries that check old dingo-client is alive during "
              "smooth upgrade");
DEFINE_validator(fuse_check_alive_max_retries, brpc::PassValidate);
DEFINE_uint32(
    fuse_check_alive_retry_interval_ms, 1000,
    "the interval in millseconds that check old dingo-client is alive "
    "during smooth upgrade");
DEFINE_validator(fuse_check_alive_retry_interval_ms, brpc::PassValidate);

// vfs meta access log
DEFINE_bool(vfs_meta_access_logging, true, "enable vfs meta system log");
DEFINE_validator(vfs_meta_access_logging, brpc::PassValidate);

DEFINE_int64(vfs_meta_access_log_threshold_us, 0, "access log threshold");
DEFINE_validator(vfs_meta_access_log_threshold_us, brpc::PassValidate);

DEFINE_uint64(vfs_meta_memo_expired_s, 3600, "modify time memo expired time");
DEFINE_validator(vfs_meta_memo_expired_s, brpc::PassValidate);

DEFINE_uint64(vfs_meta_chunk_cache_expired_s, 3600, "chunk cache expired time");
DEFINE_validator(vfs_meta_chunk_cache_expired_s, brpc::PassValidate);

DEFINE_uint64(vfs_meta_inode_cache_expired_s, 3600, "inode cache expired time");
DEFINE_validator(vfs_meta_inode_cache_expired_s, brpc::PassValidate);

DEFINE_uint64(vfs_meta_tiny_file_data_cache_expired_s, 3600,
              "tiny file data cache expired time");
DEFINE_validator(vfs_meta_tiny_file_data_cache_expired_s, brpc::PassValidate);

DEFINE_bool(vfs_tiny_file_data_enable, false, "enable vfs meta prefetch data");
DEFINE_validator(vfs_tiny_file_data_enable, brpc::PassValidate);

DEFINE_uint64(vfs_tiny_file_max_size, 1024 * 1024 * 32,
              "max size of tiny file");
DEFINE_validator(vfs_tiny_file_max_size, brpc::PassValidate);

DEFINE_int32(vfs_flush_thread, 4, "number of background flush threads");
DEFINE_validator(vfs_flush_thread, brpc::PassValidate);

DEFINE_int32(vfs_read_executor_thread, 16, "number of read executor threads");
DEFINE_validator(vfs_read_executor_thread, brpc::PassValidate);

DEFINE_int32(vfs_cb_thread, 16, "number of background callback threads");
DEFINE_validator(vfs_cb_thread, brpc::PassValidate);

DEFINE_int32(vfs_read_max_retry_block_not_found, 10,
             "max retry when block not found");
DEFINE_validator(vfs_read_max_retry_block_not_found, brpc::PassValidate);

DEFINE_int64(vfs_read_buffer_total_mb, 1024, "total read buffer size in MB");

DEFINE_bool(vfs_print_readahead_stats, false, "print readahead stats");
DEFINE_validator(vfs_print_readahead_stats, brpc::PassValidate);

// prefetch
DEFINE_uint32(vfs_prefetch_blocks, 1, "number of blocks to prefetch");
DEFINE_validator(vfs_prefetch_blocks, brpc::PassValidate);

DEFINE_uint32(vfs_prefetch_threads, 8, "number of prefetch threads");

// warmup
DEFINE_int32(vfs_warmup_threads, 4, "number of warmup threads");

// vfs handle
DEFINE_int32(vfs_bg_executor_thread, 4, "number of backgroud threads");
DEFINE_validator(vfs_bg_executor_thread, brpc::PassValidate);

DEFINE_int32(vfs_periodic_flush_interval_ms, 5000,
             "periodic flush interval in milliseconds");
DEFINE_validator(vfs_periodic_flush_interval_ms, brpc::PassValidate);

DEFINE_int32(vfs_periodic_trim_mem_ms, 3000,
             "periodic trim mem in milliseconds");
DEFINE_validator(vfs_periodic_trim_mem_ms, brpc::PassValidate);

DEFINE_bool(vfs_intime_warmup_enable, false,
            "enable intime warmup, default is false");
DEFINE_validator(vfs_intime_warmup_enable, brpc::PassValidate);

DEFINE_int64(vfs_warmup_mtime_restart_interval_secs, 120,
             "intime warmup restart interval");
DEFINE_validator(vfs_warmup_mtime_restart_interval_secs, brpc::PassValidate);
DEFINE_int64(vfs_warmup_trigger_restart_interval_secs, 1800,
             "passive warmup restart interval");
DEFINE_validator(vfs_warmup_trigger_restart_interval_secs, brpc::PassValidate);

// vfs meta

DEFINE_uint32(vfs_meta_read_dir_batch_size, 1024, "read dir batch size.");
DEFINE_uint32(vfs_meta_rpc_timeout_ms, 10000, "rpc timeout ms");
DEFINE_validator(vfs_meta_rpc_timeout_ms, brpc::PassValidate);

DEFINE_int32(vfs_meta_rpc_retry_times, 8, "rpc retry time");
DEFINE_validator(vfs_meta_rpc_retry_times, brpc::PassValidate);

DEFINE_bool(vfs_meta_batch_operation_enable, false,
            "enable batch operation, default is false");
DEFINE_validator(vfs_meta_batch_operation_enable, brpc::PassValidate);

DEFINE_uint32(vfs_meta_batch_operation_merge_delay_us, 10,
              "batch operation merge delay us.");
DEFINE_validator(vfs_meta_batch_operation_merge_delay_us, brpc::PassValidate);

DEFINE_uint32(vfs_meta_commit_slice_max_num, 2048,
              "maximum number of slices to commit at once.");
DEFINE_validator(vfs_meta_commit_slice_max_num, brpc::PassValidate);

DEFINE_bool(vfs_meta_compact_chunk_enable, true, "compact chunk enable.");
DEFINE_validator(vfs_meta_compact_chunk_enable, brpc::PassValidate);
DEFINE_uint32(vfs_meta_compact_chunk_threshold_num, 32,
              "compact chunk threshold num.");
DEFINE_validator(vfs_meta_compact_chunk_threshold_num, brpc::PassValidate);
DEFINE_uint32(vfs_meta_compact_chunk_interval_ms, 10 * 1000,
              "compact chunk interval ms.");
DEFINE_validator(vfs_meta_compact_chunk_interval_ms, brpc::PassValidate);

//  inode_blocks_service
DEFINE_uint32(format_file_offset_width, 20, "width of file offset in format");
DEFINE_validator(format_file_offset_width, brpc::PassValidate);

DEFINE_uint32(format_len_width, 15, "width of length in format");
DEFINE_validator(format_len_width, brpc::PassValidate);

DEFINE_uint32(format_block_offset_width, 15, "width of block offset in format");
DEFINE_validator(format_block_offset_width, brpc::PassValidate);

DEFINE_uint32(format_block_name_width, 100, "width of block name in format");
DEFINE_validator(format_block_name_width, brpc::PassValidate);

DEFINE_uint32(format_block_len_width, 15, "width of block length in format");
DEFINE_validator(format_block_len_width, brpc::PassValidate);

DEFINE_string(format_delimiter, "|", "delimiter used in format");
DEFINE_validator(format_delimiter,
                 [](const char* flag_name, const std::string& value) -> bool {
                   (void)flag_name;
                   if (value.length() != 1) {
                     return false;
                   }
                   return true;
                 });

// fuse module
DEFINE_bool(fuse_enable_direct_io, false, "use direct io for file");
DEFINE_validator(fuse_enable_direct_io, brpc::PassValidate);
DEFINE_bool(fuse_enable_keep_cache, true, "keep file page cache");
DEFINE_validator(fuse_enable_keep_cache, brpc::PassValidate);
DEFINE_bool(fuse_enable_splice_move, false,
            "the fuse device try to move pages instead of copying them");
DEFINE_validator(fuse_enable_splice_move, brpc::PassValidate);
DEFINE_bool(fuse_enable_splice_read, false,
            "use splice when reading from the fuse device");
DEFINE_validator(fuse_enable_splice_read, brpc::PassValidate);
DEFINE_bool(fuse_enable_splice_write, false,
            "use splice when writing to the fuse device");
DEFINE_validator(fuse_enable_splice_write, brpc::PassValidate);
DEFINE_bool(fuse_enable_auto_inval_data, true,
            "fuse will check the validity of the attributes on every read");
DEFINE_validator(fuse_enable_auto_inval_data, brpc::PassValidate);

DEFINE_int32(fuse_max_readahead_kb, 131072,
             "maximum number of bytes that the kernel will read ahead");
DEFINE_int32(fuse_max_background, 128,
             "maximum number of pending background requests");

// memory page allocator
DEFINE_uint32(vfs_write_buffer_page_size, 65536,
              "page size for vfs write buffer");
DEFINE_validator(vfs_write_buffer_page_size,
                 [](const char* /*flag_name*/, uint32_t value) -> bool {
                   if (value == 0) {
                     LOG(ERROR) << "page size must greater than 0.";
                     return false;
                   }
                   if (value % 4096 != 0) {
                     LOG(ERROR) << "page size: " << value
                                << " is not a multiple of 4K(4096) ";
                     return false;
                   }
                   return true;
                 });

DEFINE_uint64(vfs_write_buffer_total_mb, 1024,
              "total memory size for vfs write buffer in MB");
DEFINE_validator(vfs_write_buffer_total_mb,
                 [](const char* /*flag_name*/, uint64_t value) -> bool {
                   if (value < 64) {
                     LOG(ERROR) << "page total size cannot be less than 64MB.";
                     return false;
                   }
                   return true;
                 });

// vfs meta
DEFINE_uint32(vfs_meta_max_name_length, 255, "max file name length");
DEFINE_validator(vfs_meta_max_name_length, brpc::PassValidate);

// vfs data
DEFINE_bool(vfs_data_writeback, false, "whether to use writeback");
DEFINE_validator(vfs_data_writeback, brpc::PassValidate);
DEFINE_string(vfs_data_writeback_suffix, "",
              "file name with suffix for writeback");

DEFINE_uint32(vfs_dummy_server_port, 10000, "dummy server port");
DEFINE_validator(vfs_dummy_server_port, brpc::PassValidate);

// trace log
DEFINE_bool(vfs_trace_logging, false, "enable trace log");
DEFINE_validator(vfs_trace_logging, &brpc::PassValidate);

DEFINE_bool(vfs_use_fake_block_store, false, "use fake block store");

DEFINE_bool(vfs_block_store_access_log_enable, false,
            "enable block store access log");
DEFINE_int64(vfs_block_store_access_log_threshold_us, 10 * 1000,
             "block store access log threshold");
DEFINE_validator(vfs_block_store_access_log_threshold_us, brpc::PassValidate);

}  // namespace client
}  // namespace dingofs
