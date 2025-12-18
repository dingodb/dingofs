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

#include "common/const.h"

namespace dingofs {
namespace client {
DEFINE_string(client_log_dir, kDefaultClientLogDir, "set client log directory");

DEFINE_int32(client_log_level, 0, "set log level");
DEFINE_validator(client_log_level, [](const char* /*name*/, int32_t value) {
  FLAGS_v = value;
  LOG(INFO) << "current verbose logging level is `" << FLAGS_v << "`";
  return true;
});

DEFINE_string(fstype, "vfs_v2",
              "vfs type(e.g. vfs_v2|vfs_mds|vfs_local|vfs_memory)");

DEFINE_int32(client_bthread_worker_num, 0, "bthread worker num");

// access log
DEFINE_bool(client_access_logging, true, "enable access log");
DEFINE_validator(client_access_logging, brpc::PassValidate);

DEFINE_int64(client_access_log_threshold_us, 0, "access log threshold");
DEFINE_validator(client_access_log_threshold_us, brpc::PassValidate);

DEFINE_bool(fuse_enable_readdir_cache, true, "enable readdir cache");
DEFINE_validator(fuse_enable_readdir_cache, brpc::PassValidate);

DEFINE_uint32(client_fuse_entry_cache_timeout_s, 3600,
              "fuse entry cache timeout in seconds");
DEFINE_validator(client_fuse_entry_cache_timeout_s, brpc::PassValidate);

DEFINE_uint32(client_fuse_attr_cache_timeout_s, 3600,
              "fuse attr cache timeout in seconds");
DEFINE_validator(client_fuse_attr_cache_timeout_s, brpc::PassValidate);

DEFINE_bool(fuse_dryrun_bench_mode, false, "enable fuse dryrun bench mode");

// smooth upgrade
DEFINE_uint32(client_fuse_fd_get_max_retries, 100,
              "the max retries that get fuse fd from old dingo-fuse during "
              "smooth upgrade");
DEFINE_validator(client_fuse_fd_get_max_retries, brpc::PassValidate);
DEFINE_uint32(
    client_fuse_fd_get_retry_interval_ms, 100,
    "the interval in millseconds that get fuse fd from old dingo-fuse "
    "during smooth upgrade");
DEFINE_validator(client_fuse_fd_get_retry_interval_ms, brpc::PassValidate);
DEFINE_uint32(
    client_fuse_check_alive_max_retries, 600,
    "the max retries that check old dingo-fuse is alive during smooth upgrade");
DEFINE_validator(client_fuse_check_alive_max_retries, brpc::PassValidate);
DEFINE_uint32(client_fuse_check_alive_retry_interval_ms, 1000,
              "the interval in millseconds that check old dingo-fuse is alive "
              "during smooth upgrade");
DEFINE_validator(client_fuse_check_alive_retry_interval_ms, brpc::PassValidate);

// vfs meta access log
DEFINE_bool(client_vfs_meta_logging, true, "enable vfs meta system log");
DEFINE_validator(client_vfs_meta_logging, brpc::PassValidate);

DEFINE_int64(client_vfs_meta_log_threshold_us, 0, "access log threshold");
DEFINE_validator(client_vfs_meta_log_threshold_us, brpc::PassValidate);

DEFINE_uint64(client_vfs_meta_memo_expired_s, 1,
              "modify time memo expired time");
DEFINE_validator(client_vfs_meta_memo_expired_s, brpc::PassValidate);

DEFINE_uint64(client_vfs_meta_inode_cache_expired_s, 1,
              "inode cache expired time");
DEFINE_validator(client_vfs_meta_inode_cache_expired_s, brpc::PassValidate);

DEFINE_int32(client_vfs_flush_bg_thread, 16,
             "number of background flush threads");
DEFINE_validator(client_vfs_flush_bg_thread, brpc::PassValidate);

DEFINE_int32(client_vfs_periodic_flush_interval_ms, 100,
             "periodic flush interval in milliseconds");
DEFINE_validator(client_vfs_periodic_flush_interval_ms, brpc::PassValidate);

DEFINE_double(client_vfs_trigger_flush_free_page_ratio, 0.3,
              "trigger flush when free page ratio is lower than this value");
DEFINE_validator(client_vfs_trigger_flush_free_page_ratio, brpc::PassValidate);

DEFINE_int32(client_vfs_read_executor_thread, 8,
             "number of read executor threads");
DEFINE_validator(client_vfs_read_executor_thread, brpc::PassValidate);

DEFINE_int32(client_vfs_read_max_retry_block_not_found, 10,
             "max retry when block not found");
DEFINE_validator(client_vfs_read_max_retry_block_not_found, brpc::PassValidate);

DEFINE_uint32(client_vfs_read_buffer_total_mb, 1024,
              "total read buffer size in MB");

DEFINE_bool(client_vfs_print_readahead_stats, false, "print readahead stats");
DEFINE_validator(client_vfs_print_readahead_stats, brpc::PassValidate);

// prefetch
DEFINE_uint32(client_vfs_prefetch_blocks, 1, "number of blocks to prefetch");
DEFINE_validator(client_vfs_prefetch_blocks, brpc::PassValidate);

DEFINE_uint32(client_vfs_prefetch_threads, 8, "number of prefetch threads");

// warmup
DEFINE_int32(client_vfs_warmup_threads, 4, "number of warmup threads");

DEFINE_bool(client_vfs_intime_warmup_enable, false,
            "enable intime warmup, default is false");
DEFINE_validator(client_vfs_intime_warmup_enable, brpc::PassValidate);

DEFINE_int64(client_vfs_warmup_mtime_restart_interval_secs, 120,
             "intime warmup restart interval");
DEFINE_validator(client_vfs_warmup_mtime_restart_interval_secs,
                 brpc::PassValidate);
DEFINE_int64(client_vfs_warmup_trigger_restart_interval_secs, 1800,
             "passive warmup restart interval");
DEFINE_validator(client_vfs_warmup_trigger_restart_interval_secs,
                 brpc::PassValidate);

// vfs meta
DEFINE_bool(client_vfs_inode_cache_enable, true,
            "enable inode cache, default is false");
DEFINE_validator(client_vfs_inode_cache_enable, brpc::PassValidate);

DEFINE_uint32(client_vfs_read_dir_batch_size, 1024, "read dir batch size.");
DEFINE_uint32(client_vfs_rpc_timeout_ms, 10000, "rpc timeout ms");
DEFINE_validator(client_vfs_rpc_timeout_ms, brpc::PassValidate);

DEFINE_int32(client_vfs_rpc_retry_times, 8, "rpc retry time");
DEFINE_validator(client_vfs_rpc_retry_times, brpc::PassValidate);

DEFINE_uint32(client_write_slicce_operation_merge_delay_us, 10,
              "write slice operation merge delay us.");
DEFINE_validator(client_write_slicce_operation_merge_delay_us,
                 brpc::PassValidate);

//  inode_blocks_service
DEFINE_uint32(format_file_offset_width, 20, "Width of file offset in format");
DEFINE_validator(format_file_offset_width, brpc::PassValidate);

DEFINE_uint32(format_len_width, 15, "Width of length in format");
DEFINE_validator(format_len_width, brpc::PassValidate);

DEFINE_uint32(format_block_offset_width, 15, "Width of block offset in format");
DEFINE_validator(format_block_offset_width, brpc::PassValidate);

DEFINE_uint32(format_block_name_width, 100, "Width of block name in format");
DEFINE_validator(format_block_name_width, brpc::PassValidate);

DEFINE_uint32(format_block_len_width, 15, "Width of block length in format");
DEFINE_validator(format_block_len_width, brpc::PassValidate);

DEFINE_string(format_delimiter, "|", "Delimiter used in format");
DEFINE_validator(format_delimiter,
                 [](const char* flag_name, const std::string& value) -> bool {
                   (void)flag_name;
                   if (value.length() != 1) {
                     return false;
                   }
                   return true;
                 });

// fuse module
DEFINE_bool(fuse_file_info_direct_io, false, "use direct io for file");
DEFINE_validator(fuse_file_info_direct_io, brpc::PassValidate);
DEFINE_bool(fuse_file_info_keep_cache, true, "keep file page cache");
DEFINE_validator(fuse_file_info_keep_cache, brpc::PassValidate);
DEFINE_bool(fuse_conn_info_want_splice_move, false,
            "the fuse device try to move pages instead of copying them");
DEFINE_validator(fuse_conn_info_want_splice_move, brpc::PassValidate);
DEFINE_bool(fuse_conn_info_want_splice_read, false,
            "use splice when reading from the fuse device");
DEFINE_validator(fuse_conn_info_want_splice_read, brpc::PassValidate);
DEFINE_bool(fuse_conn_info_want_splice_write, false,
            "use splice when writing to the fuse device");
DEFINE_validator(fuse_conn_info_want_splice_write, brpc::PassValidate);
DEFINE_bool(fuse_conn_info_want_auto_inval_data, true,
            "fuse will check the validity of the attributes on every read");
DEFINE_validator(fuse_conn_info_want_auto_inval_data, brpc::PassValidate);

// memory page allocator
DEFINE_uint32(data_stream_page_size, 65536, "memory page size for datastream");
DEFINE_validator(data_stream_page_size, brpc::PassValidate);
DEFINE_uint64(data_stream_page_total_size_mb, 1024,
              "total memory size for data stream");
DEFINE_validator(data_stream_page_total_size_mb, brpc::PassValidate);
DEFINE_bool(data_stream_page_use_pool, true,
            "whether to use memory pool for data stream");
DEFINE_validator(data_stream_page_use_pool, brpc::PassValidate);

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
DEFINE_bool(trace_logging, false, "enable trace log");
DEFINE_validator(trace_logging, &brpc::PassValidate);

DEFINE_bool(use_fake_block_store, false, "use fake block store");

}  // namespace client
}  // namespace dingofs
