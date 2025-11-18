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

#include "options/client/option.h"

#include <brpc/reloadable_flags.h>
#include <gflags/gflags.h>

#include "options/blockaccess/option.h"
#include "options/gflag_validator.h"
#include "options/trace/trace_dynamic_option.h"
#include "utils/configuration.h"

namespace dingofs {
namespace client {

// ############## gflags ##############

DEFINE_bool(client_data_single_thread_read, false,
            "use single thread read to chunk, if true, use single thread read "
            "to block cache, otherwise use executor read with async");
DEFINE_int32(client_bthread_worker_num, 0, "bthread worker num");

// access log
DEFINE_bool(client_access_logging, true, "enable access log");
DEFINE_validator(client_access_logging, &PassBool);

DEFINE_int64(client_access_log_threshold_us, 0, "access log threshold");
DEFINE_validator(client_access_log_threshold_us, &PassInt64);

// fuse module
DEFINE_bool(client_fuse_file_info_direct_io, false, "use direct io for file");
DEFINE_validator(client_fuse_file_info_direct_io, &PassBool);

DEFINE_bool(client_fuse_file_info_keep_cache, false, "keep file page cache");
DEFINE_validator(client_fuse_file_info_keep_cache, &PassBool);

DEFINE_bool(client_fuse_enable_readdir_cache, false, "enable readdir cache");
DEFINE_validator(client_fuse_enable_readdir_cache, brpc::PassValidate);

DEFINE_uint32(client_fuse_entry_cache_timeout_s, 1,
              "fuse entry cache timeout in seconds");
DEFINE_validator(client_fuse_entry_cache_timeout_s, brpc::PassValidate);

DEFINE_uint32(client_fuse_attr_cache_timeout_s, 1,
              "fuse attr cache timeout in seconds");
DEFINE_validator(client_fuse_attr_cache_timeout_s, brpc::PassValidate);

// smooth upgrade
DEFINE_uint32(client_fuse_fd_get_max_retries, 100,
              "the max retries that get fuse fd from old dingo-fuse during "
              "smooth upgrade");
DEFINE_validator(client_fuse_fd_get_max_retries, &PassUint32);
DEFINE_uint32(
    client_fuse_fd_get_retry_interval_ms, 100,
    "the interval in millseconds that get fuse fd from old dingo-fuse "
    "during smooth upgrade");
DEFINE_validator(client_fuse_fd_get_retry_interval_ms, &PassUint32);
DEFINE_uint32(
    client_fuse_check_alive_max_retries, 600,
    "the max retries that check old dingo-fuse is alive during smooth upgrade");
DEFINE_validator(client_fuse_check_alive_max_retries, &PassUint32);
DEFINE_uint32(client_fuse_check_alive_retry_interval_ms, 1000,
              "the interval in millseconds that check old dingo-fuse is alive "
              "during smooth upgrade");
DEFINE_validator(client_fuse_check_alive_retry_interval_ms, &PassUint32);

// vfs meta access log
DEFINE_bool(client_vfs_meta_logging, true, "enable vfs meta system log");
DEFINE_validator(client_vfs_meta_logging, &PassBool);

DEFINE_int64(client_vfs_meta_log_threshold_us, 1000, "access log threshold");
DEFINE_validator(client_vfs_meta_log_threshold_us, &PassInt64);

DEFINE_uint64(client_vfs_meta_memo_expired_s, 3600,
              "modify time memo expired time");
DEFINE_validator(client_vfs_meta_memo_expired_s, brpc::PassValidate);

DEFINE_int32(client_vfs_flush_bg_thread, 16,
             "number of background flush threads");
DEFINE_validator(client_vfs_flush_bg_thread, &PassInt32);

DEFINE_int32(client_vfs_periodic_flush_interval_ms, 100,
             "periodic flush interval in milliseconds");
DEFINE_validator(client_vfs_periodic_flush_interval_ms, &PassInt32);

DEFINE_double(client_vfs_trigger_flush_free_page_ratio, 0.3,
              "trigger flush when free page ratio is lower than this value");
DEFINE_validator(client_vfs_trigger_flush_free_page_ratio, &PassDouble);

DEFINE_int32(client_vfs_read_executor_thread, 8,
             "number of read executor threads");
DEFINE_validator(client_vfs_read_executor_thread, &PassInt32);

DEFINE_int32(client_vfs_read_max_retry_block_not_found, 10,
             "max retry when block not found");
DEFINE_validator(client_vfs_read_max_retry_block_not_found, &PassInt32);

// prefetch
DEFINE_uint32(client_vfs_file_prefetch_block_cnt, 1,
              "number of blocks to prefetch for file read");
DEFINE_validator(client_vfs_file_prefetch_block_cnt, &PassUint32);

DEFINE_uint32(client_vfs_file_prefetch_executor_num, 4,
              "number of prefetch executor");

// begin warmp params
DEFINE_int32(client_vfs_warmup_executor_thread, 2,
             "number of warmup executor threads");

DEFINE_bool(client_vfs_intime_warmup_enable, false,
            "enable intime warmup, default is false");
DEFINE_validator(client_vfs_intime_warmup_enable, &PassBool);

DEFINE_int64(client_vfs_warmup_mtime_restart_interval_secs, 120,
             "intime warmup restart interval");
DEFINE_validator(client_vfs_warmup_mtime_restart_interval_secs, &PassInt64);

DEFINE_int64(client_vfs_warmup_trigger_restart_interval_secs, 1800,
             "passive warmup restart interval");
DEFINE_validator(client_vfs_warmup_trigger_restart_interval_secs, &PassInt64);
// end warmp params

// ## vfs meta
DEFINE_uint32(client_vfs_read_dir_batch_size, 1024, "read dir batch size.");
DEFINE_uint32(client_vfs_rpc_timeout_ms, 10000, "rpc timeout ms");
DEFINE_validator(client_vfs_rpc_timeout_ms, &PassUint32);

DEFINE_int32(client_vfs_rpc_retry_times, 8, "rpc retry time");
DEFINE_validator(client_vfs_rpc_retry_times, &PassInt32);

DEFINE_uint32(client_write_slicce_operation_merge_delay_us, 10,
              "write slice operation merge delay us.");
DEFINE_validator(client_write_slicce_operation_merge_delay_us, &PassUint32);

// begin used in inode_blocks_service
DEFINE_uint32(format_file_offset_width, 20, "Width of file offset in format");
DEFINE_validator(format_file_offset_width, &PassUint32);

DEFINE_uint32(format_len_width, 15, "Width of length in format");
DEFINE_validator(format_len_width, &PassUint32);

DEFINE_uint32(format_block_offset_width, 15, "Width of block offset in format");
DEFINE_validator(format_block_offset_width, &PassUint32);

DEFINE_uint32(format_block_name_width, 100, "Width of block name in format");
DEFINE_validator(format_block_name_width, &PassUint32);

DEFINE_uint32(format_block_len_width, 15, "Width of block length in format");
DEFINE_validator(format_block_len_width, &PassUint32);

DEFINE_string(format_delimiter, "|", "Delimiter used in format");
DEFINE_validator(format_delimiter,
                 [](const char* flag_name, const std::string& value) -> bool {
                   (void)flag_name;
                   if (value.length() != 1) {
                     return false;
                   }
                   return true;
                 });

// end used in inode_blocks_service

// ############## gflags end ##############

// ############## options ##############

void InitFuseOption(utils::Configuration* c, FuseOption* option) {
  {  // fuse conn info
    auto* o = &option->conn_info;
    c->GetValueFatalIfFail("fuse.conn_info.want_splice_move",
                           &o->want_splice_move);
    c->GetValueFatalIfFail("fuse.conn_info.want_splice_read",
                           &o->want_splice_read);
    c->GetValueFatalIfFail("fuse.conn_info.want_splice_write",
                           &o->want_splice_write);
    c->GetValueFatalIfFail("fuse.conn_info.want_auto_inval_data",
                           &o->want_auto_inval_data);
  }

  {  // fuse file info
    c->GetValueFatalIfFail("fuse.file_info.direct_io",
                           &FLAGS_client_fuse_file_info_direct_io);
    c->GetValueFatalIfFail("fuse.file_info.keep_cache",
                           &FLAGS_client_fuse_file_info_keep_cache);
  }
}

void InitVFSOption(utils::Configuration* conf, VFSOption* option) {
  if (!conf->GetIntValue("block_access.rados.rados_op_timeout",
                         &blockaccess::FLAGS_rados_op_timeout)) {
    LOG(INFO) << "Not found `block_access.rados.rados_op_timeout` in conf, "
                 "default to "
              << blockaccess::FLAGS_rados_op_timeout;
  }

  blockaccess::InitAwsSdkConfig(
      conf, &option->block_access_opt.s3_options.aws_sdk_config);
  blockaccess::InitBlockAccesserThrottleOptions(
      conf, &option->block_access_opt.throttle_options);

  InitMemoryPageOption(conf, &option->page_option);

  InitBlockCacheOption(conf);
  InitRemoteBlockCacheOption(conf);

  InitFuseOption(conf, &option->fuse_option);
  InitPrefetchOption(conf);

  // vfs data related
  if (!conf->GetBoolValue("vfs.data.writeback",
                          &option->data_option.writeback)) {
    LOG(INFO) << "Not found `vfs.data.writeback` in conf, default:"
              << (option->data_option.writeback ? "true" : "false");
  }

  if (!conf->GetStringValue("vfs.data.writeback_suffix",
                            &option->data_option.writeback_suffix)) {
    LOG(INFO) << "Not found `vfs.data.writeback_suffix` in conf, "
                 "default to: "
              << option->data_option.writeback_suffix;
  }

  if (!conf->GetIntValue("vfs.data.vfs_periodic_flush_interval_ms",
                         &FLAGS_client_vfs_periodic_flush_interval_ms)) {
    LOG(INFO) << "Not found `vfs.data.vfs_periodic_flush_interval_ms` in conf, "
                 "default to "
              << FLAGS_client_vfs_periodic_flush_interval_ms;
  }

  if (!conf->GetIntValue("vfs.data.flush_bg_thread",
                         &FLAGS_client_vfs_flush_bg_thread)) {
    LOG(INFO) << "Not found `vfs.data.flush_bg_thread` in conf, "
                 "default to "
              << FLAGS_client_vfs_flush_bg_thread;
  }

  if (!conf->GetBoolValue("vfs.data.single_tread_read",
                          &FLAGS_client_data_single_thread_read)) {
    LOG(INFO) << "Not found `vfs.data.single_tread_read` in conf, "
                 "default to "
              << (FLAGS_client_data_single_thread_read ? "true" : "false");
  }

  if (!conf->GetIntValue("vfs.data.read_executor_thread",
                         &FLAGS_client_vfs_read_executor_thread)) {
    LOG(INFO) << "Not found `vfs.data.read_executor_thread` in conf, "
                 "default to "
              << FLAGS_client_vfs_read_executor_thread;
  }

  // vfs meta related
  if (!conf->GetUInt32Value("vfs.meta.max_name_length",
                            &option->meta_option.max_name_length)) {
    LOG(INFO) << "Not found `vfs.meta.max_name_length` in conf, default to "
              << option->meta_option.max_name_length;
  }

  if (!conf->GetUInt32Value("vfs.dummy_server.port",
                            &option->dummy_server_port)) {
    LOG(INFO) << "Not found `vfs.dummy_server.port` in conf, default to "
              << option->dummy_server_port;
  }

  if (!conf->GetIntValue("vfs.bthread_worker_num",
                         &FLAGS_client_bthread_worker_num)) {
    FLAGS_client_bthread_worker_num = 0;
    LOG(INFO) << "Not found `vfs.bthread_worker_num` in conf, "
                 "default to 0";
  }

  if (!conf->GetBoolValue("vfs.access_logging", &FLAGS_client_access_logging)) {
    LOG(INFO) << "Not found `vfs.access_logging` in conf, default: "
              << FLAGS_client_access_logging;
  }
  if (!conf->GetInt64Value("vfs.access_log_threshold_us",
                           &FLAGS_client_access_log_threshold_us)) {
    LOG(INFO) << "Not found `vfs.access_log_threshold_us` in conf, "
                 "default: "
              << FLAGS_client_access_log_threshold_us;
  }

  if (!conf->GetBoolValue("vfs.vfs_meta_logging",
                          &FLAGS_client_vfs_meta_logging)) {
    LOG(INFO) << "Not found `vfs.vfs_meta_logging` in conf, default: "
              << FLAGS_client_vfs_meta_logging;
  }
  if (!conf->GetInt64Value("vfs.vfs_meta_log_threshold_u",
                           &FLAGS_client_vfs_meta_log_threshold_us)) {
    LOG(INFO) << "Not found `vfs.vfs_meta_log_threshold_u` in conf, "
                 "default: "
              << FLAGS_client_vfs_meta_log_threshold_us;
  }

  if (!conf->GetBoolValue("vfs.trace_logging", &FLAGS_trace_logging)) {
    LOG(INFO) << "Not found `vfs.trace_logging` in conf, default: "
              << FLAGS_trace_logging;
  }

  SetBrpcOpt(conf);
}

// ############## options end ##############

}  // namespace client
}  // namespace dingofs
