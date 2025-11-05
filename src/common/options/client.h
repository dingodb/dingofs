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

#ifndef DINGOFS_SRC_COMMON_OPTIONS_CLIENT_H_
#define DINGOFS_SRC_COMMON_OPTIONS_CLIENT_H_

#ifndef DINGOFS_SRC_COMMON_OPTIONS_CLIENT_CLIENT_H_
#define DINGOFS_SRC_COMMON_OPTIONS_CLIENT_CLIENT_H_

#ifndef DINGOFS_SRC_OPTIONS_CLIENT_OPTION_H_
#define DINGOFS_SRC_OPTIONS_CLIENT_OPTION_H_

#include <gflags/gflags_declare.h>

#include "common/blockaccess/accesser_common.h"
#include "common/const.h"
#include "common/options/cache.h"
#include "utils/configuration.h"
#include "utils/gflags_helper.h"

namespace brpc {

DECLARE_int32(defer_close_second);
DECLARE_int32(health_check_interval);
DECLARE_int32(connect_timeout_as_unreachable);
DECLARE_int32(max_connection_pool_size);

}  // namespace brpc

namespace dingofs {
namespace client {

#define USING_FLAG(name) using ::dingofs::client::FLAGS_##name;

// ############## gflags ##############

/**
 * use vlog_level to set vlog level on the fly
 * When vlog_level is set, CheckVLogLevel is called to check the validity of the
 * value. Dynamically modify the vlog level by setting FLAG_v in CheckVLogLevel.
 *
 * You can modify the vlog level to 0 using:
 * curl -s http://127.0.0.1:9000/flags/vlog_level?setvalue=0
 */
DECLARE_int32(vlog_level);

DECLARE_bool(client_data_single_thread_read);
DECLARE_int32(client_bthread_worker_num);

// access log
DECLARE_bool(client_access_logging);
DECLARE_bool(client_access_logging_verbose);
DECLARE_int64(client_access_log_threshold_us);

// fuse module
DECLARE_bool(client_fuse_file_info_direct_io);
DECLARE_bool(client_fuse_file_info_keep_cache);

// smooth upgrade
DECLARE_uint32(client_fuse_fd_get_max_retries);
DECLARE_uint32(client_fuse_fd_get_retry_interval_ms);
DECLARE_uint32(client_fuse_check_alive_max_retries);
DECLARE_uint32(client_fuse_check_alive_retry_interval_ms);

// vfs meta system log
DECLARE_bool(client_vfs_meta_logging);
DECLARE_int64(client_vfs_meta_log_threshold_us);

// vfs read
DECLARE_int32(client_vfs_read_executor_thread);
DECLARE_int32(client_vfs_read_max_retry_block_not_found);

// vfs flush
DECLARE_int32(client_vfs_flush_bg_thread);
DECLARE_int32(client_vfs_periodic_flush_interval_ms);
DECLARE_double(client_vfs_trigger_flush_free_page_ratio);

// vfs prefetch
DECLARE_uint32(client_vfs_file_prefetch_block_cnt);
DECLARE_uint32(client_vfs_file_prefetch_executor_num);

// vfs warmup
DECLARE_int32(client_vfs_warmup_executor_thread);
DECLARE_bool(client_vfs_intime_warmup_enable);
DECLARE_int64(client_vfs_warmup_mtime_restart_interval_secs);
DECLARE_int64(client_vfs_warmup_trigger_restart_interval_secs);

// vfs meta
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

// ############## gflags end ##############

// ############## options ##############

struct UdsOption {
  std::string fd_comm_path;
};

static void SetBrpcOpt(utils::Configuration* conf) {
  dingofs::utils::GflagsLoadValueFromConfIfCmdNotSet dummy;
  dummy.Load(conf, "defer_close_second", "rpc.defer.close.second",
             &brpc::FLAGS_defer_close_second);
  dummy.Load(conf, "health_check_interval", "rpc.healthCheckIntervalSec",
             &brpc::FLAGS_health_check_interval);
}

static void InitUdsOption(utils::Configuration* conf, UdsOption* uds_opt) {
  if (!conf->GetValue("uds.fdCommPath", &uds_opt->fd_comm_path)) {
    uds_opt->fd_comm_path = "/var/run";
  }
}

static void InitPrefetchOption(utils::Configuration* c) {
  c->GetValue("vfs.data.prefetch.block_cnt",
              &FLAGS_client_vfs_file_prefetch_block_cnt);
  c->GetValue("vfs.data.prefetch.executor_cnt",
              &FLAGS_client_vfs_file_prefetch_executor_num);
  c->GetValue("vfs.data.warmup.intime_warmup_enbale",
              &FLAGS_client_vfs_intime_warmup_enable);
  c->GetValue("vfs.data.warmup.executor_num",
              &FLAGS_client_vfs_warmup_executor_thread);
  c->GetValue("vfs.data.warmup.restart_mtime_interval_secs",
              &FLAGS_client_vfs_warmup_mtime_restart_interval_secs);
  c->GetValue("vfs.data.warmup.restart_trigger_interval_secs",
              &FLAGS_client_vfs_warmup_trigger_restart_interval_secs);
}

static void InitBlockCacheOption(utils::Configuration* c) {
  {  // block cache option
    c->GetValue("block_cache.cache_store", &cache::FLAGS_cache_store);
    c->GetValue("block_cache.enable_stage", &cache::FLAGS_enable_stage);
    c->GetValue("block_cache.enable_cache", &cache::FLAGS_enable_cache);
    c->GetValue("block_cache.trace_logging", &cache::FLAGS_cache_trace_logging);
    c->GetValue("block_cache.upload_stage_throttle_enable",
                &cache::FLAGS_upload_stage_throttle_enable);
    c->GetValue("block_cache.upload_stage_throttle_bandwidth_mb",
                &cache::FLAGS_upload_stage_throttle_bandwidth_mb);
    c->GetValue("block_cache.upload_stage_throttle_iops",
                &cache::FLAGS_upload_stage_throttle_iops);
    c->GetValue("block_cache.upload_stage_max_inflights",
                &cache::FLAGS_upload_stage_max_inflights);
    c->GetValue("block_cache.prefetch_max_inflights",
                &cache::FLAGS_prefetch_max_inflights);
    c->GetValue("block_cache.storage_download_retry_timeout_s",
                &cache::FLAGS_storage_download_retry_timeout_s);
  }

  {  // disk cache option
    c->GetValue("disk_cache.cache_dir", &cache::FLAGS_cache_dir);
    c->GetValue("disk_cache.cache_size_mb", &cache::FLAGS_cache_size_mb);
    c->GetValue("disk_cache.free_space_ratio", &cache::FLAGS_free_space_ratio);
    c->GetValue("disk_cache.cache_expire_s", &cache::FLAGS_cache_expire_s);
    c->GetValue("disk_cache.cleanup_expire_interval_ms",
                &cache::FLAGS_cleanup_expire_interval_ms);
    c->GetValue("disk_cache.ioring_iodepth", &cache::FLAGS_ioring_iodepth);
  }

  {  // disk state option
    c->GetValue("disk_state.tick_duration_s",
                &cache::FLAGS_disk_state_tick_duration_s);
    c->GetValue("disk_state.normal2unstable_error_num",
                &cache::FLAGS_disk_state_normal2unstable_error_num);
    c->GetValue("disk_state.unstable2normal_succ_num",
                &cache::FLAGS_disk_state_unstable2normal_succ_num);
    c->GetValue("disk_state.unstable2down_s",
                &cache::FLAGS_disk_state_unstable2down_s);
    c->GetValue("disk_state.check_duration_ms",
                &cache::FLAGS_disk_state_check_duration_ms);
  }
}

static void InitRemoteBlockCacheOption(utils::Configuration* c) {
  c->GetValue("remote_cache.cache_group", &cache::FLAGS_cache_group);
  c->GetValue("remote_cache.mds_addrs", &cache::FLAGS_mds_addrs);
  c->GetValue("remote_cache.mds_rpc_timeout_ms",
              &cache::FLAGS_mds_rpc_timeout_ms);
  c->GetValue("remote_cache.mds_rpc_retry_times",
              &cache::FLAGS_mds_rpc_retry_times);
  c->GetValue("remote_cache.mds_request_retry_times",
              &cache::FLAGS_mds_request_retry_times);
  c->GetValue("remote_cache.load_members_interval_ms",
              &cache::FLAGS_load_members_interval_ms);

  c->GetValue("remote_cache.fill_group_cache", &cache::FLAGS_fill_group_cache);
  c->GetValue("remote_cache.subrequest_range_size",
              &cache::FLAGS_subrequest_range_size);
  c->GetValue("remote_cache.rpc_connect_timeout_ms",
              &cache::FLAGS_rpc_connect_timeout_ms);
  c->GetValue("remote_cache.rpc_connect_timeout_as_unreachable",
              &brpc::FLAGS_connect_timeout_as_unreachable);
  c->GetValue("remote_cache.rpc_max_connection_pool_size",
              &brpc::FLAGS_max_connection_pool_size);
  c->GetValue("remote_cache.put_rpc_timeout_ms",
              &cache::FLAGS_put_rpc_timeout_ms);
  c->GetValue("remote_cache.range_rpc_timeout_ms",
              &cache::FLAGS_range_rpc_timeout_ms);
  c->GetValue("remote_cache.cache_rpc_timeout_ms",
              &cache::FLAGS_cache_rpc_timeout_ms);
  c->GetValue("remote_cache.prefetch_rpc_timeout_ms",
              &cache::FLAGS_prefetch_rpc_timeout_ms);
  c->GetValue("remote_cache.ping_rpc_timeout_ms",
              &cache::FLAGS_ping_rpc_timeout_ms);
  c->GetValue("remote_cache.rpc_max_retry_times",
              &cache::FLAGS_rpc_max_retry_times);
  c->GetValue("remote_cache.rpc_max_timeout_ms",
              &cache::FLAGS_rpc_max_timeout_ms);

  c->GetValue("cache_node_state.tick_duration_s",
              &cache::FLAGS_cache_node_state_tick_duration_s);
  c->GetValue("cache_node_state.normal2unstable_error_num",
              &cache::FLAGS_cache_node_state_normal2unstable_error_num);
  c->GetValue("cache_node_state.unstable2normal_succ_num",
              &cache::FLAGS_cache_node_state_unstable2normal_succ_num);
  c->GetValue("cache_node_state.check_duration_ms",
              &cache::FLAGS_cache_node_state_check_duration_ms);
}

// fuse option
struct FuseConnInfo {
  bool want_splice_move;
  bool want_splice_read;
  bool want_splice_write;
  bool want_auto_inval_data;
};

struct FuseFileInfo {
  bool keep_cache;
};

struct FuseOption {
  FuseConnInfo conn_info;
  FuseFileInfo file_info;
};

void InitFuseOption(utils::Configuration* c, FuseOption* option);

// memory option
struct PageOption {
  uint64_t page_size;
  uint64_t total_size;
  bool use_pool;
};

static void InitMemoryPageOption(utils::Configuration* c, PageOption* option) {
  // page option
  c->GetValueFatalIfFail("data_stream.page.size", &option->page_size);
  c->GetValueFatalIfFail("data_stream.page.total_size_mb", &option->total_size);
  c->GetValueFatalIfFail("data_stream.page.use_pool", &option->use_pool);

  if (option->page_size == 0) {
    CHECK(false) << "page size must greater than 0.";
  }

  option->total_size = option->total_size * kMiB;
  if (option->total_size < 64 * kMiB) {
    CHECK(false) << "page total size must greater than 64MB.";
  }
}

// vfs option
struct VFSMetaOption {
  uint32_t max_name_length{255};  // max length of file name
};

struct VFSDataOption {
  bool writeback{false};  // whether to use writeback
  std::string writeback_suffix;
};

struct VFSOption {
  blockaccess::BlockAccessOptions block_access_opt;  // from config
  PageOption page_option;
  FuseOption fuse_option;

  VFSMetaOption meta_option;
  VFSDataOption data_option;

  uint32_t dummy_server_port{10000};
};

void InitVFSOption(utils::Configuration* conf, VFSOption* option);

// ############## options end ##############

}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_COMMON_OPTIONS_CLIENT_OPTION_H_

#endif  // DINGOFS_SRC_COMMON_OPTIONS_CLIENT_CLIENT_H_

#endif  // DINGOFS_SRC_COMMON_OPTIONS_CLIENT_H_
