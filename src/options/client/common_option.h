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

#ifndef DINGOFS_SRC_OPTIONS_CLIENT_COMMON_OPTION_H_
#define DINGOFS_SRC_OPTIONS_CLIENT_COMMON_OPTION_H_

#include <gflags/gflags_declare.h>

#include "options/cache/option.h"
#include "options/client/vfs/vfs_dynamic_option.h"
#include "utils/configuration.h"
#include "utils/gflags_helper.h"

namespace brpc {
DECLARE_int32(defer_close_second);
DECLARE_int32(health_check_interval);
}  // namespace brpc

namespace dingofs {
namespace client {

DECLARE_int32(bthread_worker_num);

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
              &FLAGS_vfs_file_prefetch_block_cnt);
  c->GetValue("vfs.data.prefetch.executor_cnt",
              &fLU::FLAGS_vfs_file_prefetch_executor_num);
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
  c->GetValue("remote_cache.mds_version", &cache::FLAGS_mds_version);
  c->GetValue("remote_cache.mds_addrs", &cache::FLAGS_mds_addrs);
  c->GetValue("remote_cache.load_members_interval_ms",
              &cache::FLAGS_load_members_interval_ms);

  c->GetValue("remote_cache.fill_group_cache", &cache::FLAGS_fill_group_cache);
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

}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_OPTIONS_CLIENT_COMMON_OPTION_H_
