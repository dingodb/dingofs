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

#ifndef DINGOFS_SRC_CLIENT_OPTIONS_COMMON_OPTION_H_
#define DINGOFS_SRC_CLIENT_OPTIONS_COMMON_OPTION_H_

#include "cache/blockcache/disk_cache_layout.h"
#include "options/cache/blockcache.h"
#include "options/cache/stub.h"
#include "options/cache/tiercache.h"
#include "utils/configuration.h"

namespace dingofs {
namespace client {

DECLARE_int32(bthread_worker_num);

struct UdsOption {
  std::string fd_comm_path;
};

static void InitUdsOption(utils::Configuration* conf, UdsOption* uds_opt) {
  if (!conf->GetValue("uds.fdCommPath", &uds_opt->fd_comm_path)) {
    uds_opt->fd_comm_path = "/var/run";
  }
}

static void InitBlockCacheOption(utils::Configuration* c,
                                 cache::BlockCacheOption* option) {
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
    c->GetValue("block_cache.prefecth_max_inflights",
                &cache::FLAGS_prefetch_max_inflights);
  }

  {  // disk cache option
    c->GetValue("disk_cache.cache_dir", &cache::FLAGS_cache_dir);
    c->GetValue("disk_cache.cache_size_mb", &cache::FLAGS_cache_size_mb);
    c->GetValue("disk_cache.free_space_ratio", &cache::FLAGS_free_space_ratio);
    c->GetValue("disk_cache.cache_expire_second", &cache::FLAGS_cache_expire_s);
    c->GetValue("disk_cache.cleanup_expire_interval_millsecond",
                &cache::FLAGS_cleanup_expire_interval_ms);
    c->GetValue("disk_cache.ioring_iodepth", &cache::FLAGS_ioring_iodepth);
    c->GetValue("disk_cache.ioring_blksize", &cache::FLAGS_ioring_blksize);
    c->GetValue("disk_cache.ioring_prefetch", &cache::FLAGS_ioring_prefetch);
  }

  {  // disk state option

    c->GetValue("disk_state.tick_duration_second",
                &cache::FLAGS_state_tick_duration_s);
    c->GetValue("disk_state.normal2unstable_io_error_num",
                &cache::FLAGS_state_normal2unstable_error_num);
    c->GetValue("disk_state.unstable2normal_io_succ_num",
                &cache::FLAGS_state_unstable2normal_succ_num);
    c->GetValue("disk_state.unstable2down_second",
                &cache::FLAGS_state_unstable2down_s);
    c->GetValue("disk_state.disk_check_duration_millsecond",
                &cache::FLAGS_check_disk_state_duration_ms);
  }

  *option = cache::BlockCacheOption();
}
static void InitRemoteBlockCacheOption(utils::Configuration* c,
                                       cache::RemoteBlockCacheOption* option) {
  c->GetValue("remote_cache.cache_group", &cache::FLAGS_cache_group);
  c->GetValue("remote_cache.load_members_interval_ms",
              &cache::FLAGS_load_members_interval_ms);
  c->GetValue("remote_cache.mds_rpc_addrs", &cache::FLAGS_mds_rpc_addrs);
  c->GetValue("remote_cache.mds_rpc_retry_total_ms",
              &cache::FLAGS_mds_rpc_retry_total_ms);
  c->GetValue("remote_cache.mds_rpc_max_timeout_ms",
              &cache::FLAGS_mds_rpc_max_timeout_ms);
  c->GetValue("remote_cache.mds_rpc_timeout_ms",
              &cache::FLAGS_mds_rpc_timeout_ms);
  c->GetValue("remote_cache.mds_rpc_retry_interval_us",
              &cache::FLAGS_mds_rpc_retry_interval_us);
  c->GetValue("remote_cache.mds_rpc_max_failed_times_before_change_addr",
              &cache::FLAGS_mds_rpc_max_failed_times_before_change_addr);
  c->GetValue("remote_cache.mds_rpc_normal_retry_times_before_trigger_wait",
              &cache::FLAGS_mds_rpc_normal_retry_times_before_trigger_wait);
  c->GetValue("remote_cache.mds_rpc_wait_sleep_ms",
              &cache::FLAGS_mds_rpc_wait_sleep_ms);
  c->GetValue("remote_cache.put_rpc_timeout_ms",
              &cache::FLAGS_put_rpc_timeout_ms);
  c->GetValue("remote_cache.range_rpc_timeout_ms",
              &cache::FLAGS_range_rpc_timeout_ms);
  c->GetValue("remote_cache.cache_rpc_timeout_ms",
              &cache::FLAGS_cache_rpc_timeout_ms);
  c->GetValue("remote_cache.prefetch_rpc_timeout_ms",
              &cache::FLAGS_prefetch_rpc_timeout_ms);
  *option = cache::RemoteBlockCacheOption();
}

static void RewriteCacheDir(cache::BlockCacheOption* option, std::string uuid) {
  auto& disk_cache_options = option->disk_cache_options;
  for (auto& disk_cache_option : disk_cache_options) {
    std::string cache_dir = disk_cache_option.cache_dir;
    disk_cache_option.cache_dir = cache::RealCacheDir(cache_dir, uuid);
  }
}

}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_OPTIONS_COMMON_OPTION_H_