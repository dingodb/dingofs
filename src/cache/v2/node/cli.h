/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

#ifndef DINGOFS_CACHE_V2_NODE_CLI_H_
#define DINGOFS_CACHE_V2_NODE_CLI_H_

#include <vector>

#include "cache/v2/utils/flags.h"

namespace dingofs {
namespace cache {
namespace v2 {

inline const std::vector<FlagSection> kSections = {
    {"NODE OPTIONS",
     {"id", "listen_ip", "listen_port", "bind_ip", "rdma", "rdma_device",
      "shards", "cpuset", "pin_cpu", "poll_mode", "buffer_pool_mb",
      "daemonize", "conf",
      "group_name", "group_weight"}},
    {"MDS OPTIONS",
     {"mds_addrs", "cache_mds_rpc_timeout_ms", "cache_mds_rpc_retry_times",
      "cache_mds_request_retry_times", "periodic_heartbeat_interval_s"}},
    {"CACHE STORE OPTIONS",
     {"cache_dir", "cache_dir_uuid", "cache_size_mb", "cache_expire_s",
      "cache_cleanup_expire_interval_ms", "cache_eviction", "iodepth",
      "free_space_ratio", "disk_state_check_duration_ms",
      "disk_state_probe_timeout_ms", "disk_state_tick_duration_s",
      "disk_state_normal2unstable_error_num",
      "disk_state_unstable2normal_succ_num", "disk_state_unstable2down_s"}},
    {"STORAGE OPTIONS",
     {"storage_put_tries", "storage_get_tries", "storage_get_notfound_tries",
      "storage_put_backoff_base_ms", "storage_get_backoff_base_ms",
      "storage_get_notfound_backoff_base_ms", "upload_stage_max_inflights",
      "upload_stage_max_tries", "upload_stage_retry_delay_s",
      "max_range_size_kb"}},
    {"OFFLOAD OPTIONS",
     {"offload_threads", "offload_queue_capacity", "offload_cpu_min_bytes",
      "offload_cpu_spin_us"}},
    {"S3 SDK OPTIONS", {}, "options/blockaccess"},
    {"LOGGING OPTIONS", {"log_dir", "log_level", "log_v"}},
};

inline const FlagParser::Usage kUsage = {
    .program = "dingo-cache",
    .usage = "  dingo-cache [OPTIONS] --id <uuid> --listen_ip <ip>",
    .examples =
        "  $ dingo-cache --id=85a4b352-... --listen_ip=10.0.0.2\n"
        "  $ dingo-cache --conf cache.conf --daemonize\n",
    .sections = kSections,
    .essential = {"id", "listen_ip", "listen_port", "rdma", "daemonize", "conf",
                  "mds_addrs", "group_name", "cache_dir", "cache_size_mb",
                  "log_dir"},
    .required = {"id", "listen_ip"},
    .uuid_flag = "id",
};

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_NODE_CLI_H_
