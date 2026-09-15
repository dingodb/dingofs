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

#ifndef DINGOFS_BLOCKCACHE_NODE_CLI_H_
#define DINGOFS_BLOCKCACHE_NODE_CLI_H_

#include <vector>

#include "blockcache/utils/flags.h"

namespace dingofs {
namespace blockcache {

inline const std::vector<FlagSection> kNodeSections = {
    // node
    {"NODE OPTIONS",
     {"bind_all",
      "brpc_idle_timeout_second",
      "brpc_max_concurrency",
      "brpc_reply_on_bthread",
      "buffer_pool_mb",
      "conf",
      "cpuset",
      "daemonize",
      "group_name",
      "group_weight",
      "heartbeat_interval_s",
      "id",
      "idle_poll_us",
      "iodepth",
      "listen_ip",
      "listen_port",
      "pin_cpu",
      "poll_mode",
      "shards",
      "task_quota_us"}},

    // rdma
    {"RDMA OPTIONS",
     {"cache_rdma_device", "cache_rdma_port_num", "rdma_bulk_qps",
      "rdma_bulk_send_wr", "rdma_cq_entries", "rdma_gid_idx",
      "rdma_heartbeat_interval_s", "rdma_idle_timeout_s",
      "rdma_max_connections", "rdma_max_inflight_rpcs", "rdma_max_inline_data",
      "rdma_message_bytes", "use_rdma"}},

    // mds
    {"MDS OPTIONS",
     {"cache_mds_request_retry_times", "cache_mds_rpc_retry_times",
      "cache_mds_rpc_timeout_ms", "mds_addrs"}},

    // store
    {"CACHE STORE OPTIONS",
     {"cache_cleanup_expire_interval_ms", "cache_dir", "cache_dir_uuid",
      "cache_eviction", "cache_expire_s", "cache_size_mb",
      "disk_state_check_duration_ms", "disk_state_normal2unstable_error_num",
      "disk_state_probe_timeout_ms", "disk_state_tick_duration_s",
      "disk_state_unstable2down_s", "disk_state_unstable2normal_succ_num",
      "free_space_ratio"}},

    // storage
    {"STORAGE OPTIONS",
     {"max_range_size_kb", "storage_download_max_tries",
      "storage_download_notfound_max_tries",
      "storage_download_notfound_retry_backoff_base_ms",
      "storage_download_retry_backoff_base_ms", "storage_upload_max_tries",
      "storage_upload_retry_backoff_base_ms", "upload_stage_max_inflights",
      "upload_stage_max_tries", "upload_stage_retry_delay_s"}},

    // offload
    {"OFFLOAD OPTIONS",
     {"offload_cpu_min_bytes", "offload_cpu_spin_us", "offload_queue_capacity",
      "offload_threads"}},

    // s3
    {"S3 SDK OPTIONS", {}, "options/blockaccess"},

    // logging
    {"LOGGING OPTIONS", {"log_dir", "log_level", "log_v"}},
};

inline const FlagParser::Usage kNodeUsage = {
    .program = "dingo-cache",
    .usage = "  dingo-cache [OPTIONS] --id <uuid> --listen_ip <ip>",
    .examples =
        "  $ dingo-cache --id=85a4b352-... --listen_ip=10.0.0.2\n"
        "  $ dingo-cache --conf cache.conf --daemonize\n",
    .sections = kNodeSections,
    .essential = {"id", "listen_ip", "listen_port", "use_rdma", "daemonize",
                  "conf", "mds_addrs", "group_name", "cache_dir",
                  "cache_size_mb", "log_dir"},
    .required = {"id", "listen_ip"},
    .uuid_flag = "id",
};

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_NODE_CLI_H_
