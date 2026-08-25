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

#ifndef DINGOFS_BLOCKCACHE_TOOLS_BENCHMARK_CLI_H_
#define DINGOFS_BLOCKCACHE_TOOLS_BENCHMARK_CLI_H_

#include <vector>

#include "blockcache/utils/flags.h"

namespace dingofs {
namespace blockcache {

inline const std::vector<FlagSection> kBenchSections = {
    {"BENCH OPTIONS",
     {"op", "threads", "iodepth", "fsid", "blksize", "blocks", "offset",
      "length", "stage", "retrieve_storage", "runtime", "time_based", "conf"}},
    {"CLIENT OPTIONS",
     {"cache_store", "fill_group_cache", "queue_depth", "shards", "cpuset",
      "pin_cpu", "poll_mode", "idle_poll_us", "task_quota_us", "io_queue_depth",
      "buffer_pool_mb", "offload_threads", "offload_queue_capacity",
      "offload_cpu_min_bytes", "offload_cpu_spin_us"}},
    {"MDS OPTIONS",
     {"mds_addrs", "cache_mds_rpc_timeout_ms", "cache_mds_rpc_retry_times",
      "cache_mds_request_retry_times"}},
    {"REMOTE CACHE OPTIONS",
     {"cache_group", "periodic_sync_members_ms", "remote_rpc_timeout_ms",
      "remote_connect_timeout_ms", "remote_rpc_max_retry",
      "remote_breaker_failures", "remote_breaker_open_ms",
      "remote_breaker_max_open_ms"}},
    {"RDMA OPTIONS",
     {"remote_rdma", "remote_rdma_device", "rdma_port_num", "rdma_gid_index",
      "rdma_max_connections", "rdma_cq_entries", "rdma_max_inflight_rpcs",
      "rdma_message_bytes", "rdma_max_inline_data", "rdma_bulk_send_wr",
      "rdma_bulk_qps", "rdma_heartbeat_interval_s", "rdma_idle_timeout_s"}},
    {"CACHE STORE OPTIONS",
     {"cache_dir", "cache_dir_uuid", "cache_size_mb", "cache_expire_s",
      "cache_cleanup_expire_interval_ms", "cache_eviction", "free_space_ratio",
      "disk_state_check_duration_ms", "disk_state_probe_timeout_ms",
      "disk_state_tick_duration_s", "disk_state_normal2unstable_error_num",
      "disk_state_unstable2normal_succ_num", "disk_state_unstable2down_s"}},
    {"STORAGE OPTIONS",
     {"storage_put_tries", "storage_get_tries", "storage_get_notfound_tries",
      "storage_put_backoff_base_ms", "storage_get_backoff_base_ms",
      "storage_get_notfound_backoff_base_ms", "upload_stage_max_inflights",
      "upload_stage_max_tries", "upload_stage_retry_delay_s",
      "max_range_size_kb"}},
    {"S3 SDK OPTIONS", {}, "options/blockaccess"},
    {"LOGGING OPTIONS", {"log_dir", "log_level", "log_v"}},
};

inline const FlagParser::Usage kBenchUsage = {
    .program = "cb",
    .usage = "  cb [OPTIONS] --op <put|get|delete>",
    .examples =
        "  $ cb --mds_addrs=10.0.0.1:6900 --op=put --threads=4 --iodepth=16\n"
        "  $ cb --mds_addrs=10.0.0.1:6900 --op=get --time_based --runtime=60\n",
    .sections = kBenchSections,
    .essential = {"op", "threads", "iodepth", "fsid", "blksize", "blocks",
                  "stage", "retrieve_storage", "runtime", "time_based", "conf",
                  "mds_addrs", "cache_store", "cache_group", "remote_rdma",
                  "cache_dir", "log_dir"},
    .required = {},
    .uuid_flag = "",
};

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_TOOLS_BENCHMARK_CLI_H_
