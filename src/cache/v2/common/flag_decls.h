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

#ifndef DINGOFS_CACHE_V2_COMMON_FLAG_DECLS_H_
#define DINGOFS_CACHE_V2_COMMON_FLAG_DECLS_H_

#include <gflags/gflags_declare.h>

namespace dingofs {
namespace cache {
namespace v2 {

// node/node.cc
DECLARE_string(id);
DECLARE_string(listen_ip);
DECLARE_uint32(listen_port);
DECLARE_string(bind_ip);
DECLARE_bool(rdma);
DECLARE_string(rdma_device);
DECLARE_uint32(shards);
DECLARE_string(cpuset);
DECLARE_bool(poll_mode);
DECLARE_uint64(buffer_pool_mb);
DECLARE_bool(daemonize);

// node/membership.cc
DECLARE_string(group_name);
DECLARE_uint32(group_weight);

// node/heartbeat.cc
DECLARE_uint32(periodic_heartbeat_interval_s);

// common/mds_client.cc
DECLARE_string(mds_addrs);
DECLARE_int64(cache_mds_rpc_timeout_ms);
DECLARE_int32(cache_mds_rpc_retry_times);
DECLARE_uint32(cache_mds_request_retry_times);

// core/runtime/worker_pool.cc
DECLARE_uint32(offload_threads);
DECLARE_uint32(offload_queue_capacity);

// object/client.cc
DECLARE_uint32(storage_put_tries);
DECLARE_uint32(storage_get_tries);
DECLARE_uint32(storage_get_notfound_tries);
DECLARE_uint32(storage_put_backoff_base_ms);
DECLARE_uint32(storage_get_backoff_base_ms);
DECLARE_uint32(storage_get_notfound_backoff_base_ms);

// block/uploader.cc
DECLARE_uint32(upload_stage_max_inflights);
DECLARE_uint32(upload_stage_max_tries);
DECLARE_uint32(upload_stage_retry_delay_s);

// block/local_cache.cc
DECLARE_uint32(max_range_size_kb);

// tier/tier_cache.cc
DECLARE_bool(fill_group_cache);

// remote/remote_cache.cc
DECLARE_bool(remote_rdma);
DECLARE_uint32(remote_rpc_timeout_ms);
DECLARE_uint32(remote_connect_timeout_ms);

// remote/watcher.cc
DECLARE_uint32(group_sync_interval_ms);

// remote/circuit_breaker.cc
DECLARE_uint32(remote_breaker_failures);
DECLARE_uint32(remote_breaker_open_ms);
DECLARE_uint32(remote_breaker_max_open_ms);

// store/eviction.cc
DECLARE_string(cache_eviction);

// store/disk_cache.cc
DECLARE_string(cache_dir);
DECLARE_string(cache_dir_uuid);
DECLARE_uint32(cache_size_mb);

// store/cache_manager.cc
DECLARE_uint32(cache_expire_s);
DECLARE_uint32(cache_cleanup_expire_interval_ms);
DECLARE_double(free_space_ratio);

// store/health.cc
DECLARE_uint32(disk_state_check_duration_ms);
DECLARE_uint32(disk_state_probe_timeout_ms);
DECLARE_uint32(disk_state_tick_duration_s);
DECLARE_uint32(disk_state_normal2unstable_error_num);
DECLARE_uint32(disk_state_unstable2normal_succ_num);
DECLARE_uint32(disk_state_unstable2down_s);

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_COMMON_FLAG_DECLS_H_
