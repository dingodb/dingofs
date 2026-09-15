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

#ifndef DINGOFS_COMMON_OPTIONS_CACHE_H_
#define DINGOFS_COMMON_OPTIONS_CACHE_H_

#include <gflags/gflags_declare.h>

// Flag declarations for the blockcache module (src/blockcache). The legacy
// cache module (src/cache) keeps its own copy in cache/common/flags.h.

namespace dingofs {
namespace blockcache {

// blockcache/node/node.cc
DECLARE_string(id);
DECLARE_string(listen_ip);
DECLARE_uint32(listen_port);
DECLARE_bool(bind_all);
DECLARE_bool(daemonize);

// blockcache/infiniband/base/device.cc -- rdma transport switch and device,
// shared by both roles
DECLARE_bool(use_rdma);
DECLARE_string(cache_rdma_device);
DECLARE_uint32(cache_rdma_port_num);
DECLARE_int32(rdma_gid_idx);

// blockcache/infiniband/connection/connection.cc -- wire geometry and
// liveness, shared by both roles
DECLARE_uint32(rdma_max_inflight_rpcs);
DECLARE_uint32(rdma_message_bytes);
DECLARE_uint32(rdma_max_connections);
DECLARE_uint32(rdma_heartbeat_interval_s);
DECLARE_uint32(rdma_idle_timeout_s);

// blockcache/infiniband/connection/queue_pairs.cc
DECLARE_uint32(rdma_max_inline_data);
DECLARE_uint32(rdma_bulk_send_wr);
DECLARE_uint32(rdma_bulk_qps);

// blockcache/infiniband/base/completion_queue.cc
DECLARE_uint32(rdma_cq_entries);

// blockcache/core/runtime/runtime.cc
DECLARE_uint32(shards);
DECLARE_string(cpuset);
DECLARE_bool(pin_cpu);
DECLARE_bool(poll_mode);

// blockcache/core/reactor/reactor.cc
DECLARE_uint32(idle_poll_us);

// blockcache/core/reactor/dispatcher.cc
DECLARE_uint32(task_quota_us);

// blockcache/core/fs/io_ring.cc
DECLARE_uint32(iodepth);

// blockcache/core/memory/buffer.cc
DECLARE_uint64(buffer_pool_mb);

// blockcache/net/brpc/brpc_server.cc
DECLARE_int32(brpc_max_concurrency);
DECLARE_bool(brpc_reply_on_bthread);
DECLARE_int32(brpc_idle_timeout_second);

// blockcache/node/membership.cc
DECLARE_string(group_name);
DECLARE_uint32(group_weight);

// blockcache/node/heartbeat.cc
DECLARE_uint32(heartbeat_interval_s);

// blockcache/common/mds_client.cc
DECLARE_string(mds_addrs);
DECLARE_int64(cache_mds_rpc_timeout_ms);
DECLARE_int32(cache_mds_rpc_retry_times);
DECLARE_uint32(cache_mds_request_retry_times);

// blockcache/core/runtime/thread_pool.cc
DECLARE_uint32(offload_threads);
DECLARE_uint32(offload_queue_capacity);
DECLARE_uint32(offload_cpu_min_bytes);
DECLARE_uint32(offload_cpu_spin_us);

// blockcache/object/object.cc
DECLARE_uint32(storage_upload_max_tries);
DECLARE_uint32(storage_download_max_tries);
DECLARE_uint32(storage_download_notfound_max_tries);
DECLARE_uint32(storage_upload_retry_backoff_base_ms);
DECLARE_uint32(storage_download_retry_backoff_base_ms);
DECLARE_uint32(storage_download_notfound_retry_backoff_base_ms);

// blockcache/block/uploader.cc
DECLARE_uint32(upload_stage_max_inflights);
DECLARE_uint32(upload_stage_max_tries);
DECLARE_uint32(upload_stage_retry_delay_s);

// blockcache/block/local_cache.cc
DECLARE_uint32(max_range_size_kb);

// blockcache/tier/tier_cache.cc
DECLARE_bool(fill_group_cache);
DECLARE_string(cache_store);
DECLARE_bool(enable_stage);
DECLARE_bool(enable_cache);

// blockcache/api/cache.cc
DECLARE_uint32(queue_depth);

// blockcache/remote/remote_cache.cc
DECLARE_string(cache_group);
DECLARE_uint32(cache_rpc_timeout_ms);
DECLARE_uint32(cache_rpc_connect_timeout_ms);

// blockcache/net/brpc/brpc_channel.cc
DECLARE_int32(cache_rpc_max_retry_times);

// blockcache/remote/members.cc
DECLARE_uint32(periodic_sync_members_ms);

// blockcache/remote/circuit_breaker.cc
DECLARE_uint32(remote_breaker_failures);
DECLARE_uint32(remote_breaker_open_ms);
DECLARE_uint32(remote_breaker_max_open_ms);

// blockcache/store/eviction.cc
DECLARE_string(cache_eviction);

// blockcache/store/disk_cache.cc
DECLARE_string(cache_dir);
DECLARE_string(cache_dir_uuid);
DECLARE_uint32(cache_size_mb);

// blockcache/store/cache_manager.cc
DECLARE_uint32(cache_expire_s);
DECLARE_uint32(cache_cleanup_expire_interval_ms);
DECLARE_double(free_space_ratio);

// blockcache/store/health.cc
DECLARE_uint32(disk_state_check_duration_ms);
DECLARE_uint32(disk_state_probe_timeout_ms);
DECLARE_uint32(disk_state_tick_duration_s);
DECLARE_uint32(disk_state_normal2unstable_error_num);
DECLARE_uint32(disk_state_unstable2normal_succ_num);
DECLARE_uint32(disk_state_unstable2down_s);

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_COMMON_OPTIONS_CACHE_H_
