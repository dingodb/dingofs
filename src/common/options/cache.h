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

/*
 * Project: DingoFS
 * Created Date: 2025-08-19
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_COMMON_OPTIONS_CACHE_OPTION_H_
#define DINGOFS_COMMON_OPTIONS_CACHE_OPTION_H_

#include <gflags/gflags_declare.h>

namespace dingofs {
namespace cache {

// Local cache tier ------------------------------------------------------------

// Local cache store type: "disk" or "memory".
// "none" is accepted by some callers to disable the local tier.
DECLARE_string(cache_store);

// Enable local staging for writeback blocks before they are uploaded to
// storage. Disabling this uploads writeback blocks directly to storage.
DECLARE_bool(enable_stage);

// Enable local read cache for blocks.
DECLARE_bool(enable_cache);

// Send blocks uploaded to storage to the remote cache group at the same time.
DECLARE_bool(fill_group_cache);

// Maximum number of concurrent local prefetch requests.
DECLARE_uint32(prefetch_max_inflights);

// Blocks smaller than this size are pinned to the local tier when local cache
// is enabled. 0 disables this shortcut.
DECLARE_uint32(small_block_size_kb);

// Maximum number of concurrent uploads for staged blocks.
DECLARE_uint32(upload_stage_max_inflights);

// Maximum tries per round for uploading one stage block to storage, a failed
// round is re-enqueued on a slow cycle.
DECLARE_uint32(upload_stage_max_tries);

// Delay in seconds before re-enqueueing the stage block whose upload failed.
DECLARE_uint32(upload_stage_retry_delay_s);

// Local disk cache ------------------------------------------------------------

// Directory list for stage and cached blocks. Use comma-separated
// path[:size_mb] entries, e.g. "/data1:100,/data2:200".
DECLARE_string(cache_dir);

// UUID suffix appended to each cache directory. Set internally by the client or
// cache node to isolate different cache instances.
DECLARE_string(cache_dir_uuid);

// Default maximum local cache size in MB. Per-directory sizes in cache_dir
// override this value for each corresponding directory.
DECLARE_uint32(cache_size_mb);

// Minimum free-space ratio for the underlying disk. Cleanup is triggered when
// free space drops below this ratio.
DECLARE_double(free_space_ratio);

// Expiration time for cached blocks in seconds. Staged blocks are not expired.
DECLARE_uint32(cache_expire_s);

// Cache eviction policy: sieve | s3fifo | 2random | lru | none (restart to change).
DECLARE_string(cache_eviction);

// Interval for scanning and removing expired cached blocks in milliseconds.
DECLARE_uint32(cache_cleanup_expire_interval_ms);

// Maximum io_uring queue depth for local cache I/O.
DECLARE_uint32(iodepth);

// Disk health state -----------------------------------------------------------

// Interval for probing disk health in milliseconds.
DECLARE_uint32(disk_state_check_duration_ms);

// State-machine tick duration for disk health transitions in seconds.
DECLARE_uint32(disk_state_tick_duration_s);

// Number of disk errors needed to move from normal to unstable.
DECLARE_uint32(disk_state_normal2unstable_error_num);

// Number of successful disk checks needed to move from unstable to normal.
DECLARE_uint32(disk_state_unstable2normal_succ_num);

// Time in unstable state before moving a disk to down, in seconds.
DECLARE_uint32(disk_state_unstable2down_s);

// Cache node identity and server ---------------------------------------------

// Cache node ID. Used as the cache directory UUID for cache-node processes.
DECLARE_string(id);

// Cache group this cache node belongs to.
DECLARE_string(group_name);

// IP address configured for this cache node.
DECLARE_string(listen_ip);

// Port configured for this cache node.
DECLARE_uint32(listen_port);

// Listen on 0.0.0.0 instead of listen_ip so remote clients can connect.
DECLARE_bool(public_address);

// Node weight used by cache-group consistent hashing.
DECLARE_uint32(group_weight);

// Retrieve the whole block when a range request is at least this many KB.
DECLARE_uint32(max_range_size_kb);

// Serialize concurrent storage retrieves for the same block.
DECLARE_bool(retrieve_storage_lock);

// Timeout for waiting on another retrieve-storage task, in milliseconds.
DECLARE_uint32(retrieve_storage_lock_timeout_ms);

// Heartbeat interval from cache node to MDS, in seconds.
DECLARE_uint32(periodic_heartbeat_interval_s);

// Remote cache group and RPC --------------------------------------------------

// Cache group name used by clients. Empty disables the remote cache tier.
DECLARE_string(cache_group);

// Interval for refreshing cache group members from MDS, in milliseconds.
DECLARE_uint32(periodic_sync_members_ms);

// Number of keepalive connections per remote cache node.
DECLARE_int32(connections);

// Timeout for connecting a remote cache RPC channel, in milliseconds.
DECLARE_uint32(cache_rpc_connect_timeout_ms);

// Timeout for remote cache Put RPCs, in milliseconds.
DECLARE_uint32(cache_put_rpc_timeout_ms);

// Timeout for remote cache range RPCs, in milliseconds.
DECLARE_uint32(cache_range_rpc_timeout_ms);

// Timeout for regular remote cache RPCs, in milliseconds.
DECLARE_uint32(cache_rpc_timeout_ms);

// Timeout for remote cache prefetch RPCs, in milliseconds.
DECLARE_uint32(cache_prefetch_rpc_timeout_ms);

// Timeout for pinging a remote cache node, in milliseconds.
DECLARE_uint32(cache_ping_rpc_timeout_ms);

// Maximum retry count for remote cache RPCs.
DECLARE_uint32(cache_rpc_max_retry_times);

// Maximum timeout cap for remote cache RPC retries, in milliseconds.
DECLARE_uint32(cache_rpc_max_timeout_ms);

// Remote cache node health state ---------------------------------------------

// State-machine tick duration for remote cache-node health transitions, in
// seconds.
DECLARE_uint32(cache_node_state_tick_duration_s);

// Number of errors needed to move a remote cache node from normal to unstable.
DECLARE_uint32(cache_node_state_normal2unstable_error_num);

// Number of successful pings needed to move from unstable to normal.
DECLARE_uint32(cache_node_state_unstable2normal_succ_num);

// Time in unstable state before moving a remote cache node to down, in seconds.
DECLARE_uint32(cache_node_state_unstable2down_s);

// Interval for checking remote cache-node health, in milliseconds.
DECLARE_uint32(cache_node_state_check_duration_ms);

// Storage and MDS -------------------------------------------------------------

// Maximum tries (including the first attempt) for uploading one block to
// storage.
DECLARE_uint32(storage_upload_max_tries);

// Maximum tries (including the first attempt) for downloading one block from
// storage.
DECLARE_uint32(storage_download_max_tries);

// Base backoff in milliseconds between upload retries, the real backoff is
// base * tried * tried, capped at 60 seconds.
DECLARE_uint32(storage_upload_retry_backoff_base_ms);

// Base backoff in milliseconds between download retries, the real backoff is
// base * tried, capped at 10 seconds.
DECLARE_uint32(storage_download_retry_backoff_base_ms);

// Number of worker threads for async storage upload tasks.
DECLARE_uint64(storage_upload_thread_pool_size);

// MDS addresses used by cache group member management RPCs.
DECLARE_string(mds_addrs);

// Timeout for cache MDS RPCs, in milliseconds.
DECLARE_int64(cache_mds_rpc_timeout_ms);

// Retry count for each cache MDS RPC attempt.
DECLARE_int32(cache_mds_rpc_retry_times);

// Retry count for the cache MDS request wrapper.
DECLARE_uint32(cache_mds_request_retry_times);

// BRPC aliases ---------------------------------------------------------------

// Cache-specific alias for brpc::idle_timeout_second.
DECLARE_int32(brpc_idle_timeout_second);

// Cache-specific alias for brpc::log_idle_connection_close.
DECLARE_bool(brpc_log_idle_connection_close);

// RDMA transport --------------------------------------------------------------

// Enable Infiniband/RDMA transport for cache RPCs.
DECLARE_bool(use_rdma);

// IB device and HCA port (1-based) used by the cache RDMA path.
DECLARE_string(cache_rdma_device);
DECLARE_uint32(cache_rdma_port_num);

namespace infiniband {

// GID table index used by RoCE.
DECLARE_int32(rdma_gid_idx);

// Size of each RDMA send buffer, in bytes.
DECLARE_int32(rdma_send_buffer_size);

// Maximum number of send work requests posted to a send queue.
DECLARE_int32(rdma_send_queue_size);

// Size of each RDMA receive buffer, in bytes.
DECLARE_int32(rdma_recv_buffer_size);

// Maximum number of receive work requests posted to a receive queue.
DECLARE_int32(rdma_recv_queue_size);

// Number of RDMA event dispatcher threads.
DECLARE_int32(rdma_event_dispatcher_num);

// Timeout for an RDMA RPC response, in milliseconds.
DECLARE_int32(rdma_rpc_timeout_ms);

// Signal one client request SEND every N requests. 0 disables periodic
// signaled request SENDs.
DECLARE_uint32(rdma_client_signal_request_send_every);

}  // namespace infiniband

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_COMMON_OPTIONS_CACHE_OPTION_H_
