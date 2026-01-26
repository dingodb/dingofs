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

// Table of Contents
//   - dingo-client
//   - dingo-cache
//   - common (blockcache && mds-related)

// ###############################################
// # dingo-client
// ###############################################

// Sets the cache group name to use, e.g. "default", "group1" and etc.
DECLARE_string(cache_group);

// Sets the interval to load cache group members in milliseconds.
DECLARE_uint32(periodic_sync_members_ms);

// Sets whether the data blocks uploaded to the storage are
// simultaneously sent to the cache group node.
DECLARE_bool(fill_group_cache);

// [onfly]
// Set whether split range request into subrequests.
// (default is true)
DECLARE_bool(subrequest_ranges);

// Range size for each subrequest
DECLARE_uint32(subrequest_range_size);

// [onfly]
// Sets whether to enable prefetching for remote cache operations.
DECLARE_bool(block_prefetch);

// [onfly]
// Sets the max buffer size for cache prefetch blocks in memory
// (default is 512MB).
DECLARE_uint32(remote_prefetch_max_buffer_size_mb);

// Timeout (ms) for rpc channel connect
DECLARE_uint32(cache_rpc_connect_timeout_ms);

// Timeout (ms) for put rpc request
DECLARE_uint32(cache_put_rpc_timeout_ms);

// Timeout (ms) for range rpc request
DECLARE_uint32(cache_range_rpc_timeout_ms);

// Timeout (ms) for cache rpc request
DECLARE_uint32(cache_rpc_timeout_ms);

// Timeout (ms) for prefetch rpc request
DECLARE_uint32(cache_prefetch_rpc_timeout_ms);

// Timeout (ms) for pinging remote cache node
DECLARE_uint32(cache_ping_rpc_timeout_ms);

// Maximum retry times for rpc request
DECLARE_uint32(cache_rpc_max_retry_times);

// Maximum rpc timeout (ms) for rpc request
DECLARE_uint32(cache_rpc_max_timeout_ms);

// Sets the duration in seconds for the cache node state tick.
DECLARE_uint32(cache_node_state_tick_duration_s);

// Sets the number of errors to trigger unstable state from normal state.
DECLARE_uint32(cache_node_state_normal2unstable_error_num);

// Sets the number of successful operations to trigger normal state from
// unstable state.
DECLARE_uint32(cache_node_state_unstable2normal_succ_num);

// [onfly]
// Sets the duration in seconds to trigger down state from unstable state.
DECLARE_uint32(cache_node_state_unstable2down_s);

// Sets the duration in milliseconds to check the cache group node state.
DECLARE_uint32(cache_node_state_check_duration_ms);

// ###############################################
// # dingo-cache
// ###############################################

// Sets the cache node ID (which only for MDS v2)
DECLARE_string(id);

// Sets which group this cache node belongs to.
DECLARE_string(group_name);

// Sets the IP address to listen on for this cache group node.
DECLARE_string(listen_ip);

// Sets the port to listen on for this cache group node.
DECLARE_uint32(listen_port);

// Sets the weight of this cache group node, used for consistent hashing.
DECLARE_uint32(group_weight);

// Retrieve the whole block if length of range request is larger than this
// value.
DECLARE_uint32(max_range_size_kb);

// Sets the interval to send heartbeat to MDS in seconds.
DECLARE_uint32(periodic_heartbeat_interval_s);

// ###############################################
// # common
// ###############################################

// Sets whether to enable trace logging for cache.
DECLARE_bool(cache_trace_logging);

// Sets the cache store type, can be "none", "disk" or "3fs".
DECLARE_string(cache_store);

// Sets whether to enable stage block for writeback which will store block in
// local first then upload to storage.
// If sets to false, the block will be uploaded directly to storage even if
// using writeback mode.
DECLARE_bool(enable_stage);

// Sets whether to enable cache block .
// If sets to false, the block cache will not cache any block.
DECLARE_bool(enable_cache);

// Sets whether to enable throttling for uploading stage blocks to storage.
DECLARE_bool(upload_stage_throttle_enable);

// Sets the maximum bandwidth for uploading stage blocks to storage in MB/s.
DECLARE_uint64(upload_stage_throttle_bandwidth_mb);

// Sets the maximum IOPS for uploading stage blocks to storage.
DECLARE_uint64(upload_stage_throttle_iops);

// Sets the maximum inflight requests for uploading stage blocks to storage.
DECLARE_uint32(upload_stage_max_inflights);

// Sets the maximum inflight requests for prefetching blocks.
DECLARE_uint32(prefetch_max_inflights);

// Sets the directory to store stage and cache blocks.
// Multi directories and corresponding cache size are supported, e.g.
// "/data1:100;/data2:200;/data3:300".
DECLARE_string(cache_dir);

// [hiden]
// Sets the UUID to append to cache directory (only for internal).
DECLARE_string(cache_dir_uuid);

// Sets the maximum size of the cache in MB.
DECLARE_uint32(cache_size_mb);

// Sets the ratio of free space of total disk space.
// If the free space is less than this ratio, will trigger cleanup.
DECLARE_double(free_space_ratio);

// Sets the expiration time for cache blocks in seconds.
DECLARE_uint32(cache_expire_s);

// Sets the interval for cleaning up expired cache blocks in milliseconds.
DECLARE_uint32(cleanup_expire_interval_ms);

// Sets the IO depth for iouring.
DECLARE_uint32(iodepth);

// Sets the duration in seconds for the disk state tick.
DECLARE_uint32(disk_state_tick_duration_s);

// Sets the number of errors to trigger unstable state from normal state.
DECLARE_uint32(disk_state_normal2unstable_error_num);

// Sets the number of successful operations to trigger normal state from
// unstable state.
DECLARE_uint32(disk_state_unstable2normal_succ_num);

// Sets the duration in seconds to trigger down state from unstable state.
DECLARE_uint32(disk_state_unstable2down_s);

// Sets the duration in milliseconds to check the disk state.
DECLARE_uint32(disk_state_check_duration_ms);

// Sets the maximum retry timeout in seconds for uploading block from storage.
DECLARE_int64(storage_upload_retry_timeout_s);

// Sets the maximum retry timeout in seconds for downloading block from storage.
DECLARE_int64(storage_download_retry_timeout_s);

// Sets the MDS addresses for cache group member manager service RPC.
DECLARE_string(mds_addrs);

// Keepalive connection number per peer
DECLARE_int32(connections);

// [onfly]
DECLARE_int64(mds_rpc_timeout_ms);
DECLARE_int32(mds_rpc_retry_times);
DECLARE_uint32(mds_request_retry_times);

DECLARE_int32(brpc_idle_timeout_second);
DECLARE_bool(brpc_log_idle_connection_close);

DECLARE_bool(fix_buffer);
DECLARE_bool(block_prefetch);

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_COMMON_OPTIONS_CACHE_OPTION_H_
