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
 * Created Date: 2025-06-21
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_CONFIG_TIERCACHE_H_
#define DINGOFS_SRC_CACHE_CONFIG_TIERCACHE_H_

#include <gflags/gflags_declare.h>

#include "stub/common/config.h"

namespace dingofs {
namespace cache {

// Sets the cache group name to use, e.g. "default", "group1" and etc.
DECLARE_string(cache_group);

// Sets the interval to load cache group members in milliseconds.
DECLARE_uint32(load_members_interval_ms);

// Sets whether the data blocks uploaded to the storage are
// simultaneously sent to the cache group node.
DECLARE_bool(fill_group_cache);

// Set whether split range request into subrequests.
// (default is true)
DECLARE_bool(subrequest_ranges);

// Range size for each subrequest
DECLARE_uint32(subrequest_range_size);

// Sets whether to enable prefetching for remote cache operations.
DECLARE_bool(enable_remote_prefetch);

// Sets the max buffer size for cache prefetch blocks in memory
// (default is 512MB).
DECLARE_uint32(remote_prefetch_max_buffer_size_mb);

// Timeout (ms) for put rpc request
DECLARE_uint32(put_rpc_timeout_ms);

// Timeout (ms) for range rpc request
DECLARE_uint32(range_rpc_timeout_ms);

// Timeout (ms) for cache rpc request
DECLARE_uint32(cache_rpc_timeout_ms);

// Timeout (ms) for prefetch rpc request
DECLARE_uint32(prefetch_rpc_timeout_ms);

// Timeout (ms) for pinging remote cache node
DECLARE_uint32(ping_rpc_timeout_ms);

// Maximum retry times for rpc request
DECLARE_uint32(rpc_max_retry_times);

// Maximum rpc timeout (ms) for rpc request
DECLARE_uint32(rpc_max_timeout_ms);

// Sets the duration in seconds for the node state tick.
DECLARE_uint32(node_state_tick_duration_s);

// Sets the number of errors to trigger unstable state from normal state.
DECLARE_uint32(node_state_normal2unstable_error_num);

// Sets the number of successful operations to trigger normal state from
// unstable state.
DECLARE_uint32(node_state_unstable2normal_succ_num);

// Sets the duration in seconds to trigger down state from unstable state.
DECLARE_uint32(node_state_unstable2down_s);

// Sets the duration in milliseconds to check the cache group node state.
DECLARE_uint32(check_cache_node_state_duration_ms);

struct RemoteBlockCacheOption {
  RemoteBlockCacheOption();

  std::string cache_group;
  uint32_t load_members_interval_ms;
  stub::common::MdsOption mds_option;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_CONFIG_TIERCACHE_H_
