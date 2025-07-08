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
 * Created Date: 2025-06-20
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_CONFIG_BLOCKCACHE_H_
#define DINGOFS_SRC_CACHE_CONFIG_BLOCKCACHE_H_

#include <gflags/gflags_declare.h>

#include <vector>

namespace dingofs {
namespace cache {

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

// Sets the maximum inflight requests for prefetching blocks.
DECLARE_uint32(prefetch_max_inflights);

// Sets whether to enable throttling for uploading stage blocks to storage.
DECLARE_bool(upload_stage_throttle_enable);

// Sets the maximum bandwidth for uploading stage blocks to storage in MB/s.
DECLARE_uint64(upload_stage_throttle_bandwidth_mb);

// Sets the maximum IOPS for uploading stage blocks to storage.
DECLARE_uint64(upload_stage_throttle_iops);

// Sets the maximum inflight requests for uploading stage blocks to storage.
DECLARE_uint32(upload_stage_max_inflights);

// Sets the directory to store stage and cache blocks.
// Multi directories and corresponding cache size are supported, e.g.
// "/data1:100;/data2:200;/data3:300".
DECLARE_string(cache_dir);

// Sets the maximum size of the cache in MB.
DECLARE_uint32(cache_size_mb);

// Sets the ratio of free space of total disk space.
// If the free space is less than this ratio, will trigger cleanup.
DECLARE_double(free_space_ratio);

// Sets the expiration time for cache blocks in seconds.
DECLARE_uint32(cache_expire_s);

// Sets the interval for cleaning up expired cache blocks in milliseconds.
DECLARE_uint32(cleanup_expire_interval_ms);

// Sets the block size for iouring operations in bytes.
DECLARE_uint32(ioring_blksize);

// Sets the I/O depth for iouring operations.
DECLARE_uint32(ioring_iodepth);

// Sets whether to enable prefetching for iouring operations.
DECLARE_bool(ioring_prefetch);

// Sets the duration in seconds for the disk state tick.
DECLARE_uint32(state_tick_duration_s);

// Sets the number of errors to trigger unstable state from normal state.
DECLARE_uint32(state_normal2unstable_error_num);

// Sets the number of successful operations to trigger normal state from
// unstable state.
DECLARE_uint32(state_unstable2normal_succ_num);

// Sets the duration in seconds to trigger down state from unstable state.
DECLARE_uint32(state_unstable2down_s);

// Sets the duration in milliseconds to check the disk state.
DECLARE_uint32(check_disk_state_duration_ms);

struct DiskCacheOption {
  DiskCacheOption();

  std::string cache_store;
  uint32_t cache_index;
  std::string cache_dir;
  uint64_t cache_size_mb;
};

struct BlockCacheOption {
  BlockCacheOption();

  std::string cache_store;
  bool enable_stage;
  bool enable_cache;

  std::vector<DiskCacheOption> disk_cache_options;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_CONFIG_BLOCKCACHE_H_
