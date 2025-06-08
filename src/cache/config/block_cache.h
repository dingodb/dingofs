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
 * Created Date: 2025-06-02
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_CONFIG_BLOCK_CACHE_H_
#define DINGOFS_SRC_CACHE_CONFIG_BLOCK_CACHE_H_

#include <gflags/gflags.h>

namespace dingofs {
namespace cache {

// block cache
DECLARE_string(cache_store);
DECLARE_bool(cache_access_logging);
DECLARE_bool(upload_stage_throttle_enable);
DECLARE_uint32(upload_stage_throttle_bandwidth_mb);
DECLARE_uint32(upload_stage_throttle_iops);
DECLARE_uint32(prefetch_max_inflights);

// disk cache
DECLARE_string(cache_dir);
DECLARE_uint32(cache_size_mb);
DECLARE_double(free_space_ratio);
DECLARE_uint32(cache_expire_s);
DECLARE_uint32(cleanup_expire_interval_ms);
DECLARE_bool(enable_stage);
DECLARE_bool(enable_cache);
DECLARE_uint32(ioring_blksize);
DECLARE_uint32(ioring_iodepth);
DECLARE_bool(ioring_prefetch);
DECLARE_bool(drop_page_cache);

// disk state
DECLARE_uint32(state_tick_duration_s);
DECLARE_uint32(state_normal2unstable_io_error_num);
DECLARE_uint32(state_unstable2normal_io_succ_num);
DECLARE_uint32(state_unstable2down_s);
DECLARE_uint32(check_disk_state_duration_ms);

struct DiskCacheOption {
  DiskCacheOption();

  std::string cache_store;
  std::string cache_dir;
  uint64_t cache_size_mb;
};

struct BlockCacheOption {
  BlockCacheOption();

  std::string cache_store;

  std::vector<DiskCacheOption> disk_cache_options;
};

inline bool IsNoneCacheStore(const std::string& cache_store) {
  return cache_store == "none";
}

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_CONFIG_BLOCK_CACHE_H_
