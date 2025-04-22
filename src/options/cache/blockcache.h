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
 * WITHOUT WAR::NTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Project: DingoFS
 * Created Date: 2025-05-07
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_OPTIONS_CACHE_BLOCKCACHE_H_
#define DINGOFS_SRC_OPTIONS_CACHE_BLOCKCACHE_H_

#include <gflags/gflags_declare.h>

#include "options/options.h"

namespace dingofs {
namespace options {
namespace cache {

bool RewriteBlockCacheOption(BaseOption* base);
bool ValidateBlockCacheOption(BaseOption* base);

DECLARE_bool(block_cache_logging);
DECLARE_bool(block_cache_stage_bandwidth_throttle_enable);
DECLARE_uint32(block_cache_stage_bandwidth_throttle_mb);

DECLARE_bool(disk_cache_drop_page_cache);
DECLARE_double(disk_cache_free_space_ratio);
DECLARE_uint32(disk_cache_expire_s);
DECLARE_uint32(disk_cache_cleanup_expire_interval_ms);

DECLARE_uint32(disk_state_tick_duration_s);
DECLARE_uint32(disk_state_normal2unstable_io_error_num);
DECLARE_uint32(disk_state_unstable2normal_io_succ_num);
DECLARE_uint32(disk_state_unstable2down_s);
DECLARE_uint32(disk_state_disk_check_duration_ms);

class DiskStateOption : public BaseOption {
  BIND_FLAG_uint32(tick_duration_s, disk_state_tick_duration_s);
  BIND_FLAG_uint32(normal2unstable_io_error_num,
                   disk_state_normal2unstable_io_error_num);
  BIND_FLAG_uint32(unstable2normal_io_succ_num,
                   disk_state_unstable2normal_io_succ_num);
  BIND_FLAG_uint32(unstable2down_s, disk_state_unstable2down_s);
  BIND_FLAG_uint32(disk_check_duration_ms, disk_state_disk_check_duration_ms);
};

class DiskCacheOption : public BaseOption {
  BIND_string_array(cache_dirs, STR_ARRAY{"/tmp/dingofs-cache"}, "");
  BIND_uint32(cache_size_mb, 10240, "");
  BIND_FLAG_double(free_space_ratio, disk_cache_free_space_ratio);
  BIND_FLAG_uint32(cache_expire_s, disk_cache_expire_s);
  BIND_FLAG_uint32(cleanup_expire_interval_ms,
                   disk_cache_cleanup_expire_interval_ms);
  BIND_FLAG_bool(drop_page_cache, disk_cache_drop_page_cache);
  BIND_uint32(ioring_iodepth, 128, "");
  BIND_uint32(ioring_blksize, 1048576, "");
  BIND_bool(ioring_prefetch, true, "");

  // suboption
  BIND_suboption(disk_state_option, "disk_state", DiskStateOption);

  // anon & rewrite_by
  BIND_ANON_uint32(cache_index, 0, "");
  BIND_ANON_string(cache_dir, "", "");
  BIND_ANON_string(filesystem_type, "local", "");
};

class BlockCacheOption : public BaseOption {
  BIND_FLAG_bool(logging, block_cache_logging);
  BIND_string(cache_store, "disk", "");
  BIND_bool(stage, true, "");
  BIND_FLAG_bool(stage_bandwidth_throttle_enable,
                 block_cache_stage_bandwidth_throttle_enable);
  BIND_FLAG_uint32(stage_bandwidth_throttle_mb,
                   block_cache_stage_bandwidth_throttle_mb);
  BIND_int32(upload_stage_workers, 10, "");
  BIND_int32(upload_stage_queue_size, 10000, "");
  BIND_int32(prefetch_workers, 128, "");
  BIND_int32(prefetch_queue_size, 128, "");

  // suboption
  BIND_suboption(disk_cache_option, "disk_cache", DiskCacheOption);

  // anon && rewrite_by
  BIND_ANON_any(std::vector<DiskCacheOption>, disk_cache_options,
                std::vector<DiskCacheOption>{}, "");
  REWRITE_BY(RewriteBlockCacheOption);
  VALIDATE_BY(ValidateBlockCacheOption);
};

}  // namespace cache
}  // namespace options
}  // namespace dingofs

#endif  // DINGOFS_SRC_OPTIONS_CACHE_BLOCKCACHE_H_
