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
 * Created Date: 2025-05-07
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_OPTIONS_CACHE_BLOCKCACHE_H_
#define DINGOFS_SRC_OPTIONS_CACHE_BLOCKCACHE_H_

#include "options/options.h"

namespace dingofs {
namespace options {
namespace cache {

class DiskStateOption : public BaseOption {
  BIND_uint32(tick_duration_s, 60, "");
  BIND_uint32(normal2unstable_io_error_num, 3, "");
  BIND_uint32(unstable2normal_io_succ_num, 10, "");
  BIND_uint32(unstable2down_s, 1800, "");
  BIND_uint32(disk_check_duration_ms, 3000, "");
};

DECLARE_ONFLY_bool(block_cache_logging);
DECLARE_ONFLY_bool(block_cache_drop_page_cache);

class DiskCacheOption : public BaseOption {
  BIND_string_array(cache_dir, STR_ARRAY{"/tmp/dingofs-cache"}, "");
  BIND_int32(cache_size_mb, 10240, "");
  BIND_double(free_space_ratio, 0.1, "");
  BIND_int32(cache_expire_s, 259200, "");
  BIND_int32(cleanup_expire_interval_ms, 1000, "");
  BIND_ONFLY_bool(drop_page_cache, block_cache_drop_page_cache);
  BIND_int32(ioring_iodepth, 128, "");
  BIND_int32(ioring_blksize, 1048576, "");
  BIND_bool(ioring_prefetch, true, "");
  BIND_suboption(disk_state_option, "disk_state", DiskStateOption);
};

class BlockCacheOption : public BaseOption {
  BIND_ONFLY_bool(logging, block_cache_logging);
  BIND_string(cache_store, "disk", "");
  BIND_bool(stage, true, "");
  BIND_bool(stage_bandwidth_throttle_enable, false, "");
  BIND_int32(stage_bandwidth_throttle_mb, 10240, "");
  BIND_int32(upload_stage_workers, 10, "");
  BIND_int32(upload_stage_queue_size, 10000, "");
  BIND_int32(prefetch_workers, 128, "");
  BIND_int32(prefetch_queue_size, 128, "");
  BIND_suboption(disk_cache_option, "disk_cache", DiskCacheOption);
};

}  // namespace cache
}  // namespace options
}  // namespace dingofs

#endif  // DINGOFS_SRC_OPTIONS_CACHE_BLOCKCACHE_H_
