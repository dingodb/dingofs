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

#include "cache/config/block_cache.h"

#include <brpc/reloadable_flags.h>
#include <gflags/gflags.h>

namespace dingofs {
namespace cache {

// block cache
DEFINE_string(cache_store, "disk", "");
DEFINE_bool(cache_access_logging, true, "");
DEFINE_bool(upload_stage_throttle_enable, true, "");
DEFINE_uint32(upload_stage_throttle_bandwidth_mb, 256, "");
DEFINE_uint32(upload_stage_throttle_iops, 256, "");
DEFINE_uint32(prefetch_max_inflights, 100, "");

DEFINE_validator(cache_access_logging, brpc::PassValidate);
DEFINE_validator(upload_stage_throttle_enable, brpc::PassValidate);
DEFINE_validator(upload_stage_throttle_bandwidth_mb, brpc::PassValidate);
DEFINE_validator(upload_stage_throttle_iops, brpc::PassValidate);

// disk cache
DEFINE_string(cache_dir, "/tmp/dingofs-cache", "");
DEFINE_uint32(cache_size_mb, 10240, "");
DEFINE_double(free_space_ratio, 0.1, "");
DEFINE_uint32(cache_expire_s, 259200, "");
DEFINE_uint32(cleanup_expire_interval_ms, 1000, "");
DEFINE_bool(enable_stage, true, "");
DEFINE_bool(enable_cache, true, "");
DEFINE_uint32(ioring_blksize, 1048576, "");
DEFINE_uint32(ioring_iodepth, 128, "");
DEFINE_bool(ioring_prefetch, true, "");
DEFINE_bool(drop_page_cache, true, "");

DEFINE_validator(cache_expire_s, brpc::PassValidate);
DEFINE_validator(cleanup_expire_interval_ms, brpc::PassValidate);
DEFINE_validator(drop_page_cache, brpc::PassValidate);

// disk state
DEFINE_uint32(state_tick_duration_s, 60, "");
DEFINE_uint32(state_normal2unstable_io_error_num, 3, "");
DEFINE_uint32(state_unstable2normal_io_succ_num, 10, "");
DEFINE_uint32(state_unstable2down_s, 1800, "");
DEFINE_uint32(check_disk_state_duration_ms, 3000, "");

DEFINE_validator(state_tick_duration_s, brpc::PassValidate);
DEFINE_validator(state_normal2unstable_io_error_num, brpc::PassValidate);
DEFINE_validator(state_unstable2normal_io_succ_num, brpc::PassValidate);
DEFINE_validator(state_unstable2down_s, brpc::PassValidate);
DEFINE_validator(check_disk_state_duration_ms, brpc::PassValidate);

DiskCacheOption::DiskCacheOption()
    : cache_store(FLAGS_cache_store),
      cache_dir(FLAGS_cache_dir),
      cache_size_mb(FLAGS_cache_size_mb) {}

BlockCacheOption::BlockCacheOption()
    : disk_cache_options({DiskCacheOption()}) {}

}  // namespace cache
}  // namespace dingofs
