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

#include <absl/strings/str_split.h>
#include <brpc/reloadable_flags.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "base/string/string.h"

namespace dingofs {
namespace cache {

// block cache
DEFINE_string(cache_store, "disk", "");  // none, disk or 3fs
DEFINE_bool(enable_stage, true, "");
DEFINE_bool(enable_cache, true, "");
DEFINE_bool(cache_access_logging, true, "");
DEFINE_bool(upload_stage_throttle_enable, false, "");
DEFINE_uint32(upload_stage_throttle_bandwidth_mb, 256, "");
DEFINE_uint32(upload_stage_throttle_iops, 100, "");
DEFINE_uint32(prefetch_max_inflights, 100, "");
DEFINE_uint64(upload_stage_max_inflights, 128, "");

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
DEFINE_uint32(ioring_blksize, 1048576, "");
DEFINE_uint32(ioring_iodepth, 128, "");
DEFINE_bool(ioring_prefetch, true, "");
DEFINE_bool(drop_page_cache, true, "");

DEFINE_validator(cache_expire_s, brpc::PassValidate);
DEFINE_validator(cleanup_expire_interval_ms, brpc::PassValidate);
DEFINE_validator(drop_page_cache, brpc::PassValidate);

// disk state
DEFINE_uint32(state_tick_duration_s, 60, "");
DEFINE_uint32(state_normal2unstable_error_num, 3, "");
DEFINE_uint32(state_unstable2normal_succ_num, 10, "");
DEFINE_uint32(state_unstable2down_s, 1800, "");
DEFINE_uint32(check_disk_state_duration_ms, 3000, "");

DEFINE_validator(state_tick_duration_s, brpc::PassValidate);
DEFINE_validator(state_normal2unstable_error_num, brpc::PassValidate);
DEFINE_validator(state_unstable2normal_succ_num, brpc::PassValidate);
DEFINE_validator(state_unstable2down_s, brpc::PassValidate);
DEFINE_validator(check_disk_state_duration_ms, brpc::PassValidate);

DiskCacheOption::DiskCacheOption()
    : cache_store(FLAGS_cache_store),
      cache_dir(FLAGS_cache_dir),
      cache_size_mb(FLAGS_cache_size_mb) {}

void SplitDiskCacheOption(std::vector<DiskCacheOption>* options) {
  std::vector<std::string> dirs = absl::StrSplit(cache::FLAGS_cache_dir, ";");
  for (size_t i = 0; i < dirs.size(); i++) {
    uint64_t cache_size_mb = cache::FLAGS_cache_size_mb;
    std::vector<std::string> items = absl::StrSplit(dirs[i], ":");
    if (items.size() > 2 ||
        (items.size() == 2 &&
         !base::string::Str2Int(items[1], &cache_size_mb))) {
      CHECK(false) << "Invalid cache dir: " << dirs[i];
    } else if (cache_size_mb == 0) {
      CHECK(false) << "Cache size must greater than 0.";
    }

    cache::DiskCacheOption o;
    o.cache_store = cache::FLAGS_cache_store;
    o.cache_dir = items[0];
    o.cache_size_mb = cache_size_mb;
    options->emplace_back(o);
  }
}

BlockCacheOption::BlockCacheOption()
    : cache_store(FLAGS_cache_store),
      enable_stage(FLAGS_enable_stage),
      enable_cache(FLAGS_enable_cache) {
  SplitDiskCacheOption(&disk_cache_options);
}

}  // namespace cache
}  // namespace dingofs
