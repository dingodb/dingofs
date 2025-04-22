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

#include "options/cache/blockcache.h"

#include <brpc/reloadable_flags.h>
#include <gflags/gflags.h>

#include "base/string/string.h"

namespace dingofs {
namespace options {
namespace cache {

using dingofs::base::string::Str2Int;
using dingofs::base::string::StrSplit;

// block cache
DEFINE_bool(block_cache_logging, true, "");
DEFINE_bool(block_cache_stage_bandwidth_throttle_enable, true, "");
DEFINE_uint32(block_cache_stage_bandwidth_throttle_mb, 10240, "");

DEFINE_validator(block_cache_logging, brpc::PassValidate);
DEFINE_validator(block_cache_stage_bandwidth_throttle_enable,
                 brpc::PassValidate);
DEFINE_validator(block_cache_stage_bandwidth_throttle_mb, brpc::PassValidate);

// disk cache
DEFINE_bool(disk_cache_drop_page_cache, true, "");
DEFINE_double(disk_cache_free_space_ratio, 0.1, "");
DEFINE_uint32(disk_cache_expire_s, 259200, "");
DEFINE_uint32(disk_cache_cleanup_expire_interval_ms, 1000, "");

DEFINE_validator(disk_cache_drop_page_cache, brpc::PassValidate);
DEFINE_validator(disk_cache_free_space_ratio, brpc::PassValidate);
DEFINE_validator(disk_cache_expire_s, brpc::PassValidate);
DEFINE_validator(disk_cache_cleanup_expire_interval_ms, brpc::PassValidate);

// disk state
DEFINE_uint32(disk_state_tick_duration_s, 60, "");
DEFINE_uint32(disk_state_normal2unstable_io_error_num, 3, "");
DEFINE_uint32(disk_state_unstable2normal_io_succ_num, 10, "");
DEFINE_uint32(disk_state_unstable2down_s, 1800, "");
DEFINE_uint32(disk_state_disk_check_duration_ms, 3000, "");

DEFINE_validator(disk_state_tick_duration_s, brpc::PassValidate);
DEFINE_validator(disk_state_normal2unstable_io_error_num, brpc::PassValidate);
DEFINE_validator(disk_state_unstable2normal_io_succ_num, brpc::PassValidate);
DEFINE_validator(disk_state_unstable2down_s, brpc::PassValidate);
DEFINE_validator(disk_state_disk_check_duration_ms, brpc::PassValidate);

bool RewriteBlockCacheOption(BaseOption* base) {
  auto* option = reinterpret_cast<BlockCacheOption*>(base);
  auto& disk_cache_option = option->disk_cache_option();
  auto default_cache_size_mb = disk_cache_option.cache_size_mb();
  auto cache_dirs = disk_cache_option.cache_dirs();

  for (size_t i = 0; i < cache_dirs.size(); i++) {
    // cache_size_mb
    uint32_t cache_size_mb = default_cache_size_mb;
    std::vector<std::string> items = StrSplit(cache_dirs[i], ":");
    if (items.size() > 2 ||
        (items.size() == 2 && !Str2Int(items[1], &cache_size_mb))) {
      LOG(ERROR) << "Invalid cache dir: " << cache_dirs[i];
      return false;
    } else if (cache_size_mb == 0) {
      LOG(ERROR) << "Cache size must greater than 0.";
      return false;
    }

    DiskCacheOption o = disk_cache_option;
    o.cache_index() = i;
    o.cache_dir() = items[0];
    o.cache_size_mb() = cache_size_mb;
    o.filesystem_type() = (option->cache_store() == "3fs") ? "3fs" : "local";
    option->disk_cache_options().emplace_back(o);
  }

  return true;
}

bool ValidateBlockCacheOption(BaseOption* base) {
  auto* option = reinterpret_cast<BlockCacheOption*>(base);
  return true;
}

}  // namespace cache
}  // namespace options
}  // namespace dingofs
