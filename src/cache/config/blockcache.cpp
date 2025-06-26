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

#include "cache/config/blockcache.h"

#include <absl/strings/str_split.h>
#include <glog/logging.h>

#include "base/string/string.h"

namespace dingofs {
namespace cache {

static void SplitDiskCacheOption(std::vector<DiskCacheOption>* options) {
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

DiskCacheOption::DiskCacheOption()
    : cache_store(FLAGS_cache_store),
      cache_dir(FLAGS_cache_dir),
      cache_size_mb(FLAGS_cache_size_mb) {}

BlockCacheOption::BlockCacheOption()
    : cache_store(FLAGS_cache_store),
      enable_stage(FLAGS_enable_stage),
      enable_cache(FLAGS_enable_cache) {
  SplitDiskCacheOption(&disk_cache_options);
}

}  // namespace cache
}  // namespace dingofs
