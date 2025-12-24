/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2024-08-21
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_BLOCKCACHE_DISK_CACHE_LAYOUT_H_
#define DINGOFS_SRC_CACHE_BLOCKCACHE_DISK_CACHE_LAYOUT_H_

#include <absl/strings/match.h>
#include <absl/strings/str_join.h>

#include <string>

#include "cache/blockcache/cache_store.h"
#include "utils/time.h"

namespace dingofs {
namespace cache {

static const std::string kTempFileSuffix = ".tmp";

/*
 * disk cache layout:
 *
 *   950c9813-ea26-4726-96fd-383b0cd22b20
 *   ├── stage
 *   |   └── blocks
 *   │       └── 0
 *   |           └── 4
 *   │               ├── 2_21626898_4098_0_0
 *   |               ├── 2_21626898_4098_1_0
 *   |               ├── 2_21626898_4098_2_0
 *   |               ├── 2_21626898_4098_3_0
 *   |               └── 2_21626898_4098_4_0
 *   ├── cache
 *   │   └── blocks
 *   |       └── 0
 *   │           ├── 0
 *   |           |   ├── 2_21626898_1_0_0
 *   |           |   ├── 2_21626898_1_1_0
 *   |           |   ├── 2_21626898_1_1_0
 *   |           |   └── 2_21626898_1_1_0
 *   |           └── 4
 *   |               ├── 2_21626898_4096_0_0
 *   |               └── 2_21626898_4097_0_0
 *   ├── probe
 *   ├── .detect
 *   └── .lock
 */
class DiskCacheLayout {
 public:
  DiskCacheLayout(int cache_index, const std::string& cache_dir)
      : cache_index_(cache_index), cache_dir_(cache_dir) {}

  int CacheIndex() const { return cache_index_; }

  std::string GetRootDir() const { return cache_dir_; }
  std::string GetStageDir() const { return PathJoin(cache_dir_, "stage"); }
  std::string GetCacheDir() const { return PathJoin(cache_dir_, "cache"); }
  std::string GetProbeDir() const { return PathJoin(cache_dir_, "probe"); }
  std::string GetDetectPath() const { return PathJoin(cache_dir_, ".detect"); }
  std::string GetLockPath() const { return PathJoin(cache_dir_, ".lock"); }

  std::string GetStagePath(const BlockKey& key) const {
    return PathJoin(GetStageDir(), key.StoreKey());
  }

  std::string GetCachePath(const BlockKey& key) const {
    return PathJoin(GetCacheDir(), key.StoreKey());
  }

 private:
  std::string PathJoin(const std::string& parent,
                       const std::string& child) const {
    return absl::StrJoin({parent, child}, "/");
  }

  const int cache_index_;
  const std::string cache_dir_;  // include uuid
};

using DiskCacheLayoutSPtr = std::shared_ptr<DiskCacheLayout>;

inline std::string RealCacheDir(const std::string& cache_dir,
                                const std::string& uuid) {
  return absl::StrJoin({cache_dir, uuid}, "/");
}

inline std::string TempFilepath(const std::string& filepath) {
  return absl::StrFormat("%s.%lld%s", filepath, utils::TimestampNs(),
                         kTempFileSuffix);
}

inline bool IsTempFilepath(const std::string& filepath) {
  return absl::EndsWith(filepath, kTempFileSuffix);
}

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BLOCKCACHE_DISK_CACHE_LAYOUT_H_
