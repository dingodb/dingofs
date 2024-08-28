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

#ifndef CURVEFS_SRC_CLIENT_BLOCKCACHE_DISK_CACHE_LAYOUT_H_
#define CURVEFS_SRC_CLIENT_BLOCKCACHE_DISK_CACHE_LAYOUT_H_

#include <string>

#include "curvefs/src/base/filepath/filepath.h"
#include "curvefs/src/client/blockcache/cache_store.h"

namespace curvefs {
namespace client {
namespace blockcache {

using ::curvefs::base::filepath::PathJoin;

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
 *   └── .lock
 */
class DiskCacheLayout {
 public:
  explicit DiskCacheLayout(const std::string& root_dir) : root_dir_(root_dir) {}

  std::string GetRootDir() const { return root_dir_; }

  std::string GetStageDir() const { return PathJoin({root_dir_, "stage"}); }

  std::string GetCacheDir() const { return PathJoin({root_dir_, "cache"}); }

  std::string GetProbeDir() const { return PathJoin({root_dir_, "probe"}); }

  std::string GetLockPath() const { return PathJoin({root_dir_, ".lock"}); }

  std::string GetStagePath(const BlockKey& key) const {
    return PathJoin({GetStageDir(), key.StoreKey()});
  }

  std::string GetCachePath(const BlockKey& key) const {
    return PathJoin({GetCacheDir(), key.StoreKey()});
  }

 private:
  std::string root_dir_;
};

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_BLOCKCACHE_DISK_CACHE_LAYOUT_H_
