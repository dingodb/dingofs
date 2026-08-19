/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

#ifndef DINGOFS_BLOCKCACHE_STORE_LAYOUT_H_
#define DINGOFS_BLOCKCACHE_STORE_LAYOUT_H_

#include <string>
#include <string_view>

#include "blockcache/common/block_handle.h"
#include "blockcache/utils/time.h"

namespace dingofs {
namespace blockcache {

/*
 * disk cache layout:
 *
 *   950c9813-ea26-4726-96fd-383b0cd22b20
 *   ├── stage
 *   |   └── {fs_id}
 *   |       └── blocks
 *   │           └── 0
 *   |               └── 4
 *   │                   ├── 4098_0_4194304
 *   |                   ├── 4098_1_4194304
 *   |                   └── 4098_2_4194304
 *   ├── cache
 *   │   └── blocks
 *   |       └── 0
 *   │           ├── 0
 *   |           |   ├── 1_0_4194304
 *   |           |   └── 1_1_4194304
 *   |           └── 4
 *   |               ├── 4096_0_4194304
 *   |               └── 4097_0_4194304
 *   ├── probe
 *   └── .lock
 */
class DiskCacheLayout {
 public:
  explicit DiskCacheLayout(std::string root) : root_(std::move(root)) {}

  const std::string& RootDir() const { return root_; }
  std::string StageDir() const { return root_ + "/stage"; }
  std::string CacheDir() const { return root_ + "/cache"; }
  std::string ProbeDir() const { return root_ + "/probe"; }
  std::string LockPath() const { return root_ + "/.lock"; }

  std::string CachePath(const BlockHandle& h) const {
    return CacheDir() + "/" + h.StoreKey();
  }

  std::string StagePath(const BlockHandle& h) const {
    return StageDir() + "/" + std::to_string(h.fs_id) + "/" + h.StoreKey();
  }

  static std::string TempPath(const std::string& path) {
    return path + "." + std::to_string(TimestampNs()) +
           std::string(kTempSuffix);
  }

  static bool IsTempFilename(std::string_view name) {
    return name.ends_with(kTempSuffix);
  }

 private:
  static constexpr std::string_view kTempSuffix = ".tmp";

  std::string root_;
};

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_STORE_LAYOUT_H_
