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

#ifndef DINGOFS_BLOCKCACHE_STORE_DISK_CACHE_GROUP_H_
#define DINGOFS_BLOCKCACHE_STORE_DISK_CACHE_GROUP_H_

#include <cstdint>
#include <memory>
#include <vector>

#include "blockcache/store/cache_store.h"
#include "blockcache/store/disk_cache.h"
#include "blockcache/utils/hash.h"

namespace dingofs {
namespace blockcache {

class DiskCacheGroup final : public CacheStore {
 public:
  explicit DiskCacheGroup(const std::vector<DiskOption>& options);

  DiskCacheGroup(const DiskCacheGroup&) = delete;
  DiskCacheGroup& operator=(const DiskCacheGroup&) = delete;

  Future<> Start(UploadFunc uploader) override;
  Future<> Shutdown() override;

  Future<Status> Stage(BlockHandle handle, BufferViews block) override;
  Future<Status> RemoveStage(BlockHandle handle) override;
  Future<Status> Cache(BlockHandle handle, BufferViews block) override;
  Future<Status> Load(BlockHandle handle, uint64_t offset, uint32_t length,
                      char* buffer) override;
  Future<Status> Delete(BlockHandle handle) override;
  Future<bool> Exists(BlockHandle handle) override;
  Future<CacheStats> GetStats() override;

 private:
  DiskCache& GetStore(const BlockHandle& handle) {
    return *stores_[chash_.MemberOf(handle.id)];
  }

  std::vector<DiskCacheUPtr> stores_;
  ConsistentHash chash_;
};

using DiskCacheGroupUPtr = std::unique_ptr<DiskCacheGroup>;

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_STORE_DISK_CACHE_GROUP_H_
