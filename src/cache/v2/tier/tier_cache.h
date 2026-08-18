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

#ifndef DINGOFS_CACHE_V2_TIER_TIER_CACHE_H_
#define DINGOFS_CACHE_V2_TIER_TIER_CACHE_H_

#include "cache/v2/block/block_cache.h"
#include "cache/v2/common/mds_client.h"
#include "cache/v2/object/object.h"

namespace dingofs {
namespace cache {
namespace v2 {

class TierCache final : public BlockCache {
 public:
  TierCache(ObjectStorage* storage, MDSClient* mds_client);
  ~TierCache() override;

  TierCache(const TierCache&) = delete;
  TierCache& operator=(const TierCache&) = delete;

  Future<> Start() override;
  Future<> Shutdown() override;

  Future<Status> Put(BlockHandle handle, BufferViews block,
                     PutOption option = {}) override;
  Future<Status> Get(BlockHandle handle, uint64_t offset, uint32_t length,
                     char* buffer, GetOption option = {}) override;
  Future<Status> Prefetch(BlockHandle handle,
                          PrefetchOption option = {}) override;
  Future<Status> Delete(BlockHandle handle, DeleteOption option = {}) override;
  Future<CacheStats> GetStats() override;

 private:
  friend class TierCacheTest;
  friend class TierE2ETest;

  TierCache(ObjectStorage* storage, BlockCacheUPtr local,
            BlockCacheUPtr remote);

  bool HasLocal() const { return local_cache_ != nullptr; }
  bool HasRemote() const { return remote_cache_ != nullptr; }

  static BlockCacheUPtr MakeLocal(ObjectStorage* storage);
  static BlockCacheUPtr MakeRemote(MDSClient* mds_client);
  static void LogTierMiss(const char* tier, const BlockHandle& handle,
                          const Status& status);

  bool running_ = false;
  BlockCacheUPtr local_cache_;
  BlockCacheUPtr remote_cache_;
  ObjectStorage* storage_;
};

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_TIER_TIER_CACHE_H_
