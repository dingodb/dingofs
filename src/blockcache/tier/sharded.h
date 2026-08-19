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

#ifndef DINGOFS_BLOCKCACHE_TIER_SHARDED_H_
#define DINGOFS_BLOCKCACHE_TIER_SHARDED_H_

#include <memory>
#include <utility>

#include "blockcache/common/mds_client.h"
#include "blockcache/common/route.h"
#include "blockcache/core/runtime/sharded.h"
#include "blockcache/object/object.h"
#include "blockcache/tier/tier_cache.h"

namespace dingofs {
namespace blockcache {

class ShardedTierCache {
 public:
  ShardedTierCache(MDSClient* mds_client, ObjectStorageUPtr storage);
  ~ShardedTierCache();

  ShardedTierCache(const ShardedTierCache&) = delete;
  ShardedTierCache& operator=(const ShardedTierCache&) = delete;

  Status Start();
  void Shutdown();

  Future<Status> Put(BlockHandle handle, BufferViews block,
                     PutOption option = {});
  Future<Status> Get(BlockHandle handle, uint64_t offset, uint32_t length,
                     char* buffer, GetOption option = {});
  Future<Status> Prefetch(BlockHandle handle, PrefetchOption option = {});
  Future<Status> Delete(BlockHandle handle, DeleteOption option = {});
  Future<CacheStats> GetStats();

 private:
  template <typename Fn>
  auto InvokeOnOwner(const BlockHandle& handle, Fn fn) {
    return tiers_.InvokeOn(OwnerShard(handle), std::move(fn));
  }

  bool running_ = false;
  MDSClient* mds_client_;
  ObjectStorageUPtr storage_;
  Sharded<TierCache> tiers_;
};

using ShardedTierCacheUPtr = std::unique_ptr<ShardedTierCache>;

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_TIER_SHARDED_H_
