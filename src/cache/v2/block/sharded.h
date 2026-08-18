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

#ifndef DINGOFS_CACHE_V2_BLOCK_SHARDED_H_
#define DINGOFS_CACHE_V2_BLOCK_SHARDED_H_

#include <memory>
#include <utility>

#include "cache/v2/block/local_cache.h"
#include "cache/v2/common/mds_client.h"
#include "cache/v2/common/route.h"
#include "cache/v2/core/runtime/sharded.h"
#include "cache/v2/object/object.h"

namespace dingofs {
namespace cache {
namespace v2 {

class ShardedLocalCache {
 public:
  explicit ShardedLocalCache(MDSClient* mds_client);
  explicit ShardedLocalCache(ObjectStorageUPtr storage);
  ~ShardedLocalCache();

  ShardedLocalCache(const ShardedLocalCache&) = delete;
  ShardedLocalCache& operator=(const ShardedLocalCache&) = delete;

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
    return block_cache_.InvokeOn(OwnerShard(handle), std::move(fn));
  }

  bool running_ = false;
  StorageClientUPtr storage_client_;
  ObjectStorageUPtr storage_;
  Sharded<LocalCache> block_cache_;
};

using ShardedLocalCacheUPtr = std::unique_ptr<ShardedLocalCache>;

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_BLOCK_SHARDED_H_
