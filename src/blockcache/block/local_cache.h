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

#ifndef DINGOFS_BLOCKCACHE_BLOCK_LOCAL_CACHE_H_
#define DINGOFS_BLOCKCACHE_BLOCK_LOCAL_CACHE_H_

#include "blockcache/block/block_cache.h"
#include "blockcache/block/retriever.h"
#include "blockcache/block/uploader.h"
#include "blockcache/object/object.h"
#include "blockcache/store/cache_store.h"

namespace dingofs {
namespace blockcache {

class LocalCache final : public BlockCache {
 public:
  explicit LocalCache(ObjectStorage* storage);
  ~LocalCache() override;

  LocalCache(const LocalCache&) = delete;
  LocalCache& operator=(const LocalCache&) = delete;

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
  friend class LocalCacheBuilder;

  LocalCache() = default;

  Future<Status> GetPart(BlockHandle handle, uint64_t offset, uint32_t length,
                         char* buffer);
  Future<Status> GetWhole(BlockHandle handle, uint64_t offset, uint32_t length,
                          char* buffer);
  Future<> CacheBlock(BlockHandle handle, SharedBlock block);
  Future<> CopyBlock(SharedBlock block, uint64_t offset, uint32_t length,
                     char* buffer);

  static void MarkHit(GetOption::Stats* stats) {
    if (stats != nullptr) {
      stats->hit = true;
    }
  }

  bool running_ = false;
  CacheStoreUPtr store_;
  UploaderUPtr uploader_;
  ObjectRetrieverUPtr object_retriever_;
};

using LocalCacheUPtr = std::unique_ptr<LocalCache>;

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_BLOCK_LOCAL_CACHE_H_
