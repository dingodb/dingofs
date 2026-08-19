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

#ifndef DINGOFS_BLOCKCACHE_BLOCK_BLOCK_CACHE_H_
#define DINGOFS_BLOCKCACHE_BLOCK_BLOCK_CACHE_H_

#include <memory>

#include "blockcache/common/block_handle.h"
#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/store/cache_store.h"
#include "common/status.h"

namespace dingofs {
namespace blockcache {

struct PutOption {
  bool stage = false;
};

struct GetOption {
  struct Stats {
    bool hit = false;
  };

  bool retrieve_storage = true;
  Stats* stats = nullptr;
};

struct PrefetchOption {};

struct DeleteOption {};

class BlockCache {
 public:
  virtual ~BlockCache() = default;

  virtual Future<> Start() = 0;
  virtual Future<> Shutdown() = 0;

  virtual Future<Status> Put(BlockHandle handle, BufferViews block,
                             PutOption option = {}) = 0;
  virtual Future<Status> Get(BlockHandle handle, uint64_t offset,
                             uint32_t length, char* buffer,
                             GetOption option = {}) = 0;
  virtual Future<Status> Prefetch(BlockHandle handle,
                                  PrefetchOption option = {}) = 0;
  virtual Future<Status> Delete(BlockHandle handle,
                                DeleteOption option = {}) = 0;
  virtual Future<CacheStats> GetStats() = 0;
};

using BlockCacheUPtr = std::unique_ptr<BlockCache>;

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_BLOCK_BLOCK_CACHE_H_
