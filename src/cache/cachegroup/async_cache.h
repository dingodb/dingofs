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
 * Created Date: 2025-03-18
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_CACHEGROUP_ASYNC_CACHE_H_
#define DINGOFS_SRC_CACHE_CACHEGROUP_ASYNC_CACHE_H_

#include <bthread/execution_queue.h>
#include <butil/iobuf.h>

#include <memory>

#include "cache/blockcache/block_cache.h"
#include "cache/blockcache/cache_store.h"

namespace dingofs {
namespace cache {
namespace cachegroup {

class AsyncCache {
 public:
  virtual ~AsyncCache() = default;

  virtual Status Start() = 0;

  virtual Status Stop() = 0;

  virtual void Cache(const blockcache::BlockKey& block_key,
                     const butil::IOBuf& block) = 0;
};

class AsyncCacheImpl : public AsyncCache {
  struct CacheTask {
    CacheTask(const blockcache::BlockKey block_key, const butil::IOBuf& block)
        : block_key(block_key), block(block) {}

    blockcache::BlockKey block_key;
    butil::IOBuf block;
  };

 public:
  explicit AsyncCacheImpl(std::shared_ptr<blockcache::BlockCache> block_cache);

  Status Start() override;

  Status Stop() override;

  void Cache(const blockcache::BlockKey& block_key,
             const butil::IOBuf& block) override;

 private:
  static int DoCache(void* meta, bthread::TaskIterator<CacheTask>& iter);

 private:
  std::atomic<bool> running_;
  std::shared_ptr<blockcache::BlockCache> block_cache_;
  bthread::ExecutionQueueId<CacheTask> async_cache_queue_id_;
};

}  // namespace cachegroup
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_CACHEGROUP_ASYNC_CACHE_H_
