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

#include "cache/blockcache/block_cache.h"
#include "cache/utils/context.h"

namespace dingofs {
namespace cache {

class AsyncCacher {
 public:
  virtual ~AsyncCacher() = default;

  virtual Status Start() = 0;
  virtual Status Shutdown() = 0;

  virtual void AsyncCache(ContextSPtr ctx, const BlockKey& block_key,
                          const Block& block) = 0;
};

using AsyncCacherUPtr = std::unique_ptr<AsyncCacher>;

class AsyncCacherImpl final : public AsyncCacher {
 public:
  explicit AsyncCacherImpl(BlockCacheSPtr block_cache);

  Status Start() override;
  Status Shutdown() override;

  void AsyncCache(ContextSPtr ctx, const BlockKey& block_key,
                  const Block& block) override;

 private:
  struct Task {
    Task(ContextSPtr ctx, BlockKey key, Block block)
        : ctx(ctx), key(key), block(block) {}

    ContextSPtr ctx;
    BlockKey key;
    Block block;
  };

  static int HandleTask(void* meta, bthread::TaskIterator<Task>& iter);

  std::atomic<bool> running_;
  BlockCacheSPtr block_cache_;
  bthread::ExecutionQueueId<Task> queue_id_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_CACHEGROUP_ASYNC_CACHE_H_
