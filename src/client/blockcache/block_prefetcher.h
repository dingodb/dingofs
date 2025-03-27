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
 * Created Date: 2025-03-27
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CLIENT_BLOCK_PREFETCHER_H_
#define DINGOFS_SRC_CLIENT_BLOCK_PREFETCHER_H_

#include <bthread/execution_queue.h>

#include <memory>

#include "client/blockcache/cache_store.h"
#include "utils/concurrent/concurrent.h"
#include "utils/concurrent/task_thread_pool.h"

namespace dingofs {
namespace client {
namespace blockcache {

using ::dingofs::utils::RWLock;
using ::dingofs::utils::TaskThreadPool;

class BlockPrefetcher {
 public:
  using PrefetchFunc =
      std::function<BCACHE_ERROR(const BlockKey& key, size_t length)>;

 public:
  virtual ~BlockPrefetcher() = default;

  virtual BCACHE_ERROR Init(uint32_t workers, uint32_t queue_size,
                            PrefetchFunc prefetch_func) = 0;

  virtual BCACHE_ERROR Shutdown() = 0;

  virtual void Submit(const BlockKey& key, size_t length) = 0;
};

class BlockPrefetcherImpl : public BlockPrefetcher {
  struct Task {
    Task(const BlockKey& block_key, size_t block_size)
        : block_key(block_key), block_size(block_size) {}

    BlockKey block_key;
    size_t block_size;
  };

 public:
  BlockPrefetcherImpl();

  BCACHE_ERROR Init(uint32_t workers, uint32_t queue_size,
                    PrefetchFunc prefetch_func) override;

  BCACHE_ERROR Shutdown() override;

  void Submit(const BlockKey& key, size_t length) override;

 private:
  static int BatchSubmit(void* meta, bthread::TaskIterator<Task>& iter);

  void Prefetch(const BlockKey& key, size_t length);

 private:
  RWLock rwlock_;
  PrefetchFunc prefetch_func_;
  std::atomic<bool> running_;
  std::unordered_map<std::string, bool> busy_;
  bthread::ExecutionQueueId<Task> submit_queue_id_;
  std::shared_ptr<TaskThreadPool<>> prefetch_thread_pool_;
};

}  // namespace blockcache
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_BLOCK_PREFETCHER_H_
