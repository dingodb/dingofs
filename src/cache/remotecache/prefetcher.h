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
 * Created Date: 2025-07-15
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_REMOTECACHE_PREFETCHER_H_
#define DINGOFS_SRC_CACHE_REMOTECACHE_PREFETCHER_H_

#include <bthread/execution_queue.h>
#include <bthread/execution_queue_inl.h>

#include <cstddef>

#include "cache/blockcache/cache_store.h"
#include "cache/common/type.h"
#include "cache/remotecache/mem_cache.h"
#include "cache/remotecache/remote_cache_node.h"
#include "cache/utils/bthread.h"
#include "cache/utils/context.h"

namespace dingofs {
namespace cache {

class Prefetcher {
 public:
  Prefetcher(MemCacheSPtr mem_cache, RemoteCacheNodeSPtr remote_node);

  Status Start();
  Status Shutdown();

  void Submit(ContextSPtr ctx, const BlockKey& key, size_t length);

 private:
  struct Task {
    Task(ContextSPtr ctx, BlockKey key, size_t length)
        : ctx(ctx), key(key), length(length) {}

    ContextSPtr ctx;
    BlockKey key;
    size_t length;
  };

  static int HandleTask(void* meta, bthread::TaskIterator<Task>& iter);

  void DoPrefetch(const Task& task);

  bool IsBusy(const BlockKey& key);
  void SetBusy(const BlockKey& key);
  void SetIdle(const BlockKey& key);
  bool FilterOut(const Task& task);

  std::atomic<bool> running_;
  BthreadRWLock rwlock_;  // for protect busy_
  MemCacheSPtr memcache_;
  RemoteCacheNodeSPtr remote_node_;
  std::unordered_set<std::string> busy_;
  bthread::ExecutionQueueId<Task> queue_id_;
  BthreadJoinerUPtr joiner_;
};

using PrefetcherUPtr = std::unique_ptr<Prefetcher>;

};  // namespace cache
};  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_REMOTECACHE_PREFETCHER_H_
