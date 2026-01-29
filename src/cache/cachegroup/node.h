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
 * Created Date: 2025-02-08
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_CACHEGROUP_NODE_H_
#define DINGOFS_SRC_CACHE_CACHEGROUP_NODE_H_

#include <ostream>

#include "cache/blockcache/block_cache.h"
#include "cache/blockcache/cache_store.h"
#include "cache/cachegroup/heartbeat.h"
#include "cache/cachegroup/task_tracker.h"
#include "cache/common/context.h"
#include "cache/common/mds_client.h"
#include "cache/common/storage_client.h"
#include "cache/common/storage_client_pool.h"
#include "common/io_buffer.h"

namespace dingofs {
namespace cache {

class CacheNode {
 public:
  CacheNode();

  Status Start();
  Status Shutdown();

  Status Put(ContextSPtr ctx, const BlockKey& key, const Block& block);
  Status Range(ContextSPtr ctx, const BlockKey& key, off_t offset,
               size_t length, IOBuffer* buffer, size_t block_length);
  Status AsyncCache(ContextSPtr ctx, const BlockKey& key, const Block& block);
  Status AsyncPrefetch(ContextSPtr ctx, const BlockKey& key, size_t length);

 private:
  bool IsRunning() const { return running_.load(std::memory_order_relaxed); }

  Status JoinGroup();
  Status LeaveGroup();

  Status RetrieveCache(ContextSPtr ctx, const BlockKey& key, off_t offset,
                       size_t length, IOBuffer* buffer);
  Status RetrieveStorage(ContextSPtr ctx, const BlockKey& key, off_t offset,
                         size_t length, IOBuffer* buffer, size_t block_length);
  Status RetrievePartBlock(ContextSPtr ctx, const BlockKey& key, off_t offset,
                           size_t length, IOBuffer* buffer,
                           size_t block_length);
  Status RetrieveWholeBlock(ContextSPtr ctx, const BlockKey& key,
                            size_t block_length, IOBuffer* buffer);
  Status RunTask(StorageClient* storage_client, DownloadTaskSPtr task);
  Status WaitTask(DownloadTaskSPtr task);

 private:
  std::atomic<bool> running_;
  MDSClientSPtr mds_client_;
  StorageClientPoolSPtr storage_client_pool_;
  BlockCacheUPtr block_cache_;
  HeartbeatUPtr heartbeat_;
  TaskTrackerUPtr task_tracker_;

  bvar::Adder<int64_t> num_hit_cache_;
  bvar::Adder<int64_t> num_miss_cache_;
};

using CacheNodeSPtr = std::shared_ptr<CacheNode>;

std::ostream& operator<<(std::ostream& os, const CacheNode& node);

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_CACHEGROUP_NODE_H_
