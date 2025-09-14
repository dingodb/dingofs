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

#ifndef DINGOFS_SRC_CACHE_CACHEGROUP_CACHE_GROUP_NODE_H_
#define DINGOFS_SRC_CACHE_CACHEGROUP_CACHE_GROUP_NODE_H_

#include "cache/blockcache/block_cache.h"
#include "cache/blockcache/cache_store.h"
#include "cache/cachegroup/cache_group_node_heartbeat.h"
#include "cache/cachegroup/cache_group_node_member.h"
#include "cache/cachegroup/task_tracker.h"
#include "cache/common/mds_client.h"
#include "cache/storage/storage_pool.h"
#include "cache/utils/context.h"
#include "cache/utils/step_timer.h"
#include "common/io_buffer.h"

namespace dingofs {
namespace cache {

class CacheGroupNode {
 public:
  virtual ~CacheGroupNode() = default;

  virtual Status Start() = 0;
  virtual Status Shutdown() = 0;

  virtual Status Put(ContextSPtr ctx, const BlockKey& key, const Block& block,
                     PutOption option = PutOption()) = 0;
  virtual Status Range(ContextSPtr ctx, const BlockKey& key, off_t offset,
                       size_t length, IOBuffer* buffer,
                       RangeOption option = RangeOption()) = 0;
  virtual void AsyncCache(ContextSPtr ctx, const BlockKey& key,
                          const Block& block, AsyncCallback callback,
                          CacheOption option = CacheOption()) = 0;
  virtual void AsyncPrefetch(ContextSPtr ctx, const BlockKey& key,
                             size_t length, AsyncCallback callback,
                             PrefetchOption option = PrefetchOption()) = 0;
};

using CacheGroupNodeSPtr = std::shared_ptr<CacheGroupNode>;

class CacheGroupNodeImpl final : public CacheGroupNode {
 public:
  CacheGroupNodeImpl();

  Status Start() override;
  Status Shutdown() override;

  Status Put(ContextSPtr ctx, const BlockKey& key, const Block& block,
             PutOption option = PutOption()) override;
  Status Range(ContextSPtr ctx, const BlockKey& key, off_t offset,
               size_t length, IOBuffer* buffer,
               RangeOption option = RangeOption()) override;
  void AsyncCache(ContextSPtr ctx, const BlockKey& key, const Block& block,
                  AsyncCallback callback,
                  CacheOption option = CacheOption()) override;
  void AsyncPrefetch(ContextSPtr ctx, const BlockKey& key, size_t length,
                     AsyncCallback callback,
                     PrefetchOption option = PrefetchOption()) override;

 private:
  bool IsRunning() const { return running_.load(std::memory_order_relaxed); }

  Status StartBlockCache();

  Status RetrieveCache(ContextSPtr ctx, StepTimer& timer, const BlockKey& key,
                       off_t offset, size_t length, IOBuffer* buffer,
                       RangeOption option);
  Status RetrieveStorage(ContextSPtr ctx, StepTimer& timer, const BlockKey& key,
                         off_t offset, size_t length, IOBuffer* buffer,
                         RangeOption option);
  Status RetrievePartBlock(ContextSPtr ctx, StepTimer& timer,
                           StorageSPtr storage, const BlockKey& key,
                           off_t offset, size_t length,
                           size_t block_whole_length, IOBuffer* buffer);
  Status RetrieveWholeBlock(ContextSPtr ctx, StepTimer& timer,
                            StorageSPtr storage, const BlockKey& key,
                            size_t length, IOBuffer* buffer);

  Status RunTask(StepTimer& timer, DownloadTaskSPtr task);
  Status WaitTask(StepTimer& timer, DownloadTaskSPtr task);
  void AsyncCache(DownloadTaskSPtr task);

 private:
  std::atomic<bool> running_;
  MDSClientSPtr mds_client_;
  CacheGroupNodeMemberSPtr member_;
  BlockCacheSPtr block_cache_;
  CacheGroupNodeHeartbeatUPtr heartbeat_;
  StoragePoolSPtr storage_pool_;
  TaskTrackerUPtr task_tracker_;

  bvar::Adder<int64_t> metric_cache_hit_count_;
  bvar::Adder<int64_t> metric_cache_miss_count_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_CACHEGROUP_CACHE_GROUP_NODE_H_
