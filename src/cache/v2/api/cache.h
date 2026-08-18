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

#ifndef DINGOFS_CACHE_V2_API_CACHE_H_
#define DINGOFS_CACHE_V2_API_CACHE_H_

#include <atomic>
#include <cstdint>
#include <memory>

#include "cache/v2/api/task.h"
#include "cache/v2/api/throttle.h"
#include "cache/v2/block/block_cache.h"
#include "cache/v2/common/block_handle.h"
#include "cache/v2/common/mds_client.h"
#include "cache/v2/core/memory/buffer_view.h"
#include "cache/v2/core/runtime/runtime.h"
#include "cache/v2/core/runtime/worker_pool.h"
#include "cache/v2/object/object.h"
#include "cache/v2/tier/sharded.h"
#include "common/status.h"

namespace dingofs {
namespace cache {
namespace v2 {

class BlockCacheImpl {
 public:
  explicit BlockCacheImpl(MDSClient* mds_client, ObjectStorageUPtr storage);
  ~BlockCacheImpl();

  BlockCacheImpl(const BlockCacheImpl&) = delete;
  BlockCacheImpl& operator=(const BlockCacheImpl&) = delete;

  Status Start();
  void Shutdown();

  bool AsyncPut(BlockHandle handle, BufferViews block, AsyncCallback cb,
                PutOption option = {});
  bool AsyncGet(BlockHandle handle, uint64_t offset, uint32_t length,
                char* buffer, AsyncCallback cb, GetOption option = {});
  bool AsyncPrefetch(BlockHandle handle, AsyncCallback cb,
                     PrefetchOption option = {});
  bool AsyncDelete(BlockHandle handle, AsyncCallback cb,
                   DeleteOption option = {});
  CacheStats GetStats();

  Status RegisterBuffers(void* base, size_t bytes);

 private:
  struct InboxTask;
  template <typename Task>
  struct Context;

  void StartRuntime();
  void StopRuntime();

  Status StartTierCache();
  void StopTierCache();

  void DrainInflight();

  // task
  bool Check(BlockHandle handle, unsigned* shard);

  template <typename Task, typename... Args>
  bool SubmitTask(unsigned shard, Args&&... args);
  static void RunTask(InboxWork* base);

  Future<> AwaitTask(InboxTask* context, Future<Status> future);

  void FinishTask(InboxTask* context, Status status);
  static void CompleteTask(InboxWork* base);

  std::atomic<bool> running_{false};
  MDSClient* mds_client_;
  ObjectStorageUPtr storage_;
  RuntimeUPtr runtime_;
  WorkerPoolUPtr worker_pool_;
  ShardedTierCacheUPtr tier_cache_;
  InflightThrottleUPtr throttle_;
};

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_API_CACHE_H_
