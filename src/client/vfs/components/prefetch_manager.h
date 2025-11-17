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

#ifndef DINGOFS_CLIENT_VFS_COMPONENTS_PREFETCH_MANAGER_H_
#define DINGOFS_CLIENT_VFS_COMPONENTS_PREFETCH_MANAGER_H_

#include <bthread/execution_queue.h>
#include <fmt/format.h>

#include <cstdint>
#include <memory>

#include "bthread/countdown_event.h"
#include "bthread/mutex.h"
#include "cache/blockcache/block_cache.h"
#include "cache/blockcache/cache_store.h"
#include "common/metrics/client/vfs/prefetch_metric.h"
#include "common/status.h"
#include "utils/concurrent/rw_lock.h"
#include "utils/executor/executor.h"

namespace dingofs {
namespace client {
namespace vfs {

using BthreadRWLock = dingofs::utils::BthreadRWLock;
using BthreadMutex = bthread::Mutex;
using BCountDown = bthread::CountdownEvent;
using BlockKey = cache::BlockKey;
using PrefetchMetric = metrics::client::PrefetchMetric;

class VFSHub;
class PrefetchManager;
using PrefetchManagerUPtr = std::unique_ptr<PrefetchManager>;

struct PrefetchContext {
  PrefetchContext(uint64_t ino, uint64_t prefetch_offset, uint64_t file_size,
                  uint64_t prefetch_blocks)
      : ino(ino),
        prefetch_offset(prefetch_offset),
        file_size(file_size),
        prefetch_blocks(prefetch_blocks) {}

  uint64_t ino;
  uint64_t prefetch_offset;
  uint64_t file_size;
  uint64_t prefetch_blocks;
};

class PrefetchManager {
 public:
  PrefetchManager(VFSHub* vfs_hub)
      : vfs_hub_(vfs_hub), metrics_(std::make_unique<PrefetchMetric>()) {}

  Status Start(const uint32_t& threads);

  Status Stop();

  void SubmitTask(const BlockKey& key, size_t length);

  void SubmitTask(const PrefetchContext& context);

  static PrefetchManagerUPtr New(VFSHub* vfs_hub) {
    return std::make_unique<PrefetchManager>(vfs_hub);
  }

 private:
  struct PrefetchTask {
    PrefetchTask(BlockKey key, size_t length) : key(key), length(length) {}
    BlockKey key;
    size_t length;
  };

  static int HandlePrefetchTask(void* meta,
                                bthread::TaskIterator<PrefetchTask>& iter);

  void DoSubmitTask(const PrefetchContext& context);

  void AsyncPrefetch(const PrefetchTask& task);

  void DoPrefetch(const PrefetchTask& task);

  bool IsBusy(const BlockKey& key);

  void SetBusy(const BlockKey& key);

  void SetIdle(const BlockKey& key);

  bool FilterOut(const PrefetchTask& task);

  void IncPrefetchBlocks();

  void DecPrefetchBlocks();

  BthreadRWLock rwlock_;
  std::atomic<bool> running_{false};
  std::unordered_set<std::string> inflight_keys_;
  bthread::ExecutionQueueId<PrefetchTask> task_queue_id_;
  std::unique_ptr<Executor> prefetch_executor_;
  VFSHub* vfs_hub_;
  cache::BlockCache* block_cache_;
  std::unique_ptr<PrefetchMetric> metrics_;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_COMPONENTS_PREFETCH_MANAGER_H_
