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

#include "client/vfs/blockstore/block_store.h"
#include "common/metrics/client/vfs/prefetch_metric.h"
#include "common/status.h"
#include "utils/concurrent/rw_lock.h"
#include "utils/executor/executor.h"

namespace dingofs {
namespace client {
namespace vfs {

class VFSHub;
class PrefetchManager;
using PrefetchManagerUPtr = std::unique_ptr<PrefetchManager>;

struct PrefetchContext {
  PrefetchContext(uint64_t ino, int64_t prefetch_offset, uint64_t file_size,
                  uint64_t prefetch_blocks)
      : ino(ino),
        prefetch_offset(prefetch_offset),
        file_size(file_size),
        prefetch_blocks(prefetch_blocks) {}

  uint64_t ino;
  int64_t prefetch_offset;
  uint64_t file_size;
  uint64_t prefetch_blocks;
};

class PrefetchManager {
 public:
  PrefetchManager(VFSHub* vfs_hub)
      : vfs_hub_(vfs_hub),
        metrics_(std::make_unique<metrics::client::PrefetchMetric>()) {}

  Status Start(uint32_t threads);

  Status Stop();

  void SubmitTask(PrefetchContext context);

  static PrefetchManagerUPtr New(VFSHub* vfs_hub) {
    return std::make_unique<PrefetchManager>(vfs_hub);
  }

 private:
  void ProcessPrefetch(const PrefetchContext& context);
  void AsyncPrefetch(BlockKey key, size_t length);

  bool IsBusy(const BlockKey& key);

  void SetBusy(const BlockKey& key);

  void SetIdle(const BlockKey& key);

  static int HandlePrefetch(void* meta,
                            bthread::TaskIterator<PrefetchContext>& iter);

  VFSHub* vfs_hub_;
  BlockStore* block_store_;
  std::unique_ptr<metrics::client::PrefetchMetric> metrics_;
  std::atomic<bool> running_{false};
  bthread::ExecutionQueueId<PrefetchContext> task_queue_;
  std::unique_ptr<Executor> prefetch_executor_;

  utils::BthreadRWLock rwlock_;
  std::unordered_set<std::string> inflight_keys_;
};

using PrefetchManagerUPtr = std::unique_ptr<PrefetchManager>;

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_COMPONENTS_PREFETCH_MANAGER_H_
