// Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef DINGOFS_SRC_CLIENT_VFS_WARMUP_MANAGER_H_
#define DINGOFS_SRC_CLIENT_VFS_WARMUP_MANAGER_H_

#include <bthread/execution_queue.h>
#include <fmt/format.h>

#include <atomic>
#include <cstdint>
#include <unordered_map>
#include <vector>

#include "client/vfs/blockstore/block_store.h"
#include "client/vfs/components/context.h"
#include "client/vfs/vfs_meta.h"
#include "common/metrics/client/vfs/warmup_metric.h"
#include "common/status.h"
#include "utils/concurrent/concurrent.h"
#include "utils/concurrent/rw_lock.h"
#include "utils/executor/executor.h"

namespace dingofs {
namespace client {
namespace vfs {

class VFSHub;
class WarmupManager;

using BthreadRWLock = dingofs::utils::BthreadRWLock;
using AsyncWarmupCb = std::function<void(Status status)>;
using WarmupManagerUptr = std::unique_ptr<WarmupManager>;
using WarmupMetric = metrics::client::WarmupMetric;

class WarmupTask {
 public:
  explicit WarmupTask(const WarmupTaskContext& context)
      : context_(context), task_key_(context.task_key) {}

  uint64_t GetKey() const { return task_key_; }

  std::vector<BlockContext> GetFileBlocks(Ino file) const {
    utils::ReadLockGuard lck(rwlock_);
    auto it = file_blocks_.find(file);
    return (it != file_blocks_.end()) ? it->second
                                      : std::vector<BlockContext>();
  }

  // Store file blocks for a file
  void SetFileBlocks(Ino file, const std::vector<BlockContext>& blocks) {
    utils::WriteLockGuard lck(rwlock_);
    file_blocks_[file] = blocks;
    IncTotal(blocks.size());
  }

  std::vector<Ino> GetFileInodes() const {
    utils::ReadLockGuard lck(rwlock_);

    std::vector<Ino> keys;
    keys.reserve(file_blocks_.size());

    for (const auto& pair : file_blocks_) {
      keys.push_back(pair.first);
    }

    return keys;
  }

  uint64_t GetFileCount() const {
    utils::ReadLockGuard lck(rwlock_);
    return file_blocks_.size();
  };

  WarmupType GetType() const { return context_.type; };

  std::string GetTaskInodes() const { return context_.task_inodes; };

  void IncTotal(uint64_t blocks = 1) {
    progress_.total.fetch_add(blocks, std::memory_order_relaxed);
  };

  void IncFinished(uint64_t blocks = 1) {
    progress_.finish.fetch_add(blocks, std::memory_order_relaxed);
  };

  void IncErrors(uint64_t blocks = 1) {
    progress_.errors.fetch_add(blocks, std::memory_order_relaxed);
  };

  uint64_t GetTotal() const {
    return progress_.total.load(std::memory_order_acquire);
  };
  uint64_t GetFinished() const {
    return progress_.finish.load(std::memory_order_acquire);
  };
  uint64_t GetErrors() const {
    return progress_.errors.load(std::memory_order_acquire);
  };

 private:
  struct WarmupProgress {
    std::atomic<uint64_t> total{0};
    std::atomic<uint64_t> finish{0};
    std::atomic<uint64_t> errors{0};
  };

  Ino task_key_{0};
  std::unordered_map<Ino, std::vector<BlockContext>>
      file_blocks_;  // inode -> blocks
  mutable BthreadRWLock rwlock_;
  WarmupTaskContext context_;
  WarmupProgress progress_;
};

class WarmupManager {
 public:
  WarmupManager(VFSHub* vfs_hub)
      : vfs_hub_(vfs_hub), metrics_(std::make_unique<WarmupMetric>()) {}

  ~WarmupManager() { ClearWarmupTask(); }

  static WarmupManagerUptr New(VFSHub* vfs_hub) {
    return std::make_unique<WarmupManager>(vfs_hub);
  }

  Status Start(const uint32_t& threads);

  Status Stop();

  void SubmitTask(const WarmupTaskContext& context);

  std::string GetWarmupTaskStatus(const Ino& task_key);

 private:
  static int HandleWarmupTask(void* meta,
                              bthread::TaskIterator<WarmupTaskContext>& iter);
  void AsyncWarmupTask(const WarmupTaskContext& context);

  void DoWarmupTask(WarmupTask* task);

  void ProcessIntimeWarmup(WarmupTask* task);

  void ProcessManualWarmup(WarmupTask* task);

  void WarmupFiles(WarmupTask* task);

  void WarmupFile(Ino ino, WarmupTask* task, AsyncWarmupCb cb);

  bool NewWarmupTask(const WarmupTaskContext& context);

  WarmupTask* GetWarmupTask(const Ino& task_key);

  void RemoveWarmupTask(const Ino& task_key);

  void ClearWarmupTask();

  Status WalkFile(WarmupTask* task, Ino ino);

  void IncTaskMetric(uint64_t value) {
    metrics_->inflight_warmup_tasks << value;
  };

  void DecTaskMetric(uint64_t value) {
    metrics_->inflight_warmup_tasks << -1 * value;
  };

  void IncFileMetric(uint64_t value) {
    metrics_->inflight_warmup_files << value;
  };

  void DecFileMetric(uint64_t value) {
    metrics_->inflight_warmup_files << -1 * value;
  };

  void IncBlockMetric(uint64_t value) {
    metrics_->inflight_warmup_blocks << value;
  };

  void DecBlockMetric(uint64_t value) {
    metrics_->inflight_warmup_blocks << -1 * value;
  };

  void ResetMetrics() {
    metrics_->inflight_warmup_tasks.reset();
    metrics_->inflight_warmup_files.reset();
    metrics_->inflight_warmup_blocks.reset();
  }

  std::atomic<bool> running_{false};
  std::unordered_map<uint64_t, std::unique_ptr<WarmupTask>>
      warmup_tasks_;           // task key to warmup tasks
  BthreadRWLock task_rwlock_;  // protect warmup tasks
  std::unique_ptr<Executor> warmup_executor_;
  bthread::ExecutionQueueId<WarmupTaskContext> task_queue_id_;

  VFSHub* vfs_hub_;
  BlockStore* block_store_;
  std::unique_ptr<WarmupMetric> metrics_;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_WARMUP_MANAGER_H_