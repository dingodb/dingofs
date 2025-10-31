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
#include <set>
#include <unordered_map>

#include "cache/blockcache/block_cache.h"
#include "cache/blockcache/cache_store.h"
#include "client/vfs/vfs_meta.h"
#include "common/metrics/client/vfs/warmup_metric.h"
#include "common/status.h"
#include "common/trace/context.h"
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

enum class WarmupType : uint8_t {
  kWarmupIntime = 0,
  kWarmupManual = 1,
  kWarmupUnknown = 2,
};

struct WarmupTaskContext {
  WarmupTaskContext(const WarmupTaskContext& task) = default;
  WarmupTaskContext(Ino ino) : task_key(ino), type(WarmupType::kWarmupIntime) {}
  WarmupTaskContext(Ino ino, const std::string& xattr)
      : task_key(ino), type(WarmupType::kWarmupManual), task_inodes(xattr) {}

  WarmupType type;
  Ino task_key;
  // comma separated inodeid('1000023,10000021,...'), provide by dingo-tool
  std::string task_inodes;
};

class WarmupTask {
 public:
  explicit WarmupTask(const WarmupTaskContext& context)
      : context_(context), task_key_(context.task_key) {}

  uint64_t GetKey() const { return task_key_; }

  void AddFileInode(Ino file) {
    utils::WriteLockGuard lck(rwlock_);
    auto [_, ok] = file_inodes_.emplace(file);
    if (ok) {
      IncTotal();
    }
  }

  const std::set<Ino>& GetFileInodes() const {
    utils::ReadLockGuard lck(rwlock_);
    return file_inodes_;
  }

  uint64_t GetFileCount() const {
    utils::ReadLockGuard lck(rwlock_);
    return file_inodes_.size();
  };

  WarmupType GetType() const { return context_.type; };

  std::string GetTaskInodes() const { return context_.task_inodes; };

  void IncTotal() { progress_.total.fetch_add(1, std::memory_order_relaxed); };

  void IncFinished() {
    progress_.finish.fetch_add(1, std::memory_order_relaxed);
  };

  void IncErrors() {
    progress_.errors.fetch_add(1, std::memory_order_relaxed);
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
  std::set<Ino> file_inodes_;  // all files need to warmup
  uint64_t total_files_{0};
  mutable BthreadRWLock rwlock_;
  WarmupTaskContext context_;
  WarmupProgress progress_;
};

struct BlockContext {
  BlockContext(cache::BlockKey block_key, uint64_t block_len)
      : key(block_key), len(block_len) {}
  cache::BlockKey key;
  uint64_t len;
};

struct ChunkContext {
  ChunkContext(Ino ino, uint64_t chunk_idx, uint64_t offset, uint64_t length)
      : ino{ino}, chunk_idx(chunk_idx), offset(offset), len(length) {}

  Ino ino;
  uint64_t chunk_idx;
  uint64_t offset;
  uint64_t len;

  std::string ToString() const {
    return fmt::format("Chunk context, ino: {}, chunk: {}-{}-{}", ino,
                       chunk_idx, offset, offset + len);
  }
};

class WarmupManager {
 public:
  WarmupManager(VFSHub* vfs_hub, uint64_t fs_id, uint64_t chunk_size,
                uint64_t block_size)
      : vfs_hub_(vfs_hub),
        fs_id_(fs_id),
        chunk_size_(chunk_size),
        block_size_(block_size),
        metrics_(std::make_unique<WarmupMetric>()) {}

  ~WarmupManager() { ClearWarmupTask(); };

  static WarmupManagerUptr New(VFSHub* vfs_hub, uint64_t fs_id,
                               uint64_t chunk_size, uint64_t block_size) {
    return std::make_unique<WarmupManager>(vfs_hub, fs_id, chunk_size,
                                           block_size);
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

  void WarmupFile(Ino ino, AsyncWarmupCb cb);

  bool NewWarmupTask(const WarmupTaskContext& context);

  WarmupTask* GetWarmupTask(const Ino& task_key);

  void RemoveWarmupTask(const Ino& task_key);

  void ClearWarmupTask();

  Status WalkFile(WarmupTask* task, Ino ino);

  std::vector<ChunkContext> File2Chunk(Ino ino, uint64_t offset,
                                       uint64_t len) const;
  std::vector<BlockContext> Chunk2Block(ContextSPtr ctx, ChunkContext& req);
  std::vector<BlockContext> FileRange2BlockKey(ContextSPtr ctx, Ino ino,
                                               uint64_t offset, uint64_t len);
  std::vector<BlockContext> RemoveDuplicateBlocks(
      const std::vector<BlockContext>& blocks);

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

  uint64_t chunk_size_{0};
  uint64_t block_size_{0};
  uint64_t fs_id_;
  std::atomic<bool> running_{false};
  std::unordered_map<uint64_t, std::unique_ptr<WarmupTask>>
      warmup_tasks_;           // task key to warmup tasks
  BthreadRWLock task_rwlock_;  // protect warmup tasks
  std::unique_ptr<Executor> warmup_executor_;
  bthread::ExecutionQueueId<WarmupTaskContext> task_queue_id_;
  cache::BlockCache* block_cache_;
  VFSHub* vfs_hub_;
  std::unique_ptr<WarmupMetric> metrics_;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_WARMUP_MANAGER_H_