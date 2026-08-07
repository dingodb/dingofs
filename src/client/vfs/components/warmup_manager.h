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

#ifndef DINGOFS_SRC_CLIENT_VFS_WARMUP_MANAGER_H_
#define DINGOFS_SRC_CLIENT_VFS_WARMUP_MANAGER_H_

#include <bthread/execution_queue.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

#include "client/vfs/blockstore/block_store.h"
#include "client/vfs/components/context.h"
#include "client/vfs/components/warmup_metric.h"
#include "client/vfs/vfs_meta.h"
#include "common/status.h"
#include "utils/executor/executor.h"

namespace dingofs {
namespace client {
namespace vfs {

class VFSHub;
class WarmupManager;

using WarmupManagerUptr = std::unique_ptr<WarmupManager>;

struct ResolveResult {
  Status status{Status::OK()};
  std::vector<PrefetchBlock> blocks;
  uint64_t errors{0};
};

struct SubmitEvent {
  explicit SubmitEvent(WarmupTaskContext value) : context(value) {}
  WarmupTaskContext context;
};

struct ResolveDoneEvent {
  Ino task_key;
  ResolveResult result;
};

struct BlockDoneEvent {
  Ino task_key;
  Status status;
};

struct DispatchBlocksEvent {};

using WarmupEvent =
    std::variant<SubmitEvent, ResolveDoneEvent, BlockDoneEvent,
                 DispatchBlocksEvent>;

class WarmupEventSink {
 public:
  // Non-owning pointer to the manager's metrics, used only to account the
  // event queue depth on the enqueue path. This is safe because the manager
  // closes the sink (so later enqueues return before touching metrics) and
  // all in-flight callbacks have completed before the manager is destroyed.
  WarmupEventSink(bthread::ExecutionQueueId<WarmupEvent> queue_id,
                  WarmupMetric* metrics)
      : queue_id_(queue_id), metrics_(metrics) {}

  Status EnqueueSubmit(WarmupTaskContext context);
  Status EnqueueNormal(WarmupEvent event);
  Status EnqueueUrgent(WarmupEvent event);
  void CloseSubmitAdmission();
  void CloseAfterDrain();
  void MarkEventProcessed();
  uint64_t PendingEvents() const;

 private:
  Status Enqueue(WarmupEvent event, const bthread::TaskOptions* options);

  bthread::ExecutionQueueId<WarmupEvent> queue_id_;
  WarmupMetric* metrics_;
  std::atomic<bool> open_{true};
  std::atomic<uint64_t> pending_events_{0};
  std::mutex submit_admission_mutex_;
  bool accepting_submits_{true};
};

class WarmupTask {
 public:
  explicit WarmupTask(WarmupTaskContext context)
      : key(context.task_key),
        context(context),
        registered_at(std::chrono::steady_clock::now()) {}

  const Ino key;
  WarmupTaskContext context;
  // Timestamp of HandleSubmit registering this task. FinishTask measures
  // the end-to-end task_latency against it.
  std::chrono::steady_clock::time_point registered_at;
  std::vector<PrefetchBlock> blocks;
  size_t next_block{0};
  uint64_t outstanding{0};
  uint64_t total{0};
  uint64_t finished{0};
  uint64_t errors{0};
  Status first_error{Status::OK()};

  bool HasMoreBlocks() const { return next_block < blocks.size(); }
};

struct WarmupStatusSnapshot {
  uint64_t total{0};
  uint64_t finished{0};
  uint64_t errors{0};
};

struct FinishedWarmupRecord {
  WarmupStatusSnapshot status;
  std::chrono::steady_clock::time_point expire_at;
};

struct FinishedWarmupExpiry {
  std::chrono::steady_clock::time_point expire_at;
  Ino task_key;
};

class WarmupManager {
 public:
  explicit WarmupManager(VFSHub* vfs_hub)
      : vfs_hub_(vfs_hub), metrics_(std::make_unique<WarmupMetric>()) {}
  ~WarmupManager();

  static WarmupManagerUptr New(VFSHub* vfs_hub) {
    return std::make_unique<WarmupManager>(vfs_hub);
  }

  Status Start(const uint32_t& threads);
  Status Stop();
  Status SubmitTask(const WarmupTaskContext& context);
  std::string GetWarmupTaskStatus(const Ino& task_key);
  std::shared_ptr<WarmupEventSink> GetEventSink() const { return event_sink_; }

 private:
  static int HandleWarmupEvent(void* meta,
                               bthread::TaskIterator<WarmupEvent>& iter);
  void HandleEvent(WarmupEvent event);
  void HandleSubmit(SubmitEvent event);
  void HandleResolveDone(ResolveDoneEvent event);
  void HandleBlockDone(BlockDoneEvent event);
  void HandleDispatchBlocks(DispatchBlocksEvent event);

  ResolveResult ResolveTask(const WarmupTaskContext& context);
  Status WalkFile(const WarmupTaskContext& context, Ino ino,
                  std::vector<PrefetchBlock>* blocks);

  void ScheduleDispatchBlocks();
  void TryDispatchBlocks();
  void MaybeFinishTask(const std::shared_ptr<WarmupTask>& task);
  void FinishTask(const std::shared_ptr<WarmupTask>& task,
                  const Status& status);
  void PublishActiveStatus(const WarmupTask& task);
  void CleanupExpiredFinishedStatusLocked(
      std::chrono::steady_clock::time_point now);
  void MaybeNotifyDrained();
  bool WaitForDrain(std::chrono::milliseconds timeout);

  std::atomic<bool> running_{false};
  bthread::ExecutionQueueId<WarmupEvent> event_queue_id_;
  std::shared_ptr<WarmupEventSink> event_sink_;
  std::unique_ptr<Executor> warmup_executor_;
  std::unique_ptr<Executor> prefetch_dispatch_executor_;

  std::unordered_map<Ino, std::shared_ptr<WarmupTask>> active_tasks_;
  std::deque<std::shared_ptr<WarmupTask>> dispatch_waiters_;
  bool dispatch_event_scheduled_{false};
  uint64_t inflight_blocks_{0};

  mutable std::shared_mutex status_mutex_;
  std::unordered_map<Ino, WarmupStatusSnapshot> active_status_;
  std::unordered_map<Ino, FinishedWarmupRecord> finished_status_;
  std::deque<FinishedWarmupExpiry> finished_status_expiry_;

  std::mutex drain_mutex_;
  std::condition_variable drain_cv_;
  std::atomic<uint64_t> active_task_count_{0};

  VFSHub* vfs_hub_;
  BlockStore* block_store_{nullptr};
  std::unique_ptr<WarmupMetric> metrics_;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_WARMUP_MANAGER_H_
