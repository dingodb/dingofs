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

#include "client/vfs/components/warmup_manager.h"

#include <fmt/format.h>
#include <glog/logging.h>

#include <algorithm>
#include <type_traits>

#include "client/vfs/components/prefetch_utils.h"
#include "client/vfs/hub/vfs_hub.h"
#include "client/vfs/vfs_fh.h"
#include "common/options/client.h"
#include "utils/executor/thread/executor_impl.h"
#include "utils/string_util.h"

namespace dingofs {
namespace client {
namespace vfs {

namespace {

constexpr auto kFinishedStatusTtl = std::chrono::minutes(10);
constexpr auto kDrainLogInterval = std::chrono::seconds(60);

}  // namespace

Status WarmupEventSink::EnqueueSubmit(WarmupTaskContext context) {
  std::lock_guard<std::mutex> lock(submit_admission_mutex_);
  if (!accepting_submits_ || !open_.load(std::memory_order_acquire)) {
    return Status::Stop("warmup manager is stopping");
  }
  return EnqueueNormal(WarmupEvent(SubmitEvent(context)));
}

Status WarmupEventSink::EnqueueNormal(WarmupEvent event) {
  return Enqueue(std::move(event), &bthread::TASK_OPTIONS_NORMAL);
}

Status WarmupEventSink::EnqueueUrgent(WarmupEvent event) {
  return Enqueue(std::move(event), &bthread::TASK_OPTIONS_URGENT);
}

Status WarmupEventSink::Enqueue(WarmupEvent event,
                                const bthread::TaskOptions* options) {
  if (!open_.load(std::memory_order_acquire)) {
    return Status::Stop("warmup event sink is closed");
  }

  // Increment the depth gauge before, not after, the enqueue call: the
  // handler thread can dequeue the event and decrement the gauge the moment
  // it lands in the queue, so a post-enqueue increment could run after its
  // own decrement and drive the gauge negative. Roll the increment back if
  // the enqueue fails.
  metrics_->event_queue_depth << 1;
  pending_events_.fetch_add(1, std::memory_order_acq_rel);
  int rc =
      bthread::execution_queue_execute(queue_id_, std::move(event), options);
  if (rc != 0) {
    metrics_->event_queue_depth << -1;
    pending_events_.fetch_sub(1, std::memory_order_release);
    return Status::Internal(fmt::format("enqueue warmup event failed: {}", rc));
  }
  return Status::OK();
}

void WarmupEventSink::CloseSubmitAdmission() {
  std::lock_guard<std::mutex> lock(submit_admission_mutex_);
  accepting_submits_ = false;
}

void WarmupEventSink::CloseAfterDrain() {
  std::lock_guard<std::mutex> lock(submit_admission_mutex_);
  accepting_submits_ = false;
  open_.store(false, std::memory_order_release);
}

void WarmupEventSink::MarkEventProcessed() {
  CHECK_GT(pending_events_.load(std::memory_order_relaxed), 0);
  pending_events_.fetch_sub(1, std::memory_order_release);
}

uint64_t WarmupEventSink::PendingEvents() const {
  return pending_events_.load(std::memory_order_acquire);
}

WarmupManager::~WarmupManager() {
  if (running_.load(std::memory_order_acquire)) {
    CHECK(Stop().ok());
  }
}

Status WarmupManager::Start(const uint32_t& threads) {
  CHECK_GT(threads, 0);

  bthread::ExecutionQueueOptions options;
  options.use_pthread = true;
  int rc = bthread::execution_queue_start(
      &event_queue_id_, &options, &WarmupManager::HandleWarmupEvent, this);
  if (rc != 0) {
    return Status::Internal(fmt::format("start warmup event queue: {}", rc));
  }

  event_sink_ =
      std::make_shared<WarmupEventSink>(event_queue_id_, metrics_.get());
  warmup_executor_ = std::make_unique<ExecutorImpl>("warmup_resolve", threads);
  if (!warmup_executor_->Start()) {
    event_sink_->CloseAfterDrain();
    bthread::execution_queue_stop(event_queue_id_);
    bthread::execution_queue_join(event_queue_id_);
    return Status::Internal("start warmup executor failed");
  }
  prefetch_dispatch_executor_ =
      std::make_unique<ExecutorImpl>("warmup_prefetch_dispatch", threads);
  if (!prefetch_dispatch_executor_->Start()) {
    warmup_executor_->Stop();
    event_sink_->CloseAfterDrain();
    bthread::execution_queue_stop(event_queue_id_);
    bthread::execution_queue_join(event_queue_id_);
    return Status::Internal("start warmup prefetch dispatch executor failed");
  }

  block_store_ = vfs_hub_->GetBlockStore();
  running_.store(true, std::memory_order_release);
  return Status::OK();
}

Status WarmupManager::Stop() {
  if (!running_.exchange(false, std::memory_order_acq_rel)) {
    return Status::OK();
  }

  event_sink_->CloseSubmitAdmission();
  while (!WaitForDrain(kDrainLogInterval)) {
    LOG(ERROR) << fmt::format(
        "Warmup is still draining after {} seconds: pending={}, active={}",
        std::chrono::duration_cast<std::chrono::seconds>(kDrainLogInterval)
            .count(),
        event_sink_->PendingEvents(), active_task_count_.load());
  }

  // A drained manager cannot retain any single-writer scheduling state.
  CHECK(active_tasks_.empty());
  CHECK(dispatch_waiters_.empty());
  CHECK_EQ(inflight_blocks_, 0);
  CHECK(!dispatch_event_scheduled_);
  CHECK_EQ(active_task_count_.load(std::memory_order_acquire), 0);
  CHECK_EQ(event_sink_->PendingEvents(), 0);

  event_sink_->CloseAfterDrain();
  CHECK(prefetch_dispatch_executor_->Stop());
  CHECK(warmup_executor_->Stop());
  CHECK_EQ(0, bthread::execution_queue_stop(event_queue_id_));
  CHECK_EQ(0, bthread::execution_queue_join(event_queue_id_));
  return Status::OK();
}

Status WarmupManager::SubmitTask(const WarmupTaskContext& context) {
  if (event_sink_ == nullptr) {
    return Status::Stop("warmup manager is not started");
  }
  return event_sink_->EnqueueSubmit(context);
}

std::string WarmupManager::GetWarmupTaskStatus(const Ino& task_key) {
  std::unique_lock<std::shared_mutex> lock(status_mutex_);
  const auto now = std::chrono::steady_clock::now();
  CleanupExpiredFinishedStatusLocked(now);
  auto active = active_status_.find(task_key);
  if (active != active_status_.end()) {
    return fmt::format("{}/{}/{}", active->second.total,
                       active->second.finished, active->second.errors);
  }

  auto finished = finished_status_.find(task_key);
  if (finished != finished_status_.end()) {
    if (finished->second.expire_at > now) {
      return fmt::format("{}/{}/{}", finished->second.status.total,
                         finished->second.status.finished,
                         finished->second.status.errors);
    }
  }
  return "0/0/0";
}

int WarmupManager::HandleWarmupEvent(void* meta,
                                     bthread::TaskIterator<WarmupEvent>& iter) {
  if (iter.is_queue_stopped()) {
    return 0;
  }

  auto* self = static_cast<WarmupManager*>(meta);
  for (; iter; ++iter) {
    self->metrics_->event_queue_depth << -1;
    self->HandleEvent(std::move(*iter));
    self->event_sink_->MarkEventProcessed();
    self->MaybeNotifyDrained();
  }
  return 0;
}

void WarmupManager::HandleEvent(WarmupEvent event) {
  std::visit(
      [this](auto&& value) {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, SubmitEvent>) {
          HandleSubmit(std::forward<decltype(value)>(value));
        } else if constexpr (std::is_same_v<T, ResolveDoneEvent>) {
          HandleResolveDone(std::forward<decltype(value)>(value));
        } else if constexpr (std::is_same_v<T, BlockDoneEvent>) {
          HandleBlockDone(std::forward<decltype(value)>(value));
        } else {
          HandleDispatchBlocks(std::forward<decltype(value)>(value));
        }
      },
      std::move(event));
}

void WarmupManager::HandleSubmit(SubmitEvent event) {
  const Ino key = event.context.task_key;
  VLOG(1) << fmt::format("Warmup task submit key: {}", key);

  if (active_tasks_.find(key) != active_tasks_.end()) {
    VLOG(1) << fmt::format(
        "Warmup task already active, skip duplicate submit, key: {}", key);
    return;
  }

  {
    std::unique_lock<std::shared_mutex> lock(status_mutex_);
    CleanupExpiredFinishedStatusLocked(std::chrono::steady_clock::now());
    if (finished_status_.find(key) != finished_status_.end()) {
      VLOG(1) << fmt::format(
          "Warmup task already finished, skip duplicate submit, key: {}", key);
      return;
    }
  }

  auto task = std::make_shared<WarmupTask>(std::move(event.context));
  active_tasks_.emplace(key, task);
  active_task_count_.fetch_add(1, std::memory_order_relaxed);
  metrics_->inflight_warmup_tasks << 1;
  PublishActiveStatus(*task);

  WarmupTaskContext context = task->context;
  auto sink = event_sink_;
  metrics_->resolve_queued_tasks << 1;
  bool accepted = warmup_executor_->Execute([this, context, key, sink] {
    metrics_->resolve_queued_tasks << -1;
    metrics_->resolve_running_tasks << 1;
    const auto resolve_start = std::chrono::steady_clock::now();
    ResolveResult result = ResolveTask(context);
    metrics_->resolve_latency
        << std::chrono::duration_cast<std::chrono::microseconds>(
               std::chrono::steady_clock::now() - resolve_start)
               .count();
    metrics_->resolve_running_tasks << -1;
    Status status = sink->EnqueueUrgent(
        WarmupEvent(ResolveDoneEvent{key, std::move(result)}));
    CHECK(status.ok()) << status.ToString();
  });
  if (!accepted) {
    metrics_->resolve_queued_tasks << -1;
    FinishTask(task, Status::Stop("warmup executor rejected task"));
  }
}

void WarmupManager::HandleResolveDone(ResolveDoneEvent event) {
  auto it = active_tasks_.find(event.task_key);
  // Without cancellation, every accepted Resolve owns an active task until
  // this event is handled. A missing task is a lifecycle invariant violation.
  CHECK(it != active_tasks_.end());
  auto task = it->second;

  task->errors += event.result.errors;
  if (!event.result.status.ok() && task->first_error.ok()) {
    task->first_error = event.result.status;
  }
  task->blocks = std::move(event.result.blocks);
  task->total = task->blocks.size();
  PublishActiveStatus(*task);

  // Manual tasks can resolve several roots. Preserve the first error, but
  // still prefetch blocks successfully resolved from the remaining roots.
  if (!task->HasMoreBlocks()) {
    FinishTask(task, event.result.status);
    return;
  }

  dispatch_waiters_.push_back(task);
  metrics_->dispatch_waiting_tasks << 1;
  ScheduleDispatchBlocks();
}

void WarmupManager::ScheduleDispatchBlocks() {
  if (dispatch_event_scheduled_ || dispatch_waiters_.empty() ||
      inflight_blocks_ >= FLAGS_vfs_warmup_max_inflight_blocks) {
    return;
  }
  dispatch_event_scheduled_ = true;
  Status status =
      event_sink_->EnqueueUrgent(WarmupEvent(DispatchBlocksEvent{}));
  CHECK(status.ok()) << status.ToString();
}

void WarmupManager::HandleDispatchBlocks(DispatchBlocksEvent) {
  dispatch_event_scheduled_ = false;
  TryDispatchBlocks();
}

void WarmupManager::TryDispatchBlocks() {
  const uint64_t max_inflight = FLAGS_vfs_warmup_max_inflight_blocks;
  if (inflight_blocks_ >= max_inflight) {
    return;
  }

  uint64_t budget = max_inflight - inflight_blocks_;
  while (budget-- > 0 && !dispatch_waiters_.empty()) {
    auto task = dispatch_waiters_.front();
    dispatch_waiters_.pop_front();
    metrics_->dispatch_waiting_tasks << -1;
    CHECK(task->HasMoreBlocks());
    PrefetchBlock block = task->blocks[task->next_block++];

    ++inflight_blocks_;
    ++task->outstanding;
    metrics_->inflight_warmup_blocks << 1;

    auto sink = event_sink_;
    const Ino key = task->key;
    bool accepted = prefetch_dispatch_executor_->Execute(
        [this, sink, key, block = block]() mutable {
          PrefetchReq req;
          req.handle = BlockHandle(vfs_hub_->GetFsInfo().id, block.key);
          auto span = vfs_hub_->GetTraceManager()->StartSpan(
              "WarmupManager::DispatchBlock");
          block_store_->PrefetchAsync(
              SpanScope::GetContext(span), req,
              [sink, key, span](Status status) {
                SpanScope::End(span);
                Status enqueue_status = sink->EnqueueUrgent(
                    WarmupEvent(BlockDoneEvent{key, std::move(status)}));
                CHECK(enqueue_status.ok()) << enqueue_status.ToString();
              });
        });
    CHECK(accepted);

    if (task->HasMoreBlocks()) {
      dispatch_waiters_.push_back(task);
      metrics_->dispatch_waiting_tasks << 1;
    }
  }
}

void WarmupManager::HandleBlockDone(BlockDoneEvent event) {
  auto it = active_tasks_.find(event.task_key);
  CHECK(it != active_tasks_.end());
  auto task = it->second;
  CHECK_GT(inflight_blocks_, 0);
  CHECK_GT(task->outstanding, 0);
  --inflight_blocks_;
  --task->outstanding;
  metrics_->inflight_warmup_blocks << -1;

  if (event.status.ok() || event.status.IsExist()) {
    ++task->finished;
    metrics_->blocks_succeeded_total << 1;
  } else {
    ++task->errors;
    metrics_->blocks_failed_total << 1;
    if (task->first_error.ok()) {
      task->first_error = event.status;
    }
  }
  PublishActiveStatus(*task);
  MaybeFinishTask(task);
  ScheduleDispatchBlocks();
}

void WarmupManager::MaybeFinishTask(const std::shared_ptr<WarmupTask>& task) {
  if (!task->HasMoreBlocks() && task->outstanding == 0) {
    FinishTask(task, task->first_error);
  }
}

void WarmupManager::FinishTask(const std::shared_ptr<WarmupTask>& task,
                               const Status& status) {
  if (!status.ok() && task->first_error.ok()) {
    task->first_error = status;
    if (task->errors == 0) {
      ++task->errors;
    }
  }

  metrics_->task_latency
      << std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::steady_clock::now() - task->registered_at)
             .count();

  {
    std::unique_lock<std::shared_mutex> lock(status_mutex_);
    const auto now = std::chrono::steady_clock::now();
    CleanupExpiredFinishedStatusLocked(now);
    active_status_.erase(task->key);
    FinishedWarmupRecord record;
    record.status =
        WarmupStatusSnapshot{task->total, task->finished, task->errors};
    record.expire_at = now + kFinishedStatusTtl;
    finished_status_[task->key] = record;
    finished_status_expiry_.push_back({record.expire_at, task->key});
  }
  active_tasks_.erase(task->key);
  active_task_count_.fetch_sub(1, std::memory_order_acq_rel);
  metrics_->inflight_warmup_tasks << -1;
}

void WarmupManager::PublishActiveStatus(const WarmupTask& task) {
  std::unique_lock<std::shared_mutex> lock(status_mutex_);
  active_status_[task.key] =
      WarmupStatusSnapshot{task.total, task.finished, task.errors};
}

void WarmupManager::CleanupExpiredFinishedStatusLocked(
    std::chrono::steady_clock::time_point now) {
  while (!finished_status_expiry_.empty() &&
         finished_status_expiry_.front().expire_at <= now) {
    const auto expiry = finished_status_expiry_.front();
    finished_status_expiry_.pop_front();

    auto it = finished_status_.find(expiry.task_key);
    if (it != finished_status_.end() &&
        it->second.expire_at == expiry.expire_at) {
      finished_status_.erase(it);
    }
  }
}

void WarmupManager::MaybeNotifyDrained() {
  if (!running_.load(std::memory_order_acquire) && event_sink_ != nullptr &&
      event_sink_->PendingEvents() == 0 &&
      active_task_count_.load(std::memory_order_acquire) == 0) {
    std::lock_guard<std::mutex> lock(drain_mutex_);
    drain_cv_.notify_all();
  }
}

bool WarmupManager::WaitForDrain(std::chrono::milliseconds timeout) {
  std::unique_lock<std::mutex> lock(drain_mutex_);
  return drain_cv_.wait_for(lock, timeout, [this] {
    return event_sink_->PendingEvents() == 0 &&
           active_task_count_.load(std::memory_order_acquire) == 0;
  });
}

ResolveResult WarmupManager::ResolveTask(const WarmupTaskContext& context) {
  ResolveResult result;
  if (context.type == WarmupType::kWarmupIntime) {
    result.status = WalkFile(context, context.task_key, &result.blocks);
    result.errors = result.status.ok() ? 0 : 1;
    return result;
  }

  if (context.type != WarmupType::kWarmupManual) {
    result.status = Status::InvalidParam("invalid warmup type");
    result.errors = 1;
    return result;
  }

  for (const auto& ino : context.task_keys) {
    Status status = WalkFile(context, ino, &result.blocks);
    if (!status.ok()) {
      ++result.errors;
      if (result.status.ok()) {
        result.status = status;
      }
    }
  }

  return result;
}

Status WarmupManager::WalkFile(const WarmupTaskContext&, Ino ino,
                               std::vector<PrefetchBlock>* blocks) {
  auto span = vfs_hub_->GetTraceManager()->StartSpan("WarmupManager::WalkFile");
  Attr attr;
  Status status = vfs_hub_->GetMetaSystem()->GetAttr(
      SpanScope::GetContext(span), ino, &attr);
  if (!status.ok()) {
    return status;
  }

  if (attr.type == FileType::kFile) {
    auto values = FileRange2BlockKey(SpanScope::GetContext(span), vfs_hub_, ino,
                                     0, attr.length);
    blocks->insert(blocks->end(), values.begin(), values.end());
    return Status::OK();
  }
  if (attr.type != FileType::kDirectory) {
    return Status::NotSupport("unsupported warmup file type");
  }

  std::vector<Ino> directories{ino};
  while (!directories.empty()) {
    std::vector<Ino> children;
    for (Ino directory : directories) {
      uint64_t fh = FhGenerator::GenFh();
      bool need_cache = false;
      status = vfs_hub_->GetMetaSystem()->OpenDir(SpanScope::GetContext(span),
                                                  directory, fh, need_cache);
      if (!status.ok()) {
        continue;
      }
      uint32_t count = 0;
      Status read_status = vfs_hub_->GetMetaSystem()->ReadDir(
          SpanScope::GetContext(span), directory, fh, 0, true,
          [this, blocks, &children, &span](const DirEntry& entry, uint64_t) {
            if (entry.attr.type == FileType::kFile) {
              auto values =
                  FileRange2BlockKey(SpanScope::GetContext(span), vfs_hub_,
                                     entry.ino, 0, entry.attr.length);
              blocks->insert(blocks->end(), values.begin(), values.end());
            } else if (entry.attr.type == FileType::kDirectory) {
              children.push_back(entry.ino);
            }
            return true;
          },
          count);
      Status release_status = vfs_hub_->GetMetaSystem()->ReleaseDir(
          SpanScope::GetContext(span), directory, fh);
      if (!read_status.ok()) {
        return read_status;
      }
      if (!release_status.ok()) {
        return release_status;
      }
    }
    directories = std::move(children);
  }
  return Status::OK();
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
