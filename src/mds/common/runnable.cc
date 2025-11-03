// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
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

#include "mds/common/runnable.h"

#include <glog/logging.h>
#include <json/value.h>

#include <atomic>
#include <cstdint>
#include <sstream>
#include <string>

#include "bthread/bthread.h"
#include "butil/compiler_specific.h"
#include "fmt/core.h"
#include "mds/common/helper.h"
#include "mds/common/logging.h"
#include "mds/common/synchronization.h"
#include "mds/common/time.h"

namespace dingofs {
namespace mds {

const int kStopSignalIntervalUs = 1000;

TaskRunnable::TaskRunnable() : id_(GenId()) { start_time_us_ = Helper::TimestampUs(); }
TaskRunnable::~TaskRunnable() = default;

uint64_t TaskRunnable::Id() const { return id_; }

uint64_t TaskRunnable::GenId() {
  static std::atomic<uint64_t> gen_id = 1;
  return gen_id.fetch_add(1, std::memory_order_relaxed);
}

int ExecuteRoutine(void* meta,
                   bthread::TaskIterator<TaskRunnablePtr>& iter) {  // NOLINT
  Worker* worker = static_cast<Worker*>(meta);
  CHECK(worker != nullptr) << "[execqueue] worker is nullptr in execute routine";

  for (; iter; ++iter) {
    if (BAIDU_UNLIKELY(*iter == nullptr)) {
      DINGO_LOG(WARNING) << fmt::format("[execqueue][type()] task is nullptr.");
      continue;
    }

    worker->Notify(*iter, WorkerEventType::kHandleTask);

    if (BAIDU_LIKELY(!iter.is_queue_stopped())) {
      Duration duration;
      (*iter)->Run();
      DINGO_LOG(DEBUG) << fmt::format("[execqueue][type({})] run task elapsed time {}us.", (*iter)->Type(),
                                      duration.ElapsedUs());
    } else {
      DINGO_LOG(INFO) << fmt::format("[execqueue][type({})] task is stopped.", (*iter)->Type());
    }

    worker->PopPendingTaskTrace();
    worker->DecPendingTaskCount();
    worker->Notify(*iter, WorkerEventType::kFinishTask);
  }

  return 0;
}

Worker::Worker(NotifyFuncer notify_func) : is_available_(false), notify_func_(notify_func) {}

bool Worker::Init() {
  bthread::ExecutionQueueOptions options;
  options.bthread_attr = BTHREAD_ATTR_NORMAL;

  if (bthread::execution_queue_start(&queue_id_, &options, ExecuteRoutine, this) != 0) {
    DINGO_LOG(ERROR) << "[execqueue] start worker execution queue failed";
    return false;
  }

  is_available_.store(true, std::memory_order_relaxed);

  return true;
}

void Worker::Destroy() {
  is_available_.store(false, std::memory_order_relaxed);

  if (bthread::execution_queue_stop(queue_id_) != 0) {
    DINGO_LOG(ERROR) << "[execqueue] worker execution queue stop failed";
    return;
  }

  if (bthread::execution_queue_join(queue_id_) != 0) {
    DINGO_LOG(ERROR) << "[execqueue] worker execution queue join failed";
  }
}

bool Worker::Execute(TaskRunnablePtr task) {
  if (BAIDU_UNLIKELY(task == nullptr)) {
    DINGO_LOG(ERROR) << fmt::format("[execqueue][type({})] task is nullptr.", task->Type());
    return false;
  }

  if (BAIDU_UNLIKELY(!is_available_.load(std::memory_order_relaxed))) {
    DINGO_LOG(ERROR) << fmt::format("[execqueue][type({})] worker execute queue is not available.", task->Type());
    return false;
  }

  PushPendingTaskTrace(task->Trace());

  if (BAIDU_UNLIKELY(bthread::execution_queue_execute(queue_id_, task) != 0)) {
    DINGO_LOG(ERROR) << fmt::format("[execqueue][type({})] worker execution queue execute failed", task->Type());
    return false;
  }

  IncPendingTaskCount();
  IncTotalTaskCount();

  Notify(task, WorkerEventType::kAddTask);

  return true;
}

uint64_t Worker::TotalTaskCount() { return total_task_count_.load(std::memory_order_relaxed); }
void Worker::IncTotalTaskCount() { total_task_count_.fetch_add(1, std::memory_order_relaxed); }

int32_t Worker::PendingTaskCount() { return pending_task_count_.load(std::memory_order_relaxed); }
void Worker::IncPendingTaskCount() { pending_task_count_.fetch_add(1, std::memory_order_relaxed); }
void Worker::DecPendingTaskCount() { pending_task_count_.fetch_sub(1, std::memory_order_relaxed); }

void Worker::Notify(TaskRunnablePtr& task, WorkerEventType type) {
  if (notify_func_ != nullptr) {
    notify_func_(task, type);
  }
}

void Worker::PushPendingTaskTrace(const std::string& trace) {
  if (!trace.empty()) {
    utils::WriteLockGuard guard(lock_);
    pending_task_traces_.push_back(trace);
  }
}

void Worker::PopPendingTaskTrace() {
  utils::WriteLockGuard guard(lock_);
  pending_task_traces_.pop_front();
}

std::vector<std::string> Worker::TracePendingTasks() {
  utils::ReadLockGuard guard(lock_);

  std::vector<std::string> traces;
  traces.reserve(pending_task_traces_.size());
  for (const auto& trace : pending_task_traces_) {
    traces.push_back(trace);
  }
  return traces;
}

std::string Worker::Trace() {
  std::ostringstream oss;
  oss << "worker(";
  oss << fmt::format("{},{},", TotalTaskCount(), PendingTaskCount());

  {
    utils::ReadLockGuard guard(lock_);

    oss << "tasks:[";
    for (const auto& trace : pending_task_traces_) {
      oss << trace << ",";
    }
    oss << "]";
  }
  oss << ")";

  return oss.str();
}

void Worker::DescribeByJson(Json::Value& value) {
  value["is_available"] = is_available_.load(std::memory_order_relaxed);
  value["queue_id"] = queue_id_.value;
  value["total_task_count"] = TotalTaskCount();
  value["pending_task_count"] = PendingTaskCount();
  value["is_use_trace"] = is_use_trace_;

  if (is_use_trace_) {
    utils::ReadLockGuard guard(lock_);

    Json::Value tasks(Json::arrayValue);
    for (const auto& trace : pending_task_traces_) {
      tasks.append(trace);
    }
    value["pending_task_traces"] = tasks;
  }
}

WorkerSet::WorkerSet(std::string name, uint32_t worker_num, int64_t max_pending_task_count, bool use_pthread,
                     bool is_inplace_run)
    : name_(name),
      use_pthread_(use_pthread),
      worker_num_(worker_num),
      max_pending_task_count_(max_pending_task_count),
      total_task_count_metrics_(fmt::format("dingo_worker_set_{}_total_task_count", name)),
      pending_task_count_metrics_(fmt::format("dingo_worker_set_{}_pending_task_count", name)),
      queue_wait_metrics_(fmt::format("dingo_worker_set_{}_queue_wait_latency", name)),
      queue_run_metrics_(fmt::format("dingo_worker_set_{}_queue_run_latency", name)),
      is_inplace_run(is_inplace_run) {};

void WorkerSet::HandleNotify(TaskRunnablePtr& task, WorkerEventType type) {
  switch (type) {
    case WorkerEventType::kAddTask:
      break;
    case WorkerEventType::kHandleTask: {
      int64_t now_time_us = Helper::TimestampUs();
      QueueWaitMetrics(now_time_us - task->StartTimeUs());
      task->SetExecuteStartTimeUs(now_time_us);
    } break;

    case WorkerEventType::kFinishTask: {
      DecPendingTaskCount();
      int64_t now_time_us = Helper::TimestampUs();
      QueueRunMetrics(now_time_us - task->ExecuteStartTimeUs());
    } break;

    default:
      break;
  }
}

bool ExecqWorkerSet::Init() {
  for (uint32_t i = 0; i < WorkerNum(); ++i) {
    auto worker = Worker::New([this](TaskRunnablePtr& task, WorkerEventType type) { HandleNotify(task, type); });
    if (!worker->Init()) {
      return false;
    }

    workers_.push_back(worker);
  }

  return true;
}

void ExecqWorkerSet::Destroy() {
  for (const auto& worker : workers_) {
    worker->Destroy();
  }
}

bool ExecqWorkerSet::ExecuteRR(TaskRunnablePtr task) {
  int64_t max_pending_task_count = MaxPendingTaskCount();
  int64_t pending_task_count = PendingTaskCount();

  if (BAIDU_UNLIKELY(max_pending_task_count > 0 && pending_task_count > max_pending_task_count)) {
    DINGO_LOG(WARNING) << fmt::format("[execqueue] exceed max pending task limit, {}/{}", pending_task_count,
                                      max_pending_task_count);
    return false;
  }

  auto ret = workers_[active_worker_id_.fetch_add(1) % WorkerNum()]->Execute(task);
  if (ret) {
    IncPendingTaskCount();
    IncTotalTaskCount();
  }

  return ret;
}

bool ExecqWorkerSet::ExecuteLeastQueue(TaskRunnablePtr task) {
  int64_t max_pending_task_count = MaxPendingTaskCount();
  int64_t pending_task_count = PendingTaskCount();

  if (BAIDU_UNLIKELY(max_pending_task_count > 0 && pending_task_count > max_pending_task_count)) {
    DINGO_LOG(WARNING) << fmt::format("[execqueue] exceed max pending task limit, {}/{}", pending_task_count,
                                      max_pending_task_count);
    return false;
  }

  auto ret = workers_[LeastPendingTaskWorker()]->Execute(task);
  if (ret) {
    IncPendingTaskCount();
    IncTotalTaskCount();
  }

  return ret;
}

bool ExecqWorkerSet::ExecuteHash(int64_t id, TaskRunnablePtr task) {
  int64_t max_pending_task_count = MaxPendingTaskCount();
  int64_t pending_task_count = PendingTaskCount();

  if (BAIDU_UNLIKELY(max_pending_task_count > 0 && pending_task_count > max_pending_task_count)) {
    DINGO_LOG(WARNING) << fmt::format("[execqueue] exceed max pending task limit, {}/{}", pending_task_count,
                                      max_pending_task_count);
    return false;
  }

  auto ret = workers_[id % WorkerNum()]->Execute(task);
  if (ret) {
    IncPendingTaskCount();
    IncTotalTaskCount();
  }

  return ret;
}

uint32_t ExecqWorkerSet::LeastPendingTaskWorker() {
  uint32_t min_pending_index = 0;
  int32_t min_pending_count = INT32_MAX;
  uint32_t worker_num = workers_.size();

  for (uint32_t i = 0; i < worker_num; ++i) {
    auto& worker = workers_[i];
    int32_t pending_count = worker->PendingTaskCount();
    if (pending_count < min_pending_count) {
      min_pending_count = pending_count;
      min_pending_index = i;
    }
  }

  return min_pending_index;
}

std::string ExecqWorkerSet::Trace() {
  std::ostringstream oss;

  oss << fmt::format(
      "workerset:(use_pthread:{},worker_num:{},total_task_count:{},pending_"
      "task_count:{}/{}),",
      IsUsePthread(), WorkerNum(), TotalTaskCount(), PendingTaskCount(), MaxPendingTaskCount());

  for (auto& worker : workers_) {
    oss << worker->Trace() << ";";
  }

  return oss.str();
}

void ExecqWorkerSet::DescribeByJson(Json::Value& value) {
  value["use_pthread"] = IsUsePthread();
  value["worker_num"] = WorkerNum();
  value["total_task_count"] = TotalTaskCount();
  value["max_pending_task_count"] = MaxPendingTaskCount();
  value["pending_task_count"] = PendingTaskCount();

  Json::Value workers(Json::arrayValue);
  for (const auto& worker : workers_) {
    Json::Value worker_value;
    worker->DescribeByJson(worker_value);
    workers.append(worker_value);
  }

  value["active_worker_id"] = active_worker_id_.load();
  value["workers"] = workers;
}

SimpleWorkerSet::SimpleWorkerSet(std::string name, uint32_t worker_num, int64_t max_pending_task_count,
                                 bool use_pthread, bool is_inplace_run)
    : WorkerSet(name, worker_num, max_pending_task_count, use_pthread, is_inplace_run) {
  bthread_mutex_init(&mutex_, nullptr);
  bthread_cond_init(&cond_, nullptr);
}

SimpleWorkerSet::~SimpleWorkerSet() {
  bthread_cond_destroy(&cond_);
  bthread_mutex_destroy(&mutex_);
}

bool SimpleWorkerSet::Init() {
  auto worker_function = [this]() {
    if (IsUsePthread()) {
      pthread_setname_np(pthread_self(), GenWorkerName().c_str());
    }

    while (true) {
      bthread_mutex_lock(&mutex_);
      while (!is_stop && tasks_.empty()) {
        bthread_cond_wait(&cond_, &mutex_);
      }

      if (is_stop && tasks_.empty()) {
        bthread_mutex_unlock(&mutex_);
        break;
      }

      // get task from task queue
      TaskRunnablePtr task = nullptr;
      if (BAIDU_LIKELY(!tasks_.empty())) {
        task = tasks_.front();
        tasks_.pop();
      }

      bthread_mutex_unlock(&mutex_);

      if (BAIDU_LIKELY(task != nullptr)) {
        HandleNotify(task, WorkerEventType::kHandleTask);

        task->Run();

        HandleNotify(task, WorkerEventType::kFinishTask);
      }
    }

    stoped_count.fetch_add(1);
  };

  if (IsUsePthread()) {
    for (uint32_t i = 0; i < WorkerNum(); ++i) {
      pthread_workers_.push_back(std::thread(worker_function));
    }
  } else {
    for (uint32_t i = 0; i < WorkerNum(); ++i) {
      bthread_workers_.push_back(Bthread(worker_function));
    }
  }

  return true;
}

void SimpleWorkerSet::Destroy() {
  // guarantee idempotent
  if (IsDestroied()) {
    return;
  }

  // stop worker thread/bthread
  bthread_mutex_lock(&mutex_);
  is_stop = true;
  bthread_mutex_unlock(&mutex_);

  while (stoped_count.load() < WorkerNum()) {
    bthread_cond_signal(&cond_);
    bthread_usleep(kStopSignalIntervalUs);
  }

  // join thread/bthread
  if (IsUsePthread()) {
    for (auto& std_thread : pthread_workers_) {
      std_thread.join();
    }
  } else {
    for (auto& bthread : bthread_workers_) {
      bthread.Join();
    }
  }
}

bool SimpleWorkerSet::Execute(TaskRunnablePtr task) {
  int64_t max_pending_task_count = MaxPendingTaskCount();
  int64_t pending_task_count = PendingTaskCount();

  if (BAIDU_UNLIKELY(max_pending_task_count > 0 && pending_task_count > max_pending_task_count)) {
    DINGO_LOG(WARNING) << fmt::format("[execqueue] exceed max pending task limit, {}/{}", pending_task_count,
                                      max_pending_task_count);
    return false;
  }

  IncPendingTaskCount();
  IncTotalTaskCount();

  // if the pending task count is less than the worker number, execute the task
  // directly else push the task to the task queue the total count of pending
  // task will be decreased in the worker function and the total concurrency is
  // limited by the worker number
  if (is_inplace_run && pending_task_count < WorkerNum()) {
    HandleNotify(task, WorkerEventType::kHandleTask);

    task->Run();

    HandleNotify(task, WorkerEventType::kFinishTask);

  } else {
    bthread_mutex_lock(&mutex_);
    tasks_.push(task);
    bthread_mutex_unlock(&mutex_);
    bthread_cond_signal(&cond_);
  }

  return true;
}

bool SimpleWorkerSet::ExecuteRR(TaskRunnablePtr task) { return Execute(task); }

bool SimpleWorkerSet::ExecuteLeastQueue(TaskRunnablePtr task) { return Execute(task); }

bool SimpleWorkerSet::ExecuteHash(int64_t /*id*/, TaskRunnablePtr task) { return Execute(task); }

std::string SimpleWorkerSet::Trace() {
  std::ostringstream oss;

  oss << fmt::format(
      "workerset:(use_pthread:{},worker_num:{},total_task_count:{},pending_"
      "task_count:{}/{}),",
      IsUsePthread(), WorkerNum(), TotalTaskCount(), PendingTaskCount(), MaxPendingTaskCount());

  return oss.str();
}

void SimpleWorkerSet::DescribeByJson(Json::Value& value) {
  value["use_pthread"] = IsUsePthread();
  value["worker_num"] = WorkerNum();
  value["total_task_count"] = TotalTaskCount();
  value["max_pending_task_count"] = MaxPendingTaskCount();
  value["pending_task_count"] = PendingTaskCount();

  if (IsUsePthread()) {
    Json::Value threads(Json::arrayValue);
    for (const auto& thread : pthread_workers_) {
      std::hash<std::thread::id> hasher;
      threads.append(hasher(thread.get_id()));
    }
    value["threads"] = threads;
  } else {
    Json::Value bthreads(Json::arrayValue);
    for (const auto& bthread : bthread_workers_) {
      bthreads.append(bthread.Id());
    }
    value["bthreads"] = bthreads;
  }
}

PriorWorkerSet::PriorWorkerSet(std::string name, uint32_t worker_num, int64_t max_pending_task_count, bool use_pthread,
                               bool is_inplace_run)
    : WorkerSet(name, worker_num, max_pending_task_count, use_pthread, is_inplace_run) {
  bthread_mutex_init(&mutex_, nullptr);
  bthread_cond_init(&cond_, nullptr);
}

PriorWorkerSet::~PriorWorkerSet() {
  Destroy();

  bthread_cond_destroy(&cond_);
  bthread_mutex_destroy(&mutex_);
}

bool PriorWorkerSet::Init() {
  auto worker_function = [this]() {
    if (IsUsePthread()) {
      pthread_setname_np(pthread_self(), GenWorkerName().c_str());
    }

    while (true) {
      bthread_mutex_lock(&mutex_);
      while (!is_stop && PendingTaskCount() == 0) {
        bthread_cond_wait(&cond_, &mutex_);
      }
      if (is_stop && PendingTaskCount() == 0) {
        bthread_mutex_unlock(&mutex_);
        break;
      }

      // get task from task queue
      TaskRunnablePtr task = nullptr;
      if (BAIDU_LIKELY(!tasks_.empty())) {
        task = tasks_.top();
        tasks_.pop();
      }

      bthread_mutex_unlock(&mutex_);

      if (BAIDU_LIKELY(task != nullptr)) {
        HandleNotify(task, WorkerEventType::kHandleTask);

        task->Run();

        HandleNotify(task, WorkerEventType::kFinishTask);
      }
    }

    stoped_count.fetch_add(1);
  };

  if (IsUsePthread()) {
    for (uint32_t i = 0; i < WorkerNum(); ++i) {
      pthread_workers_.push_back(std::thread(worker_function));
    }
  } else {
    for (uint32_t i = 0; i < WorkerNum(); ++i) {
      bthread_workers_.push_back(Bthread(worker_function));
    }
  }

  return true;
}

void PriorWorkerSet::Destroy() {
  // guarantee idempotent
  if (IsDestroied()) {
    return;
  }

  // stop worker thread/bthread
  bthread_mutex_lock(&mutex_);
  is_stop = true;
  bthread_mutex_unlock(&mutex_);

  while (stoped_count.load() < WorkerNum()) {
    bthread_cond_signal(&cond_);
    bthread_usleep(kStopSignalIntervalUs);
  }

  // join thread/bthread
  if (IsUsePthread()) {
    for (auto& std_thread : pthread_workers_) {
      std_thread.join();
    }
  } else {
    for (auto& bthread : bthread_workers_) {
      bthread.Join();
    }
  }
}

bool PriorWorkerSet::Execute(TaskRunnablePtr task) {
  int64_t max_pending_task_count = MaxPendingTaskCount();
  int64_t pending_task_count = PendingTaskCount();

  if (BAIDU_UNLIKELY(max_pending_task_count > 0 && pending_task_count > max_pending_task_count)) {
    DINGO_LOG(WARNING) << fmt::format("[execqueue] exceed max pending task limit, {}/{}", pending_task_count,
                                      max_pending_task_count);
    return false;
  }

  IncPendingTaskCount();
  IncTotalTaskCount();

  // if the pending task count is less than the worker number, execute the task
  // directly else push the task to the task queue the total count of pending
  // task will be decreased in the worker function and the total concurrency is
  // limited by the worker number
  if (is_inplace_run && pending_task_count < WorkerNum()) {
    HandleNotify(task, WorkerEventType::kHandleTask);

    task->Run();

    HandleNotify(task, WorkerEventType::kFinishTask);

  } else {
    bthread_mutex_lock(&mutex_);
    tasks_.push(task);
    bthread_mutex_unlock(&mutex_);
    bthread_cond_signal(&cond_);
  }

  return true;
}

bool PriorWorkerSet::ExecuteRR(TaskRunnablePtr task) { return Execute(task); }

bool PriorWorkerSet::ExecuteLeastQueue(TaskRunnablePtr task) { return Execute(task); }

bool PriorWorkerSet::ExecuteHash(int64_t /*id*/, TaskRunnablePtr task) { return Execute(task); }

std::string PriorWorkerSet::Trace() {
  std::ostringstream oss;

  oss << fmt::format(
      "workerset:(use_pthread:{},worker_num:{},total_task_count:{},pending_"
      "task_count:{}/{}),",
      IsUsePthread(), WorkerNum(), TotalTaskCount(), PendingTaskCount(), MaxPendingTaskCount());

  return oss.str();
}

}  // namespace mds
}  // namespace dingofs
