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
#include <sys/prctl.h>

#include <atomic>
#include <cstdint>
#include <sstream>
#include <string>

#include "bthread/bthread.h"
#include "butil/compiler_specific.h"
#include "common/logging.h"
#include "fmt/core.h"
#include "mds/common/helper.h"
#include "mds/common/synchronization.h"
#include "utils/time.h"

namespace dingofs {
namespace mds {

const int kStopSignalIntervalUs = 1000;

TaskRunnable::TaskRunnable() : id_(GenId()) { start_time_us_ = utils::TimestampUs(); }
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
      LOG(WARNING) << fmt::format("[execqueue][type()] task is nullptr.");
      continue;
    }

    worker->Notify(*iter, WorkerEventType::kHandleTask);

    if (BAIDU_LIKELY(!iter.is_queue_stopped())) {
      utils::Duration duration;
      // A task that throws must not travel out through the bthread execution
      // queue: it would terminate the process, and the pending count below
      // would never be released.
      try {
        (*iter)->Run();
      } catch (const std::exception& e) {
        LOG(ERROR) << fmt::format("[execqueue][type({})] run task throw({}).", (*iter)->Type(), e.what());
      } catch (...) {
        LOG(ERROR) << fmt::format("[execqueue][type({})] run task throw unknown.", (*iter)->Type());
      }
      LOG_DEBUG << fmt::format("[execqueue][type({})] run task elapsed time {}us.", (*iter)->Type(),
                               duration.ElapsedUs());
    } else {
      LOG(INFO) << fmt::format("[execqueue][type({})] task is stopped.", (*iter)->Type());
    }

    if (BAIDU_UNLIKELY(worker->IsUseTrace())) worker->PopPendingTaskTrace();
    worker->DecPendingTaskCount();
    worker->Notify(*iter, WorkerEventType::kFinishTask);
  }

  return 0;
}

Worker::Worker(NotifyFuncer notify_func, bool use_pthread)
    : use_pthread_(use_pthread), is_available_(false), notify_func_(notify_func) {}

bool Worker::Init() {
  bthread::ExecutionQueueOptions options;
  options.use_pthread = use_pthread_;
  options.bthread_attr = BTHREAD_ATTR_NORMAL;

  if (bthread::execution_queue_start(&queue_id_, &options, ExecuteRoutine, this) != 0) {
    LOG(ERROR) << "[execqueue] start worker execution queue failed";
    return false;
  }

  is_available_.store(true, std::memory_order_relaxed);

  return true;
}

void Worker::Stop() {
  is_available_.store(false, std::memory_order_relaxed);

  if (bthread::execution_queue_stop(queue_id_) != 0) {
    LOG(ERROR) << "[execqueue] worker execution queue stop failed";
    return;
  }

  if (bthread::execution_queue_join(queue_id_) != 0) {
    LOG(ERROR) << "[execqueue] worker execution queue join failed";
  }
}

bool Worker::Execute(TaskRunnablePtr task) {
  if (BAIDU_UNLIKELY(task == nullptr)) {
    LOG(ERROR) << fmt::format("[execqueue][type({})] task is nullptr.", task->Type());
    return false;
  }

  if (BAIDU_UNLIKELY(!is_available_.load(std::memory_order_relaxed))) {
    LOG(ERROR) << fmt::format("[execqueue][type({})] worker execute queue is not available.", task->Type());
    return false;
  }

  if (BAIDU_UNLIKELY(is_use_trace_)) PushPendingTaskTrace(task->Trace());

  if (BAIDU_UNLIKELY(bthread::execution_queue_execute(queue_id_, task) != 0)) {
    LOG(ERROR) << fmt::format("[execqueue][type({})] worker execution queue execute failed", task->Type());
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
      int64_t now_time_us = utils::TimestampUs();
      QueueWaitMetrics(now_time_us - task->StartTimeUs());
      task->SetExecuteStartTimeUs(now_time_us);
    } break;

    case WorkerEventType::kFinishTask: {
      DecPendingTaskCount();
      int64_t now_time_us = utils::TimestampUs();
      QueueRunMetrics(now_time_us - task->ExecuteStartTimeUs());
    } break;

    default:
      break;
  }
}

bool ExecqWorkerSet::Init() {
  for (uint32_t i = 0; i < WorkerNum(); ++i) {
    auto worker =
        Worker::New([this](TaskRunnablePtr& task, WorkerEventType type) { HandleNotify(task, type); }, IsUsePthread());
    if (!worker->Init()) {
      return false;
    }

    workers_.push_back(worker);
  }

  return true;
}

void ExecqWorkerSet::Stop() {
  for (const auto& worker : workers_) worker->Stop();
}

bool ExecqWorkerSet::ExecuteRR(TaskRunnablePtr task) {
  int64_t max_pending_task_count = MaxPendingTaskCount();
  int64_t pending_task_count = PendingTaskCount();

  if (BAIDU_UNLIKELY(max_pending_task_count > 0 && pending_task_count > max_pending_task_count)) {
    LOG(WARNING) << fmt::format("[execqueue] exceed max pending task limit, {}/{}", pending_task_count,
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
    LOG(WARNING) << fmt::format("[execqueue] exceed max pending task limit, {}/{}", pending_task_count,
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
    LOG(WARNING) << fmt::format("[execqueue] exceed max pending task limit, {}/{}", pending_task_count,
                                max_pending_task_count);
    return false;
  }

  // Cast to uint64_t before modulo: callers pass Ino (uint64_t), and sub-trash
  // bucket inos (kTrashInodeId + offset) can exceed INT64_MAX, producing a
  // negative id and thus workers_[negative] OOB.
  auto ret = workers_[static_cast<uint64_t>(id) % WorkerNum()]->Execute(task);
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

void SimpleWorkerSet::Stop() {
  // guarantee idempotent
  if (IsDestroied()) return;

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
    LOG(WARNING) << fmt::format("[execqueue] exceed max pending task limit, {}/{}", pending_task_count,
                                max_pending_task_count);
    return false;
  }

  IncPendingTaskCount();
  IncTotalTaskCount();

  // if the pending task count is less than the worker number, execute the task
  // directly else push the task to the task queue the total count of pending
  // task will be decreased in the worker function and the total concurrency is
  // limited by the worker number
  if (BAIDU_UNLIKELY(is_inplace_run && pending_task_count < WorkerNum())) {
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

void PriorWorkerSet::Stop() {
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
    LOG(WARNING) << fmt::format("[execqueue] exceed max pending task limit, {}/{}", pending_task_count,
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

DoorbellWorkerSet::DoorbellWorkerSet(std::string name, uint32_t worker_num, int64_t max_pending_task_count)
    : name_(std::move(name)), worker_num_(worker_num), max_pending_task_count_(max_pending_task_count) {}

DoorbellWorkerSet::~DoorbellWorkerSet() { Stop(); }

bool DoorbellWorkerSet::Init() {
  CHECK(worker_num_ > 0) << "[doorbell_worker_set] worker num must be positive.";

  workers_.reserve(worker_num_);
  for (uint32_t i = 0; i < worker_num_; ++i) {
    workers_.push_back(std::make_unique<Worker>());
  }

  for (uint32_t i = 0; i < worker_num_; ++i) {
    Worker& worker = *workers_[i];
    worker.thread = std::thread([this, &worker, i] { WorkerLoop(worker, i); });
  }

  return true;
}

void DoorbellWorkerSet::Stop() {
  // Doubles as the idempotency guard: a second caller must not race the first
  // one's joins.
  if (is_stop_.exchange(true, std::memory_order_seq_cst)) return;

  // Past this point no new task is accepted, so pending can only fall: every
  // consumer drains what is already linked and then exits.
  for (auto& worker : workers_) worker->parker.Wake();
  for (auto& worker : workers_) {
    if (worker->thread.joinable()) worker->thread.join();
  }
}

bool DoorbellWorkerSet::Submit(Worker& worker, TaskRunnablePtr& task) {
  if (BAIDU_UNLIKELY(task == nullptr)) {
    LOG(ERROR) << "[doorbell_worker_set] task is nullptr.";
    return false;
  }

  // Reserve before anything else, and give the slot back on every path that
  // does not link the task. This is what makes the submit/stop race vanish
  // without any subtle ordering argument: a consumer only leaves when its
  // pending count is zero, and the only way that count reaches zero is that
  // the task was run, or that the submitter had already observed is_stop_ and
  // gave the slot back. A submitter that saw is_stop_ == false never gives it
  // back, so its task cannot be dropped by a consumer that is leaving.
  //
  // seq_cst because this reservation is also the "there is work" side of the
  // Dekker pair with WorkerParker's armed bell: the parking consumer checks
  // it after arming, and one of the two sides must see the other. The same
  // pair is what rules out a submitter and a leaving consumer missing each
  // other at Stop() time.
  worker.pending.fetch_add(1, std::memory_order_seq_cst);

  if (BAIDU_UNLIKELY(IsStopped())) {
    worker.pending.fetch_sub(1, std::memory_order_seq_cst);
    return false;
  }

  const int64_t max_pending_task_count = max_pending_task_count_;
  const int64_t pending = worker.pending.load(std::memory_order_relaxed);
  if (BAIDU_UNLIKELY(max_pending_task_count > 0 && pending > max_pending_task_count)) {
    LOG(WARNING) << fmt::format("[doorbell_worker_set][type({})] exceed max pending task limit, {}/{}", task->Type(),
                                pending, max_pending_task_count);
    worker.pending.fetch_sub(1, std::memory_order_seq_cst);
    return false;
  }

  worker.queue.Enqueue(task);

  // One seq_cst load while the consumer runs; the mutex and the wake syscall
  // only happen when this claims the bell a parking consumer armed.
  if (worker.parker.Armed() && worker.parker.Claim()) {
    worker.parker.WakeSleeper();
  }

  return true;
}

bool DoorbellWorkerSet::ExecuteRR(TaskRunnablePtr task) {
  // A per-thread cursor keeps a shared counter off the submit path.
  thread_local uint32_t cursor = 0;
  thread_local bool seeded = false;
  if (BAIDU_UNLIKELY(!seeded)) {
    cursor = next_seed_.fetch_add(1, std::memory_order_relaxed) * 0x9e3779b1u;
    seeded = true;
  }

  return Submit(*workers_[cursor++ % worker_num_], task);
}

bool DoorbellWorkerSet::ExecuteLeastQueue(TaskRunnablePtr task) {
  uint32_t min_index = 0;
  int64_t min_pending = INT64_MAX;
  for (uint32_t i = 0; i < worker_num_; ++i) {
    const int64_t pending = workers_[i]->pending.load(std::memory_order_relaxed);
    if (pending < min_pending) {
      min_pending = pending;
      min_index = i;
    }
  }

  return Submit(*workers_[min_index], task);
}

bool DoorbellWorkerSet::ExecuteHash(uint64_t id, TaskRunnablePtr task) {
  return Submit(*workers_[id % worker_num_], task);
}

int64_t DoorbellWorkerSet::PendingTaskCount() {
  int64_t total = 0;
  for (auto& worker : workers_) {
    total += worker->pending.load(std::memory_order_relaxed);
  }

  return total;
}

void DoorbellWorkerSet::WorkerLoop(Worker& worker, uint32_t index) {
  const std::string thread_name = fmt::format("{}_{}", name_, index);
  pthread_setname_np(pthread_self(), thread_name.c_str());

  while (true) {
    Run(worker);

    // Leave only once the queue is drained: a Stop() that drops queued tasks
    // loses the MDS calls those tasks carry.
    if (IsStopped() && worker.pending.load(std::memory_order_seq_cst) == 0) {
      return;
    }

    worker.parker.WaitFor([this, &worker] { return worker.pending.load(std::memory_order_seq_cst) > 0 || IsStopped(); },
                          kParkTimeoutNs);
  }
}

void DoorbellWorkerSet::Run(Worker& worker) {
  TaskRunnablePtr task = nullptr;
  while (worker.queue.Dequeue(task)) {
    // A task that throws must not take this consumer down with it: the rest of
    // the queue would be stranded with pending stuck above zero, so Stop()
    // would never see it drain.
    try {
      task->Run();
    } catch (const std::exception& e) {
      LOG(ERROR) << fmt::format("[doorbell_worker_set][type({})] run task throw({}).", task->Type(), e.what());
    } catch (...) {
      LOG(ERROR) << fmt::format("[doorbell_worker_set][type({})] run task throw unknown.", task->Type());
    }

    worker.pending.fetch_sub(1, std::memory_order_release);
  }
}

RelayWorkerSet::RelayWorkerSet(std::string name, uint32_t worker_num)
    : name_(std::move(name)),
      // use_pthread: a resident thread per downstream worker that blocks on the
      // execution queue when idle, so the relay's polling loop is the only thing
      // burning wakeups. max_pending 0: there is no backpressure (see the class
      // comment), Execute() fails only once the set is stopped.
      downstream_(ExecqWorkerSet::NewUnique(name_, worker_num, 0, true)) {}

RelayWorkerSet::~RelayWorkerSet() { Stop(); }

bool RelayWorkerSet::Init() {
  // Downstream first: once the relay thread exists it must always have a live
  // set to forward to, so a failed Init must not leave a forwarder behind.
  if (!downstream_->Init()) {
    LOG(ERROR) << fmt::format("[relay_worker_set][name({})] init downstream execq worker set fail.", name_);
    return false;
  }

  thread_ = std::thread([this] { WorkerLoop(); });

  return true;
}

void RelayWorkerSet::Stop() {
  // Doubles as the idempotency guard. There is no wake here: the relay notices
  // is_stop_ on its next poll, so joining absorbs up to kColdWaitNs.
  if (is_stop_.exchange(true, std::memory_order_seq_cst)) return;

  // Past this point no new task is accepted, so pending can only fall. Join
  // before stopping downstream: the relay must have handed every accepted task
  // over first, otherwise the forward below would hit a stopped execution queue
  // and lose the task. The downstream Stop() then drains what it was given.
  if (thread_.joinable()) thread_.join();

  downstream_->Stop();
}

bool RelayWorkerSet::Submit(RouteMode mode, uint64_t route_id, TaskRunnablePtr& task) {
  if (BAIDU_UNLIKELY(task == nullptr)) {
    LOG(ERROR) << "[relay_worker_set] task is nullptr.";
    return false;
  }

  // Reserve before anything else, and give the slot back on every path that
  // does not link the task: the relay leaves only when pending is zero, and
  // this count reaches zero only by forwarding the task or by a submitter that
  // had already seen is_stop_ and gave the slot back. A submitter that saw
  // is_stop_ == false never gives it back, so its task cannot be dropped by a
  // relay that is leaving.
  //
  // seq_cst here and on the relay's pending load is what makes that hold:
  // together with the is_stop_ store/load they form a Dekker pair, and at least
  // one side must observe the other.
  pending_.fetch_add(1, std::memory_order_seq_cst);

  if (BAIDU_UNLIKELY(IsStopped())) {
    pending_.fetch_sub(1, std::memory_order_seq_cst);
    return false;
  }

  queue_.Enqueue(RelayTask{mode, route_id, task});

  return true;
}

bool RelayWorkerSet::ExecuteRR(TaskRunnablePtr task) { return Submit(RouteMode::kRR, 0, task); }

bool RelayWorkerSet::ExecuteLeastQueue(TaskRunnablePtr task) { return Submit(RouteMode::kLeastQueue, 0, task); }

bool RelayWorkerSet::ExecuteHash(uint64_t id, TaskRunnablePtr task) { return Submit(RouteMode::kHash, id, task); }

void RelayWorkerSet::WorkerLoop() {
  // The default 50us timer slack would round a kHotWaitNs sleep up to 50us and
  // collapse the two wait tiers into one. Slack is per thread, so this only
  // affects this one.
  prctl(PR_SET_TIMERSLACK, 1UL);

  const std::string thread_name = name_ + "_relay";
  pthread_setname_np(pthread_self(), thread_name.c_str());

  while (true) {
    const bool did_work = Run();

    // Leave only once the queue is drained: a Stop() that drops queued tasks
    // loses the MDS calls those tasks carry.
    if (IsStopped() && pending_.load(std::memory_order_seq_cst) == 0) {
      return;
    }

    std::this_thread::sleep_for(std::chrono::nanoseconds(did_work ? kHotWaitNs : kColdWaitNs));
  }
}

bool RelayWorkerSet::Run() {
  bool did_work = false;
  RelayTask relay_task;
  while (queue_.Dequeue(relay_task)) {
    did_work = true;

    // Forwarding cannot run user code, so it cannot throw and the reservation
    // below is always released. Failure would mean the downstream set was
    // stopped while the relay was still forwarding, which the Stop() ordering
    // rules out; log loudly rather than silently drop.
    bool accepted = false;
    switch (relay_task.mode) {
      case RouteMode::kHash:
        accepted = downstream_->ExecuteHash(static_cast<int64_t>(relay_task.route_id), relay_task.task);
        break;
      case RouteMode::kRR:
        accepted = downstream_->ExecuteRR(relay_task.task);
        break;
      case RouteMode::kLeastQueue:
        accepted = downstream_->ExecuteLeastQueue(relay_task.task);
        break;
    }

    if (BAIDU_UNLIKELY(!accepted)) {
      LOG(ERROR) << fmt::format("[relay_worker_set][type({})] forward task fail.", relay_task.task->Type());
    }

    pending_.fetch_sub(1, std::memory_order_release);
  }

  return did_work;
}

}  // namespace mds
}  // namespace dingofs
