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

#ifndef DINGOFS_MDS_COMMON_RUNNABLE_H_
#define DINGOFS_MDS_COMMON_RUNNABLE_H_

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "bthread/execution_queue.h"
#include "bthread/types.h"
#include "butil/containers/mpsc_queue.h"
#include "bvar/latency_recorder.h"
#include "fmt/format.h"
#include "json/value.h"
#include "mds/common/synchronization.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace mds {

class TaskRunnable {
 public:
  TaskRunnable();
  virtual ~TaskRunnable();

  uint64_t Id() const;
  static uint64_t GenId();

  virtual std::string Type() = 0;

  virtual std::string Key() { return ""; }

  virtual void Run() = 0;

  virtual std::string Trace() { return fmt::format("{}[{}]", Type(), Id()); }

  int32_t Priority() const { return priority_; }
  void SetPriority(int32_t priority) { priority_ = priority; }

  // Operator overloading to compare tasks.
  bool operator<(const TaskRunnable& other) const {
    // Note: Higher priority tasks should come first.
    return priority_ < other.Priority();
  }

  int64_t StartTimeUs() const { return start_time_us_; }
  void SetExecuteStartTimeUs(int64_t time_us) { execute_start_time_us_ = time_us; }
  int64_t ExecuteStartTimeUs() const { return execute_start_time_us_; }

 private:
  uint64_t id_{0};
  int32_t priority_{0};
  int64_t start_time_us_{0};
  int64_t execute_start_time_us_{0};
};

using TaskRunnablePtr = std::shared_ptr<TaskRunnable>;

// Custom Comparator for priority_queue
struct CompareTaskRunnable {
  bool operator()(const TaskRunnablePtr& lhs, TaskRunnablePtr& rhs) const { return lhs.get() < rhs.get(); }
};

int ExecuteRoutine(void*, bthread::TaskIterator<TaskRunnablePtr>& iter);

enum class WorkerEventType : uint8_t {
  kAddTask = 0,
  kHandleTask = 1,
  kFinishTask = 2,
};
using NotifyFuncer = std::function<void(TaskRunnablePtr&, WorkerEventType)>;

// Run task worker
class Worker {
 public:
  Worker(NotifyFuncer notify_func, bool use_pthread = false);
  ~Worker() = default;

  static std::shared_ptr<Worker> New(bool use_pthread = false) {
    return std::make_shared<Worker>(nullptr, use_pthread);
  }
  static std::shared_ptr<Worker> New(NotifyFuncer notify_func, bool use_pthread = false) {
    return std::make_shared<Worker>(notify_func, use_pthread);
  }

  bool Init();
  void Stop();

  bool Execute(TaskRunnablePtr task);

  uint64_t TotalTaskCount();
  void IncTotalTaskCount();

  int32_t PendingTaskCount();
  void IncPendingTaskCount();
  void DecPendingTaskCount();

  void Notify(TaskRunnablePtr& task, WorkerEventType type);

  bool IsUseTrace() const { return is_use_trace_; }
  void PushPendingTaskTrace(const std::string& trace);
  void PopPendingTaskTrace();
  std::vector<std::string> TracePendingTasks();
  std::string Trace();
  void DescribeByJson(Json::Value& value);

 private:
  const bool use_pthread_;

  // Execution queue is available.
  std::atomic<bool> is_available_;
  bthread::ExecutionQueueId<TaskRunnablePtr> queue_id_;

  // Metrics
  std::atomic<uint64_t> total_task_count_{0};
  std::atomic<int32_t> pending_task_count_{0};

  // Notify
  NotifyFuncer notify_func_;

  // trace
  bool is_use_trace_{false};
  utils::RWLock lock_;
  std::deque<std::string> pending_task_traces_;
};

using WorkerSPtr = std::shared_ptr<Worker>;

class WorkerSet {
 public:
  WorkerSet(std::string name, uint32_t worker_num, int64_t max_pending_task_count, bool use_pthread,
            bool is_inplace_run);
  virtual ~WorkerSet() = default;

  virtual bool Init() = 0;
  virtual void Stop() = 0;

  virtual bool Execute(TaskRunnablePtr task) = 0;
  virtual bool ExecuteRR(TaskRunnablePtr task) = 0;
  virtual bool ExecuteLeastQueue(TaskRunnablePtr task) = 0;
  virtual bool ExecuteHash(int64_t id, TaskRunnablePtr task) = 0;

  virtual bool IsFull() { return PendingTaskCount() >= MaxPendingTaskCount(); }
  virtual bool IsAlmostFull() { return PendingTaskCount() >= MaxPendingTaskCount() * 0.8; }

  std::string Name() const { return name_; }
  std::string GenWorkerName() { return name_ + "_" + std::to_string(GenWorkerNo()); }
  uint32_t GenWorkerNo() { return worker_no_generator_.fetch_add(1); }
  bool IsUsePthread() const { return use_pthread_; }
  uint32_t WorkerNum() const { return worker_num_; }
  int64_t MaxPendingTaskCount() const { return max_pending_task_count_; }

  uint64_t TotalTaskCount() { return total_task_count_metrics_.get_value(); }
  void IncTotalTaskCount() { total_task_count_metrics_ << 1; }

  int64_t PendingTaskCount() { return pending_task_count_.load(std::memory_order_relaxed); }
  void IncPendingTaskCount() {
    pending_task_count_metrics_ << 1;
    pending_task_count_.fetch_add(1, std::memory_order_relaxed);
  }
  void DecPendingTaskCount() {
    pending_task_count_metrics_ << -1;
    pending_task_count_.fetch_sub(1, std::memory_order_relaxed);
  }
  void QueueWaitMetrics(int64_t value) { queue_wait_metrics_ << value; }
  void QueueRunMetrics(int64_t value) { queue_run_metrics_ << value; }

  virtual std::string Trace() { return ""; }
  virtual void DescribeByJson(Json::Value& value) {};

  virtual void HandleNotify(TaskRunnablePtr& task, WorkerEventType type);

 private:
  const std::string name_;

  std::atomic<uint32_t> worker_no_generator_{0};

  const bool use_pthread_;

  const uint32_t worker_num_{0};
  const int64_t max_pending_task_count_{0};

  std::atomic<int64_t> pending_task_count_{0};

  // Metrics
  bvar::Adder<uint64_t> total_task_count_metrics_;
  bvar::Adder<int64_t> pending_task_count_metrics_;
  bvar::LatencyRecorder queue_wait_metrics_;
  bvar::LatencyRecorder queue_run_metrics_;

 protected:
  bool IsDestroied() {
    bool expect = false;
    return !is_destroied.compare_exchange_strong(expect, true);
  }

  bool is_inplace_run{false};

  bool is_stop{false};
  std::atomic<uint32_t> stoped_count{0};
  std::atomic<bool> is_destroied{false};
};

using WorkerSetSPtr = std::shared_ptr<WorkerSet>;
using WorkerSetUPtr = std::unique_ptr<WorkerSet>;

// MPSC Multiple producer, single consumer
// Use brpc ExecutionQueueId implement
class ExecqWorkerSet : public WorkerSet {
 public:
  ExecqWorkerSet(std::string name, uint32_t worker_num, int64_t max_pending_task_count, bool use_pthread)
      : WorkerSet(name, worker_num, max_pending_task_count, use_pthread, false) {}
  ~ExecqWorkerSet() override = default;

  static WorkerSetSPtr New(std::string name, uint32_t worker_num, uint32_t max_pending_task_count,
                           bool use_pthread = false) {
    return std::make_shared<ExecqWorkerSet>(name, worker_num, max_pending_task_count, use_pthread);
  }

  static WorkerSetUPtr NewUnique(std::string name, uint32_t worker_num, uint32_t max_pending_task_count,
                                 bool use_pthread = false) {
    return std::make_unique<ExecqWorkerSet>(name, worker_num, max_pending_task_count, use_pthread);
  }

  bool Init() override;
  void Stop() override;

  bool Execute(TaskRunnablePtr task) override { return ExecuteLeastQueue(task); };
  bool ExecuteRR(TaskRunnablePtr task) override;
  bool ExecuteLeastQueue(TaskRunnablePtr task) override;
  bool ExecuteHash(int64_t id, TaskRunnablePtr task) override;

  std::string Trace() override;
  void DescribeByJson(Json::Value& value) override;

 private:
  uint32_t LeastPendingTaskWorker();

  std::vector<WorkerSPtr> workers_;
  std::atomic<uint64_t> active_worker_id_{0};
};

// MPMC multiple producer, multiple consumer
// Use std::queue implement
class SimpleWorkerSet : public WorkerSet {
 public:
  SimpleWorkerSet(std::string name, uint32_t worker_num, int64_t max_pending_task_count, bool use_pthread,
                  bool is_inplace_run);
  ~SimpleWorkerSet() override;

  static WorkerSetSPtr New(std::string name, uint32_t worker_num, uint32_t max_pending_task_count, bool use_pthread,
                           bool is_inplace_run) {
    return std::make_shared<SimpleWorkerSet>(name, worker_num, max_pending_task_count, use_pthread, is_inplace_run);
  }

  static WorkerSetUPtr NewUnique(std::string name, uint32_t worker_num, uint32_t max_pending_task_count,
                                 bool use_pthread, bool is_inplace_run) {
    return std::make_unique<SimpleWorkerSet>(name, worker_num, max_pending_task_count, use_pthread, is_inplace_run);
  }

  bool Init() override;
  void Stop() override;

  bool Execute(TaskRunnablePtr task) override;
  bool ExecuteRR(TaskRunnablePtr task) override;
  bool ExecuteLeastQueue(TaskRunnablePtr task) override;
  bool ExecuteHash(int64_t id, TaskRunnablePtr task) override;

  std::string Trace() override;
  void DescribeByJson(Json::Value& value) override;

 private:
  bthread_mutex_t mutex_;
  bthread_cond_t cond_;
  std::queue<TaskRunnablePtr> tasks_;

  std::vector<Bthread> bthread_workers_;
  std::vector<std::thread> pthread_workers_;
};

// MPMC multiple producer, multiple consumer
// Use std::priority_queue implement
class PriorWorkerSet : public WorkerSet {
 public:
  PriorWorkerSet(std::string name, uint32_t worker_num, int64_t max_pending_task_count, bool use_pthread,
                 bool is_inplace_run);
  ~PriorWorkerSet() override;

  static WorkerSetSPtr New(std::string name, uint32_t worker_num, uint32_t max_pending_task_count, bool use_pthread,
                           bool is_inplace_run) {
    return std::make_shared<PriorWorkerSet>(name, worker_num, max_pending_task_count, use_pthread, is_inplace_run);
  }

  bool Init() override;
  void Stop() override;

  bool Execute(TaskRunnablePtr task) override;
  bool ExecuteRR(TaskRunnablePtr task) override;
  bool ExecuteLeastQueue(TaskRunnablePtr task) override;
  bool ExecuteHash(int64_t id, TaskRunnablePtr task) override;

  std::string Trace() override;

 private:
  bthread_mutex_t mutex_;
  bthread_cond_t cond_;
  std::priority_queue<TaskRunnablePtr, std::vector<TaskRunnablePtr>, CompareTaskRunnable> tasks_;

  std::vector<Bthread> bthread_workers_;
  std::vector<std::thread> pthread_workers_;
};

// A cacheline-sized handshake between task submitters and their one consumer.
// The consumer arms the bell before it sleeps and re-checks its queue after
// arming; a waker only pays for the mutex and the wake syscall when it claims
// an armed bell, so a *running* consumer costs a submitter one atomic load.
class WorkerDoorbell {
 public:
  void Arm() { armed_.store(true, std::memory_order_seq_cst); }
  void Disarm() { armed_.store(false, std::memory_order_relaxed); }

  bool Armed() const { return armed_.load(std::memory_order_seq_cst); }

  // True for exactly one caller of a concurrently armed bell. seq_cst on both
  // sides is what makes Arm()/Armed() a sound Dekker pair: either the waker
  // sees the armed bell, or the armed consumer sees the submitted task.
  bool Claim() { return armed_.exchange(false, std::memory_order_seq_cst); }

 private:
  alignas(64) std::atomic<bool> armed_{false};
};

class WorkerParker {
 public:
  WorkerParker() = default;
  WorkerParker(const WorkerParker&) = delete;
  WorkerParker& operator=(const WorkerParker&) = delete;

  // Blocks until `ready` holds or the timeout expires. `ready` runs under the
  // same mutex the waker takes, so it must be cheap and must not block.
  template <typename Ready>
  void WaitFor(const Ready& ready, uint64_t timeout_ns) {
    doorbell_.Arm();
    if (ready()) {
      doorbell_.Disarm();
      return;
    }

    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait_for(lock, std::chrono::nanoseconds(timeout_ns), [this, &ready] { return woken_ || ready(); });
    woken_ = false;
    doorbell_.Disarm();
  }

  bool Armed() const { return doorbell_.Armed(); }
  bool Claim() { return doorbell_.Claim(); }

  // Only call after Claim() returned true.
  void WakeSleeper() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      woken_ = true;
    }
    cv_.notify_one();
  }

  void Wake() {
    if (Armed() && Claim()) WakeSleeper();
  }

 private:
  WorkerDoorbell doorbell_;
  std::mutex mutex_;
  std::condition_variable cv_;
  bool woken_{false};
};

// A task set optimised for one thing: the cost of submitting a task.
//
// It deliberately does not derive from WorkerSet. A WorkerSet pays per task
// for a shared pending counter, bvar metrics and latency recording; this pays
// for none of them. What it keeps is the property callers actually depend on:
// tasks hashed to the same worker run in submission order, which is what makes
// a per-ino AsyncOpen/AsyncClose pair ordered.
//
// Shape: `worker_num` MPSC queues, each with exactly one consumer thread --
// butil::MPSCQueue allows one consumer per queue and no more. Submitting picks
// a queue, reserves its pending slot, links the task and wakes that queue's
// thread only when it is genuinely asleep. That wake is a mutex plus a futex
// round trip, so a submitter pays several microseconds in full whenever the
// target worker happens to be idle -- which, with one worker per hash bucket,
// is most of the time.
class DoorbellWorkerSet {
 public:
  DoorbellWorkerSet(std::string name, uint32_t worker_num, int64_t max_pending_task_count);
  ~DoorbellWorkerSet();

  DoorbellWorkerSet(const DoorbellWorkerSet&) = delete;
  DoorbellWorkerSet& operator=(const DoorbellWorkerSet&) = delete;

  bool Init();
  void Stop();

  bool IsStopped() const { return is_stop_.load(std::memory_order_seq_cst); }

  bool Execute(TaskRunnablePtr task) { return ExecuteLeastQueue(std::move(task)); }
  bool ExecuteRR(TaskRunnablePtr task);
  bool ExecuteLeastQueue(TaskRunnablePtr task);
  bool ExecuteHash(uint64_t id, TaskRunnablePtr task);

  // Sum over workers. O(worker_num) loads: for tests and debug, not a hot path.
  int64_t PendingTaskCount();

 private:
  using TaskQueue = butil::MPSCQueue<TaskRunnablePtr, butil::ObjectPoolAllocator<TaskRunnablePtr>>;

  struct Worker {
    alignas(64) TaskQueue queue;
    alignas(64) std::atomic<int64_t> pending{0};
    alignas(64) WorkerParker parker;
    std::thread thread;
  };

  bool Submit(Worker& worker, TaskRunnablePtr& task);
  void Run(Worker& worker);
  void WorkerLoop(Worker& worker, uint32_t index);

  // Safety net. The doorbell already covers every wakeup, so this only bounds
  // the damage of a logic error to a timeout instead of a hang.
  static constexpr uint64_t kParkTimeoutNs = 1000 * 1000;

  const std::string name_;
  const uint32_t worker_num_;
  const int64_t max_pending_task_count_;

  std::atomic<bool> is_stop_{false};

  // One seed per producer thread, so a start at low concurrency does not pile
  // every producer onto worker 0.
  std::atomic<uint32_t> next_seed_{0};

  std::vector<std::unique_ptr<Worker>> workers_;
};

using DoorbellWorkerSetSPtr = std::shared_ptr<DoorbellWorkerSet>;
using DoorbellWorkerSetUPtr = std::unique_ptr<DoorbellWorkerSet>;

// A submission accelerator: one MPSC queue, one relay thread, and an internal
// ExecqWorkerSet that does the actual running.
//
// Why this exists: bthread::execution_queue_execute costs hundreds of
// nanoseconds and ExecqWorkerSet pays it on the caller's thread -- for an
// AsyncOpen, the FUSE request thread. Here the caller only reserves a slot and
// appends to an MPSC queue (no futex, no wake); the single relay thread then
// pays the execution-queue cost off the request path. The
// submitter finds the task on the relay's next poll, at most kHotWaitNs after a
// busy pass and kColdWaitNs after an idle one, and nothing in the client blocks
// on an async operation, so that delay is invisible to it.
//
// One queue, one consumer: the relay dequeues in the MPSC's linearisation order
// and forwards each task with ExecuteHash(id, ...), so tasks for the same ino
// still reach the same downstream worker in submission order -- the ordering
// AsyncOpen/AsyncClose depend on. Parallelism comes from the downstream set,
// not from the relay.
//
// There is no backpressure: neither this queue nor the downstream set caps its
// length, so Execute() fails only once the set is stopped. Stop() drains in two
// stages -- the relay forwards everything still queued, then the downstream set
// stops -- which is what keeps an accepted task from being dropped.
class RelayWorkerSet {
 public:
  RelayWorkerSet(std::string name, uint32_t worker_num);
  ~RelayWorkerSet();

  RelayWorkerSet(const RelayWorkerSet&) = delete;
  RelayWorkerSet& operator=(const RelayWorkerSet&) = delete;

  bool Init();
  void Stop();

  bool IsStopped() const { return is_stop_.load(std::memory_order_seq_cst); }

  bool Execute(TaskRunnablePtr task) { return ExecuteLeastQueue(std::move(task)); }
  bool ExecuteRR(TaskRunnablePtr task);
  bool ExecuteLeastQueue(TaskRunnablePtr task);
  bool ExecuteHash(uint64_t id, TaskRunnablePtr task);

  // Tasks accepted but not yet handed to the downstream set. For tests and
  // debug, not a hot path.
  int64_t PendingTaskCount() { return pending_.load(std::memory_order_relaxed); }

 private:
  // Which downstream entry point a queued task goes to. The relay is a single
  // thread, so RR and least-queue targets are chosen when the task is
  // forwarded, not when it is submitted.
  enum class RouteMode : uint8_t {
    kHash = 0,
    kRR = 1,
    kLeastQueue = 2,
  };

  struct RelayTask {
    RouteMode mode;
    uint64_t route_id;
    TaskRunnablePtr task;
  };

  using TaskQueue = butil::MPSCQueue<RelayTask, butil::ObjectPoolAllocator<RelayTask>>;

  bool Submit(RouteMode mode, uint64_t route_id, TaskRunnablePtr& task);
  bool Run();
  void WorkerLoop();

  // Wait after a pass that forwarded at least one task: a burst that just
  // produced work usually has more coming, so stay responsive to it.
  static constexpr uint64_t kHotWaitNs = 10 * 1000;
  // Wait after a pass that found nothing. Bounds both the added latency of a
  // task arriving at an idle relay and the cost of Stop().
  static constexpr uint64_t kColdWaitNs = 100 * 1000;

  const std::string name_;

  WorkerSetUPtr downstream_;

  TaskQueue queue_;

  // Reservation count: a task is on it from the moment Execute() accepts it
  // until the relay has forwarded it, so Stop() knows forwarding is done.
  alignas(64) std::atomic<int64_t> pending_{0};

  std::atomic<bool> is_stop_{false};

  std::thread thread_;
};

using RelayWorkerSetSPtr = std::shared_ptr<RelayWorkerSet>;
using RelayWorkerSetUPtr = std::unique_ptr<RelayWorkerSet>;

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_COMMON_RUNNABLE_H_