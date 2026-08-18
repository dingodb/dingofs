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

#include "cache/v2/core/runtime/worker_pool.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <pthread.h>

#include <string>

#include "cache/v2/core/runtime/smp.h"
#include "cache/v2/utils/cpu.h"
#include "cache/v2/utils/thread.h"
#include "cache/v2/utils/time.h"

namespace dingofs {
namespace cache {
namespace v2 {

// Workers block inside sync S3/MDS calls: this count IS backend concurrency.
DEFINE_uint32(offload_threads, 32,
              "worker threads for offloaded blocking work");
DEFINE_uint32(offload_queue_capacity, 4096,
              "queued offload jobs beyond which submits answer busy");
DEFINE_uint32(offload_cpu_min_bytes, 32768,
              "copies below this stay on the shard");
DEFINE_uint32(offload_cpu_spin_us, 0,
              "cpu workers spin this long for more work before parking");

// -- CpuWorker ---------------------------------------------------------------

CpuWorker::~CpuWorker() { Shutdown(); }

void CpuWorker::Start(unsigned shard) {
  CHECK(!thread_.joinable()) << "CpuWorker started twice";
  stopping_.store(false, std::memory_order_release);
  thread_ = std::thread([this, shard] {
    SetThreadName("cpu-" + std::to_string(shard));
    Loop();
  });
}

void CpuWorker::Shutdown() {
  if (!thread_.joinable()) {
    return;
  }
  stopping_.store(true, std::memory_order_release);
  {
    std::lock_guard<std::mutex> lock(mutex_);
  }
  cv_.notify_one();
  thread_.join();
  Drain();  // whatever landed in the window before the thread saw stopping_
}

void CpuWorker::Post(InboxWork* work) {
  queue_.Push(work);
  if (bell_.ClaimWakeup()) {
    std::lock_guard<std::mutex> lock(mutex_);  // pairs with the park below
    cv_.notify_one();
  }
}

void CpuWorker::Loop() {
  const uint64_t spin_ns = uint64_t{FLAGS_offload_cpu_spin_us} * 1000;
  for (;;) {
    if (Drain() != 0) {
      continue;
    }
    if (stopping_.load(std::memory_order_acquire)) {
      return;
    }
    if (spin_ns != 0 && SpinForWork(spin_ns)) {
      continue;
    }

    // Arm before re-checking: a post between the drain above and the arm
    // would otherwise ring a bell nobody was listening for.
    bell_.Arm();
    {
      std::unique_lock<std::mutex> lock(mutex_);
      if (queue_.empty() && !stopping_.load(std::memory_order_acquire)) {
        cv_.wait(lock);
      }
    }
    bell_.Disarm();
  }
}

unsigned CpuWorker::Drain() { return queue_.ConsumeAll(RunInboxWork); }

bool CpuWorker::SpinForWork(uint64_t spin_ns) {
  const uint64_t spin_until = TimestampNs() + spin_ns;
  while (TimestampNs() < spin_until) {
    if (!queue_.empty() || stopping_.load(std::memory_order_acquire)) {
      return true;
    }
    CpuRelax();
  }
  return !queue_.empty();
}

// -- WorkerPool ---------------------------------------------------------------

// Set by Start(), cleared by Shutdown(); both run on the external thread.
static WorkerPool* g_workers = nullptr;

WorkerPool* GetGlobalWorkers() { return g_workers; }

WorkerPool::WorkerPool() : WorkerPool(Option{}) {}

WorkerPool::WorkerPool(Option option)
    : thread_count_(option.threads != 0 ? option.threads
                                        : FLAGS_offload_threads),
      capacity_(option.queue_capacity != 0 ? option.queue_capacity
                                           : FLAGS_offload_queue_capacity) {}

WorkerPool::~WorkerPool() { Shutdown(); }

void WorkerPool::Start() {
  CHECK(closed_.load(std::memory_order_relaxed)) << "WorkerPool started twice";

  LOG(INFO) << "WorkerPool is starting...";

  // One cpu worker per shard, fixed to it. Up before the lane opens.
  cpu_worker_count_ = ShardCount();
  cpu_workers_ = std::make_unique<CpuWorker[]>(cpu_worker_count_);
  for (unsigned shard = 0; shard < cpu_worker_count_; ++shard) {
    cpu_workers_[shard].Start(shard);
  }

  CHECK(g_workers == nullptr) << "a second WorkerPool in this process";
  g_workers = this;

  closed_.store(false, std::memory_order_release);
  workers_.reserve(thread_count_);
  for (unsigned i = 0; i < thread_count_; ++i) {
    workers_.emplace_back([this, i] {
      SetThreadName("offload-" + std::to_string(i));
      WorkerLoop();
    });
  }
  LOG(INFO) << "Successfully start WorkerPool: threads=" << thread_count_
            << " queue_capacity=" << capacity_
            << " cpu_workers=" << cpu_worker_count_;
}

void WorkerPool::Shutdown() {
  if (closed_.exchange(true, std::memory_order_acq_rel)) {
    return;
  }

  LOG(INFO) << "WorkerPool is shutting down...";

  cv_.notify_all();
  for (std::thread& worker : workers_) {
    worker.join();
  }
  workers_.clear();

  // Whatever queued still gets an answer, on its shard.
  std::deque<OffloadWorkBase*> rest;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    rest.swap(queue_);
  }
  for (OffloadWorkBase* job : rest) {
    job->Abort();
    Complete(job);
  }

  // The cpu lane last: nothing in the blocking lane feeds it, and its own
  // drain still posts answers back to shards that are still running.
  for (unsigned shard = 0; shard < cpu_worker_count_; ++shard) {
    cpu_workers_[shard].Shutdown();
  }
  cpu_workers_.reset();
  cpu_worker_count_ = 0;

  // Once this clears, nothing of ours can touch the runtime.
  while (inflight_.load(std::memory_order_acquire) != 0) {
    std::this_thread::yield();
  }
  if (g_workers == this) {
    g_workers = nullptr;
  }
  LOG(INFO) << "Successfully shutdown WorkerPool: cpu lane moved "
            << cpu_copied_bytes() << " bytes";
}

void WorkerPool::Post(InboxWork* work) {
  DCHECK(HasReactor()) << "Post is shard-only";
  if (closed_.load(std::memory_order_acquire)) {
    // Unreachable under the teardown DAG: the caller drains its own in-flight
    // work before the workers stops. Run it rather than drop it -- an
    // accepted request is owed exactly one completion.
    LOG(ERROR) << "Cpu work posted after the workers stopped; "
                  "running it on the shard";
    work->run(work);
    return;
  }
  cpu_workers_[ThisShardId()].Post(work);
}

bool WorkerPool::ShouldOffload(size_t bytes) {
  return bytes >= FLAGS_offload_cpu_min_bytes;
}

Status WorkerPool::Enqueue(OffloadWorkBase* job, Lane lane) {
  if (lane == Lane::kCpu) {
    // No queue and no cap: the node is the work, so posting cannot fail, and
    // a copy has nowhere else to go.
    if (closed_.load(std::memory_order_acquire)) {
      return Status::CacheDown("offload pool is stopped");
    }
    inflight_.fetch_add(1, std::memory_order_relaxed);
    cpu_workers_[job->origin_shard].Post(job);
    return Status::OK();
  }

  std::lock_guard<std::mutex> lock(mutex_);
  if (closed_.load(std::memory_order_relaxed)) {
    return Status::CacheDown("offload pool is stopped");
  }
  if (queue_.size() >= capacity_) {
    return Status::OutOfMemory("offload queue is full");
  }
  queue_.push_back(job);
  inflight_.fetch_add(1, std::memory_order_relaxed);
  cv_.notify_one();
  return Status::OK();
}

void WorkerPool::WorkerLoop() {
  for (;;) {
    OffloadWorkBase* job = nullptr;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      cv_.wait(lock, [this] {
        return closed_.load(std::memory_order_relaxed) || !queue_.empty();
      });
      if (closed_.load(std::memory_order_relaxed)) {
        return;  // Shutdown() aborts whatever remains
      }
      job = queue_.front();
      queue_.pop_front();
    }
    job->Execute();
    Complete(job);
  }
}

void WorkerPool::Complete(OffloadWorkBase* job) {
  job->run =
      &OffloadWorkBase::OnShard;  // whatever lane it came from, this is home
  if (PostTo(job->origin_shard, job)) {
    return;
  }
  // Shard gone; unreachable under the teardown DAG (workers stops first).
  LOG(ERROR) << "Fail to deliver offloaded work back to shard "
             << job->origin_shard << ": it is stopping";
  inflight_.fetch_sub(1, std::memory_order_acq_rel);
  delete job;
}

}  // namespace v2
}  // namespace cache
}  // namespace dingofs
