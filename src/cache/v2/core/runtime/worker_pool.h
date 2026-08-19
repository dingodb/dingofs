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

#ifndef DINGOFS_CACHE_V2_CORE_RUNTIME_WORKER_POOL_H_
#define DINGOFS_CACHE_V2_CORE_RUNTIME_WORKER_POOL_H_

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "cache/v2/core/reactor/doorbell.h"
#include "cache/v2/core/runtime/shard_inbox.h"
#include "cache/v2/core/runtime/smp.h"
#include "cache/v2/utils/containers/mpsc_queue.h"
#include "common/status.h"

namespace dingofs {
namespace cache {
namespace v2 {

// The one way off a reactor. A shard thread runs to completion, so anything
// that blocks or moves bulk bytes has to leave, and this is where it goes.
//
// Two lanes, because the two kinds of work must not queue behind each other:
// a blocking worker sits inside an S3 retry for seconds, and a memcpy parked
// behind it is a memcpy wasted.
enum class Lane : uint8_t {
  kBlocking,  // S3 / MDS: the thread blocks. --offload_threads
  kCpu,       // bulk memcpy and completion callbacks: never blocks
};

// One thread per shard, fixed to it. The bytes it moves were just touched by
// that shard's core, so a fixed pair keeps the state ping-ponging between two
// cores instead of drifting across a pool.
//
// The queue is intrusive and unbounded: the node IS the work, so posting
// allocates nothing and can never fail. Work runs in FIFO order and every
// item posted runs exactly once, including through Shutdown().
class CpuWorker {
 public:
  CpuWorker() = default;
  ~CpuWorker();

  CpuWorker(const CpuWorker&) = delete;
  CpuWorker& operator=(const CpuWorker&) = delete;

  void Start(unsigned shard);
  void Shutdown();  // drains what is queued, then joins

  void Post(InboxWork* work);  // any thread

 private:
  void Loop();
  unsigned Drain();  // runs everything queued, returns how many
  bool SpinForWork(uint64_t spin_ns);

  MpscQueue queue_;
  Parker parker_;
  std::atomic<bool> stopping_{false};
  std::thread thread_;
};

class WorkerPool {
 public:
  struct Option {
    unsigned threads = 0;       // 0 => FLAGS_offload_threads
    size_t queue_capacity = 0;  // 0 => FLAGS_offload_queue_capacity
  };

  WorkerPool();
  explicit WorkerPool(Option option);
  ~WorkerPool();

  WorkerPool(const WorkerPool&) = delete;
  WorkerPool& operator=(const WorkerPool&) = delete;

  // External thread only (main); Shutdown MUST finish before the shards stop.
  void Start();
  void Shutdown();

  // SHARD-ONLY. `fn` self-contained; resolves on the calling shard. Blocking
  // is allowed on kBlocking and forbidden on kCpu.
  template <typename Fn, typename T = std::invoke_result_t<Fn>>
  Future<T> Submit(Lane lane, Fn fn) {
    static_assert(std::is_constructible_v<T, Status>,
                  "offloaded work returns Status or StatusOr<T>");
    DCHECK(HasReactor()) << "Submit is shard-only; call fn directly instead";

    auto* job =
        new OffloadWork<T, Fn>(this, ThisShardId(), lane, std::move(fn));
    Status rejected = Enqueue(job, lane);
    if (!rejected.ok()) {
      delete job;  // no future was ever taken from it
      return MakeReadyFuture<T>(T(std::move(rejected)));
    }
    // Safe: the completion runs via THIS shard's inbox, after this returns.
    return job->promise.GetFuture();
  }

  template <typename Fn, typename T = std::invoke_result_t<Fn>>
  Future<T> Submit(Fn fn) {
    return Submit<Fn, T>(Lane::kBlocking, std::move(fn));
  }

  // SHARD-ONLY. Runs on this shard's cpu worker and never comes back; the
  // work owns itself and must free itself from `run`. Used for the client's
  // completion callbacks, which have nothing to return.
  void Post(InboxWork* work);

  void CountCopy(size_t bytes) {
    cpu_copied_bytes_.fetch_add(bytes, std::memory_order_relaxed);
  }

  // Below the threshold a memcpy is cheaper on the shard than a lane round
  // trip (~0.3us for 4 KiB against ~2-4us to hand it over and come back).
  static bool ShouldOffload(size_t bytes);

  // Bytes the cpu lane has moved. On the rdma cache-group path it must stay
  // at zero -- that is what "zero copy end to end" means, measured rather
  // than asserted.
  uint64_t cpu_copied_bytes() const {
    return cpu_copied_bytes_.load(std::memory_order_relaxed);
  }
  size_t inflight() const { return inflight_.load(std::memory_order_acquire); }

 private:
  struct OffloadWorkBase : InboxWork {
    OffloadWorkBase(WorkerPool* owner, unsigned origin_shard, Lane lane)
        : owner(owner), origin_shard(origin_shard) {
      run = lane == Lane::kCpu ? &OffloadWorkBase::OnCpu
                               : &OffloadWorkBase::OnShard;
    }
    virtual ~OffloadWorkBase() = default;

    virtual void Execute() = 0;  // worker thread: run fn into the result
    virtual void Abort() = 0;    // store CacheDown instead of running
    virtual void Resolve() = 0;  // origin shard: promise.SetValue

    // Cpu lane, out. Complete() retargets the node for the trip home, so the
    // same node serves both legs and nothing is allocated for the return.
    static void OnCpu(InboxWork* base) {
      auto* self = static_cast<OffloadWorkBase*>(base);
      self->Execute();
      self->owner->Complete(self);
    }

    static void OnShard(InboxWork* base) {
      auto* self = static_cast<OffloadWorkBase*>(base);
      self->Resolve();
      self->owner->inflight_.fetch_sub(1, std::memory_order_acq_rel);
      delete self;
    }

    WorkerPool* owner;
    unsigned origin_shard;
  };

  template <typename T, typename Fn>
  struct OffloadWork final : OffloadWorkBase {
    OffloadWork(WorkerPool* owner, unsigned origin_shard, Lane lane, Fn fn)
        : OffloadWorkBase(owner, origin_shard, lane), fn(std::move(fn)) {}

    void Execute() override { result.emplace(fn()); }
    void Abort() override {
      result.emplace(T(Status::CacheDown("offload pool is stopped")));
    }
    void Resolve() override { promise.SetValue(std::move(*result)); }

    Fn fn;
    std::optional<T> result;
    Promise<T> promise;  // single-shard: created and resolved on origin_shard
  };

  Status Enqueue(OffloadWorkBase* job, Lane lane);
  void WorkerLoop();
  // PostTo(origin); on false: count down, delete.
  void Complete(OffloadWorkBase* job);

  unsigned thread_count_;
  size_t capacity_;

  std::mutex mutex_;
  std::condition_variable cv_;
  std::deque<OffloadWorkBase*> queue_;
  // Read off the mutex on the cpu lane, which has no queue to guard.
  std::atomic<bool> closed_{true};  // Start() opens

  // enqueue -> resolved-on-shard (or dropped); Shutdown spin-waits it to zero.
  std::atomic<size_t> inflight_{0};
  std::atomic<uint64_t> cpu_copied_bytes_{0};
  std::vector<std::thread> workers_;
  std::unique_ptr<CpuWorker[]> cpu_workers_;  // one per shard
  unsigned cpu_worker_count_ = 0;
};

using WorkerPoolUPtr = std::unique_ptr<WorkerPool>;

// The process's pool, installed by Start() and dropped by Shutdown(). Null is
// a real answer -- a bench or a test with no pool keeps its copies on the
// shard -- so callers must handle it, as Mesh::built() is handled.
WorkerPool* GetGlobalWorkers();

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_CORE_RUNTIME_WORKER_POOL_H_
