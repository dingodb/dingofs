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

#ifndef DINGOFS_CACHE_V2_CORE_NET_RDMA_RPC_CALL_TABLE_H_
#define DINGOFS_CACHE_V2_CORE_NET_RDMA_RPC_CALL_TABLE_H_

#include <glog/logging.h>

#include <coroutine>
#include <cstdint>
#include <utility>
#include <vector>

#include "cache/v2/core/net/rdma/rpc/frame.h"
#include "cache/v2/core/reactor/coroutine.h"
#include "cache/v2/core/reactor/reactor.h"
#include "cache/v2/utils/containers/park_queue.h"
#include "common/status.h"

namespace dingofs {
namespace cache {
namespace v2 {

// Matches responses to in-flight requests: fixed array + per-slot generation.
// Running out of slots is the back pressure; callers park in arrival order.
template <typename T>
class CallTable {
 public:
  struct Entry {
    Promise<T> promise;
    uint64_t generation = 0;
    bool busy = false;
  };

  // co_await -> slot index; check failed() first, the conn may have died.
  class Acquire {
   public:
    explicit Acquire(CallTable* table) : table_(table) {}

    bool await_ready() const noexcept { return table_->HasFree(); }

    template <TaskPromise P>
    void await_suspend(std::coroutine_handle<P> h) noexcept {
      task_ = &h.promise();
      table_->parked_.Push(this);
    }

    uint16_t await_resume() noexcept {
      if (failed_) {
        return 0;
      }
      // Parked path got a slot from Release; ready path takes a free one.
      return index_ != kNone ? index_ : table_->TakeFree();
    }

    bool failed() const { return failed_; }

    Acquire* park_next = nullptr;

   private:
    friend class CallTable;
    static constexpr uint16_t kNone = 0xffff;

    CallTable* table_;
    Task* task_ = nullptr;
    uint16_t index_ = kNone;
    bool failed_ = false;
  };

  void Init(uint16_t count) {
    slots_.resize(count);
    free_.reserve(count);
    for (uint16_t i = count; i > 0; --i) {
      free_.push_back(static_cast<uint16_t>(i - 1));
    }
  }

  Acquire AcquireEntry() { return Acquire(this); }

  Entry& operator[](uint16_t index) { return slots_[index]; }

  uint64_t CorrelationOf(uint16_t index) const {
    return MakeCorrelation(index, slots_[index].generation);
  }

  // Returns nullptr for a stale/duplicated/out-of-range correlation id.
  Entry* Match(uint64_t correlation) {
    const uint16_t index = CorrelationSlot(correlation);
    if (index >= slots_.size()) {
      return nullptr;
    }
    Entry& slot = slots_[index];
    if (!slot.busy || slot.generation != CorrelationGeneration(correlation)) {
      return nullptr;
    }
    return &slot;
  }

  void Release(uint16_t index) {
    Entry& slot = slots_[index];
    slot.busy = false;
    ++slot.generation;  // any reply still in flight for it is now stale
    slot.promise = Promise<T>();

    // Straight to the oldest waiter; never returns to the free list.
    Acquire* waiter = parked_.Pop();
    if (waiter == nullptr) {
      free_.push_back(index);
      return;
    }
    slot.busy = true;
    waiter->index_ = index;
    ThisReactor().Schedule(waiter->task_);
  }

  // Connection died: wakes both slot waiters and parked reply waiters.
  void FailAll(const Status& reason) {
    parked_.TakeAllAnd([](Acquire* waiter) {
      waiter->failed_ = true;
      ThisReactor().Schedule(waiter->task_);
    });

    for (uint16_t i = 0; i < slots_.size(); ++i) {
      Entry& slot = slots_[i];
      if (!slot.busy) {
        continue;
      }
      slot.busy = false;
      ++slot.generation;
      free_.push_back(i);
      slot.promise.SetValue(reason);
    }
  }

 private:
  friend class Acquire;

  bool HasFree() const { return !free_.empty(); }

  uint16_t TakeFree() {
    DCHECK(!free_.empty());
    const uint16_t index = free_.back();
    free_.pop_back();
    slots_[index].busy = true;
    return index;
  }

  std::vector<Entry> slots_;
  std::vector<uint16_t> free_;
  ParkQueue<Acquire> parked_;
};

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_CORE_NET_RDMA_RPC_CALL_TABLE_H_
