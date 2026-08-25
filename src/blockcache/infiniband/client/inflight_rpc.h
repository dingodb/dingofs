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

#ifndef DINGOFS_BLOCKCACHE_INFINIBAND_CLIENT_INFLIGHT_RPC_H_
#define DINGOFS_BLOCKCACHE_INFINIBAND_CLIENT_INFLIGHT_RPC_H_

#include <glog/logging.h>
#include <google/protobuf/message.h>

#include <coroutine>
#include <cstdint>
#include <vector>

#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/core/reactor/reactor.h"
#include "blockcache/utils/containers/park_queue.h"
#include "common/status.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

template <typename T>
class InflightRpcTable {
 public:
  struct Slot {
    Promise<T> promise;
    google::protobuf::Message* response = nullptr;
    uint64_t generation = 0;
    bool in_use = false;
  };

  class SlotAwaiter {
   public:
    SlotAwaiter(const SlotAwaiter&) = delete;
    SlotAwaiter& operator=(const SlotAwaiter&) = delete;

    bool await_ready() const noexcept { return table_->HasFreeSlot(); }

    template <TaskPromise P>
    void await_suspend(std::coroutine_handle<P> h) noexcept {
      task_ = &h.promise();
      table_->slot_waiters_.Push(this);
    }

    uint16_t await_resume() noexcept {
      if (failed_) {
        return 0;
      }
      return slot_index_ != kNone ? slot_index_ : table_->TakeFreeSlot();
    }

    bool failed() const { return failed_; }

    SlotAwaiter* park_next = nullptr;

   private:
    friend class InflightRpcTable;
    static constexpr uint16_t kNone = 0xffff;

    explicit SlotAwaiter(InflightRpcTable* table) : table_(table) {}

    InflightRpcTable* table_;
    Task* task_ = nullptr;
    uint16_t slot_index_ = kNone;
    bool failed_ = false;
  };

  InflightRpcTable() = default;

  InflightRpcTable(const InflightRpcTable&) = delete;
  InflightRpcTable& operator=(const InflightRpcTable&) = delete;

  void Init(uint16_t capacity) {
    slots_.resize(capacity);
    free_slots_.reserve(capacity);
    for (uint16_t i = capacity; i > 0; --i) {
      free_slots_.push_back(static_cast<uint16_t>(i - 1));
    }
  }

  SlotAwaiter AcquireSlot() { return SlotAwaiter(this); }

  void Release(uint16_t slot_index) {
    Slot& slot = slots_[slot_index];
    if (!slot.in_use) {
      return;
    }
    slot.in_use = false;
    ++slot.generation;
    slot.promise = Promise<T>();
    slot.response = nullptr;

    SlotAwaiter* waiter = slot_waiters_.Pop();
    if (waiter == nullptr) {
      free_slots_.push_back(slot_index);
      return;
    }
    slot.in_use = true;
    waiter->slot_index_ = slot_index;
    NotifyWaiter(waiter);
  }

  void FailAll(const Status& reason) {
    slot_waiters_.TakeAllAnd([this](SlotAwaiter* waiter) {
      waiter->failed_ = true;
      NotifyWaiter(waiter);
    });

    for (uint16_t i = 0; i < slots_.size(); ++i) {
      Slot& slot = slots_[i];
      if (!slot.in_use) {
        continue;
      }
      slot.in_use = false;
      ++slot.generation;
      free_slots_.push_back(i);
      slot.promise.SetValue(reason);
    }
  }

  Slot& operator[](uint16_t slot_index) { return slots_[slot_index]; }

  uint64_t GetCorrelationId(uint16_t slot_index) const {
    return MakeCorrelation(slot_index, slots_[slot_index].generation);
  }

  Slot* FindByCorrelationId(uint64_t correlation_id) {
    const uint16_t slot_index = CorrelationSlot(correlation_id);
    if (slot_index >= slots_.size()) {
      return nullptr;
    }
    Slot& slot = slots_[slot_index];
    if (!slot.in_use ||
        slot.generation != CorrelationGeneration(correlation_id)) {
      return nullptr;
    }
    return &slot;
  }

  void ReleaseByCorrelationId(uint64_t correlation_id) {
    const uint16_t slot_index = CorrelationSlot(correlation_id);
    if (slot_index < slots_.size()) {
      Release(slot_index);
    }
  }

 private:
  static uint64_t MakeCorrelation(uint16_t slot, uint64_t generation) {
    return (generation << 16) | slot;
  }

  static uint16_t CorrelationSlot(uint64_t correlation_id) {
    return static_cast<uint16_t>(correlation_id & 0xffff);
  }

  static uint64_t CorrelationGeneration(uint64_t correlation_id) {
    return correlation_id >> 16;
  }

  void NotifyWaiter(SlotAwaiter* waiter) {
    ThisReactor().Schedule(waiter->task_);
  }

  bool HasFreeSlot() const { return !free_slots_.empty(); }

  uint16_t TakeFreeSlot() {
    DCHECK(!free_slots_.empty());
    const uint16_t slot_index = free_slots_.back();
    free_slots_.pop_back();
    slots_[slot_index].in_use = true;
    return slot_index;
  }

  std::vector<Slot> slots_;
  std::vector<uint16_t> free_slots_;
  ParkQueue<SlotAwaiter> slot_waiters_;
};

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs

#endif
