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

#ifndef DINGOFS_CLIENT_VFS_OPERATION_TRACKER_H_
#define DINGOFS_CLIENT_VFS_OPERATION_TRACKER_H_

#include <glog/logging.h>

#include <array>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <limits>
#include <mutex>
#include <optional>
#include <utility>

namespace dingofs {
namespace client {

// One-shot admission tracker. The lifecycle control plane serializes OpenOnce
// against Close; only the elected Stop owner calls Close and WaitForDrain.
// The tracker starts closed and cannot reopen after its one successful
// OpenOnce.
//
// A lease must cover ALL protected runtime accesses, including their tails.
// No operation or callback holding a lease may synchronously reenter Stop or
// WaitForDrain: that would wait for its own lease. The tracker invokes no
// production callbacks and Leave never acquires a lifecycle lock.
//
// WaitForDrain protects admitted runtime accesses, NOT this object's lifetime:
// a rejected TryEnter can still roll back a late tentative increment, and a
// Leave can still be notifying after the scan sees zero. The external owner
// must close entry points and join ALL callers and notification tails before
// destroying the tracker. A zero-counter destructor check cannot replace
// joining.
class OperationTracker {
 public:
  static constexpr unsigned kSlotCount = 64;

  class Lease {
   public:
    Lease(const Lease&) = delete;
    Lease& operator=(const Lease&) = delete;

    Lease(Lease&& other) noexcept
        : tracker_(std::exchange(other.tracker_, nullptr)),
          slot_(other.slot_) {}

    Lease& operator=(Lease&& other) noexcept {
      if (this != &other) {
        Release();
        tracker_ = std::exchange(other.tracker_, nullptr);
        slot_ = other.slot_;
      }
      return *this;
    }

    ~Lease() { Release(); }

    explicit operator bool() const noexcept { return tracker_ != nullptr; }

   private:
    friend class OperationTracker;

    Lease(OperationTracker* tracker, unsigned slot) noexcept
        : tracker_(tracker), slot_(slot) {}

    void Release() noexcept {
      if (tracker_ != nullptr) {
        tracker_->Leave(slot_);
        tracker_ = nullptr;
      }
    }

    OperationTracker* tracker_;
    unsigned slot_;
  };

  OperationTracker() = default;
  OperationTracker(const OperationTracker&) = delete;
  OperationTracker& operator=(const OperationTracker&) = delete;

  void OpenOnce();

  std::optional<Lease> TryEnter() {
    const auto before = epoch_.load(std::memory_order_seq_cst);
    if ((before & 1) != 0) return std::nullopt;

    const auto slot = ThreadSlot();
    const auto old =
        counts_[slot].value.fetch_add(1, std::memory_order_seq_cst);
    CHECK_NE(old, std::numeric_limits<uint64_t>::max())
        << "Operation counter overflow";
    const auto after = epoch_.load(std::memory_order_seq_cst);
    if (after != before || (after & 1) != 0) {
      // A failed second check owns the same decrement/notification duty as an
      // admitted lease, even if the owner already observed a drained tracker.
      Leave(slot);
      return std::nullopt;
    }
    return Lease(this, slot);
  }

  void Close() { epoch_.fetch_or(1, std::memory_order_seq_cst); }

  // Call after Close, without the lifecycle mutex. Timeouts only diagnose;
  // they never authorize destruction of runtime still protected by a lease.
  void WaitForDrain();

 private:
  static uint64_t AllocateThreadIndex();

  static unsigned ThreadSlot() {
    // Shared across tracker instances, allocated once per OS thread, not once
    // per operation. A slot is not a CPU, worker, or bthread identity.
    static thread_local const unsigned slot =
        static_cast<unsigned>(AllocateThreadIndex() & (kSlotCount - 1));
    return slot;
  }

  void Leave(unsigned slot) {
    const auto old =
        counts_[slot].value.fetch_sub(1, std::memory_order_seq_cst);
    CHECK_GT(old, 0) << "Operation counter underflow or double release";
    // Read the epoch AFTER decrementing. Reusing a prior open observation can
    // miss the final notification when Close races with this release.
    if (old == 1 && (epoch_.load(std::memory_order_seq_cst) & 1) != 0) {
      std::lock_guard<std::mutex> lock(drain_mutex_);
      drain_cv_.notify_all();
    }
  }

  bool IsDrained() const;

  struct alignas(64) Counter {
    std::atomic<uint64_t> value{0};
  };
  static_assert(sizeof(Counter) == 64,
                "Each tracker counter needs one cacheline");

  alignas(64) std::atomic<uint64_t> epoch_{1};
  std::array<Counter, kSlotCount> counts_{};
  std::mutex drain_mutex_;
  std::condition_variable drain_cv_;
  bool opened_{false};  // Accessed only by the serialized lifecycle owner.
};

}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_OPERATION_TRACKER_H_
