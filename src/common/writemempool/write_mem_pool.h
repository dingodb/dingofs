/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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

#ifndef DINGOFS_SRC_COMMON_WRITEMEMPOOL_WRITE_MEM_POOL_H_
#define DINGOFS_SRC_COMMON_WRITEMEMPOOL_WRITE_MEM_POOL_H_

#include <sys/types.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <mutex>

#include "bvar/passive_status.h"
#include "bvar/reducer.h"
#include "bvar/status.h"
#include "common/status.h"
#include "common/writemempool/write_page_lease.h"
#include "common/writemempool/write_page_pool.h"
#include "common/writemempool/write_pressure_observer.h"

namespace dingofs {

class WriteMemPool {
 public:
  explicit WriteMemPool(int64_t total_bytes, int64_t page_size);
  ~WriteMemPool();

  WriteMemPool(const WriteMemPool&) = delete;
  WriteMemPool& operator=(const WriteMemPool&) = delete;

  // FIFO, exact-capacity admission. The caller blocks without holding any
  // FileWriter/ChunkWriter/SliceWriter lock until all requested pages can be
  // granted, the pool closes, or an internal allocator invariant breaks.
  Status Acquire(size_t count, WritePageLease* lease);

  // Non-blocking exact admission for best-effort work such as compaction.
  // It never bypasses queued FIFO waiters.
  Status TryAcquire(size_t count, WritePageLease* lease);

  // Returns exactly count pairwise-distinct pages previously transferred from
  // a lease. Valid leases may still call Release after Close.
  void Release(char* const* pages, size_t count);

  // Stops admission and wakes every ungranted Acquire with Status::Stop.
  // Existing leases and page owners remain valid and must release normally.
  void Close();

  // Registering requires no observer to be installed. Clearing prevents new
  // callbacks and waits for every in-flight callback to return. The observer
  // must remain alive until clearing completes and must not clear itself.
  void SetPressureObserver(WritePressureObserver* observer);
  void NotifyDirtyPublished();
  bool IsPressured() const {
    return pressured_.load(std::memory_order_acquire);
  }

  int64_t GetPageSize() const;
  int64_t GetTotalBytes() const;
  int64_t GetUsedBytes() const;

  char* BaseAddr() const;
  size_t BufferSize() const;
  size_t BufferCount() const;
  size_t TotalSize() const;

 private:
  friend class WriteMemPoolTestPeer;
  enum class AdmissionResult : uint8_t {
    kGranted,
    kUnavailable,
    kClosed,
    kBroken
  };
  static constexpr uint64_t kAvailableMask = UINT32_MAX;
  static constexpr uint64_t kContended = uint64_t{1} << 32;
  static constexpr uint64_t kClosed = uint64_t{1} << 33;
  static constexpr uint64_t kBroken = uint64_t{1} << 34;

  struct Waiter {
    explicit Waiter(size_t requested)
        : need(requested), queued_at(std::chrono::steady_clock::now()) {}

    enum class Result : uint8_t { kWaiting, kGranted, kStopped, kBroken };

    const size_t need;
    const std::chrono::steady_clock::time_point queued_at;
    std::condition_variable cv;
    Result result{Result::kWaiting};
  };

  AdmissionResult TryReserveFast(size_t count);
  bool ReserveContendedLocked(size_t count);
  void ClearContendedLocked();
  bool GrantWaitersLocked();
  Status AcquireSlow(size_t count, WritePageLease* lease);
  Status MaterializeLease(size_t count, WritePageLease* lease);
  void BreakPool(size_t reserved_count);
  void NotifyPressure();
  static size_t AvailablePages(uint64_t state) {
    return static_cast<size_t>(state & kAvailableMask);
  }

  static int64_t UsedBytes(void* arg);
  static int64_t UsedPages(void* arg);
  static int64_t WaiterCount(void* arg);
  static int64_t OldestWaiterUs(void* arg);

  const int64_t total_bytes_{0};
  const int64_t page_size_{0};
  WritePagePoolUPtr page_pool_;
  const size_t capacity_pages_{0};
  std::atomic<uint64_t> admission_state_{0};

  mutable std::mutex waiter_mutex_;
  std::deque<Waiter*> waiters_;
  std::atomic_bool pressured_{false};
  std::mutex pressure_observer_mutex_;
  std::condition_variable pressure_observer_cv_;
  WritePressureObserver* pressure_observer_{nullptr};
  size_t pressure_observer_calls_{0};

  bvar::Status<int64_t> capacity_pages_var_;
  bvar::PassiveStatus<int64_t> used_pages_var_;
  bvar::PassiveStatus<int64_t> used_bytes_var_;
  bvar::PassiveStatus<int64_t> waiter_count_var_;
  bvar::PassiveStatus<int64_t> oldest_waiter_us_var_;
  bvar::Adder<int64_t> acquire_wait_num_;
  bvar::Adder<int64_t> acquire_stop_num_;
  bvar::Adder<int64_t> try_acquire_busy_num_;
};

}  // namespace dingofs

#endif  // DINGOFS_SRC_COMMON_WRITEMEMPOOL_WRITE_MEM_POOL_H_
