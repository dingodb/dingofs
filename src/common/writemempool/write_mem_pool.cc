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

#include "common/writemempool/write_mem_pool.h"

#include <glog/logging.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <mutex>
namespace dingofs {
namespace {

WritePagePoolUPtr CreatePagePool(int64_t total_bytes, int64_t page_size) {
  CHECK_GT(page_size, 0);
  CHECK_GE(total_bytes, page_size);
  CHECK_EQ(total_bytes % page_size, 0)
      << "write pool capacity must be an exact multiple of page size";
  CHECK_LE(static_cast<uint64_t>(total_bytes),
           static_cast<uint64_t>(std::numeric_limits<size_t>::max()));

  auto pool =
      WritePagePool::Create(static_cast<size_t>(page_size),
                            static_cast<size_t>(total_bytes / page_size));
  CHECK(pool != nullptr) << "WriteMemPool failed to create WritePagePool: page="
                         << page_size << " total_bytes=" << total_bytes;
  return pool;
}

}  // namespace

WriteMemPool::WriteMemPool(int64_t total_bytes, int64_t page_size)
    : total_bytes_(total_bytes),
      page_size_(page_size),
      page_pool_(CreatePagePool(total_bytes, page_size)),
      capacity_pages_(page_pool_->BufferCount()),
      admission_state_(capacity_pages_),
      capacity_pages_var_("vfs_write_buffer_capacity_pages",
                          static_cast<int64_t>(capacity_pages_)),
      used_pages_var_("vfs_write_buffer_used_pages", UsedPages, this),
      used_bytes_var_("vfs_write_buffer_used_bytes", UsedBytes, this),
      waiter_count_var_("vfs_write_buffer_waiter_count", WaiterCount, this),
      oldest_waiter_us_var_("vfs_write_buffer_oldest_waiter_us", OldestWaiterUs,
                            this),
      acquire_wait_num_("vfs_write_buffer_acquire_wait_num"),
      acquire_stop_num_("vfs_write_buffer_acquire_stop_num"),
      try_acquire_busy_num_("vfs_write_buffer_try_acquire_busy_num") {}

WriteMemPool::~WriteMemPool() { Close(); }

WriteMemPool::AdmissionResult WriteMemPool::TryReserveFast(size_t count) {
  uint64_t state = admission_state_.load(std::memory_order_acquire);
  for (;;) {
    if ((state & kBroken) != 0) return AdmissionResult::kBroken;
    if ((state & kClosed) != 0) return AdmissionResult::kClosed;
    if ((state & kContended) != 0 || AvailablePages(state) < count) {
      return AdmissionResult::kUnavailable;
    }
    if (admission_state_.compare_exchange_weak(state, state - count,
                                               std::memory_order_acq_rel,
                                               std::memory_order_acquire)) {
      return AdmissionResult::kGranted;
    }
  }
}

bool WriteMemPool::ReserveContendedLocked(size_t count) {
  uint64_t state = admission_state_.load(std::memory_order_acquire);
  for (;;) {
    CHECK_NE(state & kContended, 0);
    if ((state & (kClosed | kBroken)) != 0 || AvailablePages(state) < count) {
      return false;
    }
    if (admission_state_.compare_exchange_weak(state, state - count,
                                               std::memory_order_acq_rel,
                                               std::memory_order_acquire)) {
      return true;
    }
  }
}

void WriteMemPool::ClearContendedLocked() {
  CHECK(waiters_.empty());
  uint64_t state = admission_state_.load(std::memory_order_acquire);
  while ((state & kContended) != 0 &&
         !admission_state_.compare_exchange_weak(state, state & ~kContended,
                                                 std::memory_order_release,
                                                 std::memory_order_acquire)) {
  }
}

Status WriteMemPool::MaterializeLease(size_t count, WritePageLease* lease) {
  WritePageLease::Pages pages;
  pages.resize(count);
  const size_t allocated = page_pool_->RequireBatchExact(pages.data(), count);
  if (allocated != count) {
    if (allocated != 0) {
      page_pool_->ReleaseBatch(pages.data(), allocated);
    }
    BreakPool(count);
    return Status::Internal(
        "write page allocator violated exact admission reservation");
  }
  *lease = WritePageLease(this, std::move(pages));
  return Status::OK();
}

Status WriteMemPool::Acquire(size_t count, WritePageLease* lease) {
  CHECK_NOTNULL(lease);
  CHECK(lease->Empty());
  if (count == 0) return Status::OK();
  if (count > capacity_pages_) {
    return Status::NoSpace("write request exceeds page pool capacity");
  }

  switch (TryReserveFast(count)) {
    case AdmissionResult::kGranted:
      return MaterializeLease(count, lease);
    case AdmissionResult::kClosed:
      acquire_stop_num_ << 1;
      return Status::Stop("write page pool closed");
    case AdmissionResult::kBroken:
      return Status::Internal("write page pool broken");
    case AdmissionResult::kUnavailable:
      return AcquireSlow(count, lease);
  }
  LOG(FATAL) << "unreachable admission result";
  return Status::Internal("unreachable admission result");
}

Status WriteMemPool::AcquireSlow(size_t count, WritePageLease* lease) {
  Waiter waiter(count);
  bool notify_first_waiter = false;
  std::unique_lock<std::mutex> lock(waiter_mutex_);
  admission_state_.fetch_or(kContended, std::memory_order_acq_rel);

  const uint64_t state = admission_state_.load(std::memory_order_acquire);
  if ((state & kBroken) != 0) {
    ClearContendedLocked();
    return Status::Internal("write page pool broken");
  }
  if ((state & kClosed) != 0) {
    ClearContendedLocked();
    acquire_stop_num_ << 1;
    return Status::Stop("write page pool closed");
  }

  if (waiters_.empty() && ReserveContendedLocked(count)) {
    ClearContendedLocked();
    lock.unlock();
    return MaterializeLease(count, lease);
  }

  notify_first_waiter = waiters_.empty();
  waiters_.push_back(&waiter);
  pressured_.store(true, std::memory_order_release);
  acquire_wait_num_ << 1;

  if (notify_first_waiter) {
    lock.unlock();
    NotifyPressure();
    lock.lock();
  }

  waiter.cv.wait(lock,
                 [&] { return waiter.result != Waiter::Result::kWaiting; });
  if (waiter.result == Waiter::Result::kStopped) {
    acquire_stop_num_ << 1;
    return Status::Stop("write page pool closed");
  }
  if (waiter.result == Waiter::Result::kBroken) {
    return Status::Internal("write page pool broken");
  }
  CHECK(waiter.result == Waiter::Result::kGranted);
  lock.unlock();
  return MaterializeLease(count, lease);
}

Status WriteMemPool::TryAcquire(size_t count, WritePageLease* lease) {
  CHECK_NOTNULL(lease);
  CHECK(lease->Empty());
  if (count == 0) return Status::OK();
  if (count > capacity_pages_) {
    return Status::NotFit("write request exceeds page pool capacity");
  }

  switch (TryReserveFast(count)) {
    case AdmissionResult::kGranted:
      return MaterializeLease(count, lease);
    case AdmissionResult::kClosed:
      return Status::Stop("write page pool closed");
    case AdmissionResult::kBroken:
      return Status::Internal("write page pool broken");
    case AdmissionResult::kUnavailable:
      try_acquire_busy_num_ << 1;
      return Status::NotFit("write page capacity temporarily unavailable");
  }
  LOG(FATAL) << "unreachable admission result";
  return Status::Internal("unreachable admission result");
}

bool WriteMemPool::GrantWaitersLocked() {
  while (!waiters_.empty()) {
    Waiter* waiter = waiters_.front();
    if (!ReserveContendedLocked(waiter->need)) break;
    waiters_.pop_front();
    waiter->result = Waiter::Result::kGranted;
    waiter->cv.notify_one();
  }
  const bool still_pressured = !waiters_.empty();
  pressured_.store(still_pressured, std::memory_order_release);
  if (!still_pressured) {
    ClearContendedLocked();
  }
  return still_pressured;
}

void WriteMemPool::Release(char* const* pages, size_t count) {
  if (count == 0) return;

  // Physical pages become reusable before admission capacity is published.
  page_pool_->ReleaseBatch(pages, count);
  const uint64_t old =
      admission_state_.fetch_add(count, std::memory_order_acq_rel);
  CHECK_LE(AvailablePages(old) + count, capacity_pages_);

  if ((old & kContended) == 0) {
    return;
  }

  bool still_pressured = false;
  {
    std::lock_guard<std::mutex> lock(waiter_mutex_);
    still_pressured = GrantWaitersLocked();
  }
  if (still_pressured) {
    NotifyPressure();
  }
}

void WriteMemPool::BreakPool(size_t reserved_count) {
  std::lock_guard<std::mutex> lock(waiter_mutex_);
  uint64_t state = admission_state_.load(std::memory_order_acquire);
  for (;;) {
    CHECK_LE(AvailablePages(state) + reserved_count, capacity_pages_);
    const uint64_t desired = (state + reserved_count) | kBroken | kContended;
    if (admission_state_.compare_exchange_weak(state, desired,
                                               std::memory_order_acq_rel,
                                               std::memory_order_acquire)) {
      break;
    }
  }
  pressured_.store(false, std::memory_order_release);
  for (Waiter* waiter : waiters_) {
    waiter->result = Waiter::Result::kBroken;
    waiter->cv.notify_one();
  }
  waiters_.clear();
  ClearContendedLocked();
}

void WriteMemPool::Close() {
  std::lock_guard<std::mutex> lock(waiter_mutex_);
  uint64_t state = admission_state_.load(std::memory_order_acquire);
  for (;;) {
    if ((state & kClosed) != 0) return;
    if (admission_state_.compare_exchange_weak(
            state, state | kClosed | kContended, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      break;
    }
  }
  pressured_.store(false, std::memory_order_release);
  for (Waiter* waiter : waiters_) {
    waiter->result = Waiter::Result::kStopped;
    waiter->cv.notify_one();
  }
  waiters_.clear();
  ClearContendedLocked();
}

void WriteMemPool::SetPressureObserver(WritePressureObserver* observer) {
  std::unique_lock<std::mutex> lock(pressure_observer_mutex_);
  if (observer != nullptr) {
    CHECK_EQ(pressure_observer_, nullptr);
    CHECK_EQ(pressure_observer_calls_, 0);
    pressure_observer_ = observer;
    return;
  }

  pressure_observer_ = nullptr;
  pressure_observer_cv_.wait(lock,
                             [this] { return pressure_observer_calls_ == 0; });
}

void WriteMemPool::NotifyPressure() {
  WritePressureObserver* observer = nullptr;
  {
    std::lock_guard<std::mutex> lock(pressure_observer_mutex_);
    observer = pressure_observer_;
    if (observer == nullptr) return;
    ++pressure_observer_calls_;
  }

  observer->OnWritePressure();

  {
    std::lock_guard<std::mutex> lock(pressure_observer_mutex_);
    CHECK_GT(pressure_observer_calls_, 0);
    --pressure_observer_calls_;
    if (pressure_observer_calls_ == 0) {
      pressure_observer_cv_.notify_all();
    }
  }
}

void WriteMemPool::NotifyDirtyPublished() {
  if (IsPressured()) {
    NotifyPressure();
  }
}

int64_t WriteMemPool::GetPageSize() const { return page_size_; }

int64_t WriteMemPool::GetTotalBytes() const { return total_bytes_; }

int64_t WriteMemPool::GetUsedBytes() const {
  const uint64_t state = admission_state_.load(std::memory_order_acquire);
  return page_size_ *
         static_cast<int64_t>(capacity_pages_ - AvailablePages(state));
}

char* WriteMemPool::BaseAddr() const { return page_pool_->BaseAddr(); }

size_t WriteMemPool::BufferSize() const { return page_pool_->BufferSize(); }

size_t WriteMemPool::BufferCount() const { return page_pool_->BufferCount(); }

size_t WriteMemPool::TotalSize() const { return page_pool_->TotalSize(); }

int64_t WriteMemPool::UsedBytes(void* arg) {
  return static_cast<WriteMemPool*>(arg)->GetUsedBytes();
}

int64_t WriteMemPool::UsedPages(void* arg) {
  auto* pool = static_cast<WriteMemPool*>(arg);
  const uint64_t state = pool->admission_state_.load(std::memory_order_acquire);
  return static_cast<int64_t>(pool->capacity_pages_ - AvailablePages(state));
}

int64_t WriteMemPool::WaiterCount(void* arg) {
  auto* pool = static_cast<WriteMemPool*>(arg);
  std::lock_guard<std::mutex> lock(pool->waiter_mutex_);
  return static_cast<int64_t>(pool->waiters_.size());
}

int64_t WriteMemPool::OldestWaiterUs(void* arg) {
  auto* pool = static_cast<WriteMemPool*>(arg);
  std::lock_guard<std::mutex> lock(pool->waiter_mutex_);
  if (pool->waiters_.empty()) return 0;
  return std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::steady_clock::now() -
             pool->waiters_.front()->queued_at)
      .count();
}

}  // namespace dingofs
