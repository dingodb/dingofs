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

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <chrono>
#include <thread>

namespace dingofs {

DEFINE_int64(vfs_write_pool_acquire_timeout_ms, 5000,
             "Bounded-acquire deadline (ms) for one write-page Allocate(): "
             "loop MemoryPool::Require() until this elapses before returning "
             "nullptr. A single Require()==nullptr is transient contention, "
             "not exhaustion; only a full window with no free buffer is.");

namespace {
// Bounded-acquire backoff: spin-yield this many times before each short sleep.
constexpr uint32_t kAcquireSpinYields = 64;
constexpr uint32_t kAcquireBackoffUs = 50;
}  // namespace

WriteMemPool::WriteMemPool(int64_t total_bytes, int64_t page_size)
    : total_bytes_(total_bytes),
      page_size_(page_size),
      pool_(MemoryPool::Create(static_cast<size_t>(page_size),
                               static_cast<size_t>(total_bytes / page_size))),
      write_buffer_total_bytes_("vfs_write_buffer_total_bytes_", total_bytes),
      write_buffer_used_pages_("vfs_write_buffer_used_pages", UsedPages, this),
      write_buffer_used_bytes_("vfs_write_buffer_used_bytes", UsedBytes, this) {
  CHECK(pool_ != nullptr) << "WriteMemPool failed to create MemoryPool: page="
                          << page_size
                          << " count=" << (total_bytes / page_size);
}

char* WriteMemPool::Allocate() {
  // A single Require() is try-semantics: nullptr means this attempt lost a
  // cache/shard race OR the pool is exhausted -- it does NOT prove exhaustion.
  // Bounded-retry until the deadline so transient contention under concurrency
  // doesn't surface as a false failure; only a full window with no free buffer
  // counts as genuine exhaustion (returns nullptr -> caller maps to ENOSPC).
  char* page = pool_->Require();
  if (page == nullptr) {
    const auto deadline =
        std::chrono::steady_clock::now() +
        std::chrono::milliseconds(FLAGS_vfs_write_pool_acquire_timeout_ms);
    uint32_t spins = 0;
    while ((page = pool_->Require()) == nullptr) {
      if (std::chrono::steady_clock::now() >= deadline) {
        return nullptr;  // genuine exhaustion (bounded wait elapsed)
      }
      if (spins < kAcquireSpinYields) {
        std::this_thread::yield();
        ++spins;
      } else {
        std::this_thread::sleep_for(
            std::chrono::microseconds(kAcquireBackoffUs));
      }
    }
  }
  used_pages_.fetch_add(1);
  return page;
}

void WriteMemPool::DeAllocate(char* page) {
  if (page == nullptr) {
    return;
  }
  pool_->Release(page);
  used_pages_.fetch_sub(1);
}

int64_t WriteMemPool::GetPageSize() const { return page_size_; }

int64_t WriteMemPool::GetTotalBytes() const { return total_bytes_; }

int64_t WriteMemPool::GetUsedBytes() const {
  return page_size_ * used_pages_.load(std::memory_order_relaxed);
}

double WriteMemPool::GetUsageRatio() const {
  int64_t total = GetTotalBytes();
  if (total == 0) {
    return 0.0;
  }
  return static_cast<double>(GetUsedBytes()) / total;
}

bool WriteMemPool::IsHighPressure(double threshold) const {
  return GetUsageRatio() >= threshold;
}

}  // namespace dingofs