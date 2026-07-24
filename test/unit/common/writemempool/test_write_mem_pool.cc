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

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <thread>
#include <vector>

#include "common/writemempool/write_mem_pool.h"

namespace dingofs {

// ─── WriteMemPool ──────────────────────────────────────────────────────

TEST(WriteMemPoolDeathTest, RejectsZeroPageSizeBeforeDivision) {
  EXPECT_DEATH({ WriteMemPool pool(4096, 0); }, "page_size > 0");
}

TEST(WriteMemPoolDeathTest, RejectsCapacitySmallerThanOnePage) {
  EXPECT_DEATH({ WriteMemPool pool(4096, 8192); }, "total_bytes >= page_size");
}

TEST(WriteMemPoolDeathTest, RejectsNonIntegralPageCapacity) {
  EXPECT_DEATH({ WriteMemPool pool(10 * 1024, 4096); }, "exact multiple");
}

TEST(WriteMemPoolTest, DeAllocateBatch_DecreasesUsed) {
  constexpr int64_t kPageSize = 4096;
  WriteMemPool mgr(kPageSize * 4, kPageSize);

  char* page = nullptr;
  ASSERT_EQ(mgr.TryAllocateBatch(1, &page), 1);
  EXPECT_EQ(mgr.GetUsedBytes(), kPageSize);

  mgr.DeAllocateBatch(&page, 1);
  EXPECT_EQ(mgr.GetUsedBytes(), 0);
}

TEST(WriteMemPoolTest, GetPageSize_GetTotalBytes) {
  constexpr int64_t kPageSize = 8192;
  constexpr int64_t kTotal = kPageSize * 8;
  WriteMemPool mgr(kTotal, kPageSize);

  EXPECT_EQ(mgr.GetPageSize(), kPageSize);
  EXPECT_EQ(mgr.GetTotalBytes(), kTotal);
  EXPECT_NE(mgr.BaseAddr(), nullptr);
  EXPECT_EQ(mgr.BufferSize(), kPageSize);
  EXPECT_EQ(mgr.BufferCount(), 8);
  EXPECT_EQ(mgr.TotalSize(), kTotal);
}

TEST(WriteMemPoolTest, IsHighPressure_True_WhenAboveThreshold) {
  constexpr int64_t kPageSize = 1024;
  // 2 pages total; allocate 2 to hit 100% usage
  WriteMemPool mgr(kPageSize * 2, kPageSize);

  std::array<char*, 2> pages{};
  ASSERT_EQ(mgr.TryAllocateBatch(pages.size(), pages.data()), pages.size());

  EXPECT_TRUE(mgr.IsHighPressure());

  mgr.DeAllocateBatch(pages.data(), pages.size());
}

TEST(WriteMemPoolTest, Concurrent_AllocAndDealloc_FinalZero) {
  constexpr int kThreads = 4;
  constexpr int kIters = 200;
  constexpr int64_t kPageSize = 4096;

  WriteMemPool mgr(static_cast<int64_t>(kThreads) * kIters * kPageSize,
                   kPageSize);

  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&mgr]() {
      for (int i = 0; i < kIters; ++i) {
        char* page = nullptr;
        const size_t count = mgr.TryAllocateBatch(1, &page);
        mgr.DeAllocateBatch(&page, count);
      }
    });
  }
  for (auto& th : threads) {
    th.join();
  }

  EXPECT_EQ(mgr.GetUsedBytes(), 0);
}

// ─── TryAllocateBatch ──────────────────────────────────────────────────

TEST(WriteMemPoolTest, TryAllocateBatch_ReturnsPage_IncreasesUsed) {
  constexpr int64_t kPageSize = 4096;
  WriteMemPool mgr(kPageSize * 4, kPageSize);

  char* page = nullptr;
  ASSERT_EQ(mgr.TryAllocateBatch(1, &page), 1);
  EXPECT_EQ(mgr.GetUsedBytes(), kPageSize);

  mgr.DeAllocateBatch(&page, 1);
  EXPECT_EQ(mgr.GetUsedBytes(), 0);
}

TEST(WriteMemPoolTest, TryAllocateBatch_ReturnsShort_WhenExhausted_NoBlock) {
  constexpr int64_t kPageSize = 4096;
  WriteMemPool mgr(kPageSize * 2, kPageSize);  // 2 slots

  std::array<char*, 2> pages{};
  ASSERT_EQ(mgr.TryAllocateBatch(pages.size(), pages.data()), pages.size());

  // Pool drained -- TryAllocateBatch returns immediately (no bounded wait) and
  // used stays at capacity (a short result must not bump used_pages_).
  char* extra = nullptr;
  EXPECT_EQ(mgr.TryAllocateBatch(1, &extra), 0);
  EXPECT_EQ(mgr.GetUsedBytes(), 2 * kPageSize);

  mgr.DeAllocateBatch(pages.data(), pages.size());
}

TEST(WriteMemPoolTest, EmptyBatchIsNoOp) {
  constexpr int64_t kPageSize = 4096;
  WriteMemPool mgr(kPageSize * 2, kPageSize);

  EXPECT_EQ(mgr.TryAllocateBatch(0, nullptr), 0);
  mgr.DeAllocateBatch(nullptr, 0);
  EXPECT_EQ(mgr.GetUsedBytes(), 0);
}

TEST(WriteMemPoolTest, CrossThreadBatchAccountingReturnsToZero) {
  constexpr int64_t kPageSize = 4096;
  constexpr size_t kMaxBatch = 64;
  constexpr int kIterations = 2000;
  constexpr std::array<size_t, 3> kBatchSizes = {1, 16, 64};
  WriteMemPool mgr(kPageSize * 256, kPageSize);

  struct TransferSlot {
    // 0: producer may allocate; 1: releaser owns the returned prefix.
    std::atomic<uint32_t> state{0};
    size_t count{0};
    std::array<char*, kMaxBatch> pages{};
  } slot;
  std::atomic<bool> short_allocation{false};

  std::thread producer([&]() {
    for (int iteration = 0; iteration < kIterations; ++iteration) {
      while (slot.state.load(std::memory_order_acquire) != 0) {
        std::this_thread::yield();
      }
      const size_t requested =
          kBatchSizes[iteration % kBatchSizes.size()];
      slot.count = mgr.TryAllocateBatch(requested, slot.pages.data());
      if (slot.count != requested) {
        short_allocation.store(true, std::memory_order_relaxed);
      }
      slot.state.store(1, std::memory_order_release);
    }
  });
  std::thread releaser([&]() {
    for (int iteration = 0; iteration < kIterations; ++iteration) {
      while (slot.state.load(std::memory_order_acquire) != 1) {
        std::this_thread::yield();
      }
      mgr.DeAllocateBatch(slot.pages.data(), slot.count);
      slot.state.store(0, std::memory_order_release);
    }
  });

  producer.join();
  releaser.join();
  EXPECT_FALSE(short_allocation.load(std::memory_order_relaxed));
  EXPECT_EQ(mgr.GetUsedBytes(), 0);
}

}  // namespace dingofs
