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

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <limits>
#include <mutex>
#include <thread>
#include <vector>

#include "common/writemempool/cpu_local.h"
#include "common/writemempool/write_page_pool.h"

namespace dingofs {

class WritePagePoolTestPeer {
 public:
  static size_t RequireBatchFromCache(WritePagePool* pool, char** pages,
                                      size_t count, uint32_t cache_index) {
    return pool->RequireBatchFromCache(pages, count, cache_index);
  }

  static size_t FreePages(const WritePagePool* pool) {
    return pool->TEST_FreePages();
  }

  static uint32_t OwnerShard(const WritePagePool* pool, const char* page) {
    return pool->OwnerShard(page);
  }

  static constexpr uint32_t NumShards() { return WritePagePool::kNumShards; }
};

namespace {

TEST(WritePagePoolDeathTest, RejectsArenaAlignmentOverflow) {
  constexpr size_t kNearSizeMax =
      std::numeric_limits<size_t>::max() & ~size_t{3};
  EXPECT_DEATH({ WritePagePool::Create(kNearSizeMax, 1); },
               "arena size overflows huge-page alignment");
}

TEST(WritePagePoolTest, SideCarAllowsUnalignedPageSize) {
  constexpr size_t kPageSize = 5;
  constexpr size_t kPageCount = 16;
  auto pool = WritePagePool::Create(kPageSize, kPageCount);
  ASSERT_NE(pool, nullptr);

  std::array<char*, kPageCount> pages{};
  ASSERT_EQ(pool->RequireBatch(pages.data(), pages.size()), pages.size());
  for (size_t i = 0; i < pages.size(); ++i) {
    EXPECT_EQ((pages[i] - pool->BaseAddr()) % kPageSize, 0);
  }
  pool->ReleaseBatch(pages.data(), pages.size());
  EXPECT_EQ(WritePagePoolTestPeer::FreePages(pool.get()), kPageCount);
}

TEST(CpuLocalTest, CacheCountAndCurrentIndexAreValid) {
  const uint32_t count = CpuLocalCacheCount();
  EXPECT_GE(count, 8);
  EXPECT_EQ(count & (count - 1), 0);
  EXPECT_LT(CurrentCpuLocalCache(count), count);
}

TEST(WritePagePoolTest, ExposesCompleteRdmaArenaGeometry) {
  constexpr size_t kPageSize = 64 * 1024;
  constexpr size_t kPageCount = 4096;
  auto pool = WritePagePool::Create(kPageSize, kPageCount);
  ASSERT_NE(pool, nullptr);

  EXPECT_NE(pool->BaseAddr(), nullptr);
  EXPECT_EQ(pool->BufferSize(), kPageSize);
  EXPECT_EQ(pool->BufferCount(), kPageCount);
  EXPECT_EQ(pool->TotalSize(), kPageSize * kPageCount);
}

TEST(WritePagePoolTest, RefillServesTwoMaxWritesFromOneOwner) {
  auto pool = WritePagePool::Create(64, 4096);
  ASSERT_NE(pool, nullptr);

  std::array<char*, 16> first{};
  std::array<char*, 16> second{};
  ASSERT_EQ(pool->RequireBatch(first.data(), first.size()), first.size());
  ASSERT_EQ(pool->RequireBatch(second.data(), second.size()), second.size());

  const uint32_t owner =
      WritePagePoolTestPeer::OwnerShard(pool.get(), first[0]);
  for (size_t i = 0; i < first.size(); ++i) {
    EXPECT_EQ(WritePagePoolTestPeer::OwnerShard(pool.get(), first[i]), owner);
    if (i != 0) EXPECT_EQ(first[i - 1] - first[i], 64);
  }
  for (size_t i = 0; i < second.size(); ++i) {
    EXPECT_EQ(WritePagePoolTestPeer::OwnerShard(pool.get(), second[i]), owner);
    if (i != 0) EXPECT_EQ(second[i - 1] - second[i], 64);
  }
  EXPECT_EQ(second[0] - first.back(), -64);

  pool->ReleaseBatch(first.data(), first.size());
  pool->ReleaseBatch(second.data(), second.size());
  EXPECT_EQ(WritePagePoolTestPeer::FreePages(pool.get()), 4096);
}

TEST(WritePagePoolTest, MixedOwnerBatchReturnsToOwnerShards) {
  constexpr size_t kPageCount = 4096;
  auto pool = WritePagePool::Create(64, kPageCount);
  ASSERT_NE(pool, nullptr);

  std::vector<char*> pages(kPageCount);
  ASSERT_EQ(pool->RequireBatch(pages.data(), pages.size()), pages.size());
  EXPECT_EQ(WritePagePoolTestPeer::FreePages(pool.get()), 0);

  pool->ReleaseBatch(pages.data(), pages.size());
  EXPECT_EQ(WritePagePoolTestPeer::FreePages(pool.get()), kPageCount);

  std::vector<char*> reacquired(kPageCount);
  ASSERT_EQ(pool->RequireBatch(reacquired.data(), reacquired.size()),
            reacquired.size());
  std::vector<size_t> owners(WritePagePoolTestPeer::NumShards());
  for (char* page : reacquired) {
    ++owners[WritePagePoolTestPeer::OwnerShard(pool.get(), page)];
  }
  for (size_t count : owners) {
    EXPECT_EQ(count, kPageCount / WritePagePoolTestPeer::NumShards());
  }
}

TEST(WritePagePoolTest, ReclaimsPagesStrandedInIdleCache) {
  constexpr size_t kPageCount = 4096;
  auto pool = WritePagePool::Create(64, kPageCount);
  ASSERT_NE(pool, nullptr);

  char* worker_page = nullptr;
  ASSERT_EQ(WritePagePoolTestPeer::RequireBatchFromCache(pool.get(),
                                                         &worker_page, 1, 0),
            1);
  ASSERT_NE(worker_page, nullptr);

  std::vector<char*> pages(kPageCount - 1);
  ASSERT_EQ(WritePagePoolTestPeer::RequireBatchFromCache(
                pool.get(), pages.data(), pages.size(), 1),
            pages.size());
  EXPECT_EQ(WritePagePoolTestPeer::FreePages(pool.get()), 0);

  pool->ReleaseBatch(pages.data(), pages.size());
  pool->ReleaseBatch(&worker_page, 1);
  EXPECT_EQ(WritePagePoolTestPeer::FreePages(pool.get()), kPageCount);
}

TEST(WritePagePoolTest, ConcurrentBatchAllocationIsUnique) {
  constexpr size_t kPageSize = 64;
  constexpr size_t kPageCount = 4096;
  constexpr size_t kBatchSize = 16;
  constexpr int kThreads = 8;
  constexpr int kIterations = 1000;
  auto pool = WritePagePool::Create(kPageSize, kPageCount);
  ASSERT_NE(pool, nullptr);

  std::vector<bool> held(kPageCount, false);
  std::mutex held_mutex;
  std::atomic<bool> duplicate{false};
  std::atomic<bool> start{false};
  std::vector<std::thread> workers;
  workers.reserve(kThreads);
  for (int thread = 0; thread < kThreads; ++thread) {
    workers.emplace_back([&]() {
      std::array<char*, kBatchSize> pages{};
      while (!start.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      for (int iteration = 0; iteration < kIterations; ++iteration) {
        const size_t count = pool->RequireBatch(pages.data(), pages.size());
        if (count != pages.size()) {
          duplicate.store(true, std::memory_order_relaxed);
        }
        std::array<char*, kBatchSize> owned_pages{};
        size_t owned_count = 0;
        {
          std::lock_guard<std::mutex> lock(held_mutex);
          for (size_t i = 0; i < count; ++i) {
            char* page = pages[i];
            const size_t index =
                static_cast<size_t>(page - pool->BaseAddr()) / kPageSize;
            if (held[index]) {
              duplicate.store(true, std::memory_order_relaxed);
            } else {
              held[index] = true;
              owned_pages[owned_count++] = page;
            }
          }
        }
        {
          // Keep ownership bookkeeping serialized through publication back to
          // the pool. A page cannot be legitimately reacquired before
          // ReleaseBatch publishes it.
          std::lock_guard<std::mutex> lock(held_mutex);
          for (size_t i = 0; i < owned_count; ++i) {
            char* page = owned_pages[i];
            const size_t index =
                static_cast<size_t>(page - pool->BaseAddr()) / kPageSize;
            if (!held[index]) {
              duplicate.store(true, std::memory_order_relaxed);
            }
            held[index] = false;
          }
          pool->ReleaseBatch(owned_pages.data(), owned_count);
        }
      }
    });
  }
  start.store(true, std::memory_order_release);
  for (auto& worker : workers) worker.join();

  EXPECT_FALSE(duplicate.load(std::memory_order_relaxed));
  EXPECT_EQ(WritePagePoolTestPeer::FreePages(pool.get()), kPageCount);
  std::vector<char*> all_pages(kPageCount);
  EXPECT_EQ(pool->RequireBatch(all_pages.data(), all_pages.size()), kPageCount);
  pool->ReleaseBatch(all_pages.data(), all_pages.size());
  EXPECT_EQ(WritePagePoolTestPeer::FreePages(pool.get()), kPageCount);
}

TEST(WritePagePoolTest, ConcurrentBatchReleasePreservesAllPages) {
  constexpr size_t kPageSize = 64;
  constexpr size_t kPageCount = 4096;
  constexpr size_t kThreads = 8;
  auto pool = WritePagePool::Create(kPageSize, kPageCount);
  ASSERT_NE(pool, nullptr);

  std::vector<char*> pages(kPageCount);
  ASSERT_EQ(pool->RequireBatch(pages.data(), pages.size()), pages.size());
  ASSERT_EQ(WritePagePoolTestPeer::FreePages(pool.get()), 0);

  // Round-robin distribution gives every releaser pages from many owner
  // shards, so the threads contend on the same shard mutexes.
  std::array<std::vector<char*>, kThreads> batches;
  for (size_t i = 0; i < pages.size(); ++i) {
    batches[i % kThreads].push_back(pages[i]);
  }

  std::atomic<size_t> ready{0};
  std::atomic<bool> start{false};
  std::vector<std::thread> workers;
  workers.reserve(kThreads);
  for (size_t i = 0; i < kThreads; ++i) {
    workers.emplace_back([&, i]() {
      ready.fetch_add(1, std::memory_order_release);
      while (!start.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      pool->ReleaseBatch(batches[i].data(), batches[i].size());
    });
  }
  while (ready.load(std::memory_order_acquire) != kThreads) {
    std::this_thread::yield();
  }
  start.store(true, std::memory_order_release);
  for (auto& worker : workers) worker.join();

  EXPECT_EQ(WritePagePoolTestPeer::FreePages(pool.get()), kPageCount);

  std::vector<char*> reacquired(kPageCount);
  ASSERT_EQ(pool->RequireBatch(reacquired.data(), reacquired.size()),
            reacquired.size());
  std::vector<bool> seen(kPageCount, false);
  for (char* page : reacquired) {
    const size_t index =
        static_cast<size_t>(page - pool->BaseAddr()) / kPageSize;
    ASSERT_LT(index, kPageCount);
    EXPECT_FALSE(seen[index]);
    seen[index] = true;
  }
  pool->ReleaseBatch(reacquired.data(), reacquired.size());
  EXPECT_EQ(WritePagePoolTestPeer::FreePages(pool.get()), kPageCount);
}

TEST(WritePagePoolTest, CrossThreadAllocateAndReleasePreservesInventory) {
  constexpr size_t kPageSize = 64;
  constexpr size_t kPageCount = 4096;
  constexpr size_t kPairs = 2;
  constexpr size_t kMaxBatch = 64;
  constexpr int kIterations = 2000;
  constexpr std::array<size_t, 3> kBatchSizes = {1, 16, 64};

  struct alignas(64) TransferSlot {
    // 0: producer owns the slot; 1: releaser owns it.
    std::atomic<uint32_t> state{0};
    size_t count{0};
    std::array<char*, kMaxBatch> pages{};
  };

  auto pool = WritePagePool::Create(kPageSize, kPageCount);
  ASSERT_NE(pool, nullptr);
  std::array<TransferSlot, kPairs> slots;
  std::atomic<bool> short_allocation{false};
  std::vector<std::thread> workers;
  workers.reserve(kPairs * 2);

  for (size_t pair = 0; pair < kPairs; ++pair) {
    workers.emplace_back([&, pair]() {
      TransferSlot& slot = slots[pair];
      for (int iteration = 0; iteration < kIterations; ++iteration) {
        while (slot.state.load(std::memory_order_acquire) != 0) {
          std::this_thread::yield();
        }
        const size_t requested =
            kBatchSizes[(iteration + pair) % kBatchSizes.size()];
        slot.count = pool->RequireBatch(slot.pages.data(), requested);
        if (slot.count != requested) {
          short_allocation.store(true, std::memory_order_relaxed);
        }
        slot.state.store(1, std::memory_order_release);
      }
    });
    workers.emplace_back([&, pair]() {
      TransferSlot& slot = slots[pair];
      for (int iteration = 0; iteration < kIterations; ++iteration) {
        while (slot.state.load(std::memory_order_acquire) != 1) {
          std::this_thread::yield();
        }
        pool->ReleaseBatch(slot.pages.data(), slot.count);
        slot.state.store(0, std::memory_order_release);
      }
    });
  }
  for (auto& worker : workers) worker.join();

  EXPECT_FALSE(short_allocation.load(std::memory_order_relaxed));
  EXPECT_EQ(WritePagePoolTestPeer::FreePages(pool.get()), kPageCount);
}

}  // namespace
}  // namespace dingofs
