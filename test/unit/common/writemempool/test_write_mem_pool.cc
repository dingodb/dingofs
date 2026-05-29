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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "common/writemempool/write_mem_pool.h"

namespace dingofs {

// ─── WriteMemPool ──────────────────────────────────────────────────────

TEST(WriteMemPoolTest, DeAllocate_DecreasesUsed) {
  constexpr int64_t kPageSize = 4096;
  WriteMemPool mgr(kPageSize * 4, kPageSize);

  char* page = mgr.TryAllocate();
  ASSERT_NE(page, nullptr);
  EXPECT_EQ(mgr.GetUsedBytes(), kPageSize);

  mgr.DeAllocate(page);
  EXPECT_EQ(mgr.GetUsedBytes(), 0);
}

TEST(WriteMemPoolTest, GetPageSize_GetTotalBytes) {
  constexpr int64_t kPageSize = 8192;
  constexpr int64_t kTotal = kPageSize * 8;
  WriteMemPool mgr(kTotal, kPageSize);

  EXPECT_EQ(mgr.GetPageSize(), kPageSize);
  EXPECT_EQ(mgr.GetTotalBytes(), kTotal);
}

TEST(WriteMemPoolTest, IsHighPressure_True_WhenAboveThreshold) {
  constexpr int64_t kPageSize = 1024;
  // 2 pages total; allocate 2 to hit 100% usage
  WriteMemPool mgr(kPageSize * 2, kPageSize);

  char* p1 = mgr.TryAllocate();
  char* p2 = mgr.TryAllocate();
  ASSERT_NE(p1, nullptr);
  ASSERT_NE(p2, nullptr);

  EXPECT_TRUE(mgr.IsHighPressure());

  mgr.DeAllocate(p1);
  mgr.DeAllocate(p2);
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
        char* page = mgr.TryAllocate();
        mgr.DeAllocate(page);
      }
    });
  }
  for (auto& th : threads) {
    th.join();
  }

  EXPECT_EQ(mgr.GetUsedBytes(), 0);
}

// ─── TryAllocate ───────────────────────────────────────────────────────

TEST(WriteMemPoolTest, TryAllocate_ReturnsNonNull_IncreasesUsed) {
  constexpr int64_t kPageSize = 4096;
  WriteMemPool mgr(kPageSize * 4, kPageSize);

  char* page = mgr.TryAllocate();
  ASSERT_NE(page, nullptr);
  EXPECT_EQ(mgr.GetUsedBytes(), kPageSize);

  mgr.DeAllocate(page);
  EXPECT_EQ(mgr.GetUsedBytes(), 0);
}

TEST(WriteMemPoolTest, TryAllocate_ReturnsNull_WhenExhausted_NoBlock) {
  constexpr int64_t kPageSize = 4096;
  WriteMemPool mgr(kPageSize * 2, kPageSize);  // 2 slots

  char* a = mgr.TryAllocate();
  char* b = mgr.TryAllocate();
  ASSERT_NE(a, nullptr);
  ASSERT_NE(b, nullptr);

  // Pool drained -- TryAllocate returns immediately (no bounded wait) and used
  // stays at capacity (a null result must not bump used_pages_).
  char* c = mgr.TryAllocate();
  EXPECT_EQ(c, nullptr);
  EXPECT_EQ(mgr.GetUsedBytes(), 2 * kPageSize);

  mgr.DeAllocate(a);
  mgr.DeAllocate(b);
}

// ─── Perf: pooled Allocate/DeAllocate vs new char[] baseline ───────────

namespace {

// One op = Allocate + DeAllocate. Pool is sized so the fast path always hits
// (no bounded-acquire, no slow-path metric bumps), so this measures the
// steady-state hot path: Require() + used_pages atomic (+ the untriggered
// metric branch). Returns ops/sec across all threads.
double RunPooledBench(WriteMemPool* wmp, int num_threads, int iters) {
  std::atomic<bool> start{false};
  std::vector<std::thread> ts;
  ts.reserve(num_threads);
  for (int t = 0; t < num_threads; ++t) {
    ts.emplace_back([&] {
      while (!start.load(std::memory_order_acquire)) std::this_thread::yield();
      for (int i = 0; i < iters; ++i) {
        char* p = wmp->TryAllocate();
        if (p != nullptr) wmp->DeAllocate(p);
      }
    });
  }
  auto t0 = std::chrono::steady_clock::now();
  start.store(true, std::memory_order_release);
  for (auto& t : ts) t.join();
  double sec =
      std::chrono::duration<double>(std::chrono::steady_clock::now() - t0)
          .count();
  return static_cast<double>(num_threads) * iters / sec;
}

// Baseline: new char[page]/delete[] -- exactly what WriteMemPool::Allocate did
// before pooling. Touch byte 0 so the alloc isn't optimized away.
double RunMallocBench(int64_t page_size, int num_threads, int iters) {
  std::atomic<bool> start{false};
  std::vector<std::thread> ts;
  ts.reserve(num_threads);
  for (int t = 0; t < num_threads; ++t) {
    ts.emplace_back([&] {
      while (!start.load(std::memory_order_acquire)) std::this_thread::yield();
      for (int i = 0; i < iters; ++i) {
        char* p = new char[page_size];
        p[0] = static_cast<char>(i);
        delete[] p;
      }
    });
  }
  auto t0 = std::chrono::steady_clock::now();
  start.store(true, std::memory_order_release);
  for (auto& t : ts) t.join();
  double sec =
      std::chrono::duration<double>(std::chrono::steady_clock::now() - t0)
          .count();
  return static_cast<double>(num_threads) * iters / sec;
}

}  // namespace

// Prints throughput + ns/op for pooled (incl. metrics埋点) vs new char[] at
// 1/4/16/64 threads. Not asserting numbers -- visibility for the "接池+埋点 vs
// new char[]" comparison. Pool sized large (256 MiB) so the fast path is hit
// and slow-path metric branches never fire.
TEST(WriteMemPoolPerf, PooledVsMalloc) {
  constexpr int64_t kPageSize = 65536;
  constexpr int64_t kTotalBytes = 4096LL * kPageSize;  // 256 MiB, 4096 slots
  constexpr int kIters = 200000;
  WriteMemPool wmp(kTotalBytes, kPageSize);

  for (int n : {1, 4, 16, 64}) {
    double pooled = RunPooledBench(&wmp, n, kIters);
    double mallocd = RunMallocBench(kPageSize, n, kIters);
    LOG(INFO) << "threads=" << n
              << "  pooled=" << static_cast<long long>(pooled) << " ops/s ("
              << (1e9 / pooled * n)
              << " ns/op)  malloc=" << static_cast<long long>(mallocd)
              << " ops/s (" << (1e9 / mallocd * n)
              << " ns/op)  pooled/malloc=" << (pooled / mallocd) << "x";
  }
}

}  // namespace dingofs
