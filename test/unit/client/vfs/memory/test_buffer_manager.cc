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

#include <thread>
#include <vector>

#include "client/vfs/memory/write_buffer_manager.h"

namespace dingofs {
namespace client {
namespace vfs {

// ─── WriteBufferManager ──────────────────────────────────────────────────────

TEST(WriteBufferManagerTest, Allocate_ReturnsNonNull_IncreasesUsed) {
  constexpr int64_t kPageSize = 4096;
  WriteBufferManager mgr(kPageSize * 4, kPageSize);

  char* page = mgr.Allocate();
  ASSERT_NE(page, nullptr);
  EXPECT_EQ(mgr.GetUsedBytes(), kPageSize);

  mgr.DeAllocate(page);
}

TEST(WriteBufferManagerTest, DeAllocate_DecreasesUsed) {
  constexpr int64_t kPageSize = 4096;
  WriteBufferManager mgr(kPageSize * 4, kPageSize);

  char* page = mgr.Allocate();
  ASSERT_NE(page, nullptr);
  EXPECT_EQ(mgr.GetUsedBytes(), kPageSize);

  mgr.DeAllocate(page);
  EXPECT_EQ(mgr.GetUsedBytes(), 0);
}

TEST(WriteBufferManagerTest, GetPageSize_GetTotalBytes) {
  constexpr int64_t kPageSize = 8192;
  constexpr int64_t kTotal = kPageSize * 8;
  WriteBufferManager mgr(kTotal, kPageSize);

  EXPECT_EQ(mgr.GetPageSize(), kPageSize);
  EXPECT_EQ(mgr.GetTotalBytes(), kTotal);
}

TEST(WriteBufferManagerTest, IsHighPressure_True_WhenAboveThreshold) {
  constexpr int64_t kPageSize = 1024;
  // 2 pages total; allocate 2 to hit 100% usage
  WriteBufferManager mgr(kPageSize * 2, kPageSize);

  char* p1 = mgr.Allocate();
  char* p2 = mgr.Allocate();
  ASSERT_NE(p1, nullptr);
  ASSERT_NE(p2, nullptr);

  EXPECT_TRUE(mgr.IsHighPressure());

  mgr.DeAllocate(p1);
  mgr.DeAllocate(p2);
}

TEST(WriteBufferManagerTest, Concurrent_AllocAndDealloc_FinalZero) {
  constexpr int kThreads = 4;
  constexpr int kIters = 200;
  constexpr int64_t kPageSize = 4096;

  WriteBufferManager mgr(static_cast<int64_t>(kThreads) * kIters * kPageSize,
                         kPageSize);

  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&mgr]() {
      for (int i = 0; i < kIters; ++i) {
        char* page = mgr.Allocate();
        mgr.DeAllocate(page);
      }
    });
  }
  for (auto& th : threads) {
    th.join();
  }

  EXPECT_EQ(mgr.GetUsedBytes(), 0);
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
