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

/*
 * Project: DingoFS
 * Created Date: 2026-06-21
 * Author: AI
 */

#include "cache/iutil/buffer_pool.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <set>
#include <thread>

namespace dingofs {
namespace cache {

class BufferPoolTest : public ::testing::Test {};

TEST_F(BufferPoolTest, FetchDescribesAllBuffers) {
  BufferPool pool(4096, 4, 4096);
  auto iovecs = pool.Fetch();
  ASSERT_EQ(iovecs.size(), 4u);
  for (const auto& iov : iovecs) {
    EXPECT_EQ(iov.iov_len, 4096u);
    EXPECT_NE(iov.iov_base, nullptr);
  }
}

TEST_F(BufferPoolTest, AllocReturnsDistinctAlignedBuffers) {
  BufferPool pool(4096, 4, 4096);
  std::set<char*> ptrs;
  for (int i = 0; i < 4; ++i) {
    char* p = pool.Alloc();
    ASSERT_NE(p, nullptr);
    EXPECT_EQ(reinterpret_cast<uintptr_t>(p) % 4096, 0u);
    ptrs.insert(p);
  }
  EXPECT_EQ(ptrs.size(), 4u);
}

TEST_F(BufferPoolTest, IndexMatchesFetchOrder) {
  BufferPool pool(8192, 3, 4096);
  auto iovecs = pool.Fetch();
  for (int i = 0; i < 3; ++i) {
    char* base = static_cast<char*>(iovecs[i].iov_base);
    EXPECT_EQ(pool.Index(base), i);
  }
}

TEST_F(BufferPoolTest, FreeReturnsBufferToPool) {
  BufferPool pool(4096, 2, 4096);
  char* a = pool.Alloc();
  char* b = pool.Alloc();
  ASSERT_NE(a, b);

  pool.Free(a);
  // The freed slot is the only one available, so it is handed back.
  char* c = pool.Alloc();
  EXPECT_EQ(c, a);

  pool.Free(b);
  pool.Free(c);
}

TEST_F(BufferPoolTest, AllocBlocksWhenExhausted) {
  BufferPool pool(4096, 1, 4096);
  char* held = pool.Alloc();

  std::atomic<bool> allocated{false};
  std::thread waiter([&]() {
    char* p = pool.Alloc();  // blocks until `held` is freed
    allocated = true;
    pool.Free(p);
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_FALSE(allocated.load());

  pool.Free(held);
  waiter.join();
  EXPECT_TRUE(allocated.load());
}

}  // namespace cache
}  // namespace dingofs
