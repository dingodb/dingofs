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
 * Created Date: 2026-02-02
 * Author: Wine93
 */
#include <gtest/gtest.h>

#include <thread>
#include <vector>

#include "cache/iutil/buffer_pool.h"

namespace dingofs {
namespace cache {

class BufferPoolTest : public ::testing::Test {
 protected:
  static constexpr size_t kBufferSize = 4096;
  static constexpr size_t kBufferCount = 4;
  static constexpr size_t kAlignment = 4096;
};

TEST_F(BufferPoolTest, BasicAllocFree) {
  BufferPool pool(kBufferSize, kBufferCount, kAlignment);

  char* buf1 = pool.Alloc();
  ASSERT_NE(buf1, nullptr);
  EXPECT_EQ(reinterpret_cast<uintptr_t>(buf1) % kAlignment, 0);

  char* buf2 = pool.Alloc();
  ASSERT_NE(buf2, nullptr);
  EXPECT_NE(buf1, buf2);

  pool.Free(buf1);
  pool.Free(buf2);
}

TEST_F(BufferPoolTest, AllocAllBuffers) {
  BufferPool pool(kBufferSize, kBufferCount, kAlignment);

  std::vector<char*> buffers;
  for (size_t i = 0; i < kBufferCount; ++i) {
    char* buf = pool.Alloc();
    ASSERT_NE(buf, nullptr);
    buffers.push_back(buf);
  }

  // All buffers should be different
  for (size_t i = 0; i < buffers.size(); ++i) {
    for (size_t j = i + 1; j < buffers.size(); ++j) {
      EXPECT_NE(buffers[i], buffers[j]);
    }
  }

  // Free all
  for (auto* buf : buffers) {
    pool.Free(buf);
  }
}

TEST_F(BufferPoolTest, ReuseAfterFree) {
  BufferPool pool(kBufferSize, kBufferCount, kAlignment);

  char* buf1 = pool.Alloc();
  pool.Free(buf1);

  char* buf2 = pool.Alloc();
  // Buffer may or may not be reused, just verify it's valid
  EXPECT_NE(buf2, nullptr);

  pool.Free(buf2);
}

TEST_F(BufferPoolTest, Index) {
  BufferPool pool(kBufferSize, kBufferCount, kAlignment);

  std::vector<char*> buffers;
  for (size_t i = 0; i < kBufferCount; ++i) {
    buffers.push_back(pool.Alloc());
  }

  for (size_t i = 0; i < kBufferCount; ++i) {
    EXPECT_GE(pool.Index(buffers[i]), 0);
    EXPECT_LT(pool.Index(buffers[i]), static_cast<int>(kBufferCount));
  }

  for (auto* buf : buffers) {
    pool.Free(buf);
  }
}

TEST_F(BufferPoolTest, Fetch) {
  BufferPool pool(kBufferSize, kBufferCount, kAlignment);

  auto iovecs = pool.Fetch();
  EXPECT_EQ(iovecs.size(), kBufferCount);

  for (const auto& iov : iovecs) {
    EXPECT_NE(iov.iov_base, nullptr);
    EXPECT_EQ(iov.iov_len, kBufferSize);
    EXPECT_EQ(reinterpret_cast<uintptr_t>(iov.iov_base) % kAlignment, 0);
  }
}

TEST_F(BufferPoolTest, WriteReadBuffer) {
  BufferPool pool(kBufferSize, kBufferCount, kAlignment);

  char* buf = pool.Alloc();
  ASSERT_NE(buf, nullptr);

  // Write data to buffer
  const char* test_data = "Hello, BufferPool!";
  size_t len = strlen(test_data);
  memcpy(buf, test_data, len + 1);

  // Read data back
  EXPECT_STREQ(buf, test_data);

  pool.Free(buf);
}

TEST_F(BufferPoolTest, ConcurrentAllocFree) {
  BufferPool pool(kBufferSize, 16, kAlignment);
  std::atomic<int> alloc_count{0};

  auto worker = [&pool, &alloc_count]() {
    for (int i = 0; i < 100; ++i) {
      char* buf = pool.Alloc();
      ASSERT_NE(buf, nullptr);
      alloc_count++;
      // Simulate some work
      memset(buf, 'x', 100);
      pool.Free(buf);
    }
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < 4; ++i) {
    threads.emplace_back(worker);
  }

  for (auto& t : threads) {
    t.join();
  }

  EXPECT_EQ(alloc_count.load(), 400);
}

TEST_F(BufferPoolTest, DifferentAlignment) {
  {
    BufferPool pool(4096, 2, 512);
    char* buf = pool.Alloc();
    EXPECT_EQ(reinterpret_cast<uintptr_t>(buf) % 512, 0);
    pool.Free(buf);
  }
  {
    BufferPool pool(8192, 2, 4096);
    char* buf = pool.Alloc();
    EXPECT_EQ(reinterpret_cast<uintptr_t>(buf) % 4096, 0);
    pool.Free(buf);
  }
}

TEST_F(BufferPoolTest, LargeBufferSize) {
  constexpr size_t large_size = 1024 * 1024;  // 1MB
  BufferPool pool(large_size, 2, 4096);

  char* buf = pool.Alloc();
  ASSERT_NE(buf, nullptr);

  // Write to end of buffer to ensure it's fully allocated
  buf[large_size - 1] = 'x';
  EXPECT_EQ(buf[large_size - 1], 'x');

  pool.Free(buf);
}

}  // namespace cache
}  // namespace dingofs
