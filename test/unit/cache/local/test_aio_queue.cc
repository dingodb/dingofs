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

#include "cache/local/aio_queue.h"

#include <fcntl.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "cache/local/aio.h"

namespace dingofs {
namespace cache {

class AioQueueTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = "/tmp/dingofs_test_aio_queue_" + std::to_string(getpid());
    std::filesystem::create_directories(test_dir_);
  }
  void TearDown() override { std::filesystem::remove_all(test_dir_); }

  std::string CreateFile(const std::string& name, size_t size) {
    std::string path = test_dir_ + "/" + name;
    int fd = open(path.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    EXPECT_GE(fd, 0);
    if (size > 0) {
      EXPECT_EQ(ftruncate(fd, size), 0);
    }
    close(fd);
    return path;
  }

  int OpenDirect(const std::string& path, int flags) {
    int fd = open(path.c_str(), flags | O_DIRECT);
    if (fd < 0 && errno == EINVAL) {  // e.g. tmpfs has no O_DIRECT
      fd = open(path.c_str(), flags);
    }
    return fd;
  }

  static char* AlignedBuffer(size_t size, char fill) {
    char* buf = static_cast<char*>(aligned_alloc(4096, size));
    std::memset(buf, fill, size);
    return buf;
  }

  std::string test_dir_;
};

TEST_F(AioQueueTest, StartAndShutdownIdempotent) {
  AioQueue queue({});
  EXPECT_TRUE(queue.Start().ok());
  EXPECT_TRUE(queue.Start().ok());  // already started
  EXPECT_TRUE(queue.Shutdown().ok());
  EXPECT_TRUE(queue.Shutdown().ok());  // already shutdown
}

TEST_F(AioQueueTest, SubmitWriteThenRead) {
  AioQueue queue({});
  ASSERT_TRUE(queue.Start().ok());

  std::string path = CreateFile("rw.dat", 4096);
  int fd = OpenDirect(path, O_RDWR);
  ASSERT_GE(fd, 0);

  char* write_buf = AlignedBuffer(4096, 'A');
  {
    Aio aio(fd, 0, 4096, write_buf, -1, false);
    queue.Submit(&aio);
    aio.Wait();
    EXPECT_TRUE(aio.Result().status.ok());
  }

  char* read_buf = AlignedBuffer(4096, 0);
  {
    Aio aio(fd, 0, 4096, read_buf, -1, true);
    queue.Submit(&aio);
    aio.Wait();
    EXPECT_TRUE(aio.Result().status.ok());
    EXPECT_EQ(std::memcmp(read_buf, write_buf, 4096), 0);
  }

  free(write_buf);
  free(read_buf);
  close(fd);
  EXPECT_TRUE(queue.Shutdown().ok());
}

TEST_F(AioQueueTest, SubmitManyExercisesBatching) {
  // More than kSubmitBatchSize (16) concurrent writes to exercise the batch
  // submit path inside PrepareIO.
  constexpr int kNumOps = 40;
  constexpr size_t kBlockSize = 4096;

  AioQueue queue({});
  ASSERT_TRUE(queue.Start().ok());

  std::string path = CreateFile("batch.dat", kNumOps * kBlockSize);
  int fd = OpenDirect(path, O_RDWR);
  ASSERT_GE(fd, 0);

  std::vector<char*> buffers;
  std::vector<std::unique_ptr<Aio>> aios;
  for (int i = 0; i < kNumOps; ++i) {
    buffers.push_back(AlignedBuffer(kBlockSize, static_cast<char>('A' + i % 26)));
    aios.push_back(std::make_unique<Aio>(fd, i * kBlockSize, kBlockSize,
                                         buffers[i], -1, false));
  }
  for (auto& aio : aios) {
    queue.Submit(aio.get());
  }
  for (auto& aio : aios) {
    aio->Wait();
    EXPECT_TRUE(aio->Result().status.ok());
  }

  for (auto* b : buffers) {
    free(b);
  }
  close(fd);
  EXPECT_TRUE(queue.Shutdown().ok());
}

TEST_F(AioQueueTest, SubmitFailsOnBadFd) {
  AioQueue queue({});
  ASSERT_TRUE(queue.Start().ok());

  char* buf = AlignedBuffer(4096, 'X');
  Aio aio(9999, 0, 4096, buf, -1, true);  // invalid fd
  queue.Submit(&aio);
  aio.Wait();
  EXPECT_FALSE(aio.Result().status.ok());

  free(buf);
  EXPECT_TRUE(queue.Shutdown().ok());
}

// Performance: time writing and reading a 4MB file through the AioQueue in
// 4KB blocks. Reports throughput so regressions in the IO path are visible.
TEST_F(AioQueueTest, PerfReadWrite4MB) {
  constexpr size_t kFileSize = 4 * 1024 * 1024;
  constexpr size_t kBlockSize = 4096;
  constexpr int kNumBlocks = kFileSize / kBlockSize;

  AioQueue queue({});
  ASSERT_TRUE(queue.Start().ok());

  std::string path = CreateFile("perf_4mb.dat", kFileSize);
  int fd = OpenDirect(path, O_RDWR);
  ASSERT_GE(fd, 0);

  std::vector<char*> buffers;
  buffers.reserve(kNumBlocks);
  for (int i = 0; i < kNumBlocks; ++i) {
    buffers.push_back(AlignedBuffer(kBlockSize, static_cast<char>(i % 256)));
  }

  auto run = [&](bool for_read) {
    std::vector<std::unique_ptr<Aio>> aios;
    aios.reserve(kNumBlocks);
    for (int i = 0; i < kNumBlocks; ++i) {
      aios.push_back(std::make_unique<Aio>(fd, i * kBlockSize, kBlockSize,
                                           buffers[i], -1, for_read));
    }
    auto start = std::chrono::steady_clock::now();
    for (auto& aio : aios) {
      queue.Submit(aio.get());
    }
    for (auto& aio : aios) {
      aio->Wait();
      ASSERT_TRUE(aio->Result().status.ok());
    }
    auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                  std::chrono::steady_clock::now() - start)
                  .count();
    double mbps = (kFileSize / (1024.0 * 1024.0)) / (us / 1e6);
    std::cout << "[ PERF     ] AioQueue " << (for_read ? "read " : "write")
              << " 4MB in " << us << " us (" << mbps << " MB/s)\n";
  };

  run(false);  // write
  run(true);   // read

  for (auto* b : buffers) {
    free(b);
  }
  close(fd);
  EXPECT_TRUE(queue.Shutdown().ok());
}

}  // namespace cache
}  // namespace dingofs
