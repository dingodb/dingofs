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
 * Created Date: 2026-02-25
 * Author: AI
 */

#include <fcntl.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <unistd.h>

#include <chrono>
#include <cstring>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <random>
#include <sstream>
#include <string>
#include <vector>

#include "cache/blockcache/aio.h"
#include "cache/blockcache/io_uring.h"
#include "cache/helper/test_log_sink.h"

DEFINE_string(test_dir, "/tmp", "Base directory for IOUring test files");
DEFINE_bool(run_perf_test, false, "Run performance benchmark tests");

namespace dingofs {
namespace cache {

class IOUringTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = FLAGS_test_dir + "/io_uring_test_" + std::to_string(getpid());
    std::filesystem::create_directories(test_dir_);
    ctx_ = std::make_shared<Context>();
  }

  void TearDown() override { std::filesystem::remove_all(test_dir_); }

  std::string CreateTestFile(const std::string& name, size_t size) {
    std::string path = test_dir_ + "/" + name;
    int fd = open(path.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    EXPECT_GE(fd, 0);

    if (size > 0) {
      ftruncate(fd, size);
    }
    close(fd);
    return path;
  }

  // Open file with O_DIRECT if supported, otherwise fallback to normal mode
  int OpenFileForDirectIO(const std::string& path, int flags) {
    int fd = open(path.c_str(), flags | O_DIRECT);
    if (fd < 0 && errno == EINVAL) {
      // O_DIRECT not supported (e.g., tmpfs), fallback to normal mode
      fd = open(path.c_str(), flags);
    }
    return fd;
  }

  void FillFileWithPattern(const std::string& path, size_t size,
                           uint8_t pattern) {
    int fd = open(path.c_str(), O_WRONLY);
    EXPECT_GE(fd, 0);

    std::vector<char> buffer(size, pattern);
    write(fd, buffer.data(), size);
    close(fd);
  }

  void FillFileWithRandomData(const std::string& path, size_t size) {
    int fd = open(path.c_str(), O_WRONLY);
    EXPECT_GE(fd, 0);

    std::vector<char> buffer(size);
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 255);

    for (auto& b : buffer) {
      b = static_cast<char>(dis(gen));
    }

    write(fd, buffer.data(), size);
    close(fd);
  }

  bool VerifyFileContent(const std::string& path, size_t offset, size_t length,
                         const char* expected) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) return false;

    std::vector<char> buffer(length);
    lseek(fd, offset, SEEK_SET);
    read(fd, buffer.data(), length);
    close(fd);

    return std::memcmp(buffer.data(), expected, length) == 0;
  }

  std::string test_dir_;
  ContextSPtr ctx_;
};

TEST_F(IOUringTest, StartAndShutdown) {
  test::TestLogSink log_sink;
  log_sink.Register("IOUring already started");
  log_sink.Register("IOUring already shutdown");
  google::AddLogSink(&log_sink);

  {
    IOUringOptions options = {.entries = 1024, .use_sqpoll = false};
    IOUring io_uring(options);

    EXPECT_TRUE(io_uring.Start().ok());
    EXPECT_TRUE(io_uring.Shutdown().ok());
  }

  {
    IOUringOptions options = {.entries = 2048, .use_sqpoll = true};
    IOUring io_uring(options);

    EXPECT_TRUE(io_uring.Start().ok());
    EXPECT_TRUE(io_uring.Shutdown().ok());
  }

  {
    IOUringOptions options = {.entries = 512};
    IOUring io_uring(options);

    EXPECT_TRUE(io_uring.Start().ok());
    EXPECT_EQ(log_sink.Count("IOUring already started"), 0);
    EXPECT_TRUE(io_uring.Start().ok());
    EXPECT_EQ(log_sink.Count("IOUring already started"), 1);

    EXPECT_TRUE(io_uring.Shutdown().ok());
    EXPECT_EQ(log_sink.Count("IOUring already shutdown"), 0);
    EXPECT_TRUE(io_uring.Shutdown().ok());
    EXPECT_EQ(log_sink.Count("IOUring already shutdown"), 1);
  }

  {
    constexpr size_t kBufferSize = 4096;
    char* raw_write_buffer =
        static_cast<char*>(aligned_alloc(4096, kBufferSize));
    char* raw_read_buffer =
        static_cast<char*>(aligned_alloc(4096, kBufferSize));
    std::vector<iovec> write_buffers = {{raw_write_buffer, kBufferSize}};
    std::vector<iovec> read_buffers = {{raw_read_buffer, kBufferSize}};

    IOUringOptions options = {.entries = 1024,
                              .use_sqpoll = false,
                              .fixed_write_buffers = write_buffers,
                              .fixed_read_buffers = read_buffers};

    IOUring io_uring(std::move(options));
    EXPECT_TRUE(io_uring.Start().ok());
    EXPECT_TRUE(io_uring.Shutdown().ok());

    free(raw_write_buffer);
    free(raw_read_buffer);
  }

  google::RemoveLogSink(&log_sink);
}

TEST_F(IOUringTest, FixedBuffers) {
  constexpr size_t kBufferSize = 4096;
  constexpr size_t kNumWriteBuffers = 4;
  constexpr size_t kNumReadBuffers = 3;

  std::vector<char*> raw_write_buffers(kNumWriteBuffers);
  std::vector<char*> raw_read_buffers(kNumReadBuffers);
  std::vector<iovec> write_buffers(kNumWriteBuffers);
  std::vector<iovec> read_buffers(kNumReadBuffers);

  for (size_t i = 0; i < kNumWriteBuffers; ++i) {
    raw_write_buffers[i] = static_cast<char*>(aligned_alloc(4096, kBufferSize));
    write_buffers[i] = {raw_write_buffers[i], kBufferSize};
  }

  for (size_t i = 0; i < kNumReadBuffers; ++i) {
    raw_read_buffers[i] = static_cast<char*>(aligned_alloc(4096, kBufferSize));
    read_buffers[i] = {raw_read_buffers[i], kBufferSize};
  }

  {
    IOUringOptions options = {.entries = 1024,
                              .use_sqpoll = false,
                              .fixed_write_buffers = write_buffers,
                              .fixed_read_buffers = read_buffers};

    IOUring io_uring(std::move(options));
    ASSERT_TRUE(io_uring.Start().ok());

    auto& fixed_buffers = io_uring.fixed_buffers_;
    EXPECT_EQ(fixed_buffers->write_count, kNumWriteBuffers);
    EXPECT_EQ(fixed_buffers->buffers.size(),
              kNumWriteBuffers + kNumReadBuffers);

    EXPECT_EQ(fixed_buffers->GetIndex(false, 0), 0);
    EXPECT_EQ(fixed_buffers->GetIndex(false, 1), 1);
    EXPECT_EQ(fixed_buffers->GetIndex(false, 3), 3);
    EXPECT_EQ(fixed_buffers->GetIndex(false, 4), -1);
    EXPECT_EQ(fixed_buffers->GetIndex(false, -1), -1);

    EXPECT_EQ(fixed_buffers->GetIndex(true, 0), 4);
    EXPECT_EQ(fixed_buffers->GetIndex(true, 1), 5);
    EXPECT_EQ(fixed_buffers->GetIndex(true, 2), 6);
    EXPECT_EQ(fixed_buffers->GetIndex(true, 3), -1);
    EXPECT_EQ(fixed_buffers->GetIndex(true, -1), -1);

    EXPECT_TRUE(io_uring.Shutdown().ok());
  }

  {
    IOUringOptions options = {.entries = 1024,
                              .use_sqpoll = false,
                              .fixed_write_buffers = write_buffers};

    IOUring io_uring(std::move(options));
    ASSERT_TRUE(io_uring.Start().ok());

    auto& fixed_buffers = io_uring.fixed_buffers_;
    EXPECT_EQ(fixed_buffers->write_count, kNumWriteBuffers);
    EXPECT_EQ(fixed_buffers->buffers.size(), kNumWriteBuffers);

    EXPECT_EQ(fixed_buffers->GetIndex(false, 0), 0);
    EXPECT_EQ(fixed_buffers->GetIndex(false, 3), 3);
    EXPECT_EQ(fixed_buffers->GetIndex(false, 4), -1);

    EXPECT_EQ(fixed_buffers->GetIndex(true, 0), -1);

    EXPECT_TRUE(io_uring.Shutdown().ok());
  }

  {
    IOUringOptions options = {.entries = 1024,
                              .use_sqpoll = false,
                              .fixed_read_buffers = read_buffers};

    IOUring io_uring(std::move(options));
    ASSERT_TRUE(io_uring.Start().ok());

    auto& fixed_buffers = io_uring.fixed_buffers_;
    EXPECT_EQ(fixed_buffers->write_count, 0);
    EXPECT_EQ(fixed_buffers->buffers.size(), kNumReadBuffers);

    EXPECT_EQ(fixed_buffers->GetIndex(false, 0), -1);

    EXPECT_EQ(fixed_buffers->GetIndex(true, 0), 0);
    EXPECT_EQ(fixed_buffers->GetIndex(true, 2), 2);
    EXPECT_EQ(fixed_buffers->GetIndex(true, 3), -1);

    EXPECT_TRUE(io_uring.Shutdown().ok());
  }

  {
    IOUringOptions options = {.entries = 1024, .use_sqpoll = false};

    IOUring io_uring(std::move(options));
    ASSERT_TRUE(io_uring.Start().ok());

    auto& fixed_buffers = io_uring.fixed_buffers_;
    EXPECT_EQ(fixed_buffers->write_count, 0);
    EXPECT_EQ(fixed_buffers->buffers.size(), 0);

    EXPECT_EQ(fixed_buffers->GetIndex(false, 0), -1);
    EXPECT_EQ(fixed_buffers->GetIndex(true, 0), -1);

    EXPECT_TRUE(io_uring.Shutdown().ok());
  }

  for (auto* buf : raw_write_buffers) {
    free(buf);
  }
  for (auto* buf : raw_read_buffers) {
    free(buf);
  }
}

TEST_F(IOUringTest, BasicWrite) {
  auto test_write = [this](bool use_sqpoll) {
    // Step 1: Initialize IOUring
    IOUringOptions options = {.entries = 256, .use_sqpoll = use_sqpoll};
    IOUring io_uring(options);
    ASSERT_TRUE(io_uring.Start().ok());

    // Step 2: Create test file and open it
    std::string suffix = use_sqpoll ? "_sqpoll" : "_normal";
    std::string file_path =
        CreateTestFile("write_test" + suffix + ".dat", 4096);
    int fd = OpenFileForDirectIO(file_path, O_RDWR);
    ASSERT_GE(fd, 0);

    // Step 3: Prepare write buffer
    char* buffer = static_cast<char*>(aligned_alloc(4096, 4096));
    std::memset(buffer, 'A', 4096);

    // Step 4: Create and submit write request
    Aio aio(ctx_, fd, 0, 4096, buffer, -1, false);
    EXPECT_TRUE(io_uring.PrepareIO(&aio).ok());
    EXPECT_TRUE(io_uring.SubmitIO().ok());

    // Step 5: Wait for completion
    Aio* completed_aios[16];
    int completed;
    do {
      completed = io_uring.WaitIO(1000, completed_aios);
    } while (completed == 0);

    // Step 6: Verify results
    EXPECT_EQ(completed, 1);
    EXPECT_EQ(completed_aios[0], &aio);
    EXPECT_TRUE(aio.Result().status.ok());
    EXPECT_TRUE(VerifyFileContent(file_path, 0, 4096, buffer));

    // Step 7: Cleanup
    close(fd);
    free(buffer);
    EXPECT_TRUE(io_uring.Shutdown().ok());
  };

  test_write(false);
  test_write(true);
}

TEST_F(IOUringTest, BasicRead) {
  auto test_read = [this](bool use_sqpoll) {
    // Step 1: Initialize IOUring
    IOUringOptions options = {.entries = 256, .use_sqpoll = use_sqpoll};
    IOUring io_uring(options);
    ASSERT_TRUE(io_uring.Start().ok());

    // Step 2: Create test file and fill with pattern
    std::string suffix = use_sqpoll ? "_sqpoll" : "_normal";
    std::string file_path = CreateTestFile("read_test" + suffix + ".dat", 4096);
    FillFileWithPattern(file_path, 4096, 'B');

    // Step 3: Open file and prepare read buffer
    int fd = OpenFileForDirectIO(file_path, O_RDONLY);
    ASSERT_GE(fd, 0);
    char* buffer = static_cast<char*>(aligned_alloc(4096, 4096));
    std::memset(buffer, 0, 4096);

    // Step 4: Create and submit read request
    Aio aio(ctx_, fd, 0, 4096, buffer, -1, true);
    EXPECT_TRUE(io_uring.PrepareIO(&aio).ok());
    EXPECT_TRUE(io_uring.SubmitIO().ok());

    // Step 5: Wait for completion
    Aio* completed_aios[16];
    int completed;
    do {
      completed = io_uring.WaitIO(1000, completed_aios);
    } while (completed == 0);

    // Step 6: Verify results
    EXPECT_EQ(completed, 1);
    EXPECT_EQ(completed_aios[0], &aio);
    EXPECT_TRUE(aio.Result().status.ok());
    for (size_t i = 0; i < 4096; ++i) {
      EXPECT_EQ(buffer[i], 'B') << "Mismatch at position " << i;
    }

    // Step 7: Cleanup
    close(fd);
    free(buffer);
    EXPECT_TRUE(io_uring.Shutdown().ok());
  };

  test_read(false);
  test_read(true);
}

TEST_F(IOUringTest, WriteAndReadWithOffset) {
  IOUringOptions options = {.entries = 256, .use_sqpoll = false};
  IOUring io_uring(options);
  ASSERT_TRUE(io_uring.Start().ok());

  std::string file_path = CreateTestFile("offset_test.dat", 16384);

  int fd = OpenFileForDirectIO(file_path, O_RDWR);
  ASSERT_GE(fd, 0);

  char* write_buffer = static_cast<char*>(aligned_alloc(4096, 4096));
  std::memset(write_buffer, 'C', 4096);

  {
    Aio write_aio(ctx_, fd, 4096, 4096, write_buffer, -1, false);

    EXPECT_TRUE(io_uring.PrepareIO(&write_aio).ok());
    EXPECT_TRUE(io_uring.SubmitIO().ok());

    Aio* completed_aios[16];
    int completed;
    do {
      completed = io_uring.WaitIO(1000, completed_aios);
    } while (completed == 0);

    EXPECT_EQ(completed, 1);
    EXPECT_TRUE(write_aio.Result().status.ok());
  }

  {
    char* read_buffer = static_cast<char*>(aligned_alloc(4096, 4096));
    std::memset(read_buffer, 0, 4096);

    Aio read_aio(ctx_, fd, 4096, 4096, read_buffer, -1, true);

    EXPECT_TRUE(io_uring.PrepareIO(&read_aio).ok());
    EXPECT_TRUE(io_uring.SubmitIO().ok());

    Aio* completed_aios[16];
    int completed;
    do {
      completed = io_uring.WaitIO(1000, completed_aios);
    } while (completed == 0);

    EXPECT_EQ(completed, 1);
    EXPECT_TRUE(read_aio.Result().status.ok());

    EXPECT_EQ(std::memcmp(read_buffer, write_buffer, 4096), 0);

    free(read_buffer);
  }

  close(fd);
  free(write_buffer);
  EXPECT_TRUE(io_uring.Shutdown().ok());
}

TEST_F(IOUringTest, MultipleIOOperations) {
  IOUringOptions options = {.entries = 256, .use_sqpoll = false};
  IOUring io_uring(options);
  ASSERT_TRUE(io_uring.Start().ok());

  constexpr int kNumOps = 8;
  std::string file_path = CreateTestFile("multi_io.dat", kNumOps * 4096);

  // Step 1: Fill each 4096-byte block with a unique pattern (block index)
  std::vector<std::vector<char>> expected_data(kNumOps);
  for (int i = 0; i < kNumOps; ++i) {
    expected_data[i].resize(4096, static_cast<char>('A' + i));
  }
  FillFileWithPattern(file_path, kNumOps * 4096, 0);
  {
    int fd = open(file_path.c_str(), O_WRONLY);
    ASSERT_GE(fd, 0);
    for (int i = 0; i < kNumOps; ++i) {
      lseek(fd, i * 4096, SEEK_SET);
      write(fd, expected_data[i].data(), 4096);
    }
    close(fd);
  }

  // Step 2: Open file and prepare multiple read operations
  int fd = OpenFileForDirectIO(file_path, O_RDONLY);
  ASSERT_GE(fd, 0);

  std::vector<char*> buffers(kNumOps);
  std::vector<std::unique_ptr<Aio>> aios;

  for (int i = 0; i < kNumOps; ++i) {
    buffers[i] = static_cast<char*>(aligned_alloc(4096, 4096));
    std::memset(buffers[i], 0, 4096);

    aios.push_back(
        std::make_unique<Aio>(ctx_, fd, i * 4096, 4096, buffers[i], -1, true));
  }

  // Step 3: Submit all read operations
  for (auto& aio : aios) {
    EXPECT_TRUE(io_uring.PrepareIO(aio.get()).ok());
  }
  EXPECT_TRUE(io_uring.SubmitIO().ok());

  // Step 4: Wait for all operations to complete
  int total_completed = 0;
  Aio* completed_aios[kNumOps];
  while (total_completed < kNumOps) {
    int n = io_uring.WaitIO(1000, completed_aios + total_completed);
    total_completed += n;
  }

  EXPECT_EQ(total_completed, kNumOps);

  // Step 5: Verify all operations succeeded
  for (const auto& aio : aios) {
    EXPECT_TRUE(aio->Result().status.ok());
  }

  // Step 6: Verify read content matches expected data
  for (int i = 0; i < kNumOps; ++i) {
    EXPECT_EQ(std::memcmp(buffers[i], expected_data[i].data(), 4096), 0)
        << "Content mismatch at block " << i;
  }

  // Step 7: Cleanup
  for (auto* buf : buffers) {
    free(buf);
  }

  close(fd);
  EXPECT_TRUE(io_uring.Shutdown().ok());
}

TEST_F(IOUringTest, MixedReadWrite) {
  IOUringOptions options = {.entries = 256, .use_sqpoll = false};
  IOUring io_uring(options);
  ASSERT_TRUE(io_uring.Start().ok());

  constexpr int kNumBlocks = 8;
  constexpr size_t kBlockSize = 4096;
  std::string file_path =
      CreateTestFile("mixed_rw.dat", kNumBlocks * kBlockSize);

  // Step 1: Initialize file with known pattern (each block filled with
  // 'A'+block_index)
  {
    int fd = open(file_path.c_str(), O_WRONLY);
    ASSERT_GE(fd, 0);
    for (int i = 0; i < kNumBlocks; ++i) {
      std::vector<char> block(kBlockSize, static_cast<char>('A' + i));
      lseek(fd, i * kBlockSize, SEEK_SET);
      write(fd, block.data(), kBlockSize);
    }
    close(fd);
  }

  int fd = OpenFileForDirectIO(file_path, O_RDWR);
  ASSERT_GE(fd, 0);

  // Step 2: Prepare mixed read and write operations
  // - Read blocks 0, 2, 4, 6 (even indices)
  // - Write blocks 1, 3, 5, 7 (odd indices) with new pattern 'X'+block_index
  std::vector<char*> read_buffers(kNumBlocks / 2);
  std::vector<char*> write_buffers(kNumBlocks / 2);
  std::vector<std::unique_ptr<Aio>> aios;

  // Prepare read operations for even blocks
  for (int i = 0; i < kNumBlocks / 2; ++i) {
    int block_idx = i * 2;  // 0, 2, 4, 6
    read_buffers[i] = static_cast<char*>(aligned_alloc(4096, kBlockSize));
    std::memset(read_buffers[i], 0, kBlockSize);
    aios.push_back(std::make_unique<Aio>(ctx_, fd, block_idx * kBlockSize,
                                         kBlockSize, read_buffers[i], -1,
                                         true));
  }

  // Prepare write operations for odd blocks
  for (int i = 0; i < kNumBlocks / 2; ++i) {
    int block_idx = (i * 2) + 1;  // 1, 3, 5, 7
    write_buffers[i] = static_cast<char*>(aligned_alloc(4096, kBlockSize));
    std::memset(write_buffers[i], static_cast<char>('X' + block_idx),
                kBlockSize);
    aios.push_back(std::make_unique<Aio>(ctx_, fd, block_idx * kBlockSize,
                                         kBlockSize, write_buffers[i], -1,
                                         false));
  }

  // Step 3: Submit all operations (reads and writes mixed)
  for (auto& aio : aios) {
    EXPECT_TRUE(io_uring.PrepareIO(aio.get()).ok());
  }
  EXPECT_TRUE(io_uring.SubmitIO().ok());

  // Step 4: Wait for all operations to complete
  int total_ops = kNumBlocks;  // 4 reads + 4 writes
  int total_completed = 0;
  std::vector<Aio*> completed_aios(total_ops);
  while (total_completed < total_ops) {
    int n = io_uring.WaitIO(1000, completed_aios.data() + total_completed);
    total_completed += n;
  }
  EXPECT_EQ(total_completed, total_ops);

  // Step 5: Verify all operations succeeded
  for (const auto& aio : aios) {
    EXPECT_TRUE(aio->Result().status.ok());
  }

  // Step 6: Verify read content (even blocks should have original pattern)
  for (int i = 0; i < kNumBlocks / 2; ++i) {
    int block_idx = i * 2;  // 0, 2, 4, 6
    char expected_char = static_cast<char>('A' + block_idx);
    std::vector<char> expected(kBlockSize, expected_char);
    EXPECT_EQ(std::memcmp(read_buffers[i], expected.data(), kBlockSize), 0)
        << "Read content mismatch at block " << block_idx;
  }

  // Step 7: Verify written content by reading back
  close(fd);
  fd = OpenFileForDirectIO(file_path, O_RDONLY);
  ASSERT_GE(fd, 0);

  for (int i = 0; i < kNumBlocks / 2; ++i) {
    int block_idx = (i * 2) + 1;  // 1, 3, 5, 7
    char expected_char = static_cast<char>('X' + block_idx);
    EXPECT_TRUE(
        VerifyFileContent(file_path, block_idx * kBlockSize, kBlockSize,
                          std::vector<char>(kBlockSize, expected_char).data()))
        << "Write content mismatch at block " << block_idx;
  }

  // Step 8: Cleanup
  for (auto* buf : read_buffers) {
    free(buf);
  }
  for (auto* buf : write_buffers) {
    free(buf);
  }

  close(fd);
  EXPECT_TRUE(io_uring.Shutdown().ok());
}

TEST_F(IOUringTest, WriteWithFixedBuffer) {
  constexpr size_t kBufferSize = 4096;

  char* raw_write_buffer = static_cast<char*>(aligned_alloc(4096, kBufferSize));
  std::memset(raw_write_buffer, 'X', kBufferSize);

  std::vector<iovec> write_buffers = {{raw_write_buffer, kBufferSize}};

  IOUringOptions options = {.entries = 256,
                            .use_sqpoll = false,
                            .fixed_write_buffers = write_buffers};

  IOUring io_uring(std::move(options));
  ASSERT_TRUE(io_uring.Start().ok());

  std::string file_path = CreateTestFile("fixed_write.dat", kBufferSize);
  int fd = OpenFileForDirectIO(file_path, O_RDWR);
  ASSERT_GE(fd, 0);

  Aio aio(ctx_, fd, 0, kBufferSize, raw_write_buffer, 0, false);

  EXPECT_TRUE(io_uring.PrepareIO(&aio).ok());
  EXPECT_TRUE(io_uring.SubmitIO().ok());

  Aio* completed_aios[16];
  int completed;
  do {
    completed = io_uring.WaitIO(1000, completed_aios);
  } while (completed == 0);

  EXPECT_EQ(completed, 1);
  EXPECT_TRUE(aio.Result().status.ok());
  EXPECT_TRUE(VerifyFileContent(file_path, 0, kBufferSize, raw_write_buffer));

  close(fd);
  EXPECT_TRUE(io_uring.Shutdown().ok());
  free(raw_write_buffer);
}

TEST_F(IOUringTest, ReadWithFixedBuffer) {
  constexpr size_t kBufferSize = 4096;

  char* raw_read_buffer = static_cast<char*>(aligned_alloc(4096, kBufferSize));
  std::memset(raw_read_buffer, 0, kBufferSize);

  std::vector<iovec> read_buffers = {{raw_read_buffer, kBufferSize}};

  IOUringOptions options = {
      .entries = 256, .use_sqpoll = false, .fixed_read_buffers = read_buffers};

  IOUring io_uring(std::move(options));
  ASSERT_TRUE(io_uring.Start().ok());

  std::string file_path = CreateTestFile("fixed_read.dat", kBufferSize);
  FillFileWithPattern(file_path, kBufferSize, 'Y');

  int fd = OpenFileForDirectIO(file_path, O_RDONLY);
  ASSERT_GE(fd, 0);

  Aio aio(ctx_, fd, 0, kBufferSize, raw_read_buffer, 0, true);

  EXPECT_TRUE(io_uring.PrepareIO(&aio).ok());
  EXPECT_TRUE(io_uring.SubmitIO().ok());

  Aio* completed_aios[16];
  int completed;
  do {
    completed = io_uring.WaitIO(1000, completed_aios);
  } while (completed == 0);

  EXPECT_EQ(completed, 1);
  EXPECT_TRUE(aio.Result().status.ok());

  for (size_t i = 0; i < kBufferSize; ++i) {
    EXPECT_EQ(raw_read_buffer[i], 'Y');
  }

  close(fd);
  EXPECT_TRUE(io_uring.Shutdown().ok());
  free(raw_read_buffer);
}

TEST_F(IOUringTest, WaitIO) {
  IOUringOptions options = {.entries = 256, .use_sqpoll = false};
  IOUring io_uring(options);
  ASSERT_TRUE(io_uring.Start().ok());

  std::string file_path = CreateTestFile("wait_io_test.dat", 4096);
  FillFileWithPattern(file_path, 4096, 'W');

  int fd = OpenFileForDirectIO(file_path, O_RDONLY);
  ASSERT_GE(fd, 0);

  {
    Aio* completed_aios[16];
    auto start = std::chrono::steady_clock::now();
    int completed = io_uring.WaitIO(100, completed_aios);
    auto end = std::chrono::steady_clock::now();

    auto elapsed =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
            .count();

    EXPECT_EQ(completed, 0);
    EXPECT_GE(elapsed, 100);
    EXPECT_LT(elapsed, 200);
  }

  {
    char* buffer = static_cast<char*>(aligned_alloc(4096, 4096));
    std::memset(buffer, 0, 4096);

    Aio aio(ctx_, fd, 0, 4096, buffer, -1, true);
    EXPECT_TRUE(io_uring.PrepareIO(&aio).ok());
    EXPECT_TRUE(io_uring.SubmitIO().ok());

    Aio* completed_aios[16];
    int completed = io_uring.WaitIO(1000, completed_aios);

    EXPECT_EQ(completed, 1);
    EXPECT_EQ(completed_aios[0], &aio);
    EXPECT_TRUE(aio.Result().status.ok());

    free(buffer);
  }

  {
    constexpr int kNumOps = 4;
    std::vector<char*> buffers(kNumOps);
    std::vector<std::unique_ptr<Aio>> aios;

    for (int i = 0; i < kNumOps; ++i) {
      buffers[i] = static_cast<char*>(aligned_alloc(4096, 4096));
      std::memset(buffers[i], 0, 4096);
      aios.push_back(
          std::make_unique<Aio>(ctx_, fd, 0, 4096, buffers[i], -1, true));
      EXPECT_TRUE(io_uring.PrepareIO(aios[i].get()).ok());
    }
    EXPECT_TRUE(io_uring.SubmitIO().ok());

    int total_completed = 0;
    Aio* completed_aios[kNumOps];
    while (total_completed < kNumOps) {
      int n = io_uring.WaitIO(1000, completed_aios + total_completed);
      EXPECT_GT(n, 0);
      total_completed += n;
    }
    EXPECT_EQ(total_completed, kNumOps);

    for (const auto& aio : aios) {
      EXPECT_TRUE(aio->Result().status.ok());
    }

    for (auto* buf : buffers) {
      free(buf);
    }
  }

  close(fd);
  EXPECT_TRUE(io_uring.Shutdown().ok());
}

TEST_F(IOUringTest, OnComplete) {
  IOUringOptions options = {.entries = 256, .use_sqpoll = false};
  IOUring io_uring(options);
  ASSERT_TRUE(io_uring.Start().ok());

  std::string file_path = CreateTestFile("oncomplete_test.dat", 4096);
  int fd = OpenFileForDirectIO(file_path, O_RDWR);
  ASSERT_GE(fd, 0);

  char* buffer = static_cast<char*>(aligned_alloc(4096, 4096));

  {
    Aio aio(ctx_, fd, 0, 4096, buffer, -1, true);
    io_uring.OnComplete(&aio, 4096);
    EXPECT_TRUE(aio.Result().status.ok());
  }

  {
    Aio aio(ctx_, fd, 0, 4096, buffer, -1, false);
    io_uring.OnComplete(&aio, 4096);
    EXPECT_TRUE(aio.Result().status.ok());
  }

  {
    Aio aio(ctx_, fd, 0, 4096, buffer, -1, true);
    io_uring.OnComplete(&aio, -EIO);
    EXPECT_FALSE(aio.Result().status.ok());
    EXPECT_TRUE(aio.Result().status.IsIoError());
  }

  {
    Aio aio(ctx_, fd, 0, 4096, buffer, -1, true);
    io_uring.OnComplete(&aio, -ENOENT);
    EXPECT_FALSE(aio.Result().status.ok());
    EXPECT_TRUE(aio.Result().status.IsIoError());
  }

  {
    Aio aio(ctx_, fd, 0, 4096, buffer, -1, true);
    io_uring.OnComplete(&aio, 2048);
    EXPECT_FALSE(aio.Result().status.ok());
    EXPECT_TRUE(aio.Result().status.IsIoError());
  }

  {
    Aio aio(ctx_, fd, 0, 4096, buffer, -1, false);
    io_uring.OnComplete(&aio, 1024);
    EXPECT_FALSE(aio.Result().status.ok());
    EXPECT_TRUE(aio.Result().status.IsIoError());
  }

  {
    Aio aio(ctx_, fd, 0, 4096, buffer, -1, true);
    io_uring.OnComplete(&aio, 0);
    EXPECT_FALSE(aio.Result().status.ok());
    EXPECT_TRUE(aio.Result().status.IsIoError());
  }

  free(buffer);
  close(fd);
  EXPECT_TRUE(io_uring.Shutdown().ok());
}

TEST_F(IOUringTest, RealIOErrors) {
  IOUringOptions options = {.entries = 256, .use_sqpoll = false};
  IOUring io_uring(options);
  ASSERT_TRUE(io_uring.Start().ok());

  char* buffer = static_cast<char*>(aligned_alloc(4096, 4096));
  std::memset(buffer, 'E', 4096);

  {
    // Read from invalid fd
    int invalid_fd = 9999;
    Aio aio(ctx_, invalid_fd, 0, 4096, buffer, -1, true);

    EXPECT_TRUE(io_uring.PrepareIO(&aio).ok());
    EXPECT_TRUE(io_uring.SubmitIO().ok());

    Aio* completed_aios[16];
    int completed;
    do {
      completed = io_uring.WaitIO(1000, completed_aios);
    } while (completed == 0);

    EXPECT_EQ(completed, 1);
    EXPECT_FALSE(aio.Result().status.ok());
    EXPECT_TRUE(aio.Result().status.IsIoError());
  }

  {
    // Write to invalid fd
    int invalid_fd = 9999;
    Aio aio(ctx_, invalid_fd, 0, 4096, buffer, -1, false);

    EXPECT_TRUE(io_uring.PrepareIO(&aio).ok());
    EXPECT_TRUE(io_uring.SubmitIO().ok());

    Aio* completed_aios[16];
    int completed;
    do {
      completed = io_uring.WaitIO(1000, completed_aios);
    } while (completed == 0);

    EXPECT_EQ(completed, 1);
    EXPECT_FALSE(aio.Result().status.ok());
    EXPECT_TRUE(aio.Result().status.IsIoError());
  }

  {
    // Read from closed fd
    std::string file_path = CreateTestFile("closed_fd_read.dat", 4096);
    FillFileWithPattern(file_path, 4096, 'R');
    int fd = OpenFileForDirectIO(file_path, O_RDONLY);
    ASSERT_GE(fd, 0);
    close(fd);

    Aio aio(ctx_, fd, 0, 4096, buffer, -1, true);

    EXPECT_TRUE(io_uring.PrepareIO(&aio).ok());
    EXPECT_TRUE(io_uring.SubmitIO().ok());

    Aio* completed_aios[16];
    int completed;
    do {
      completed = io_uring.WaitIO(1000, completed_aios);
    } while (completed == 0);

    EXPECT_EQ(completed, 1);
    EXPECT_FALSE(aio.Result().status.ok());
    EXPECT_TRUE(aio.Result().status.IsIoError());
  }

  {
    // Write to closed fd
    std::string file_path = CreateTestFile("closed_fd_write.dat", 4096);
    int fd = OpenFileForDirectIO(file_path, O_RDWR);
    ASSERT_GE(fd, 0);
    close(fd);

    Aio aio(ctx_, fd, 0, 4096, buffer, -1, false);

    EXPECT_TRUE(io_uring.PrepareIO(&aio).ok());
    EXPECT_TRUE(io_uring.SubmitIO().ok());

    Aio* completed_aios[16];
    int completed;
    do {
      completed = io_uring.WaitIO(1000, completed_aios);
    } while (completed == 0);

    EXPECT_EQ(completed, 1);
    EXPECT_FALSE(aio.Result().status.ok());
    EXPECT_TRUE(aio.Result().status.IsIoError());
  }

  {
    // Read beyond file size (short read)
    std::string file_path = CreateTestFile("short_read.dat", 512);
    FillFileWithPattern(file_path, 512, 'S');
    int fd = OpenFileForDirectIO(file_path, O_RDONLY);
    ASSERT_GE(fd, 0);

    Aio aio(ctx_, fd, 0, 4096, buffer, -1, true);

    EXPECT_TRUE(io_uring.PrepareIO(&aio).ok());
    EXPECT_TRUE(io_uring.SubmitIO().ok());

    Aio* completed_aios[16];
    int completed;
    do {
      completed = io_uring.WaitIO(1000, completed_aios);
    } while (completed == 0);

    EXPECT_EQ(completed, 1);
    EXPECT_FALSE(aio.Result().status.ok());
    EXPECT_TRUE(aio.Result().status.IsIoError());

    close(fd);
  }

  free(buffer);
  EXPECT_TRUE(io_uring.Shutdown().ok());
}

TEST_F(IOUringTest, OutputStream) {
  {
    IOUringOptions options = {.entries = 1024, .use_sqpoll = true};
    IOUring io_uring(options);
    ASSERT_TRUE(io_uring.Start().ok());

    std::ostringstream oss;
    oss << io_uring;

    std::string output = oss.str();
    EXPECT_NE(output.find("IOUring{"), std::string::npos);
    EXPECT_NE(output.find("entries=1024"), std::string::npos);
    EXPECT_NE(output.find("sqpoll=1"), std::string::npos);

    EXPECT_TRUE(io_uring.Shutdown().ok());
  }

  {
    char* raw_buffer = static_cast<char*>(aligned_alloc(4096, 4096));
    std::vector<iovec> write_buffers = {{raw_buffer, 4096}};
    std::vector<iovec> read_buffers = {{raw_buffer, 4096}};

    IOUringOptions options = {.entries = 512,
                              .use_sqpoll = false,
                              .fixed_write_buffers = write_buffers,
                              .fixed_read_buffers = read_buffers};

    IOUring io_uring(std::move(options));
    ASSERT_TRUE(io_uring.Start().ok());

    std::ostringstream oss;
    oss << io_uring;

    std::string output = oss.str();
    EXPECT_NE(output.find("fixed_buffers=2"), std::string::npos);

    EXPECT_TRUE(io_uring.Shutdown().ok());
    free(raw_buffer);
  }
}

// =============================================================================
// Performance Benchmark Test - Comprehensive Performance Report
// =============================================================================

class IOUringPerfTest : public IOUringTest {
 protected:
  // Performance result structure
  struct PerfResult {
    std::string name;
    double throughput_mbs;  // MB/s
    double iops;            // operations per second
    double latency_us;      // microseconds per operation
    size_t total_bytes;
    int64_t duration_us;
  };

  std::vector<PerfResult> results_;

  void AddResult(const std::string& name, size_t total_bytes,
                 int64_t duration_us, int num_ops = 0) {
    PerfResult result;
    result.name = name;
    result.total_bytes = total_bytes;
    result.duration_us = duration_us;
    result.throughput_mbs =
        (total_bytes / (1024.0 * 1024.0)) / (duration_us / 1e6);
    result.iops = (num_ops > 0) ? (num_ops / (duration_us / 1e6))
                                : (total_bytes / 4096.0) / (duration_us / 1e6);
    result.latency_us = (num_ops > 0)
                            ? (duration_us / static_cast<double>(num_ops))
                            : (duration_us / (total_bytes / 4096.0));
    results_.push_back(result);
  }

  void PrintReport() {
    std::cout << "\n";
    std::cout << "╔════════════════════════════════════════════════════════════"
                 "══════════════════════════════════════════════╗\n";
    std::cout << "║                              IOUring Performance Benchmark "
                 "Report                                       ║\n";
    std::cout << "╠════════════════════════════════════════════════════════════"
                 "══════════════════════════════════════════════╣\n";
    std::cout << "║ Test Directory: " << std::left << std::setw(88) << test_dir_
              << "║\n";
    std::cout << "╠════════════════════════════════════════════════════════════"
                 "══════════════════════════════════════════════╣\n";
    std::cout << "║ " << std::left << std::setw(40) << "Test Name"
              << " │ " << std::right << std::setw(12) << "Throughput"
              << " │ " << std::setw(12) << "IOPS"
              << " │ " << std::setw(12) << "Latency"
              << " │ " << std::setw(10) << "Duration"
              << " ║\n";
    std::cout << "║ " << std::left << std::setw(40) << ""
              << " │ " << std::right << std::setw(12) << "(MB/s)"
              << " │ " << std::setw(12) << "(ops/s)"
              << " │ " << std::setw(12) << "(us/op)"
              << " │ " << std::setw(10) << "(ms)"
              << " ║\n";
    std::cout << "╠══════════════════════════════════════════╪══════════════╪══"
                 "════════════╪══════════════╪════════════╣\n";

    for (const auto& r : results_) {
      std::cout << "║ " << std::left << std::setw(40) << r.name << " │ "
                << std::right << std::fixed << std::setprecision(2)
                << std::setw(12) << r.throughput_mbs << " │ " << std::setw(12)
                << static_cast<int64_t>(r.iops) << " │ " << std::setw(12)
                << r.latency_us << " │ " << std::setw(10)
                << (r.duration_us / 1000.0) << " ║\n";
    }

    std::cout << "╚════════════════════════════════════════════════════════════"
                 "══════════════════════════════════════════════╝\n";
    std::cout << "\n";
  }

  // Benchmark: Sequential Write
  void BenchSequentialWrite(size_t file_size, size_t block_size,
                            const std::string& label) {
    const int num_blocks = file_size / block_size;

    IOUringOptions options = {.entries = 4096, .use_sqpoll = false};
    IOUring io_uring(options);
    ASSERT_TRUE(io_uring.Start().ok());

    std::string file_path = CreateTestFile(label + ".dat", file_size);
    int fd = OpenFileForDirectIO(file_path, O_RDWR);
    ASSERT_GE(fd, 0);

    std::vector<char*> buffers(num_blocks);
    std::vector<std::unique_ptr<Aio>> aios;

    for (int i = 0; i < num_blocks; ++i) {
      buffers[i] = static_cast<char*>(aligned_alloc(4096, block_size));
      std::memset(buffers[i], static_cast<char>(i % 256), block_size);
      aios.push_back(std::make_unique<Aio>(ctx_, fd, i * block_size, block_size,
                                           buffers[i], -1, false));
    }

    auto start = std::chrono::high_resolution_clock::now();

    constexpr int kBatchSize = 256;
    int submitted = 0;
    int total_completed = 0;

    while (submitted < num_blocks || total_completed < num_blocks) {
      while (submitted < num_blocks &&
             submitted - total_completed < kBatchSize) {
        ASSERT_TRUE(io_uring.PrepareIO(aios[submitted].get()).ok());
        submitted++;
      }

      if (submitted > total_completed) {
        ASSERT_TRUE(io_uring.SubmitIO().ok());
      }

      Aio* completed_aios[kBatchSize];
      int n = io_uring.WaitIO(100, completed_aios);
      total_completed += n;
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::microseconds>(end - start)
            .count();

    EXPECT_EQ(total_completed, num_blocks);
    for (const auto& aio : aios) {
      EXPECT_TRUE(aio->Result().status.ok());
    }

    AddResult(label, file_size, duration, num_blocks);

    for (auto* buf : buffers) {
      free(buf);
    }
    close(fd);
    EXPECT_TRUE(io_uring.Shutdown().ok());
  }

  // Benchmark: Sequential Read
  void BenchSequentialRead(size_t file_size, size_t block_size,
                           const std::string& label) {
    const int num_blocks = file_size / block_size;

    std::string file_path = CreateTestFile(label + ".dat", file_size);
    FillFileWithRandomData(file_path, file_size);

    IOUringOptions options = {.entries = 4096, .use_sqpoll = false};
    IOUring io_uring(options);
    ASSERT_TRUE(io_uring.Start().ok());

    int fd = OpenFileForDirectIO(file_path, O_RDONLY);
    ASSERT_GE(fd, 0);

    std::vector<char*> buffers(num_blocks);
    std::vector<std::unique_ptr<Aio>> aios;

    for (int i = 0; i < num_blocks; ++i) {
      buffers[i] = static_cast<char*>(aligned_alloc(4096, block_size));
      std::memset(buffers[i], 0, block_size);
      aios.push_back(std::make_unique<Aio>(ctx_, fd, i * block_size, block_size,
                                           buffers[i], -1, true));
    }

    auto start = std::chrono::high_resolution_clock::now();

    constexpr int kBatchSize = 256;
    int submitted = 0;
    int total_completed = 0;

    while (submitted < num_blocks || total_completed < num_blocks) {
      while (submitted < num_blocks &&
             submitted - total_completed < kBatchSize) {
        ASSERT_TRUE(io_uring.PrepareIO(aios[submitted].get()).ok());
        submitted++;
      }

      if (submitted > total_completed) {
        ASSERT_TRUE(io_uring.SubmitIO().ok());
      }

      Aio* completed_aios[kBatchSize];
      int n = io_uring.WaitIO(100, completed_aios);
      total_completed += n;
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::microseconds>(end - start)
            .count();

    EXPECT_EQ(total_completed, num_blocks);
    for (const auto& aio : aios) {
      EXPECT_TRUE(aio->Result().status.ok());
    }

    AddResult(label, file_size, duration, num_blocks);

    for (auto* buf : buffers) {
      free(buf);
    }
    close(fd);
    EXPECT_TRUE(io_uring.Shutdown().ok());
  }

  // Benchmark: Random Read/Write
  void BenchRandomIO(size_t file_size, size_t block_size, int num_ops,
                     const std::string& label) {
    IOUringOptions options = {.entries = 256, .use_sqpoll = false};
    IOUring io_uring(options);
    ASSERT_TRUE(io_uring.Start().ok());

    std::string file_path = CreateTestFile(label + ".dat", file_size);
    FillFileWithRandomData(file_path, file_size);

    int fd = OpenFileForDirectIO(file_path, O_RDWR);
    ASSERT_GE(fd, 0);

    std::vector<char*> buffers(num_ops);
    std::vector<std::unique_ptr<Aio>> aios;

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> offset_dis(0, (file_size / block_size) - 1);
    std::uniform_int_distribution<> op_dis(0, 1);

    for (int i = 0; i < num_ops; ++i) {
      off_t offset = offset_dis(gen) * block_size;
      bool is_read = (op_dis(gen) == 0);

      buffers[i] = static_cast<char*>(aligned_alloc(4096, block_size));
      if (is_read) {
        std::memset(buffers[i], 0, block_size);
      } else {
        std::memset(buffers[i], static_cast<char>(i), block_size);
      }

      aios.push_back(std::make_unique<Aio>(ctx_, fd, offset, block_size,
                                           buffers[i], -1, is_read));
    }

    auto start = std::chrono::high_resolution_clock::now();

    for (auto& aio : aios) {
      ASSERT_TRUE(io_uring.PrepareIO(aio.get()).ok());
    }
    ASSERT_TRUE(io_uring.SubmitIO().ok());

    int total_completed = 0;
    while (total_completed < num_ops) {
      Aio* completed_aios[256];
      int n = io_uring.WaitIO(1000, completed_aios);
      total_completed += n;
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::microseconds>(end - start)
            .count();

    EXPECT_EQ(total_completed, num_ops);
    for (const auto& aio : aios) {
      EXPECT_TRUE(aio->Result().status.ok());
    }

    AddResult(label, num_ops * block_size, duration, num_ops);

    for (auto* buf : buffers) {
      free(buf);
    }
    close(fd);
    EXPECT_TRUE(io_uring.Shutdown().ok());
  }

  // Benchmark: Queue Depth Scaling
  void BenchQueueDepth(size_t file_size, size_t block_size, int queue_depth,
                       const std::string& label) {
    const int num_blocks = file_size / block_size;

    std::string file_path = CreateTestFile(label + ".dat", file_size);
    FillFileWithRandomData(file_path, file_size);

    IOUringOptions options = {
        .entries = static_cast<uint32_t>(std::max(256, queue_depth * 2)),
        .use_sqpoll = false};
    IOUring io_uring(options);
    ASSERT_TRUE(io_uring.Start().ok());

    int fd = OpenFileForDirectIO(file_path, O_RDONLY);
    ASSERT_GE(fd, 0);

    std::vector<char*> buffers(queue_depth);
    for (int i = 0; i < queue_depth; ++i) {
      buffers[i] = static_cast<char*>(aligned_alloc(4096, block_size));
    }

    auto start = std::chrono::high_resolution_clock::now();

    int submitted = 0;
    int completed = 0;

    while (completed < num_blocks) {
      while (submitted < num_blocks && submitted - completed < queue_depth) {
        int buf_idx = submitted % queue_depth;
        auto aio = new Aio(ctx_, fd, submitted * block_size, block_size,
                           buffers[buf_idx], -1, true);
        ASSERT_TRUE(io_uring.PrepareIO(aio).ok());
        submitted++;
      }

      if (submitted > completed) {
        ASSERT_TRUE(io_uring.SubmitIO().ok());
      }

      Aio* completed_aios[256];
      int n = io_uring.WaitIO(100, completed_aios);
      for (int i = 0; i < n; ++i) {
        EXPECT_TRUE(completed_aios[i]->Result().status.ok());
        delete completed_aios[i];
      }
      completed += n;
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::microseconds>(end - start)
            .count();

    AddResult(label, file_size, duration, num_blocks);

    for (auto* buf : buffers) {
      free(buf);
    }
    close(fd);
    EXPECT_TRUE(io_uring.Shutdown().ok());
  }

  // Benchmark: Block Size Scaling
  void BenchBlockSize(size_t file_size, size_t block_size,
                      const std::string& label) {
    const int num_blocks = file_size / block_size;

    std::string file_path = CreateTestFile(label + ".dat", file_size);
    FillFileWithRandomData(file_path, file_size);

    IOUringOptions options = {.entries = 256, .use_sqpoll = false};
    IOUring io_uring(options);
    ASSERT_TRUE(io_uring.Start().ok());

    int fd = OpenFileForDirectIO(file_path, O_RDONLY);
    ASSERT_GE(fd, 0);

    char* buffer = static_cast<char*>(aligned_alloc(4096, block_size));

    auto start = std::chrono::high_resolution_clock::now();

    int completed = 0;
    for (int i = 0; i < num_blocks; ++i) {
      Aio aio(ctx_, fd, i * block_size, block_size, buffer, -1, true);

      ASSERT_TRUE(io_uring.PrepareIO(&aio).ok());
      ASSERT_TRUE(io_uring.SubmitIO().ok());

      Aio* completed_aios[16];
      while (io_uring.WaitIO(100, completed_aios) == 0) {
      }
      completed++;
      EXPECT_TRUE(aio.Result().status.ok());
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::microseconds>(end - start)
            .count();

    AddResult(label, file_size, duration, num_blocks);

    free(buffer);
    close(fd);
    EXPECT_TRUE(io_uring.Shutdown().ok());
  }

  // Benchmark: Fixed Buffer vs Normal Buffer
  void BenchFixedBuffer(size_t file_size, size_t block_size,
                        bool use_fixed_buffer, const std::string& label) {
    const int num_blocks = file_size / block_size;
    constexpr int kNumBuffers = 16;

    std::vector<char*> raw_buffers(kNumBuffers);
    std::vector<iovec> read_buffers(kNumBuffers);

    for (int i = 0; i < kNumBuffers; ++i) {
      raw_buffers[i] = static_cast<char*>(aligned_alloc(4096, block_size));
      read_buffers[i] = {raw_buffers[i], block_size};
    }

    std::string file_path = CreateTestFile(label + ".dat", file_size);
    FillFileWithRandomData(file_path, file_size);

    IOUringOptions options;
    if (use_fixed_buffer) {
      options = {.entries = 1024,
                 .use_sqpoll = false,
                 .fixed_read_buffers = read_buffers};
    } else {
      options = {.entries = 1024, .use_sqpoll = false};
    }

    IOUring io_uring(std::move(options));
    ASSERT_TRUE(io_uring.Start().ok());

    int fd = OpenFileForDirectIO(file_path, O_RDONLY);
    ASSERT_GE(fd, 0);

    auto start = std::chrono::high_resolution_clock::now();

    int completed = 0;
    for (int i = 0; i < num_blocks; ++i) {
      int buf_idx = i % kNumBuffers;
      Aio aio(ctx_, fd, i * block_size, block_size, raw_buffers[buf_idx],
              use_fixed_buffer ? buf_idx : -1, true);

      ASSERT_TRUE(io_uring.PrepareIO(&aio).ok());
      ASSERT_TRUE(io_uring.SubmitIO().ok());

      Aio* completed_aios[16];
      while (io_uring.WaitIO(100, completed_aios) == 0) {
      }
      completed++;
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::microseconds>(end - start)
            .count();

    AddResult(label, file_size, duration, num_blocks);

    for (auto* buf : raw_buffers) {
      free(buf);
    }
    close(fd);
    EXPECT_TRUE(io_uring.Shutdown().ok());
  }
};

// Comprehensive Performance Benchmark Test
TEST_F(IOUringPerfTest, ComprehensiveBenchmark) {
  if (!FLAGS_run_perf_test) {
    GTEST_SKIP() << "Performance test skipped. Use --run_perf_test=true to run";
  }

  std::cout << "\n[IOUring Performance Benchmark] Starting...\n";
  std::cout << "Test directory: " << test_dir_ << "\n\n";

  // 1. Sequential Write Tests
  std::cout << "Running Sequential Write tests...\n";
  BenchSequentialWrite(4 * 1024 * 1024, 4096, "SeqWrite_4MB_4KB");
  BenchSequentialWrite(4 * 1024 * 1024, 64 * 1024, "SeqWrite_4MB_64KB");
  BenchSequentialWrite(4 * 1024 * 1024, 1024 * 1024, "SeqWrite_4MB_1MB");

  // 2. Sequential Read Tests
  std::cout << "Running Sequential Read tests...\n";
  BenchSequentialRead(4 * 1024 * 1024, 4096, "SeqRead_4MB_4KB");
  BenchSequentialRead(4 * 1024 * 1024, 64 * 1024, "SeqRead_4MB_64KB");
  BenchSequentialRead(4 * 1024 * 1024, 1024 * 1024, "SeqRead_4MB_1MB");

  // 3. Random I/O Tests
  std::cout << "Running Random I/O tests...\n";
  BenchRandomIO(4 * 1024 * 1024, 4096, 256, "RandIO_4MB_4KB_256ops");
  BenchRandomIO(4 * 1024 * 1024, 64 * 1024, 64, "RandIO_4MB_64KB_64ops");

  // 4. Queue Depth Scaling Tests
  std::cout << "Running Queue Depth scaling tests...\n";
  BenchQueueDepth(2 * 1024 * 1024, 4096, 1, "QD1_2MB_4KB");
  BenchQueueDepth(2 * 1024 * 1024, 4096, 4, "QD4_2MB_4KB");
  BenchQueueDepth(2 * 1024 * 1024, 4096, 16, "QD16_2MB_4KB");
  BenchQueueDepth(2 * 1024 * 1024, 4096, 64, "QD64_2MB_4KB");
  BenchQueueDepth(2 * 1024 * 1024, 4096, 128, "QD128_2MB_4KB");

  // 5. Block Size Scaling Tests
  std::cout << "Running Block Size scaling tests...\n";
  BenchBlockSize(4 * 1024 * 1024, 4 * 1024, "BlkSize_4KB_4MB");
  BenchBlockSize(4 * 1024 * 1024, 16 * 1024, "BlkSize_16KB_4MB");
  BenchBlockSize(4 * 1024 * 1024, 64 * 1024, "BlkSize_64KB_4MB");
  BenchBlockSize(4 * 1024 * 1024, 256 * 1024, "BlkSize_256KB_4MB");
  BenchBlockSize(4 * 1024 * 1024, 1024 * 1024, "BlkSize_1MB_4MB");

  // 6. Fixed Buffer Comparison Tests
  std::cout << "Running Fixed Buffer comparison tests...\n";
  BenchFixedBuffer(1 * 1024 * 1024, 4096, false, "NormalBuf_1MB_4KB");
  BenchFixedBuffer(1 * 1024 * 1024, 4096, true, "FixedBuf_1MB_4KB");

  // Print comprehensive report
  PrintReport();
}

// =============================================================================
// Data Integrity Tests
// =============================================================================

TEST_F(IOUringTest, WriteReadDataIntegrity) {
  constexpr size_t kFileSize = 256 * 1024;  // 256KB
  constexpr size_t kBlockSize = 4096;
  constexpr int kNumBlocks = kFileSize / kBlockSize;

  IOUringOptions options = {.entries = 256, .use_sqpoll = false};
  IOUring io_uring(options);
  ASSERT_TRUE(io_uring.Start().ok());

  std::string file_path = CreateTestFile("integrity_test.dat", kFileSize);
  int fd = OpenFileForDirectIO(file_path, O_RDWR);
  ASSERT_GE(fd, 0);

  std::vector<std::vector<char>> original_data(kNumBlocks);
  std::vector<char*> write_buffers(kNumBlocks);

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(0, 255);

  for (int i = 0; i < kNumBlocks; ++i) {
    original_data[i].resize(kBlockSize);
    for (size_t j = 0; j < kBlockSize; ++j) {
      original_data[i][j] = static_cast<char>(dis(gen));
    }

    write_buffers[i] = static_cast<char*>(aligned_alloc(4096, kBlockSize));
    std::memcpy(write_buffers[i], original_data[i].data(), kBlockSize);
  }

  {
    for (int i = 0; i < kNumBlocks; ++i) {
      Aio aio(ctx_, fd, i * kBlockSize, kBlockSize, write_buffers[i], -1,
              false);

      ASSERT_TRUE(io_uring.PrepareIO(&aio).ok());
      ASSERT_TRUE(io_uring.SubmitIO().ok());

      Aio* completed_aios[16];
      while (io_uring.WaitIO(100, completed_aios) == 0) {
      }
      EXPECT_TRUE(aio.Result().status.ok());
    }
  }

  std::vector<char*> read_buffers(kNumBlocks);
  for (int i = 0; i < kNumBlocks; ++i) {
    read_buffers[i] = static_cast<char*>(aligned_alloc(4096, kBlockSize));
    std::memset(read_buffers[i], 0, kBlockSize);
  }

  {
    for (int i = 0; i < kNumBlocks; ++i) {
      Aio aio(ctx_, fd, i * kBlockSize, kBlockSize, read_buffers[i], -1, true);

      ASSERT_TRUE(io_uring.PrepareIO(&aio).ok());
      ASSERT_TRUE(io_uring.SubmitIO().ok());

      Aio* completed_aios[16];
      while (io_uring.WaitIO(100, completed_aios) == 0) {
      }
      EXPECT_TRUE(aio.Result().status.ok());
    }
  }

  for (int i = 0; i < kNumBlocks; ++i) {
    EXPECT_EQ(std::memcmp(read_buffers[i], original_data[i].data(), kBlockSize),
              0)
        << "Data mismatch at block " << i;
  }

  for (auto* buf : write_buffers) {
    free(buf);
  }
  for (auto* buf : read_buffers) {
    free(buf);
  }

  close(fd);
  EXPECT_TRUE(io_uring.Shutdown().ok());
}

TEST_F(IOUringTest, ConcurrentWritesDifferentOffsets) {
  constexpr size_t kFileSize = 1 * 1024 * 1024;  // 1MB
  constexpr size_t kBlockSize = 4096;
  constexpr int kNumConcurrent = 32;

  IOUringOptions options = {.entries = 256, .use_sqpoll = false};
  IOUring io_uring(options);
  ASSERT_TRUE(io_uring.Start().ok());

  std::string file_path = CreateTestFile("concurrent_writes.dat", kFileSize);
  int fd = OpenFileForDirectIO(file_path, O_RDWR);
  ASSERT_GE(fd, 0);

  std::vector<char*> buffers(kNumConcurrent);
  std::vector<std::unique_ptr<Aio>> aios;

  for (int i = 0; i < kNumConcurrent; ++i) {
    buffers[i] = static_cast<char*>(aligned_alloc(4096, kBlockSize));
    std::memset(buffers[i], static_cast<char>('A' + (i % 26)), kBlockSize);

    aios.push_back(std::make_unique<Aio>(ctx_, fd, i * kBlockSize, kBlockSize,
                                         buffers[i], -1, false));
  }

  for (auto& aio : aios) {
    ASSERT_TRUE(io_uring.PrepareIO(aio.get()).ok());
  }
  ASSERT_TRUE(io_uring.SubmitIO().ok());

  int total_completed = 0;
  while (total_completed < kNumConcurrent) {
    Aio* completed_aios[kNumConcurrent];
    int n = io_uring.WaitIO(1000, completed_aios);
    total_completed += n;
  }

  EXPECT_EQ(total_completed, kNumConcurrent);

  for (const auto& aio : aios) {
    EXPECT_TRUE(aio->Result().status.ok());
  }

  for (int i = 0; i < kNumConcurrent; ++i) {
    std::vector<char> expected(kBlockSize, static_cast<char>('A' + (i % 26)));
    EXPECT_TRUE(VerifyFileContent(file_path, i * kBlockSize, kBlockSize,
                                  expected.data()));
  }

  for (auto* buf : buffers) {
    free(buf);
  }

  close(fd);
  EXPECT_TRUE(io_uring.Shutdown().ok());
}

}  // namespace cache
}  // namespace dingofs
