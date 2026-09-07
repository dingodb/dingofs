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

#include "blockcache/core/fs/filesystem.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <string>

#include "blockcache/core/runtime/bootstrap.h"
#include "blockcache/core/runtime/smp.h"
#include "common/options/cache.h"

namespace dingofs {
namespace blockcache {
namespace {

constexpr size_t kAlign = 4096;
constexpr uint32_t kBlock = 4096;

class FileSystemTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    FLAGS_shards = 1;
    StartProcessRuntime();
  }
  static void TearDownTestSuite() { StopProcessRuntime(); }

  void SetUp() override {
    dir_ = std::filesystem::current_path() /
           ("blockcache_fs_test_" + std::to_string(::getpid()));
    std::filesystem::create_directories(dir_);
    buffer_ = static_cast<char*>(std::aligned_alloc(kAlign, 4 * kBlock));
  }
  void TearDown() override {
    std::free(buffer_);
    std::filesystem::remove_all(dir_);
  }

  std::string Path(const char* name) const { return (dir_ / name).string(); }

  Status WriteBlocks(const std::string& path, uint32_t blocks, char fill) {
    char* data = buffer_;
    std::memset(data, fill, size_t{blocks} * kBlock);
    return RunOnAndWait(0, [path, data, blocks]() -> Future<Status> {
      OpenOption option{.register_fd = false};
      StatusOr<File> open = co_await FileSystem::Open(
          path, OpenFlags::kWrite | OpenFlags::kCreate, option);
      if (!open.ok()) {
        co_return open.status();
      }
      File file = std::move(open).value();
      StatusOr<size_t> n = co_await file.Write(0, data, blocks * kBlock);
      const Status close = co_await file.Close();
      co_return n.ok() ? close : n.status();
    });
  }

  StatusOr<size_t> Read(const std::string& path, uint64_t pos, uint32_t len,
                        bool direct = true) {
    char* data = buffer_;
    return RunOnAndWait(
        0, [path, pos, data, len, direct]() -> Future<StatusOr<size_t>> {
          OpenOption option{.register_fd = false, .direct = direct};
          co_return co_await FileSystem::Read(path, pos, data, len,
                                             OpenFlags::kRead, option);
        });
  }

  size_t FreeSlots() {
    return RunOnAndWait(0, []() -> Future<size_t> {
      co_return ThisIoRing().files().free_slots();
    });
  }

  std::filesystem::path dir_;
  char* buffer_ = nullptr;
};

TEST_F(FileSystemTest, ReadRoundTrip) {
  const std::string path = Path("block");
  ASSERT_TRUE(WriteBlocks(path, 2, 'a').ok());
  std::memset(buffer_, 0, 2 * kBlock);

  StatusOr<size_t> n = Read(path, 0, 2 * kBlock);
  ASSERT_TRUE(n.ok()) << n.status().ToString();
  EXPECT_EQ(*n, 2 * kBlock);
  EXPECT_EQ(buffer_[0], 'a');
  EXPECT_EQ(buffer_[2 * kBlock - 1], 'a');

  n = Read(path, kBlock, kBlock);
  ASSERT_TRUE(n.ok());
  EXPECT_EQ(*n, kBlock);

  n = Read(path, 0, 2 * kBlock, false);
  ASSERT_TRUE(n.ok());
  EXPECT_EQ(*n, 2 * kBlock);
}

TEST_F(FileSystemTest, ReadShortAndMissing) {
  const std::string path = Path("short");
  ASSERT_TRUE(WriteBlocks(path, 1, 'b').ok());

  StatusOr<size_t> n = Read(path, 0, 4 * kBlock);
  ASSERT_TRUE(n.ok());
  EXPECT_EQ(*n, kBlock);

  n = Read(Path("absent"), 0, kBlock);
  ASSERT_FALSE(n.ok());
  EXPECT_TRUE(n.status().IsNotExist()) << n.status().ToString();
}

TEST_F(FileSystemTest, ReadRecyclesSlots) {
  const std::string path = Path("slots");
  ASSERT_TRUE(WriteBlocks(path, 1, 'c').ok());
  ASSERT_TRUE(Read(path, 0, kBlock).ok());
  const size_t before = FreeSlots();

  for (int i = 0; i < 2000; ++i) {
    ASSERT_TRUE(Read(path, 0, kBlock).ok());
    ASSERT_FALSE(Read(Path("absent"), 0, kBlock).ok());
  }
  EXPECT_EQ(FreeSlots(), before);
}

}  // namespace
}  // namespace blockcache
}  // namespace dingofs
