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
 * Created Date: 2026-07-20
 * Author: AI
 */

#include <gtest/gtest.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <memory>
#include <string>

#include "cache/common/slab_pool.h"
#include "cache/local/disk_cache_layout.h"
#include "cache/local/local_filesystem.h"
#include "common/io_buffer.h"
#include "common/options/cache.h"

namespace dingofs {
namespace cache {

class LocalFileSystemTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    // The fixed read/write buffers come from the global slab pools
    // (2 x iodepth x 4MiB); shrink iodepth before the once-only
    // initialization so the test binary does not reserve 1GiB.
    FLAGS_iodepth = 4;
    ASSERT_TRUE(InitializeGlobalSlabPool().ok());
  }

  void SetUp() override {
    root_dir_ =
        "/tmp/dingofs_test_local_filesystem_" + std::to_string(getpid());
    auto layout = std::make_shared<DiskCacheLayout>(0, root_dir_);
    for (const auto& dir : {layout->GetRootDir(), layout->GetStageDir(),
                            layout->GetCacheDir(), layout->GetProbeDir()}) {
      std::filesystem::create_directories(dir);
    }

    lfs_ = std::make_unique<LocalFileSystem>(layout);
    auto status = lfs_->Start();
    if (status.IsNotSupport()) {
      GTEST_SKIP() << "io_uring is unavailable in this environment";
    }
    ASSERT_TRUE(status.ok()) << status.ToString();
    started_ = true;
  }

  void TearDown() override {
    if (started_) {
      lfs_->Shutdown();
    }
    std::filesystem::remove_all(root_dir_);
  }

  std::string CreateFile(const std::string& name, size_t size) {
    auto path = root_dir_ + "/cache/" + name;
    std::ofstream out(path, std::ios::binary);
    for (size_t i = 0; i < size; i++) {
      out.put(static_cast<char>('a' + (i % 26)));
    }
    out.close();
    return path;
  }

  std::string root_dir_;
  LocalFileSystemUPtr lfs_;
  bool started_{false};
};

TEST_F(LocalFileSystemTest, ReadFile) {
  auto path = CreateFile("block_8k", 8192);

  {  // whole file
    IOBuffer buffer;
    auto status = lfs_->ReadFile(path, 0, 8192, &buffer);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_EQ(buffer.Size(), 8192);

    std::string data(buffer.Size(), 0);
    buffer.CopyTo(data.data());
    ASSERT_EQ(data[0], 'a');
    ASSERT_EQ(data[8191], static_cast<char>('a' + (8191 % 26)));
  }

  {  // unaligned range
    IOBuffer buffer;
    auto status = lfs_->ReadFile(path, 1000, 2000, &buffer);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_EQ(buffer.Size(), 2000);

    std::string data(buffer.Size(), 0);
    buffer.CopyTo(data.data());
    ASSERT_EQ(data[0], static_cast<char>('a' + (1000 % 26)));
    ASSERT_EQ(data[1999], static_cast<char>('a' + (2999 % 26)));
  }
}

TEST_F(LocalFileSystemTest, NoGarbageInBufferOnFailure) {
  {  // short file: aio reads fewer bytes than requested
    auto path = CreateFile("block_short", 4096);

    IOBuffer buffer;
    auto status = lfs_->ReadFile(path, 0, 8192, &buffer);
    ASSERT_FALSE(status.ok());
    ASSERT_EQ(buffer.Size(), 0);  // failed read must not leave a fragment
  }

  {  // buffer with existing data stays intact on failure
    auto path = CreateFile("block_short2", 4096);

    IOBuffer buffer("hello", 5);
    auto status = lfs_->ReadFile(path, 0, 8192, &buffer);
    ASSERT_FALSE(status.ok());
    ASSERT_EQ(buffer.Size(), 5);
  }
}

}  // namespace cache
}  // namespace dingofs
