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

#include <filesystem>
#include <fstream>
#include <string>

#include "cache/blockcache/disk_cache_layout.h"
#include "cache/blockcache/local_filesystem.h"
#include "cache/common/context.h"
#include "common/io_buffer.h"
#include "common/options/cache.h"
#include "utils/uuid.h"

namespace dingofs {
namespace cache {

class LocalFileSystemTest : public ::testing::Test {
 protected:
  void SetUp() override {
    saved_fix_buffer_ = FLAGS_fix_buffer;
    saved_iodepth_ = FLAGS_iodepth;
    FLAGS_fix_buffer = false;
    FLAGS_iodepth = 4;  // shrink the aio buffer pools (2 x iodepth x 4MiB)

    root_dir_ = "/tmp/test_local_filesystem." + utils::GenerateUUID();
    auto layout = std::make_shared<DiskCacheLayout>(0, root_dir_);
    for (const auto& dir :
         {layout->GetRootDir(), layout->GetStageDir(), layout->GetCacheDir(),
          layout->GetProbeDir()}) {
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
    FLAGS_fix_buffer = saved_fix_buffer_;
    FLAGS_iodepth = saved_iodepth_;
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
  bool saved_fix_buffer_;
  uint32_t saved_iodepth_;
};

TEST_F(LocalFileSystemTest, ReadFile) {
  auto path = CreateFile("block_8k", 8192);

  {  // whole file
    IOBuffer buffer;
    auto status = lfs_->ReadFile(NewContext(), path, 0, 8192, &buffer);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(buffer.Size(), 8192);

    std::string data(buffer.Size(), 0);
    buffer.CopyTo(data.data());
    ASSERT_EQ(data[0], 'a');
    ASSERT_EQ(data[8191], static_cast<char>('a' + (8191 % 26)));
  }

  {  // unaligned range
    IOBuffer buffer;
    auto status = lfs_->ReadFile(NewContext(), path, 1000, 2000, &buffer);
    ASSERT_TRUE(status.ok());
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
    auto status = lfs_->ReadFile(NewContext(), path, 0, 8192, &buffer);
    ASSERT_FALSE(status.ok());
    ASSERT_EQ(buffer.Size(), 0);  // failed read must not leave a fragment
  }

  {  // buffer with existing data stays intact on failure
    auto path = CreateFile("block_short2", 4096);

    IOBuffer buffer("hello", 5);
    auto status = lfs_->ReadFile(NewContext(), path, 0, 8192, &buffer);
    ASSERT_FALSE(status.ok());
    ASSERT_EQ(buffer.Size(), 5);
  }
}

}  // namespace cache
}  // namespace dingofs
