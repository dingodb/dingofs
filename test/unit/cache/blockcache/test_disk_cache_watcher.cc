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

#include "cache/blockcache/disk_cache_watcher.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <string>

namespace dingofs {
namespace cache {

class DiskCacheWatcherTest : public ::testing::Test {
 protected:
  void SetUp() override {
    static int seq = 0;
    cache_index_ = 900 + (seq++);
    root_dir_ = "/tmp/dingofs_test_disk_cache_watcher_" +
                std::to_string(getpid()) + "_" +
                std::to_string(cache_index_);
  }

  void TearDown() override { std::filesystem::remove_all(root_dir_); }

  bool CheckUuid(const std::string& lock_path, const std::string& uuid) {
    return DiskCacheWatcher::CheckUuid(lock_path, uuid);
  }

  uint32_t cache_index_{0};
  std::string root_dir_;
};

TEST_F(DiskCacheWatcherTest, ShutdownBeforeStartIsOk) {
  DiskCacheWatcher watcher;

  watcher.Shutdown();
}

TEST_F(DiskCacheWatcherTest, CheckUuid) {
  std::filesystem::create_directories(root_dir_);
  auto lock_path = root_dir_ + "/.lock";
  std::ofstream(lock_path) << " uuid-1 \n";

  EXPECT_TRUE(CheckUuid(lock_path, "uuid-1"));
  EXPECT_FALSE(CheckUuid(lock_path, "uuid-2"));
  EXPECT_FALSE(CheckUuid(root_dir_ + "/missing.lock", "uuid-1"));
}

}  // namespace cache
}  // namespace dingofs
