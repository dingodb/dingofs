/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2024-09-04
 * Author: Jingli Chen (Wine93)
 */

#include <sstream>
#include <thread>

#include "absl/cleanup/cleanup.h"
#include "cache/blockcache/builder/builder.h"
#include "cache/blockcache/cache_store.h"
#include "cache/utils/access_log.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

namespace dingofs {
namespace cache {

using absl::MakeCleanup;

class DiskCacheLoaderTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(DiskCacheLoaderTest, LoadStage) {
  auto builder = DiskCacheBuilder();
  auto disk_cache = builder.Build();
  auto defer = MakeCleanup([&]() {
    disk_cache->Shutdown();
    builder.Cleanup();
  });

  auto key = BlockKeyBuilder().Build(100);
  auto fs = LocalFileSystem();
  auto root_dir = builder.GetRootDir();
  auto stage_path = PathJoin({root_dir, "stage", key.StoreKey()});
  auto rc = fs.WriteFile(stage_path, "xyz", 3);
  ASSERT_EQ(rc, Status::OK());

  std::vector<BlockKey> uploading;
  rc = disk_cache->Init([&](const BlockKey& key, const std::string&,
                            BlockContext) { uploading.emplace_back(key); });
  ASSERT_EQ(rc, Status::OK());
  std::this_thread::sleep_for(std::chrono::seconds(3));  // wait for reload
  ASSERT_FALSE(disk_cache->IsCached(key));
  ASSERT_EQ(uploading.size(), 1);
  ASSERT_EQ(uploading[0].id, 100);
}

TEST_F(DiskCacheLoaderTest, LoadCache) {
  auto builder = DiskCacheBuilder();
  auto disk_cache = builder.Build();
  auto defer = MakeCleanup([&]() {
    disk_cache->Shutdown();
    builder.Cleanup();
  });

  auto key = BlockKeyBuilder().Build(100);
  auto fs = LocalFileSystem();
  auto root_dir = builder.GetRootDir();
  auto cache_path = PathJoin({root_dir, "cache", key.StoreKey()});
  auto rc = fs.WriteFile(cache_path, "xyz", 3);
  ASSERT_EQ(rc, Status::OK());

  rc = disk_cache->Init(
      [](const BlockKey&, const std::string&, BlockContext) {});
  ASSERT_EQ(rc, Status::OK());
  std::this_thread::sleep_for(std::chrono::seconds(3));  // wait for reload
  ASSERT_TRUE(disk_cache->IsCached(key));
}

}  // namespace cache
}  // namespace dingofs
