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
 * Created Date: 2024-09-08
 * Author: Jingli Chen (Wine93)
 */

#include "absl/cleanup/cleanup.h"
#include "base/filepath/filepath.h"
#include "cache/blockcache/builder/builder.h"
#include "cache/blockcache/cache_store.h"
#include "cache/utils/local_filesystem.h"
#include "gtest/gtest.h"

namespace dingofs {
namespace cache {

using ::absl::MakeCleanup;
using base::filepath::PathJoin;

class DiskCacheTest : public ::testing::Test {
 protected:
  void SetUp() override {}

  void TearDown() override {}
};

TEST_F(DiskCacheTest, Basic) {
  auto builder = DiskCacheBuilder();
  auto _ = MakeCleanup([&]() { builder.Cleanup(); });

  auto disk_cache = builder.Build();
  auto rc = disk_cache->Init(
      [](const BlockKey&, const std::string&, BlockContext) {});
  ASSERT_EQ(rc, Status::OK());

  std::string id = disk_cache->Id();
  ASSERT_GT(id.size(), 0);
  LOG(INFO) << "disk cache uuid=" << id;

  rc = disk_cache->Shutdown();
  ASSERT_EQ(rc, Status::OK());
}

TEST_F(DiskCacheTest, Stage) {
  auto builder = DiskCacheBuilder();
  auto _ = MakeCleanup([&]() { builder.Cleanup(); });

  auto disk_cache = builder.Build();
  std::vector<BlockKey> staging;
  auto rc = disk_cache->Init([&](const BlockKey& key, const std::string&,
                                 BlockContext) { staging.emplace_back(key); });
  ASSERT_EQ(rc, Status::OK());
  auto defer = MakeCleanup([&]() { disk_cache->Shutdown(); });

  auto key = BlockKeyBuilder().Build(100);
  auto block = BlockBuilder().Build("");
  auto ctx = BlockContext(BlockFrom::kCtoFlush);
  rc = disk_cache->Stage(key, block, ctx);
  ASSERT_EQ(rc, Status::OK());

  auto fs = LocalFileSystem();
  auto root_dir = builder.GetRootDir();
  auto stage_path = PathJoin({root_dir, "stage", key.StoreKey()});
  auto cache_path = PathJoin({root_dir, "cache", key.StoreKey()});
  ASSERT_TRUE(disk_cache->IsCached(key));
  ASSERT_TRUE(fs.FileExists(stage_path));
  ASSERT_TRUE(fs.FileExists(cache_path));

  ASSERT_EQ(staging.size(), 1);
  ASSERT_EQ(staging[0].id, 100);
}

TEST_F(DiskCacheTest, RemoveStage) {
  auto builder = DiskCacheBuilder();
  auto _ = MakeCleanup([&]() { builder.Cleanup(); });

  auto disk_cache = builder.Build();
  auto rc = disk_cache->Init(
      [](const BlockKey&, const std::string&, BlockContext) {});
  ASSERT_EQ(rc, Status::OK());
  auto defer = MakeCleanup([&]() { disk_cache->Shutdown(); });

  auto key = BlockKeyBuilder().Build(100);
  auto block = BlockBuilder().Build("");
  auto ctx = BlockContext(BlockFrom::kCtoFlush);
  rc = disk_cache->Stage(key, block, ctx);
  ASSERT_EQ(rc, Status::OK());

  auto fs = LocalFileSystem();
  auto root_dir = builder.GetRootDir();
  auto stage_path = PathJoin({root_dir, "stage", key.StoreKey()});
  ASSERT_TRUE(fs.FileExists(stage_path));

  rc = disk_cache->RemoveStage(key, ctx);
  ASSERT_EQ(rc, Status::OK());
  ASSERT_FALSE(fs.FileExists(stage_path));
}

TEST_F(DiskCacheTest, Cache) {
  auto builder = DiskCacheBuilder();
  auto _ = MakeCleanup([&]() { builder.Cleanup(); });

  auto disk_cache = builder.Build();
  auto rc = disk_cache->Init(
      [](const BlockKey&, const std::string&, BlockContext) {});
  ASSERT_EQ(rc, Status::OK());
  auto defer = MakeCleanup([&]() { disk_cache->Shutdown(); });

  auto key = BlockKeyBuilder().Build(100);
  auto block = BlockBuilder().Build("xyz");
  rc = disk_cache->Cache(key, block);
  ASSERT_EQ(rc, Status::OK());

  auto fs = LocalFileSystem();
  auto root_dir = builder.GetRootDir();
  auto cache_path = PathJoin({root_dir, "cache", key.StoreKey()});
  ASSERT_TRUE(fs.FileExists(cache_path));

  char buffer[5];
  std::shared_ptr<BlockReader> reader;
  rc = disk_cache->Load(key, reader);
  ASSERT_EQ(rc, Status::OK());
  rc = reader->ReadAt(0, 3, buffer);
  ASSERT_EQ(std::string(buffer, 3), "xyz");
}

TEST_F(DiskCacheTest, IsCached) {
  auto builder = DiskCacheBuilder();
  auto _ = MakeCleanup([&]() { builder.Cleanup(); });

  auto disk_cache = builder.Build();
  auto rc = disk_cache->Init(
      [](const BlockKey&, const std::string&, BlockContext) {});
  ASSERT_EQ(rc, Status::OK());
  auto defer = MakeCleanup([&]() { disk_cache->Shutdown(); });

  auto key_100 = BlockKeyBuilder().Build(100);
  auto key_200 = BlockKeyBuilder().Build(200);
  ASSERT_FALSE(disk_cache->IsCached(key_100));
  ASSERT_FALSE(disk_cache->IsCached(key_200));

  auto block = BlockBuilder().Build("xyz");
  auto ctx = BlockContext(BlockFrom::kReload);
  rc = disk_cache->Stage(key_100, block, ctx);
  ASSERT_EQ(rc, Status::OK());
  ASSERT_TRUE(disk_cache->IsCached(key_100));

  rc = disk_cache->Cache(key_200, block);
  ASSERT_EQ(rc, Status::OK());
  ASSERT_TRUE(disk_cache->IsCached(key_200));
}

}  // namespace cache
}  // namespace dingofs
