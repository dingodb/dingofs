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
#include "curvefs/src/client/blockcache/log.h"
#include "curvefs/test/client/blockcache/helper/builder.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

namespace curvefs {
namespace client {
namespace blockcache {

using ::absl::MakeCleanup;

class DiskCacheManagerTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(DiskCacheManagerTest, CleanupFull) {
  auto builder = DiskCacheBuilder();
  builder.SetOption([](DiskCacheOption* option) { option->cache_size = 30; });
  auto disk_cache = builder.Build();
  auto defer = MakeCleanup([&]() {
    disk_cache->Shutdown();
    builder.Cleanup();
  });

  auto rc = disk_cache->Init([](const BlockKey&, const std::string&, bool) {});
  ASSERT_EQ(rc, BCACHE_ERROR::OK);

  auto key_100 = BlockKeyBuilder().Build(100);
  auto key_200 = BlockKeyBuilder().Build(200);
  auto block = BlockBuilder().Build(std::string(10, '0'));
  ASSERT_EQ(disk_cache->Stage(key_100, block), BCACHE_ERROR::OK);
  ASSERT_EQ(disk_cache->Stage(key_200, block), BCACHE_ERROR::OK);
  ASSERT_TRUE(disk_cache->IsCached(key_100));
  ASSERT_TRUE(disk_cache->IsCached(key_200));

  // NOTE: key_100, key_200 is in active, so we will evict key_300 firstly
  auto key_300 = BlockKeyBuilder().Build(300);
  ASSERT_EQ(disk_cache->Stage(key_300, block), BCACHE_ERROR::OK);
  ASSERT_TRUE(disk_cache->IsCached(key_100));
  ASSERT_TRUE(disk_cache->IsCached(key_200));
  ASSERT_FALSE(disk_cache->IsCached(key_300));
}

TEST_F(DiskCacheManagerTest, CleanupExpire) {
  FLAGS_disk_cache_expire_second = 3;
  auto builder = DiskCacheBuilder();
  auto disk_cache = builder.Build();
  auto defer = MakeCleanup([&]() {
    disk_cache->Shutdown();
    builder.Cleanup();
  });

  auto rc = disk_cache->Init([](const BlockKey&, const std::string&, bool) {});
  ASSERT_EQ(rc, BCACHE_ERROR::OK);

  auto key = BlockKeyBuilder().Build(100);
  auto block = BlockBuilder().Build(std::string(10, '0'));
  ASSERT_EQ(disk_cache->Stage(key, block), BCACHE_ERROR::OK);
  ASSERT_TRUE(disk_cache->IsCached(key));

  std::this_thread::sleep_for(std::chrono::seconds(5));
  ASSERT_FALSE(disk_cache->IsCached(key));
}

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs
