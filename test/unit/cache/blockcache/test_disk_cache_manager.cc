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

#include "cache/blockcache/disk_cache_manager.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <thread>

#include "cache/blockcache/disk_cache_layout.h"
#include "cache/iutil/time_util.h"
#include "common/block/block_handle.h"
#include "common/block/block_key.h"

namespace dingofs {
namespace cache {

class DiskCacheManagerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    static int seq = 0;
    cache_index_ = 100 + (seq++);
    root_dir_ = "/tmp/dingofs_test_disk_cache_manager_" +
                std::to_string(getpid()) + "_" + std::to_string(cache_index_);
    std::filesystem::create_directories(root_dir_);
    layout_ = std::make_shared<DiskCacheLayout>(cache_index_, root_dir_);
  }

  void TearDown() override { std::filesystem::remove_all(root_dir_); }

  static CacheKey Key(uint64_t id) {
    return BlockHandle(1, BlockKey(id, 0, 4194304));
  }
  static CacheValue Val(size_t size) {
    return CacheValue(size, iutil::TimeNow());
  }

  int cache_index_;
  std::string root_dir_;
  DiskCacheLayoutSPtr layout_;
};

TEST_F(DiskCacheManagerTest, StartAndShutdownIdempotent) {
  DiskCacheManager manager(100 * 1024 * 1024, layout_);
  manager.Start();
  manager.Start();  // already running
  manager.Shutdown();
  manager.Shutdown();  // already down
}

TEST_F(DiskCacheManagerTest, AddAndExist) {
  DiskCacheManager manager(100 * 1024 * 1024, layout_);

  EXPECT_FALSE(manager.Exist(Key(1)));

  manager.Add(Key(1), Val(4096), BlockPhase::kStaging);
  EXPECT_TRUE(manager.Exist(Key(1)));

  manager.Add(Key(2), Val(4096), BlockPhase::kCached);
  EXPECT_TRUE(manager.Exist(Key(2)));

  EXPECT_FALSE(manager.Exist(Key(3)));
}

TEST_F(DiskCacheManagerTest, StagingToUploaded) {
  DiskCacheManager manager(100 * 1024 * 1024, layout_);

  manager.Add(Key(1), Val(4096), BlockPhase::kStaging);
  EXPECT_TRUE(manager.Exist(Key(1)));

  // Upload transitions the block from the staging map into the cached LRU.
  manager.Add(Key(1), Val(4096), BlockPhase::kUploaded);
  EXPECT_TRUE(manager.Exist(Key(1)));

  // Now that it is cached, Delete removes it.
  manager.Delete(Key(1));
  EXPECT_FALSE(manager.Exist(Key(1)));
}

TEST_F(DiskCacheManagerTest, DeleteOnlyAffectsCachedBlocks) {
  DiskCacheManager manager(100 * 1024 * 1024, layout_);

  {  // a cached block can be deleted
    manager.Add(Key(1), Val(4096), BlockPhase::kCached);
    manager.Delete(Key(1));
    EXPECT_FALSE(manager.Exist(Key(1)));
  }

  {  // a staging block is intentionally NOT removed by Delete (it must stay
     // until uploaded, otherwise it is lost from both disk and storage)
    manager.Add(Key(2), Val(4096), BlockPhase::kStaging);
    manager.Delete(Key(2));
    EXPECT_TRUE(manager.Exist(Key(2)));
  }
}

TEST_F(DiskCacheManagerTest, CapacityTriggersEviction) {
  // Started so the async delete queue exists; the evicted block files do not
  // exist on disk, so the background delete simply logs NotFound.
  DiskCacheManager manager(1000, layout_);
  manager.Start();

  // No Exist() probing before the trigger: Exist() calls LRUCache::Get(), which
  // promotes the entry to the active list and would change the eviction victim.
  manager.Add(Key(1), Val(400), BlockPhase::kCached);
  manager.Add(Key(2), Val(400), BlockPhase::kCached);

  // used (1200) >= capacity (1000) trips CleanupFull, evicting the LRU block.
  manager.Add(Key(3), Val(400), BlockPhase::kCached);

  EXPECT_FALSE(manager.Exist(Key(1)));  // oldest, evicted
  EXPECT_TRUE(manager.Exist(Key(2)));
  EXPECT_TRUE(manager.Exist(Key(3)));

  manager.Shutdown();
}

TEST_F(DiskCacheManagerTest, DeleteNonExistentKeyIsNoop) {
  DiskCacheManager manager(100 * 1024 * 1024, layout_);
  manager.Add(Key(1), Val(4096), BlockPhase::kCached);

  manager.Delete(Key(2));  // not present: must not touch the existing block
  EXPECT_TRUE(manager.Exist(Key(1)));
  EXPECT_FALSE(manager.Exist(Key(2)));
}

TEST_F(DiskCacheManagerTest, EvictionDeletesRealCacheFiles) {
  DiskCacheManager manager(1000, layout_);
  manager.Start();

  // Pre-create the on-disk file for the block that will be evicted, so the
  // async DeleteBlocks path actually unlinks a real file.
  auto victim_path = layout_->GetCachePath(Key(1));
  std::filesystem::create_directories(
      std::filesystem::path(victim_path).parent_path());
  std::ofstream(victim_path) << "block-data";
  ASSERT_TRUE(std::filesystem::exists(victim_path));

  manager.Add(Key(1), Val(400), BlockPhase::kCached);
  manager.Add(Key(2), Val(400), BlockPhase::kCached);
  manager.Add(Key(3), Val(400), BlockPhase::kCached);  // evicts LRU Key(1)

  bool deleted = false;
  for (int i = 0; i < 500 && !deleted; ++i) {
    deleted = !std::filesystem::exists(victim_path);
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  EXPECT_TRUE(deleted);

  manager.Shutdown();
}

TEST_F(DiskCacheManagerTest, StageAndCacheNotFullByDefault) {
  DiskCacheManager manager(100 * 1024 * 1024, layout_);
  EXPECT_FALSE(manager.StageFull());
  EXPECT_FALSE(manager.CacheFull());
}

TEST_F(DiskCacheManagerTest, UploadWithoutStagingDies) {
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  DiskCacheManager manager(100 * 1024 * 1024, layout_);
  // kUploaded requires the block to already be in the staging map.
  EXPECT_DEATH(manager.Add(Key(1), Val(4096), BlockPhase::kUploaded), "");
}

}  // namespace cache
}  // namespace dingofs
