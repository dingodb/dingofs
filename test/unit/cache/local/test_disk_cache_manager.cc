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

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "cache/iutil/time_util.h"
#include "cache/local/disk_cache_layout.h"
#include "cache/local/disk_cache_manager.h"
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

  // Returns `n` keys whose Filename() hashes to the same shard, so tests that
  // need a deterministic per-shard LRU eviction order can opt in.
  static std::vector<CacheKey> SameShardKeys(size_t n) {
    std::vector<std::vector<CacheKey>> buckets(DiskCacheManager::kShardCount);
    for (uint64_t id = 1; id < 1000000; ++id) {
      auto key = Key(id);
      auto& bucket = buckets[DiskCacheManager::ShardIndex(key.Filename())];
      bucket.push_back(key);
      if (bucket.size() >= n) {
        bucket.resize(n);
        return bucket;
      }
    }
    CHECK(false) << "could not find " << n << " keys in one shard";
    return {};
  }

  // Returns a key that maps to a different shard than `other`.
  static CacheKey KeyInOtherShard(const CacheKey& other) {
    auto avoid = DiskCacheManager::ShardIndex(other.Filename());
    for (uint64_t id = 1; id < 1000000; ++id) {
      auto key = Key(id);
      if (DiskCacheManager::ShardIndex(key.Filename()) != avoid) {
        return key;
      }
    }
    CHECK(false) << "could not find a key in a different shard";
    return Key(0);
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

TEST_F(DiskCacheManagerTest, PerShardCapacityTriggersEviction) {
  // Eviction is now per-shard: capacity is split into kShardCount shards. Scale
  // the whole-disk capacity so each shard holds 1000 bytes, and drive three
  // same-shard 400B blocks into one shard.
  constexpr uint64_t kShardCap = 1000;
  DiskCacheManager manager(kShardCap * DiskCacheManager::kShardCount, layout_);
  manager.Start();

  auto keys = SameShardKeys(3);

  // No Exist() probing before the trigger: Exist() calls LRUCache::Get(), which
  // promotes the entry to the active list and would change the eviction victim.
  manager.Add(keys[0], Val(400), BlockPhase::kCached);
  manager.Add(keys[1], Val(400), BlockPhase::kCached);

  // shard used (1200) >= shard capacity (1000) trips CleanupFull on this shard.
  manager.Add(keys[2], Val(400), BlockPhase::kCached);

  EXPECT_FALSE(manager.Exist(keys[0]));  // oldest in the shard, evicted
  EXPECT_TRUE(manager.Exist(keys[1]));
  EXPECT_TRUE(manager.Exist(keys[2]));

  manager.Shutdown();
}

TEST_F(DiskCacheManagerTest, EvictionIsPerShardIndependent) {
  // Filling one shard must not evict blocks living in another shard.
  constexpr uint64_t kShardCap = 1000;
  DiskCacheManager manager(kShardCap * DiskCacheManager::kShardCount, layout_);
  manager.Start();

  auto keys = SameShardKeys(3);            // all land on the same shard X
  auto other = KeyInOtherShard(keys[0]);   // lands on a different shard Y

  manager.Add(other, Val(400), BlockPhase::kCached);
  manager.Add(keys[0], Val(400), BlockPhase::kCached);
  manager.Add(keys[1], Val(400), BlockPhase::kCached);
  manager.Add(keys[2], Val(400), BlockPhase::kCached);  // X over quota: evict X

  EXPECT_FALSE(manager.Exist(keys[0]));  // evicted within shard X
  EXPECT_TRUE(manager.Exist(keys[1]));
  EXPECT_TRUE(manager.Exist(keys[2]));
  EXPECT_TRUE(manager.Exist(other));     // shard Y untouched

  manager.Shutdown();
}

TEST_F(DiskCacheManagerTest, StagingBlockSurvivesShardEviction) {
  // A staging block counts toward the shard's used bytes but must never be
  // evicted by CleanupFull; only cached blocks are eligible victims.
  constexpr uint64_t kShardCap = 1000;
  DiskCacheManager manager(kShardCap * DiskCacheManager::kShardCount, layout_);
  manager.Start();

  auto keys = SameShardKeys(3);

  manager.Add(keys[0], Val(400), BlockPhase::kStaging);
  manager.Add(keys[1], Val(400), BlockPhase::kCached);
  manager.Add(keys[2], Val(400), BlockPhase::kCached);  // shard over quota

  EXPECT_TRUE(manager.Exist(keys[0]));   // staging: never evicted
  EXPECT_FALSE(manager.Exist(keys[1]));  // oldest cached: evicted
  EXPECT_TRUE(manager.Exist(keys[2]));

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
  constexpr uint64_t kShardCap = 1000;
  DiskCacheManager manager(kShardCap * DiskCacheManager::kShardCount, layout_);
  manager.Start();

  auto keys = SameShardKeys(3);

  // Pre-create the on-disk file for the block that will be evicted, so the
  // async DeleteBlocks path actually unlinks a real file.
  auto victim_path = layout_->GetCachePath(keys[0]);
  std::filesystem::create_directories(
      std::filesystem::path(victim_path).parent_path());
  std::ofstream(victim_path) << "block-data";
  ASSERT_TRUE(std::filesystem::exists(victim_path));

  manager.Add(keys[0], Val(400), BlockPhase::kCached);
  manager.Add(keys[1], Val(400), BlockPhase::kCached);
  manager.Add(keys[2], Val(400), BlockPhase::kCached);  // evicts LRU keys[0]

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

TEST_F(DiskCacheManagerTest, ShardIndexIsStableAndDistributes) {
  {  // stable: the same filename always maps to the same shard
    auto key = Key(42);
    EXPECT_EQ(DiskCacheManager::ShardIndex(key.Filename()),
              DiskCacheManager::ShardIndex(key.Filename()));
  }

  {  // in range and spreads across more than one shard over many keys
    std::set<size_t> shards;
    for (uint64_t id = 1; id <= 1000; ++id) {
      auto shard = DiskCacheManager::ShardIndex(Key(id).Filename());
      EXPECT_LT(shard, DiskCacheManager::kShardCount);
      shards.insert(shard);
    }
    EXPECT_GT(shards.size(), 1u);
  }
}

TEST_F(DiskCacheManagerTest, ConcurrentAddDeleteExistIsSafe) {
  // Large capacity so no eviction interferes; each thread owns a disjoint key
  // range, making the final state deterministic. Exercises the per-shard locks
  // under concurrent Add/Exist/Delete (run under TSan to catch data races).
  DiskCacheManager manager(1024ULL * 1024 * 1024, layout_);
  manager.Start();

  constexpr int kThreads = 8;
  constexpr int kKeysPerThread = 200;
  std::vector<std::thread> workers;
  workers.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    workers.emplace_back([&, t] {
      for (int i = 0; i < kKeysPerThread; ++i) {
        auto key = Key((t * kKeysPerThread) + i);
        manager.Add(key, Val(4096), BlockPhase::kCached);
        manager.Exist(key);
        if (i % 2 == 0) {
          manager.Delete(key);
        }
      }
    });
  }
  for (auto& w : workers) {
    w.join();
  }

  for (int t = 0; t < kThreads; ++t) {
    for (int i = 0; i < kKeysPerThread; ++i) {
      auto key = Key((t * kKeysPerThread) + i);
      EXPECT_EQ(manager.Exist(key), i % 2 != 0);  // even keys were deleted
    }
  }

  manager.Shutdown();
}

}  // namespace cache
}  // namespace dingofs
