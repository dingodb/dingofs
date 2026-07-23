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
 * Created Date: 2026-07-23
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

#include "cache/local/disk_cache_layout.h"
#include "cache/local/disk_cache_manager.h"
#include "common/block/block_handle.h"
#include "common/block/block_key.h"
#include "common/options/cache.h"

namespace dingofs {
namespace cache {

class DiskCacheManagerTest : public ::testing::Test {
 protected:
  static constexpr uint32_t kAtime = 1000000;  // fixed access time for tests

  void SetUp() override {
    static int seq = 0;
    cache_index_ = 100 + (seq++);
    root_dir_ = "/tmp/dingofs_test_disk_cache_manager_" +
                std::to_string(getpid()) + "_" + std::to_string(cache_index_);
    std::filesystem::create_directories(root_dir_);
    layout_ = std::make_shared<DiskCacheLayout>(cache_index_, root_dir_);
    // Disable the whole-disk free-space watchdog so the per-shard capacity
    // tests are hermetic regardless of how full the real /tmp filesystem is.
    saved_free_space_ratio_ = FLAGS_free_space_ratio;
    FLAGS_free_space_ratio = 0.0;
  }

  void TearDown() override {
    FLAGS_free_space_ratio = saved_free_space_ratio_;
    std::filesystem::remove_all(root_dir_);
  }

  static BlockHandle Key(uint64_t id) {
    return BlockHandle(1, BlockKey(id, 0, 4194304));
  }

  static void Cache(DiskCacheManager& m, const BlockHandle& k,
                    uint32_t size = 4096) {
    m.AddCached(k, size, kAtime);
  }
  static void Stage(DiskCacheManager& m, const BlockHandle& k,
                    uint32_t size = 4096) {
    m.AddStaging(k, size, kAtime);
  }

  // Returns `n` keys whose Hash() maps to the same shard, so tests that need a
  // deterministic per-shard eviction order can opt in.
  static std::vector<BlockHandle> SameShardKeys(size_t n) {
    std::vector<std::vector<BlockHandle>> buckets(DiskCacheManager::kShardCount);
    for (uint64_t id = 1; id < 1000000; ++id) {
      auto key = Key(id);
      auto& bucket = buckets[DiskCacheManager::ShardIndex(key)];
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
  static BlockHandle KeyInOtherShard(const BlockHandle& other) {
    auto avoid = DiskCacheManager::ShardIndex(other);
    for (uint64_t id = 1; id < 1000000; ++id) {
      auto key = Key(id);
      if (DiskCacheManager::ShardIndex(key) != avoid) {
        return key;
      }
    }
    CHECK(false) << "could not find a key in a different shard";
    return Key(0);
  }

  int cache_index_;
  std::string root_dir_;
  DiskCacheLayoutSPtr layout_;
  double saved_free_space_ratio_{0.1};
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

  Stage(manager, Key(1));
  EXPECT_TRUE(manager.Exist(Key(1)));

  Cache(manager, Key(2));
  EXPECT_TRUE(manager.Exist(Key(2)));

  EXPECT_FALSE(manager.Exist(Key(3)));
}

TEST_F(DiskCacheManagerTest, WarmCacheMatchesAcrossFsId) {
  // The loader registers cache blocks with fs_id 0 (the on-disk cache path has
  // no fs_id); a runtime read uses the real fs_id. They must resolve to the
  // same index entry, else the warm cache is defeated after every restart.
  DiskCacheManager manager(100 * 1024 * 1024, layout_);
  BlockHandle from_loader(0, BlockKey(10, 0, 4194304));
  BlockHandle from_runtime(7, BlockKey(10, 0, 4194304));

  manager.AddCached(from_loader, 4096, kAtime);
  EXPECT_TRUE(manager.Exist(from_runtime));
}

TEST_F(DiskCacheManagerTest, PromoteStagingToCached) {
  DiskCacheManager manager(100 * 1024 * 1024, layout_);

  Stage(manager, Key(1));
  EXPECT_TRUE(manager.Exist(Key(1)));

  // Upload completion transitions the block from pinned staging into the cached
  // eviction order.
  manager.PromoteStagingToCached(Key(1));
  EXPECT_TRUE(manager.Exist(Key(1)));

  // Now that it is cached, Delete removes it.
  manager.DeleteCached(Key(1));
  EXPECT_FALSE(manager.Exist(Key(1)));
}

TEST_F(DiskCacheManagerTest, DeleteOnlyAffectsCachedBlocks) {
  DiskCacheManager manager(100 * 1024 * 1024, layout_);

  {  // a cached block can be deleted
    Cache(manager, Key(1));
    manager.DeleteCached(Key(1));
    EXPECT_FALSE(manager.Exist(Key(1)));
  }

  {  // a staging block is intentionally NOT removed by Delete (it must stay
     // until uploaded, otherwise it is lost from both disk and storage)
    Stage(manager, Key(2));
    manager.DeleteCached(Key(2));
    EXPECT_TRUE(manager.Exist(Key(2)));
  }
}

TEST_F(DiskCacheManagerTest, AddCachedDoesNotUnpinStaging) {
  DiskCacheManager manager(100 * 1024 * 1024, layout_);
  auto key = Key(1);

  Stage(manager, key);
  Cache(manager, key);  // must not turn the pinned staging block evictable
  manager.DeleteCached(key);
  EXPECT_TRUE(manager.Exist(key));

  manager.PromoteStagingToCached(key);
  manager.DeleteCached(key);
  EXPECT_FALSE(manager.Exist(key));
}

TEST_F(DiskCacheManagerTest, PerShardCapacityTriggersEviction) {
  // Eviction is per-shard: capacity is split into kShardCount shards. Scale the
  // whole-disk capacity so each shard holds 1000 bytes, and drive three
  // same-shard 400B blocks into one shard.
  constexpr uint64_t kShardCap = 1000;
  DiskCacheManager manager(kShardCap * DiskCacheManager::kShardCount, layout_);
  manager.Start();

  auto keys = SameShardKeys(3);

  // No Exist() probing before the trigger: a cached Exist() promotes the entry
  // in the LRU order and would change the eviction victim.
  Cache(manager, keys[0], 400);
  Cache(manager, keys[1], 400);

  // shard used (1200) >= shard capacity (1000) trips CleanupFull on this shard.
  Cache(manager, keys[2], 400);

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

  auto keys = SameShardKeys(3);           // all land on the same shard X
  auto other = KeyInOtherShard(keys[0]);  // lands on a different shard Y

  Cache(manager, other, 400);
  Cache(manager, keys[0], 400);
  Cache(manager, keys[1], 400);
  Cache(manager, keys[2], 400);  // X over quota: evict X

  EXPECT_FALSE(manager.Exist(keys[0]));  // evicted within shard X
  EXPECT_TRUE(manager.Exist(keys[1]));
  EXPECT_TRUE(manager.Exist(keys[2]));
  EXPECT_TRUE(manager.Exist(other));  // shard Y untouched

  manager.Shutdown();
}

TEST_F(DiskCacheManagerTest, StagingBlockSurvivesShardEviction) {
  // A staging block counts toward the shard's used bytes but must never be
  // evicted by CleanupFull; only cached blocks are eligible victims.
  constexpr uint64_t kShardCap = 1000;
  DiskCacheManager manager(kShardCap * DiskCacheManager::kShardCount, layout_);
  manager.Start();

  auto keys = SameShardKeys(3);

  Stage(manager, keys[0], 400);
  Cache(manager, keys[1], 400);
  Cache(manager, keys[2], 400);  // shard over quota

  EXPECT_TRUE(manager.Exist(keys[0]));   // staging: never evicted
  EXPECT_FALSE(manager.Exist(keys[1]));  // oldest cached: evicted
  EXPECT_TRUE(manager.Exist(keys[2]));

  manager.Shutdown();
}

TEST_F(DiskCacheManagerTest, NonePolicyNeverEvicts) {
  // Pin mode: a shard may exceed capacity without any eviction.
  constexpr uint64_t kShardCap = 1000;
  DiskCacheManager manager(kShardCap * DiskCacheManager::kShardCount, layout_,
                           "none");
  manager.Start();

  auto keys = SameShardKeys(4);
  for (const auto& key : keys) {
    Cache(manager, key, 400);  // 1600 bytes into a 1000-byte shard
  }

  for (const auto& key : keys) {
    EXPECT_TRUE(manager.Exist(key));  // nothing evicted under "none"
  }

  manager.Shutdown();
}

TEST_F(DiskCacheManagerTest, EvictionWorksForEachPolicy) {
  constexpr uint64_t kShardCap = 1000;
  for (const std::string& policy : {"lru", "sieve", "s3fifo", "2random"}) {
    DiskCacheManager manager(kShardCap * DiskCacheManager::kShardCount, layout_,
                             policy);
    manager.Start();

    auto keys = SameShardKeys(4);  // 4 * 400 = 1600 into a 1000-byte shard
    for (const auto& key : keys) {
      Cache(manager, key, 400);
    }

    int present = 0;
    for (const auto& key : keys) {
      present += manager.Exist(key) ? 1 : 0;
    }
    EXPECT_LT(present, 4) << "policy=" << policy;  // some were evicted
    EXPECT_GT(present, 0) << "policy=" << policy;  // but not all

    manager.Shutdown();
  }
}

TEST_F(DiskCacheManagerTest, DeleteNonExistentKeyIsNoop) {
  DiskCacheManager manager(100 * 1024 * 1024, layout_);
  Cache(manager, Key(1));

  manager.DeleteCached(Key(2));  // not present: must not touch existing blocks
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

  Cache(manager, keys[0], 400);
  Cache(manager, keys[1], 400);
  Cache(manager, keys[2], 400);  // evicts LRU keys[0]

  bool deleted = false;
  for (int i = 0; i < 500 && !deleted; ++i) {
    deleted = !std::filesystem::exists(victim_path);
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  EXPECT_TRUE(deleted);

  manager.Shutdown();
}

TEST_F(DiskCacheManagerTest, StagingHardLinkSurvivesEvictionDeleteTask) {
  constexpr uint64_t kShardCap = 1000;
  DiskCacheManager manager(kShardCap * DiskCacheManager::kShardCount, layout_);
  manager.Start();

  auto keys = SameShardKeys(3);
  auto stage_path = layout_->GetStagePath(keys[0]);
  auto cache_path = layout_->GetCachePath(keys[0]);
  std::filesystem::create_directories(
      std::filesystem::path(stage_path).parent_path());
  std::filesystem::create_directories(
      std::filesystem::path(cache_path).parent_path());
  std::ofstream(stage_path) << "stage-data";
  std::filesystem::create_hard_link(stage_path, cache_path);

  auto victim_path = layout_->GetCachePath(keys[1]);
  std::filesystem::create_directories(
      std::filesystem::path(victim_path).parent_path());
  std::ofstream(victim_path) << "victim-data";

  Stage(manager, keys[0], 400);
  Cache(manager, keys[1], 400);
  Cache(manager, keys[2], 400);  // evicts cached keys[1]

  bool victim_deleted = false;
  for (int i = 0; i < 500 && !victim_deleted; ++i) {
    victim_deleted = !std::filesystem::exists(victim_path);
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  EXPECT_TRUE(victim_deleted);
  EXPECT_TRUE(std::filesystem::exists(stage_path));
  EXPECT_TRUE(std::filesystem::exists(cache_path));

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
  // PromoteStagingToCached requires the block to already be staging.
  EXPECT_DEATH(manager.PromoteStagingToCached(Key(1)), "");
}

TEST_F(DiskCacheManagerTest, ShardIndexIsStableAndDistributes) {
  {  // stable: the same handle always maps to the same shard
    auto key = Key(42);
    EXPECT_EQ(DiskCacheManager::ShardIndex(key),
              DiskCacheManager::ShardIndex(key));
  }

  {  // in range and spreads across more than one shard over many keys
    std::set<size_t> shards;
    for (uint64_t id = 1; id <= 1000; ++id) {
      auto shard = DiskCacheManager::ShardIndex(Key(id));
      EXPECT_LT(shard, DiskCacheManager::kShardCount);
      shards.insert(shard);
    }
    EXPECT_GT(shards.size(), 1u);
  }
}

TEST_F(DiskCacheManagerTest, ExpiredBlocksAreReaped) {
  // The background CleanupExpire thread must evict blocks past their TTL. Use a
  // short expiry + interval and a block whose atime is far in the past, then
  // wait (without probing, since Exist refreshes atime) and check once.
  uint32_t saved_expire = FLAGS_cache_expire_s;
  uint32_t saved_interval = FLAGS_cache_cleanup_expire_interval_ms;
  FLAGS_cache_expire_s = 1;
  FLAGS_cache_cleanup_expire_interval_ms = 20;

  DiskCacheManager manager(100 * 1024 * 1024, layout_);
  manager.Start();
  manager.AddCached(Key(1), 4096, /*atime_sec=*/1);  // atime far in the past

  std::this_thread::sleep_for(std::chrono::milliseconds(400));  // let it reap
  EXPECT_FALSE(manager.Exist(Key(1)));

  manager.Shutdown();
  FLAGS_cache_expire_s = saved_expire;
  FLAGS_cache_cleanup_expire_interval_ms = saved_interval;
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
        Cache(manager, key);
        manager.Exist(key);
        if (i % 2 == 0) {
          manager.DeleteCached(key);
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
