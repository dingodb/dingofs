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
 * Created Date: 2026-07-22
 * Author: Jingli Chen (Wine93)
 */

#include <gtest/gtest.h>

#include <filesystem>

#include "cache/blockcache/disk_cache_layout.h"
#include "cache/blockcache/disk_cache_manager.h"
#include "utils/uuid.h"

namespace dingofs {
namespace cache {

constexpr uint64_t kMiB = 1ULL << 20;
constexpr uint64_t kBlockSize = 4 * kMiB;
constexpr uint64_t kCapacity = 40 * kMiB;  // 10 blocks

class DiskCacheManagerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    root_dir_ = "/tmp/test_disk_cache_manager." + utils::GenerateUUID();
    auto layout = std::make_shared<DiskCacheLayout>(0, root_dir_);
    std::filesystem::create_directories(layout->GetRootDir());
    std::filesystem::create_directories(layout->GetStageDir());
    std::filesystem::create_directories(layout->GetCacheDir());

    manager_ = std::make_shared<DiskCacheManager>(kCapacity, layout);
    manager_->Start();
  }

  void TearDown() override {
    manager_->Shutdown();
    std::filesystem::remove_all(root_dir_);
  }

  static CacheKey Key(uint64_t id) { return BlockKey(1, 1, id, 1, 0); }

  static CacheValue Value() {
    return CacheValue(kBlockSize, iutil::TimeNow());
  }

  size_t CountCached(uint64_t first_id, uint64_t n) {
    size_t count = 0;
    for (uint64_t i = 0; i < n; i++) {
      count += manager_->Exist(Key(first_id + i)) ? 1 : 0;
    }
    return count;
  }

  std::string root_dir_;
  DiskCacheManagerSPtr manager_;
};

TEST_F(DiskCacheManagerTest, CapacityEvictionKeepsUsageBounded) {
  // add 2x capacity worth of blocks: the inline capacity trigger must keep
  // the resident set at or below capacity throughout
  const uint64_t blocks = 2 * kCapacity / kBlockSize;
  for (uint64_t i = 0; i < blocks; i++) {
    manager_->Add(Key(i), Value(), BlockPhase::kCached);
  }
  ASSERT_LE(CountCached(0, blocks) * kBlockSize, kCapacity);
  ASSERT_GT(CountCached(0, blocks), 0);
}

TEST_F(DiskCacheManagerTest, StagingBlocksAreNeverEvicted) {
  manager_->Add(Key(1000), Value(), BlockPhase::kStaging);

  // overflow the cache with cached blocks to force eviction rounds
  const uint64_t blocks = 3 * kCapacity / kBlockSize;
  for (uint64_t i = 0; i < blocks; i++) {
    manager_->Add(Key(i), Value(), BlockPhase::kCached);
  }

  // the staging block survives every round: not uploaded yet, deleting it
  // would lose data
  ASSERT_TRUE(manager_->Exist(Key(1000)));
}

TEST_F(DiskCacheManagerTest, UploadedPromotionKeepsAccounting) {
  manager_->Add(Key(1), Value(), BlockPhase::kStaging);
  ASSERT_TRUE(manager_->Exist(Key(1)));

  // promotion moves it from the staging map into the eviction policy
  manager_->Add(Key(1), CacheValue(), BlockPhase::kUploaded);
  ASSERT_TRUE(manager_->Exist(Key(1)));

  manager_->Delete(Key(1));
  ASSERT_FALSE(manager_->Exist(Key(1)));
}

TEST_F(DiskCacheManagerTest, TouchAndExistContract) {
  manager_->Add(Key(1), Value(), BlockPhase::kCached);

  // heavy probing then a touch: neither crashes nor evicts
  for (int i = 0; i < 100; i++) {
    ASSERT_TRUE(manager_->Exist(Key(1)));
  }
  manager_->Touch(Key(1));
  manager_->Touch(Key(404));  // unknown key is a no-op
  ASSERT_TRUE(manager_->Exist(Key(1)));
}

}  // namespace cache
}  // namespace dingofs
