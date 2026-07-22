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

#include "cache/blockcache/s3fifo_policy.h"

namespace dingofs {
namespace cache {

constexpr uint64_t kMiB = 1ULL << 20;
constexpr uint64_t kBlockSize = 4 * kMiB;
constexpr uint64_t kCapacity = 400 * kMiB;  // 100 blocks, small queue = 10

class S3FIFOPolicyTest : public ::testing::Test {
 protected:
  void SetUp() override {
    policy_ = std::make_unique<S3FIFOPolicy>(kCapacity, /*small_ratio=*/0.1);
    used_bytes_ = 0;
  }

  static CacheKey Key(uint64_t id) { return BlockKey(1, 1, id, 1, 0); }

  void Add(uint64_t id) {
    policy_->Add(Key(id), CacheValue(kBlockSize, iutil::TimeNow()));
    used_bytes_ += kBlockSize;
  }

  void Touch(uint64_t id) {
    CacheValue value;
    policy_->Touch(Key(id), &value);
  }

  // mimics DiskCacheManager::CleanupFull: evict down to the capacity goal
  void EvictToCapacity() {
    if (used_bytes_ < kCapacity) {
      return;
    }
    uint64_t want_free = used_bytes_ - kCapacity * 0.95;
    uint64_t freed = 0;
    auto evicted = policy_->Evict([&](const CacheValue& value) {
      if (freed >= want_free) {
        return FilterStatus::kFinish;
      }
      freed += value.size;
      return FilterStatus::kEvictIt;
    });
    used_bytes_ -= freed;
  }

  std::unique_ptr<S3FIFOPolicy> policy_;
  uint64_t used_bytes_;
};

TEST_F(S3FIFOPolicyTest, NewBlocksEnterSmallQueue) {
  for (uint64_t i = 0; i < 5; i++) {
    Add(i);
  }
  ASSERT_EQ(policy_->GetStats().small_bytes, 5 * kBlockSize);
}

TEST_F(S3FIFOPolicyTest, EvictedFromSmallGoesToGhostAndReadmitsToMain) {
  // fill the small queue past its quota (10 blocks)
  for (uint64_t i = 0; i < 12; i++) {
    Add(i);
  }

  // evict two blocks: victims are the oldest of small, remembered in ghost
  int evict = 2;
  auto evicted = policy_->Evict([&](const CacheValue&) {
    return (evict-- > 0) ? FilterStatus::kEvictIt : FilterStatus::kFinish;
  });
  ASSERT_EQ(evicted.size(), 2);
  ASSERT_EQ(evicted[0].key.id, 0);  // FIFO: oldest first
  ASSERT_EQ(evicted[1].key.id, 1);
  ASSERT_EQ(policy_->GetStats().ghost_entries, 2);

  // refetch block 0: proven worth, readmitted straight into main
  auto small_bytes = policy_->GetStats().small_bytes;
  Add(0);
  ASSERT_TRUE(policy_->Exist(Key(0)));
  ASSERT_EQ(policy_->GetStats().ghost_hits, 1);
  ASSERT_EQ(policy_->GetStats().small_bytes, small_bytes);  // not in small
}

TEST_F(S3FIFOPolicyTest, TouchedBlockIsPromotedNotEvicted) {
  for (uint64_t i = 0; i < 12; i++) {
    Add(i);
  }
  Touch(0);  // oldest block in small, but it has a hit

  int evict = 1;
  auto evicted = policy_->Evict([&](const CacheValue&) {
    return (evict-- > 0) ? FilterStatus::kEvictIt : FilterStatus::kFinish;
  });

  // block 0 was lazily promoted to main, block 1 (freq==0) was the victim
  ASSERT_EQ(evicted.size(), 1);
  ASSERT_EQ(evicted[0].key.id, 1);
  ASSERT_TRUE(policy_->Exist(Key(0)));
}

TEST_F(S3FIFOPolicyTest, ScanDoesNotFlushHotBlocks) {
  // hot set: 20 blocks, each read once after insertion
  for (uint64_t i = 0; i < 20; i++) {
    Add(i);
    Touch(i);
  }

  // one-pass sequential scan of 500 cold blocks under capacity pressure
  for (uint64_t i = 1000; i < 1500; i++) {
    Add(i);
    EvictToCapacity();
  }

  // every hot block survived the scan
  for (uint64_t i = 0; i < 20; i++) {
    ASSERT_TRUE(policy_->Exist(Key(i))) << "hot block " << i << " evicted";
  }
}

TEST_F(S3FIFOPolicyTest, MainBlockWithHitsIsReinserted) {
  // land blocks 0..2 in main via ghost readmission
  for (uint64_t i = 0; i < 11; i++) {
    Add(i);
  }
  int evict = 3;
  policy_->Evict([&](const CacheValue&) {
    return (evict-- > 0) ? FilterStatus::kEvictIt : FilterStatus::kFinish;
  });
  for (uint64_t i = 0; i < 3; i++) {
    Add(i);  // ghost hit -> main, FIFO order 0,1,2
  }
  Touch(0);  // only block 0 has a hit

  // small is now under quota, so victims come from main: block 0 is
  // reinserted on its round while the hitless 1 and 2 are evicted
  int rounds = 2;
  auto evicted = policy_->Evict([&](const CacheValue&) {
    return (rounds-- > 0) ? FilterStatus::kEvictIt : FilterStatus::kFinish;
  });
  ASSERT_EQ(evicted.size(), 2);
  ASSERT_EQ(evicted[0].key.id, 1);
  ASSERT_EQ(evicted[1].key.id, 2);
  ASSERT_TRUE(policy_->Exist(Key(0)));
}

}  // namespace cache
}  // namespace dingofs
