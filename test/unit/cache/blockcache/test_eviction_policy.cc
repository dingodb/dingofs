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

#include "cache/blockcache/eviction_policy.h"

namespace dingofs {
namespace cache {

constexpr uint64_t kMiB = 1ULL << 20;
constexpr uint64_t kBlockSize = 4 * kMiB;

// contract tests shared by every eviction policy implementation
class EvictionPolicyTest : public ::testing::TestWithParam<std::string> {
 protected:
  void SetUp() override {
    policy_ = NewEvictionPolicy(GetParam(), /*capacity_bytes=*/400 * kMiB);
  }

  static CacheKey Key(uint64_t id) { return BlockKey(1, 1, id, 1, 0); }

  static CacheValue Value() {
    return CacheValue(kBlockSize, iutil::TimeNow());
  }

  void AddN(uint64_t n, uint64_t first_id = 0) {
    for (uint64_t i = 0; i < n; i++) {
      policy_->Add(Key(first_id + i), Value());
    }
  }

  EvictionPolicyUPtr policy_;
};

TEST_P(EvictionPolicyTest, Basic) {
  ASSERT_EQ(policy_->Size(), 0);

  AddN(10);
  ASSERT_EQ(policy_->Size(), 10);
  ASSERT_TRUE(policy_->Exist(Key(0)));
  ASSERT_FALSE(policy_->Exist(Key(100)));

  CacheValue value;
  ASSERT_TRUE(policy_->Get(Key(0), &value));
  ASSERT_EQ(value.size, kBlockSize);

  CacheValue deleted;
  ASSERT_TRUE(policy_->Delete(Key(0), &deleted));
  ASSERT_EQ(deleted.size, kBlockSize);
  ASSERT_FALSE(policy_->Exist(Key(0)));
  ASSERT_FALSE(policy_->Delete(Key(0), &deleted));
  ASSERT_EQ(policy_->Size(), 9);

  policy_->Clear();
  ASSERT_EQ(policy_->Size(), 0);
}

TEST_P(EvictionPolicyTest, ExistAndGetHaveNoSideEffects) {
  AddN(10);

  CacheValue before, after;
  ASSERT_TRUE(policy_->Get(Key(3), &before));
  for (int i = 0; i < 100; i++) {  // heavy probing
    policy_->Exist(Key(3));
    policy_->Get(Key(3), &after);
  }
  ASSERT_TRUE(policy_->Get(Key(3), &after));
  ASSERT_EQ(after.freq, before.freq);
  ASSERT_EQ(after.atime, before.atime);
}

TEST_P(EvictionPolicyTest, TouchRefreshesAtimeAndFreq) {
  policy_->Add(Key(1), CacheValue(kBlockSize, iutil::TimeSpec(0, 0)));

  CacheValue value;
  ASSERT_TRUE(policy_->Touch(Key(1), &value));
  ASSERT_EQ(value.freq, 1);
  ASSERT_GT(value.atime, iutil::TimeSpec(0, 0));

  for (int i = 0; i < 10; i++) {
    policy_->Touch(Key(1), &value);
  }
  ASSERT_EQ(value.freq, 3);  // saturates
  ASSERT_FALSE(policy_->Touch(Key(100), &value));
}

TEST_P(EvictionPolicyTest, EvictHonorsFilter) {
  AddN(10);

  // evict exactly 4 blocks
  int evict = 4;
  auto evicted = policy_->Evict([&](const CacheValue&) {
    return (evict-- > 0) ? FilterStatus::kEvictIt : FilterStatus::kFinish;
  });
  ASSERT_EQ(evicted.size(), 4);
  ASSERT_EQ(policy_->Size(), 6);
  for (const auto& item : evicted) {
    ASSERT_FALSE(policy_->Exist(item.key));
    ASSERT_EQ(item.value.size, kBlockSize);
  }
}

TEST_P(EvictionPolicyTest, EvictTerminatesWhenAllSkipped) {
  AddN(10);

  size_t calls = 0;
  auto evicted = policy_->Evict([&](const CacheValue&) {
    calls++;
    return FilterStatus::kSkip;  // e.g. everything protected
  });
  ASSERT_TRUE(evicted.empty());
  ASSERT_EQ(policy_->Size(), 10);
  ASSERT_LE(calls, 2048 + 10);  // bounded, no livelock
}

TEST_P(EvictionPolicyTest, SweepVisitsEverythingWithoutReordering) {
  AddN(10);

  // sweep evicts stale entries only (here: none), touching nothing
  size_t visited = 0;
  auto evicted = policy_->Sweep([&](const CacheValue&) {
    visited++;
    return FilterStatus::kSkip;
  });
  ASSERT_TRUE(evicted.empty());
  ASSERT_EQ(visited, 10);

  // sweep can evict everything
  evicted = policy_->Sweep(
      [&](const CacheValue&) { return FilterStatus::kEvictIt; });
  ASSERT_EQ(evicted.size(), 10);
  ASSERT_EQ(policy_->Size(), 0);
}

TEST_P(EvictionPolicyTest, DuplicateAddRefreshesInPlace) {
  policy_->Add(Key(1), Value());
  policy_->Add(Key(1), Value());
  ASSERT_EQ(policy_->Size(), 1);

  auto evicted = policy_->Sweep(
      [&](const CacheValue&) { return FilterStatus::kEvictIt; });
  ASSERT_EQ(evicted.size(), 1);
}

INSTANTIATE_TEST_SUITE_P(AllPolicies, EvictionPolicyTest,
                         ::testing::Values("lru", "s3fifo", "2random"),
                         [](const auto& info) { return info.param; });

}  // namespace cache
}  // namespace dingofs
