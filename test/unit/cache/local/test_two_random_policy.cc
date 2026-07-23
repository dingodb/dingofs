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

#include "cache/local/two_random_policy.h"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "cache/local/cache_entry.h"

namespace dingofs {
namespace cache {

class TwoRandomPolicyTest : public ::testing::Test {
 protected:
  CacheEntry* MakeEntry(uint32_t size, uint32_t atime = 0) {
    entries_.push_back(std::make_unique<CacheEntry>());
    CacheEntry* e = entries_.back().get();
    e->size = size;
    e->atime.store(atime);
    e->staged = false;
    return e;
  }

  std::vector<std::unique_ptr<CacheEntry>> entries_;
};

TEST_F(TwoRandomPolicyTest, EvictsOlderOfTwoSamples) {
  // With exactly two entries the two random samples are both entries, so the
  // older one is deterministically evicted.
  TwoRandomPolicy p;
  auto* older = MakeEntry(100, /*atime=*/10);
  auto* newer = MakeEntry(100, /*atime=*/100);
  p.OnInsert(older);
  p.OnInsert(newer);

  CacheVictims victims;
  p.Evict(100, 0, &victims);
  ASSERT_EQ(victims.size(), 1u);
  EXPECT_EQ(victims[0], older);
}

TEST_F(TwoRandomPolicyTest, EvictHonorsByteAndFileGoals) {
  TwoRandomPolicy p;
  for (int i = 0; i < 5; ++i) {
    p.OnInsert(MakeEntry(100, i));
  }

  CacheVictims victims;
  p.Evict(/*want_bytes=*/300, /*want_files=*/0, &victims);
  EXPECT_EQ(victims.size(), 3u);
}

TEST_F(TwoRandomPolicyTest, EvictStopsWhenEmpty) {
  TwoRandomPolicy p;
  p.OnInsert(MakeEntry(100, 0));
  p.OnInsert(MakeEntry(100, 0));

  CacheVictims victims;
  p.Evict(1'000'000, 0, &victims);
  EXPECT_EQ(victims.size(), 2u);
}

TEST_F(TwoRandomPolicyTest, OnEraseRemoves) {
  TwoRandomPolicy p;
  auto* e1 = MakeEntry(100, 0);
  auto* e2 = MakeEntry(100, 0);
  auto* e3 = MakeEntry(100, 0);
  p.OnInsert(e1);
  p.OnInsert(e2);
  p.OnInsert(e3);

  p.OnErase(e2);  // swap-remove from the middle

  CacheVictims victims;
  p.Evict(1'000'000, 0, &victims);
  EXPECT_EQ(victims.size(), 2u);  // e2 already gone
  for (auto* v : victims) {
    EXPECT_NE(v, e2);
  }
}

TEST_F(TwoRandomPolicyTest, EvictExpiredDropsStaleKeepsFresh) {
  TwoRandomPolicy p;
  std::vector<CacheEntry*> stale;
  for (int i = 0; i < 3; ++i) {
    auto* e = MakeEntry(100, /*atime=*/100);
    p.OnInsert(e);
    stale.push_back(e);
  }
  for (int i = 0; i < 2; ++i) {
    p.OnInsert(MakeEntry(100, /*atime=*/990));  // fresh
  }

  CacheVictims victims;
  p.EvictExpired(/*now_sec=*/1000, /*expire_sec=*/100, /*budget=*/100, &victims);
  EXPECT_EQ(victims.size(), 3u);  // exactly the three stale entries
}

}  // namespace cache
}  // namespace dingofs
