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

#include "cache/local/lru_policy.h"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "cache/local/cache_entry.h"

namespace dingofs {
namespace cache {

class LruPolicyTest : public ::testing::Test {
 protected:
  CacheEntry* MakeEntry(uint32_t size, uint32_t atime) {
    entries_.push_back(std::make_unique<CacheEntry>());
    CacheEntry* e = entries_.back().get();
    e->size = size;
    e->atime.store(atime);
    e->staged = false;
    return e;
  }

  std::vector<std::unique_ptr<CacheEntry>> entries_;
};

TEST_F(LruPolicyTest, EvictsOldestFirst) {
  LruPolicy p;
  auto* e1 = MakeEntry(100, 0);
  auto* e2 = MakeEntry(100, 0);
  auto* e3 = MakeEntry(100, 0);
  p.OnInsert(e1);
  p.OnInsert(e2);
  p.OnInsert(e3);

  CacheVictims victims;
  p.Evict(/*want_bytes=*/100, /*want_files=*/0, &victims);
  ASSERT_EQ(victims.size(), 1u);
  EXPECT_EQ(victims[0], e1);  // oldest inserted
}

TEST_F(LruPolicyTest, AccessMovesToNewest) {
  LruPolicy p;
  auto* e1 = MakeEntry(100, 0);
  auto* e2 = MakeEntry(100, 0);
  auto* e3 = MakeEntry(100, 0);
  p.OnInsert(e1);
  p.OnInsert(e2);
  p.OnInsert(e3);

  p.OnAccess(e1);  // e1 becomes most-recently-used; e2 is now the oldest

  CacheVictims victims;
  p.Evict(100, 0, &victims);
  ASSERT_EQ(victims.size(), 1u);
  EXPECT_EQ(victims[0], e2);
}

TEST_F(LruPolicyTest, EvictHonorsByteAndFileGoals) {
  LruPolicy p;
  for (int i = 0; i < 5; ++i) {
    p.OnInsert(MakeEntry(100, 0));
  }

  {  // byte goal dominates: need 250 bytes -> 3 blocks of 100
    CacheVictims victims;
    p.Evict(/*want_bytes=*/250, /*want_files=*/0, &victims);
    EXPECT_EQ(victims.size(), 3u);
  }
  {  // file goal dominates: 0 bytes wanted but 2 files -> 2 blocks
    CacheVictims victims;
    p.Evict(/*want_bytes=*/0, /*want_files=*/2, &victims);
    EXPECT_EQ(victims.size(), 2u);
  }
}

TEST_F(LruPolicyTest, EvictStopsWhenEmpty) {
  LruPolicy p;
  p.OnInsert(MakeEntry(100, 0));
  p.OnInsert(MakeEntry(100, 0));

  CacheVictims victims;
  p.Evict(/*want_bytes=*/1'000'000, /*want_files=*/0, &victims);  // more than held
  EXPECT_EQ(victims.size(), 2u);  // evicts everything, then stops
}

TEST_F(LruPolicyTest, OnEraseUnlinks) {
  LruPolicy p;
  auto* e1 = MakeEntry(100, 0);
  auto* e2 = MakeEntry(100, 0);
  p.OnInsert(e1);
  p.OnInsert(e2);

  p.OnErase(e1);  // e1 no longer participates in eviction

  CacheVictims victims;
  p.Evict(1'000'000, 0, &victims);
  ASSERT_EQ(victims.size(), 1u);
  EXPECT_EQ(victims[0], e2);
}

TEST_F(LruPolicyTest, EvictExpiredDropsStaleKeepsFresh) {
  LruPolicy p;
  auto* stale = MakeEntry(100, /*atime=*/800);
  auto* fresh = MakeEntry(100, /*atime=*/950);
  p.OnInsert(stale);
  p.OnInsert(fresh);

  CacheVictims victims;
  // now=1000, expire=100: stale(800+100<=1000) expired, fresh(950+100>1000) kept
  p.EvictExpired(/*now_sec=*/1000, /*expire_sec=*/100, /*budget=*/100, &victims);
  ASSERT_EQ(victims.size(), 1u);
  EXPECT_EQ(victims[0], stale);
}

TEST_F(LruPolicyTest, EvictExpiredHonorsBudget) {
  LruPolicy p;
  for (int i = 0; i < 5; ++i) {
    p.OnInsert(MakeEntry(100, /*atime=*/0));  // all long expired
  }

  CacheVictims victims;
  p.EvictExpired(/*now_sec=*/1000, /*expire_sec=*/100, /*budget=*/2, &victims);
  EXPECT_EQ(victims.size(), 2u);  // budget caps the per-tick scan
}

}  // namespace cache
}  // namespace dingofs
