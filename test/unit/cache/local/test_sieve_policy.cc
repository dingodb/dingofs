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

#include "cache/local/sieve_policy.h"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "cache/local/cache_entry.h"

namespace dingofs {
namespace cache {

class SievePolicyTest : public ::testing::Test {
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

TEST_F(SievePolicyTest, EvictsOldestUnvisited) {
  SievePolicy p;
  auto* e1 = MakeEntry(100);
  auto* e2 = MakeEntry(100);
  auto* e3 = MakeEntry(100);
  p.OnInsert(e1);
  p.OnInsert(e2);
  p.OnInsert(e3);

  CacheVictims victims;
  p.Evict(100, 0, &victims);
  ASSERT_EQ(victims.size(), 1u);
  EXPECT_EQ(victims[0], e1);  // oldest, never visited
}

TEST_F(SievePolicyTest, VisitedEntrySurvivesScan) {
  // The core scan-resistance property: a block that was accessed survives, and
  // an un-accessed (even older) block is evicted first.
  SievePolicy p;
  auto* e1 = MakeEntry(100);
  auto* e2 = MakeEntry(100);
  auto* e3 = MakeEntry(100);
  p.OnInsert(e1);
  p.OnInsert(e2);
  p.OnInsert(e3);

  p.OnAccess(e1);  // e1 gets its visited bit set

  CacheVictims victims;
  p.Evict(100, 0, &victims);
  ASSERT_EQ(victims.size(), 1u);
  EXPECT_EQ(victims[0], e2);  // e1 kept (second chance), e2 evicted
}

TEST_F(SievePolicyTest, VisitedBitClearedByHandThenEvictable) {
  SievePolicy p;
  auto* e1 = MakeEntry(100);
  auto* e2 = MakeEntry(100);
  p.OnInsert(e1);
  p.OnInsert(e2);
  p.OnAccess(e1);

  CacheVictims victims;
  p.Evict(1'000'000, 0, &victims);  // evict everything
  // First pass clears e1's bit and evicts e2; second pass evicts e1.
  ASSERT_EQ(victims.size(), 2u);
  EXPECT_EQ(victims[0], e2);
  EXPECT_EQ(victims[1], e1);
}

TEST_F(SievePolicyTest, OnEraseUnlinks) {
  SievePolicy p;
  auto* e1 = MakeEntry(100);
  auto* e2 = MakeEntry(100);
  auto* e3 = MakeEntry(100);
  p.OnInsert(e1);
  p.OnInsert(e2);
  p.OnInsert(e3);

  p.OnErase(e2);  // remove the middle entry (also exercises the hand fixup)

  CacheVictims victims;
  p.Evict(1'000'000, 0, &victims);
  ASSERT_EQ(victims.size(), 2u);
  EXPECT_EQ(victims[0], e1);
  EXPECT_EQ(victims[1], e3);
}

TEST_F(SievePolicyTest, EvictExpiredDropsStaleKeepsFresh) {
  SievePolicy p;
  auto* stale = MakeEntry(100, /*atime=*/800);
  auto* fresh = MakeEntry(100, /*atime=*/950);
  p.OnInsert(stale);
  p.OnInsert(fresh);

  CacheVictims victims;
  p.EvictExpired(/*now_sec=*/1000, /*expire_sec=*/100, /*budget=*/100, &victims);
  ASSERT_EQ(victims.size(), 1u);
  EXPECT_EQ(victims[0], stale);
}

}  // namespace cache
}  // namespace dingofs
