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

#include "cache/local/s3fifo_policy.h"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "cache/local/cache_entry.h"
#include "common/block/block_handle.h"
#include "common/block/block_key.h"

namespace dingofs {
namespace cache {

class S3FifoPolicyTest : public ::testing::Test {
 protected:
  // Each entry carries a real handle keyed by `id` so ghost lookups (which use
  // BlockHandle equality) work; re-inserting the same id re-uses the key.
  CacheEntry* MakeEntry(uint64_t id, uint32_t size, uint32_t atime = 0) {
    handles_.push_back(std::make_unique<BlockHandle>(1, BlockKey(id, 0, size)));
    entries_.push_back(std::make_unique<CacheEntry>());
    CacheEntry* e = entries_.back().get();
    e->key = handles_.back().get();
    e->size = size;
    e->atime.store(atime);
    e->staged = false;
    return e;
  }

  std::vector<std::unique_ptr<BlockHandle>> handles_;
  std::vector<std::unique_ptr<CacheEntry>> entries_;
};

TEST_F(S3FifoPolicyTest, FreqZeroEvictsOldestFirst) {
  S3FifoPolicy p;
  auto* e1 = MakeEntry(1, 100);
  auto* e2 = MakeEntry(2, 100);
  auto* e3 = MakeEntry(3, 100);
  p.OnInsert(e1);
  p.OnInsert(e2);
  p.OnInsert(e3);

  CacheVictims victims;
  p.Evict(100, 0, &victims);
  ASSERT_EQ(victims.size(), 1u);
  EXPECT_EQ(victims[0], e1);  // oldest one-hit-wonder in the small queue
}

TEST_F(S3FifoPolicyTest, AccessedBlockPromotedNotEvicted) {
  S3FifoPolicy p;
  auto* e1 = MakeEntry(1, 100);
  auto* e2 = MakeEntry(2, 100);
  p.OnInsert(e1);
  p.OnInsert(e2);
  p.OnAccess(e1);  // e1 frequency -> 1

  CacheVictims victims;
  p.Evict(100, 0, &victims);  // free one block
  ASSERT_EQ(victims.size(), 1u);
  EXPECT_EQ(victims[0], e2);  // e1 promoted to main, e2 (freq 0) evicted
}

TEST_F(S3FifoPolicyTest, GhostReadmitsToMain) {
  S3FifoPolicy p;

  auto* e1 = MakeEntry(1, 100);
  p.OnInsert(e1);
  {  // evict e1 (freq 0) -> its key enters the ghost queue
    CacheVictims v;
    p.Evict(100, 0, &v);
    ASSERT_EQ(v.size(), 1u);
    EXPECT_EQ(v[0], e1);
  }

  auto* e1b = MakeEntry(1, 100);  // same id -> ghost hit -> admitted to main
  p.OnInsert(e1b);
  auto* e2 = MakeEntry(2, 100);
  auto* e3 = MakeEntry(3, 100);
  p.OnInsert(e2);  // small
  p.OnInsert(e3);  // small

  CacheVictims victims;
  p.Evict(100, 0, &victims);  // eviction targets the small queue
  ASSERT_EQ(victims.size(), 1u);
  EXPECT_EQ(victims[0], e2);  // e1b in main survived thanks to the ghost hit
}

TEST_F(S3FifoPolicyTest, OnEraseRemovesFromEitherQueue) {
  S3FifoPolicy p;
  auto* e1 = MakeEntry(1, 100);
  auto* e2 = MakeEntry(2, 100);
  p.OnInsert(e1);
  p.OnInsert(e2);

  p.OnErase(e1);

  CacheVictims victims;
  p.Evict(1'000'000, 0, &victims);
  ASSERT_EQ(victims.size(), 1u);
  EXPECT_EQ(victims[0], e2);
}

TEST_F(S3FifoPolicyTest, TerminatesWhenEverythingIsHot) {
  S3FifoPolicy p;
  std::vector<CacheEntry*> es;
  for (uint64_t id = 1; id <= 5; ++id) {
    auto* e = MakeEntry(id, 100);
    p.OnInsert(e);
    p.OnAccess(e);  // freq -> 1, 2, 3 (saturating)
    p.OnAccess(e);
    p.OnAccess(e);
    es.push_back(e);
  }

  CacheVictims victims;
  p.Evict(/*want_bytes=*/1'000'000, 0, &victims);  // must not loop forever
  EXPECT_EQ(victims.size(), 5u);  // all eventually evicted
}

TEST_F(S3FifoPolicyTest, EvictExpiredDropsStaleKeepsFresh) {
  S3FifoPolicy p;
  auto* stale = MakeEntry(1, 100, /*atime=*/800);
  auto* fresh = MakeEntry(2, 100, /*atime=*/950);
  p.OnInsert(stale);
  p.OnInsert(fresh);

  CacheVictims victims;
  p.EvictExpired(/*now_sec=*/1000, /*expire_sec=*/100, /*budget=*/100, &victims);
  ASSERT_EQ(victims.size(), 1u);
  EXPECT_EQ(victims[0], stale);
}

}  // namespace cache
}  // namespace dingofs
