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

#include "cache/local/cache_policy.h"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "common/block/block_handle.h"
#include "common/block/block_key.h"

namespace dingofs {
namespace cache {

class CachePolicyTest : public ::testing::Test {
 protected:
  // Every entry gets a real handle keyed by `id` so S3-FIFO ghost lookups work;
  // re-using the same id re-uses the key.
  CacheEntry* Make(uint64_t id, uint32_t size = 100, uint32_t atime = 0) {
    handles_.push_back(std::make_unique<BlockHandle>(1, BlockKey(id, 0, size)));
    entries_.push_back(std::make_unique<CacheEntry>());
    CacheEntry* e = entries_.back().get();
    e->key = handles_.back().get();
    e->size = size;
    e->atime.store(atime);
    e->staged = false;
    return e;
  }

  static CacheVictims Evict(EvictionPolicy* p, uint64_t bytes, uint64_t files) {
    CacheVictims v;
    p->Evict(bytes, files, &v);
    return v;
  }

  std::vector<std::unique_ptr<BlockHandle>> handles_;
  std::vector<std::unique_ptr<CacheEntry>> entries_;
};

// ---------- cross-policy invariants ----------

TEST_F(CachePolicyTest, EmptyAndZeroWantEvictNothing) {
  for (const char* name : {"lru", "sieve", "s3fifo", "2random", "none"}) {
    auto p = NewEvictionPolicy(name);
    EXPECT_TRUE(Evict(p.get(), 1000, 10).empty()) << name;  // empty policy
    p->OnInsert(Make(1));
    EXPECT_TRUE(Evict(p.get(), 0, 0).empty()) << name;  // want nothing
  }
}

TEST_F(CachePolicyTest, EvictAllWhenWantExceedsContents) {
  for (const char* name : {"lru", "sieve", "s3fifo", "2random"}) {
    auto p = NewEvictionPolicy(name);
    for (uint64_t id = 1; id <= 5; ++id) {
      p->OnInsert(Make(id));
    }
    EXPECT_EQ(Evict(p.get(), 1'000'000, 0).size(), 5u) << name;
  }
}

// ---------- LRU ----------

TEST_F(CachePolicyTest, LruEvictsOldestAndAccessPromotes) {
  LruPolicy p;
  auto* e1 = Make(1);
  auto* e2 = Make(2);
  auto* e3 = Make(3);
  p.OnInsert(e1);
  p.OnInsert(e2);
  p.OnInsert(e3);

  {  // oldest first
    auto v = Evict(&p, 100, 0);
    ASSERT_EQ(v.size(), 1u);
    EXPECT_EQ(v[0], e1);
  }
  {  // access moves to newest, so e3 (now oldest) goes next
    p.OnAccess(e2);
    auto v = Evict(&p, 100, 0);
    ASSERT_EQ(v.size(), 1u);
    EXPECT_EQ(v[0], e3);
  }
}

TEST_F(CachePolicyTest, LruHonorsByteAndFileGoals) {
  LruPolicy p;
  for (uint64_t id = 1; id <= 5; ++id) {
    p.OnInsert(Make(id, 100));
  }
  EXPECT_EQ(Evict(&p, 250, 0).size(), 3u);  // bytes goal
  EXPECT_EQ(Evict(&p, 0, 1).size(), 1u);    // file goal
}

TEST_F(CachePolicyTest, LruOnEraseUnlinks) {
  LruPolicy p;
  auto* e1 = Make(1);
  auto* e2 = Make(2);
  p.OnInsert(e1);
  p.OnInsert(e2);
  p.OnErase(e1);
  auto v = Evict(&p, 1'000'000, 0);
  ASSERT_EQ(v.size(), 1u);
  EXPECT_EQ(v[0], e2);
}

TEST_F(CachePolicyTest, LruExpiryDropsStaleKeepsFreshAndBudget) {
  LruPolicy p;
  p.OnInsert(Make(1, 100, /*atime=*/800));
  p.OnInsert(Make(2, 100, /*atime=*/950));
  {
    CacheVictims v;
    p.EvictExpired(1000, 100, 100, &v);  // 800 expired, 950 fresh
    ASSERT_EQ(v.size(), 1u);
    EXPECT_EQ(v[0]->atime.load(), 800u);
  }
  {  // budget caps the scan
    LruPolicy q;
    for (int i = 0; i < 5; ++i) {
      q.OnInsert(Make(100 + i, 100, 0));
    }
    CacheVictims v;
    q.EvictExpired(1000, 100, 2, &v);
    EXPECT_EQ(v.size(), 2u);
  }
}

// ---------- SIEVE ----------

TEST_F(CachePolicyTest, SieveVisitedSurvivesScan) {
  SievePolicy p;
  auto* e1 = Make(1);
  auto* e2 = Make(2);
  auto* e3 = Make(3);
  p.OnInsert(e1);
  p.OnInsert(e2);
  p.OnInsert(e3);
  p.OnAccess(e1);  // e1 keeps its second chance

  auto v = Evict(&p, 100, 0);
  ASSERT_EQ(v.size(), 1u);
  EXPECT_EQ(v[0], e2);  // e1 kept, e2 (unvisited) evicted
}

TEST_F(CachePolicyTest, SieveHandClearsBitThenEvicts) {
  SievePolicy p;
  auto* e1 = Make(1);
  auto* e2 = Make(2);
  p.OnInsert(e1);
  p.OnInsert(e2);
  p.OnAccess(e1);

  auto v = Evict(&p, 1'000'000, 0);  // clear e1, evict e2, then evict e1
  ASSERT_EQ(v.size(), 2u);
  EXPECT_EQ(v[0], e2);
  EXPECT_EQ(v[1], e1);
}

TEST_F(CachePolicyTest, SieveOnEraseFixesHand) {
  SievePolicy p;
  auto* e1 = Make(1);
  auto* e2 = Make(2);
  auto* e3 = Make(3);
  p.OnInsert(e1);
  p.OnInsert(e2);
  p.OnInsert(e3);
  p.OnErase(e2);  // remove middle (hand fixup path)

  auto v = Evict(&p, 1'000'000, 0);
  ASSERT_EQ(v.size(), 2u);
  EXPECT_EQ(v[0], e1);
  EXPECT_EQ(v[1], e3);
}

TEST_F(CachePolicyTest, SieveExpiry) {
  SievePolicy p;
  auto* stale = Make(1, 100, 800);
  p.OnInsert(stale);
  p.OnInsert(Make(2, 100, 950));
  CacheVictims v;
  p.EvictExpired(1000, 100, 100, &v);
  ASSERT_EQ(v.size(), 1u);
  EXPECT_EQ(v[0], stale);
}

// ---------- S3-FIFO ----------

TEST_F(CachePolicyTest, S3FifoFreqZeroEvictsOldest) {
  S3FifoPolicy p;
  auto* e1 = Make(1);
  p.OnInsert(e1);
  p.OnInsert(Make(2));
  p.OnInsert(Make(3));
  auto v = Evict(&p, 100, 0);
  ASSERT_EQ(v.size(), 1u);
  EXPECT_EQ(v[0], e1);
}

TEST_F(CachePolicyTest, S3FifoAccessedPromotedNotEvicted) {
  S3FifoPolicy p;
  auto* e1 = Make(1);
  auto* e2 = Make(2);
  p.OnInsert(e1);
  p.OnInsert(e2);
  p.OnAccess(e1);  // freq 1 -> promoted to main on eviction

  auto v = Evict(&p, 100, 0);
  ASSERT_EQ(v.size(), 1u);
  EXPECT_EQ(v[0], e2);
}

TEST_F(CachePolicyTest, S3FifoGhostReadmitsToMain) {
  S3FifoPolicy p;
  auto* e1 = Make(1);
  p.OnInsert(e1);
  ASSERT_EQ(Evict(&p, 100, 0)[0], e1);  // evicted freq 0 -> ghost

  p.OnInsert(Make(1));  // same id -> ghost hit -> main
  auto* e2 = Make(2);
  p.OnInsert(e2);
  p.OnInsert(Make(3));
  auto v = Evict(&p, 100, 0);  // hits the small queue, not the re-admitted main
  ASSERT_EQ(v.size(), 1u);
  EXPECT_EQ(v[0], e2);
}

TEST_F(CachePolicyTest, S3FifoTerminatesWhenAllHot) {
  S3FifoPolicy p;
  for (uint64_t id = 1; id <= 5; ++id) {
    auto* e = Make(id);
    p.OnInsert(e);
    p.OnAccess(e);
    p.OnAccess(e);
    p.OnAccess(e);  // saturate frequency
  }
  EXPECT_EQ(Evict(&p, 1'000'000, 0).size(), 5u);  // must not loop forever
}

TEST_F(CachePolicyTest, S3FifoVariableSizesByteSplit) {
  // One big block plus small ones; eviction must free bytes without corrupting
  // the small/main byte accounting or looping.
  S3FifoPolicy p;
  p.OnInsert(Make(1, 4096));
  p.OnInsert(Make(2, 100));
  p.OnInsert(Make(3, 100));
  auto v = Evict(&p, 200, 0);
  uint64_t freed = 0;
  for (auto* e : v) {
    freed += e->size;
  }
  EXPECT_GE(freed, 200u);
}

TEST_F(CachePolicyTest, S3FifoExpiry) {
  S3FifoPolicy p;
  auto* stale = Make(1, 100, 800);
  p.OnInsert(stale);
  p.OnInsert(Make(2, 100, 950));
  CacheVictims v;
  p.EvictExpired(1000, 100, 100, &v);
  ASSERT_EQ(v.size(), 1u);
  EXPECT_EQ(v[0], stale);
}

// ---------- 2-random ----------

TEST_F(CachePolicyTest, TwoRandomEvictsOlderOfTwo) {
  TwoRandomPolicy p;
  auto* older = Make(1, 100, /*atime=*/10);
  p.OnInsert(older);
  p.OnInsert(Make(2, 100, /*atime=*/100));
  auto v = Evict(&p, 100, 0);  // two entries -> both sampled -> older wins
  ASSERT_EQ(v.size(), 1u);
  EXPECT_EQ(v[0], older);
}

TEST_F(CachePolicyTest, TwoRandomHonorsGoalsAndErase) {
  TwoRandomPolicy p;
  auto* e2 = Make(2);
  for (uint64_t id = 1; id <= 5; ++id) {
    p.OnInsert(id == 2 ? e2 : Make(id));
  }
  p.OnErase(e2);
  auto v = Evict(&p, 300, 0);  // e2 already erased -> only 4 remain, evict 3
  EXPECT_EQ(v.size(), 3u);
  for (auto* victim : v) {
    EXPECT_NE(victim, e2);
  }
}

TEST_F(CachePolicyTest, TwoRandomExpiry) {
  TwoRandomPolicy p;
  for (int i = 0; i < 3; ++i) {
    p.OnInsert(Make(1 + i, 100, /*atime=*/100));  // stale
  }
  for (int i = 0; i < 2; ++i) {
    p.OnInsert(Make(10 + i, 100, /*atime=*/990));  // fresh
  }
  CacheVictims v;
  p.EvictExpired(1000, 100, 100, &v);
  EXPECT_EQ(v.size(), 3u);
}

// ---------- none + factory ----------

TEST_F(CachePolicyTest, NoneNeverEvicts) {
  NonePolicy p;
  p.OnInsert(Make(1));
  p.OnInsert(Make(2));
  EXPECT_TRUE(Evict(&p, 1'000'000, 100).empty());
  CacheVictims v;
  p.EvictExpired(1'000'000, 0, 100, &v);
  EXPECT_TRUE(v.empty());
}

TEST_F(CachePolicyTest, Factory) {
  EXPECT_NE(NewEvictionPolicy("lru"), nullptr);
  EXPECT_NE(NewEvictionPolicy("sieve"), nullptr);
  EXPECT_NE(NewEvictionPolicy("s3fifo"), nullptr);
  EXPECT_NE(NewEvictionPolicy("2random"), nullptr);
  EXPECT_NE(NewEvictionPolicy("none"), nullptr);
  EXPECT_NE(NewEvictionPolicy(""), nullptr);       // flag default
  EXPECT_NE(NewEvictionPolicy("bogus"), nullptr);  // -> lru fallback
}

}  // namespace cache
}  // namespace dingofs
