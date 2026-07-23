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

#include "cache/local/cache_entry.h"

namespace dingofs {
namespace cache {

class CachePolicyTest : public ::testing::Test {
 protected:
  CacheEntry* MakeEntry(uint32_t size) {
    entries_.push_back(std::make_unique<CacheEntry>());
    CacheEntry* e = entries_.back().get();
    e->size = size;
    e->staged = false;
    return e;
  }

  std::vector<std::unique_ptr<CacheEntry>> entries_;
};

TEST_F(CachePolicyTest, FactoryReturnsRequestedPolicy) {
  EXPECT_NE(NewEvictionPolicy("lru"), nullptr);
  EXPECT_NE(NewEvictionPolicy("none"), nullptr);
  EXPECT_NE(NewEvictionPolicy(""), nullptr);       // falls back to flag default
  EXPECT_NE(NewEvictionPolicy("bogus"), nullptr);  // unknown -> lru fallback
}

TEST_F(CachePolicyTest, LruFactoryEvictsOldest) {
  auto p = NewEvictionPolicy("lru");
  auto* e1 = MakeEntry(100);
  auto* e2 = MakeEntry(100);
  p->OnInsert(e1);
  p->OnInsert(e2);

  CacheVictims victims;
  p->Evict(100, 0, &victims);
  ASSERT_EQ(victims.size(), 1u);
  EXPECT_EQ(victims[0], e1);
}

TEST_F(CachePolicyTest, NoneFactoryNeverEvicts) {
  auto p = NewEvictionPolicy("none");
  p->OnInsert(MakeEntry(100));
  p->OnInsert(MakeEntry(100));

  CacheVictims victims;
  p->Evict(1'000'000, 100, &victims);
  EXPECT_TRUE(victims.empty());

  p->EvictExpired(/*now_sec=*/1'000'000, /*expire_sec=*/0, /*budget=*/100,
                  &victims);
  EXPECT_TRUE(victims.empty());
}

}  // namespace cache
}  // namespace dingofs
