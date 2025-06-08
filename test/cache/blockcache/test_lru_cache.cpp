/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2024-09-04
 * Author: Jingli Chen (Wine93)
 */

#include "base/time/time.h"
#include "cache/blockcache/lru_cache.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

namespace dingofs {
namespace cache {

class LRUCacheTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}

  static CacheKey Key(uint64_t id) { return BlockKey(1, 1, id, 1, 0); }

  static CacheValue Value(size_t size) {
    return CacheValue(size, TimeSpec(0, 0));
  }
};

TEST_F(LRUCacheTest, Basic) {
  auto cache = std::make_unique<LRUCache>();

  auto key = Key(1);
  auto value = Value(100);

  // CASE 1: Get(1): not found
  CacheValue out;
  ASSERT_FALSE(cache->Get(key, &out));

  // CASE 2: Add(1,100), Get(1): OK
  cache->Add(key, value);
  ASSERT_TRUE(cache->Get(key, &out));
  ASSERT_EQ(out.size, 100);
}

TEST_F(LRUCacheTest, Add) {
  auto cache = std::make_unique<LRUCache>();

  // CASE 1: Add keys
  cache->Add(Key(1), Value(1));
  cache->Add(Key(2), Value(2));
  cache->Add(Key(3), Value(3));
  ASSERT_EQ(cache->Size(), 3);

  // CASE 2: Get keys
  CacheValue out;
  ASSERT_TRUE(cache->Get(Key(1), &out));
  ASSERT_EQ(out.size, 1);
  ASSERT_TRUE(cache->Get(Key(2), &out));
  ASSERT_EQ(out.size, 2);
  ASSERT_TRUE(cache->Get(Key(3), &out));
  ASSERT_EQ(out.size, 3);
  ASSERT_FALSE(cache->Get(Key(4), &out));
}

TEST_F(LRUCacheTest, Get) {
  auto cache = std::make_unique<LRUCache>();

  CacheValue out;
  ASSERT_FALSE(cache->Get(Key(1), &out));
  ASSERT_FALSE(cache->Get(Key(2), &out));
  ASSERT_FALSE(cache->Get(Key(3), &out));

  cache->Add(Key(2), Value(2));

  ASSERT_FALSE(cache->Get(Key(1), &out));
  ASSERT_TRUE(cache->Get(Key(2), &out));
  ASSERT_FALSE(cache->Get(Key(3), &out));
}

TEST_F(LRUCacheTest, Evict) {
  auto cache = std::make_unique<LRUCache>();

  // CASE 1: Add keys
  cache->Add(Key(1), Value(1));
  cache->Add(Key(2), Value(2));
  cache->Add(Key(3), Value(3));
  ASSERT_EQ(cache->Size(), 3);

  // CASE 2: Evict keys
  auto evicted = cache->Evict([&](const CacheValue& value) {
    if (value.size == 1) {
      return FilterStatus::kSkip;
    }
    return FilterStatus::kEvictIt;
  });
  ASSERT_EQ(evicted.size(), 2);
  ASSERT_EQ(evicted[0].value.size, 2);
  ASSERT_EQ(evicted[1].value.size, 3);
  ASSERT_EQ(cache->Size(), 1);
}

TEST_F(LRUCacheTest, EvictPolicy) {
  auto cache = std::make_unique<LRUCache>();

  cache->Add(Key(1), Value(1));
  cache->Add(Key(2), Value(2));
  ASSERT_EQ(cache->Size(), 2);

  CacheValue out;
  ASSERT_TRUE(cache->Get(Key(1), &out));
  ASSERT_TRUE(cache->Get(Key(2), &out));

  cache->Add(Key(3), Value(3));
  ASSERT_EQ(cache->Size(), 3);

  int cnt = 0;
  auto evicted = cache->Evict([&](const CacheValue&) {
    if (++cnt > 1) {
      return FilterStatus::kFinish;
    }
    return FilterStatus::kEvictIt;
  });
  ASSERT_EQ(evicted.size(), 1);
  ASSERT_EQ(cache->Size(), 2);

  ASSERT_TRUE(cache->Get(Key(1), &out));
  ASSERT_TRUE(cache->Get(Key(2), &out));
  ASSERT_FALSE(cache->Get(Key(3), &out));
}

TEST_F(LRUCacheTest, Size) {
  auto cache = std::make_unique<LRUCache>();

  ASSERT_EQ(cache->Size(), 0);

  cache->Add(Key(1), Value(1));
  cache->Add(Key(2), Value(2));
  cache->Add(Key(3), Value(3));
  ASSERT_EQ(cache->Size(), 3);

  auto evicted =
      cache->Evict([&](const CacheValue&) { return FilterStatus::kEvictIt; });
  ASSERT_EQ(evicted.size(), 3);
  ASSERT_EQ(cache->Size(), 0);

  cache->Add(Key(1), Value(1));
  cache->Add(Key(2), Value(2));
  ASSERT_EQ(cache->Size(), 2);
}

}  // namespace cache
}  // namespace dingofs
