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
 * Created Date: 2026-02-02
 * Author: AI
 */

#include <gtest/gtest.h>

#include <memory>

#include "cache/blockcache/lru_cache.h"
#include "cache/iutil/time_util.h"

namespace dingofs {
namespace cache {

class LRUCacheTest : public ::testing::Test {
 protected:
  void SetUp() override { cache_ = std::make_unique<LRUCache>(); }

  static CacheKey Key(uint64_t id) { return BlockKey(1, 1, id, 1, 0); }

  static CacheValue Value(size_t size) {
    return CacheValue(size, iutil::TimeSpec(0, 0));
  }

  std::unique_ptr<LRUCache> cache_;
};

TEST_F(LRUCacheTest, AddAndGet) {
  auto key = Key(1);
  auto value = Value(100);

  cache_->Add(key, value);

  CacheValue out;
  EXPECT_TRUE(cache_->Get(key, &out));
  EXPECT_EQ(out.size, 100);
}

TEST_F(LRUCacheTest, GetNotFound) {
  CacheValue out;
  EXPECT_FALSE(cache_->Get(Key(999), &out));
}

TEST_F(LRUCacheTest, Exist) {
  auto key = Key(1);
  EXPECT_FALSE(cache_->Exist(key));

  cache_->Add(key, Value(100));
  EXPECT_TRUE(cache_->Exist(key));
}

TEST_F(LRUCacheTest, Delete) {
  auto key = Key(1);
  cache_->Add(key, Value(100));

  CacheValue deleted;
  EXPECT_TRUE(cache_->Delete(key, &deleted));
  EXPECT_EQ(deleted.size, 100);

  EXPECT_FALSE(cache_->Exist(key));
}

TEST_F(LRUCacheTest, DeleteNotFound) {
  CacheValue deleted;
  EXPECT_FALSE(cache_->Delete(Key(999), &deleted));
}

TEST_F(LRUCacheTest, Size) {
  EXPECT_EQ(cache_->Size(), 0);

  cache_->Add(Key(1), Value(100));
  EXPECT_EQ(cache_->Size(), 1);

  cache_->Add(Key(2), Value(200));
  EXPECT_EQ(cache_->Size(), 2);

  CacheValue deleted;
  cache_->Delete(Key(1), &deleted);
  EXPECT_EQ(cache_->Size(), 1);
}

TEST_F(LRUCacheTest, Clear) {
  cache_->Add(Key(1), Value(100));
  cache_->Add(Key(2), Value(200));
  cache_->Add(Key(3), Value(300));

  EXPECT_EQ(cache_->Size(), 3);

  cache_->Clear();
  EXPECT_EQ(cache_->Size(), 0);
}

TEST_F(LRUCacheTest, Evict) {
  cache_->Add(Key(1), Value(100));
  cache_->Add(Key(2), Value(200));
  cache_->Add(Key(3), Value(300));

  auto evicted = cache_->Evict([](const CacheValue& value) {
    if (value.size <= 200) {
      return FilterStatus::kEvictIt;
    }
    return FilterStatus::kSkip;
  });

  EXPECT_GE(evicted.size(), 1);
}

TEST_F(LRUCacheTest, EvictFinish) {
  cache_->Add(Key(1), Value(100));
  cache_->Add(Key(2), Value(200));
  cache_->Add(Key(3), Value(300));

  int count = 0;
  auto evicted = cache_->Evict([&count](const CacheValue& value) {
    count++;
    if (count >= 1) {
      return FilterStatus::kFinish;
    }
    return FilterStatus::kEvictIt;
  });

  EXPECT_LE(evicted.size(), 1);
}

TEST_F(LRUCacheTest, UpdateExisting) {
  auto key = Key(1);
  cache_->Add(key, Value(100));

  CacheValue out;
  EXPECT_TRUE(cache_->Get(key, &out));
  EXPECT_EQ(out.size, 100);

  cache_->Add(key, Value(200));

  EXPECT_TRUE(cache_->Get(key, &out));
  EXPECT_EQ(out.size, 200);
}

}  // namespace cache
}  // namespace dingofs
