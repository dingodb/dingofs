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
 * Created Date: 2026-06-21
 * Author: AI
 */

#include "cache/local/lru_cache.h"

#include <gtest/gtest.h>

#include "cache/iutil/time_util.h"
#include "common/block/block_handle.h"
#include "common/block/block_key.h"

namespace dingofs {
namespace cache {

class LRUCacheTest : public ::testing::Test {
 protected:
  static CacheKey Key(uint64_t id) {
    return BlockHandle(1, BlockKey(id, 0, 4194304));
  }

  static CacheValue Val(size_t size) {
    return CacheValue(size, iutil::TimeNow());
  }
};

TEST_F(LRUCacheTest, AddGetExistDelete) {
  LRUCache cache;
  EXPECT_FALSE(cache.Exist(Key(1)));

  cache.Add(Key(1), Val(100));
  EXPECT_TRUE(cache.Exist(Key(1)));

  CacheValue value;
  ASSERT_TRUE(cache.Get(Key(1), &value));
  EXPECT_EQ(value.size, 100u);

  CacheValue deleted;
  ASSERT_TRUE(cache.Delete(Key(1), &deleted));
  EXPECT_EQ(deleted.size, 100u);

  EXPECT_FALSE(cache.Exist(Key(1)));
  EXPECT_FALSE(cache.Get(Key(1), &value));

  CacheValue ignore;
  EXPECT_FALSE(cache.Delete(Key(1), &ignore));
}

TEST_F(LRUCacheTest, SizeTracksEntries) {
  LRUCache cache;
  EXPECT_EQ(cache.Size(), 0u);

  cache.Add(Key(1), Val(10));
  cache.Add(Key(2), Val(20));
  EXPECT_EQ(cache.Size(), 2u);

  CacheValue deleted;
  cache.Delete(Key(1), &deleted);
  EXPECT_EQ(cache.Size(), 1u);
}

TEST_F(LRUCacheTest, Clear) {
  LRUCache cache;
  cache.Add(Key(1), Val(10));
  cache.Add(Key(2), Val(20));

  cache.Clear();
  EXPECT_EQ(cache.Size(), 0u);
  EXPECT_FALSE(cache.Exist(Key(1)));
  EXPECT_FALSE(cache.Exist(Key(2)));
}

TEST_F(LRUCacheTest, Evict) {
  {  // kEvictIt removes everything
    LRUCache cache;
    cache.Add(Key(1), Val(10));
    cache.Add(Key(2), Val(20));
    cache.Add(Key(3), Val(30));

    auto evicted = cache.Evict(
        [](const CacheValue&) { return FilterStatus::kEvictIt; });
    EXPECT_EQ(evicted.size(), 3u);
    EXPECT_EQ(cache.Size(), 0u);
  }

  {  // kSkip keeps everything
    LRUCache cache;
    cache.Add(Key(1), Val(10));
    cache.Add(Key(2), Val(20));

    auto evicted =
        cache.Evict([](const CacheValue&) { return FilterStatus::kSkip; });
    EXPECT_TRUE(evicted.empty());
    EXPECT_EQ(cache.Size(), 2u);
  }

  {  // kFinish on the inactive list stops before touching the active list
    LRUCache cache;
    cache.Add(Key(1), Val(10));
    cache.Add(Key(2), Val(20));

    auto evicted =
        cache.Evict([](const CacheValue&) { return FilterStatus::kFinish; });
    EXPECT_TRUE(evicted.empty());
    EXPECT_EQ(cache.Size(), 2u);
  }
}

TEST_F(LRUCacheTest, EvictInactiveBeforeActive) {
  // Get() promotes a node from the inactive list to the active list. Eviction
  // must scan inactive first, then active.
  LRUCache cache;
  cache.Add(Key(1), Val(10));
  cache.Add(Key(2), Val(20));

  CacheValue value;
  ASSERT_TRUE(cache.Get(Key(1), &value));  // promote key 1 to active

  auto evicted =
      cache.Evict([](const CacheValue&) { return FilterStatus::kEvictIt; });
  ASSERT_EQ(evicted.size(), 2u);
  EXPECT_EQ(evicted[0].key.Filename(), Key(2).Filename());  // inactive first
  EXPECT_EQ(evicted[1].key.Filename(), Key(1).Filename());  // active second
}

TEST_F(LRUCacheTest, EvictSizeBasedFilter) {
  // A realistic filter: evict blocks smaller than a threshold.
  LRUCache cache;
  cache.Add(Key(1), Val(50));
  cache.Add(Key(2), Val(150));
  cache.Add(Key(3), Val(80));

  auto evicted = cache.Evict([](const CacheValue& v) {
    return v.size < 100 ? FilterStatus::kEvictIt : FilterStatus::kSkip;
  });

  EXPECT_EQ(evicted.size(), 2u);  // 50 and 80
  EXPECT_EQ(cache.Size(), 1u);    // 150 remains
  EXPECT_TRUE(cache.Exist(Key(2)));
}

}  // namespace cache
}  // namespace dingofs
