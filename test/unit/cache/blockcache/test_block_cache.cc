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

#include <sstream>

#include "cache/blockcache/block_cache.h"

namespace dingofs {
namespace cache {

class BlockCacheTest : public ::testing::Test {};

// A simple test implementation of BlockCache for testing
class TestBlockCache : public BlockCache {
 public:
  bool IsEnabled() const override { return enabled_; }
  bool EnableStage() const override { return stage_enabled_; }
  bool EnableCache() const override { return cache_enabled_; }

  void SetEnabled(bool enabled) { enabled_ = enabled; }
  void SetStageEnabled(bool enabled) { stage_enabled_ = enabled; }
  void SetCacheEnabled(bool enabled) { cache_enabled_ = enabled; }

 private:
  bool enabled_{false};
  bool stage_enabled_{false};
  bool cache_enabled_{false};
};

TEST_F(BlockCacheTest, PutOptionDefaults) {
  PutOption option;
  EXPECT_FALSE(option.writeback);
  EXPECT_EQ(option.block_attr.from, BlockAttr::kFromUnknown);
}

TEST_F(BlockCacheTest, RangeOptionDefaults) {
  RangeOption option;
  EXPECT_TRUE(option.retrieve_storage);
  EXPECT_EQ(option.block_whole_length, 0);
  EXPECT_FALSE(option.is_subrequest);
}

TEST_F(BlockCacheTest, DefaultImplementation) {
  TestBlockCache cache;
  auto ctx = NewContext();
  BlockKey key(1, 2, 3, 4, 5);
  Block block;
  IOBuffer buffer;

  EXPECT_TRUE(cache.Start().ok());
  EXPECT_TRUE(cache.Shutdown().ok());
  EXPECT_TRUE(cache.Put(ctx, key, block).IsNotSupport());
  EXPECT_TRUE(cache.Range(ctx, key, 0, 100, &buffer).IsNotSupport());
  EXPECT_TRUE(cache.Cache(ctx, key, block).IsNotSupport());
  EXPECT_TRUE(cache.Prefetch(ctx, key, 100).IsNotSupport());
}

TEST_F(BlockCacheTest, AsyncDefaultImplementation) {
  TestBlockCache cache;
  auto ctx = NewContext();
  BlockKey key(1, 2, 3, 4, 5);
  Block block;
  IOBuffer buffer;

  bool called = false;
  Status result;

  cache.AsyncPut(ctx, key, block, [&](Status s) {
    called = true;
    result = s;
  });
  EXPECT_TRUE(called);
  EXPECT_TRUE(result.IsNotSupport());

  called = false;
  cache.AsyncRange(ctx, key, 0, 100, &buffer, [&](Status s) {
    called = true;
    result = s;
  });
  EXPECT_TRUE(called);
  EXPECT_TRUE(result.IsNotSupport());

  called = false;
  cache.AsyncCache(ctx, key, block, [&](Status s) {
    called = true;
    result = s;
  });
  EXPECT_TRUE(called);
  EXPECT_TRUE(result.IsNotSupport());

  called = false;
  cache.AsyncPrefetch(ctx, key, 100, [&](Status s) {
    called = true;
    result = s;
  });
  EXPECT_TRUE(called);
  EXPECT_TRUE(result.IsNotSupport());
}

TEST_F(BlockCacheTest, UtilityMethods) {
  TestBlockCache cache;
  BlockKey key(1, 2, 3, 4, 5);

  EXPECT_FALSE(cache.IsEnabled());
  EXPECT_FALSE(cache.EnableStage());
  EXPECT_FALSE(cache.EnableCache());
  EXPECT_FALSE(cache.IsCached(key));

  cache.SetEnabled(true);
  cache.SetStageEnabled(true);
  cache.SetCacheEnabled(true);

  EXPECT_TRUE(cache.IsEnabled());
  EXPECT_TRUE(cache.EnableStage());
  EXPECT_TRUE(cache.EnableCache());
}

TEST_F(BlockCacheTest, Dump) {
  TestBlockCache cache;
  Json::Value value;
  EXPECT_TRUE(cache.Dump(value));
}

TEST_F(BlockCacheTest, StreamOperator) {
  TestBlockCache cache;
  cache.SetEnabled(true);
  cache.SetStageEnabled(true);
  cache.SetCacheEnabled(false);

  std::ostringstream oss;
  oss << cache;

  EXPECT_NE(oss.str().find("BlockCache"), std::string::npos);
  EXPECT_NE(oss.str().find("enable=1"), std::string::npos);
  EXPECT_NE(oss.str().find("stage=1"), std::string::npos);
  EXPECT_NE(oss.str().find("cache=0"), std::string::npos);
}

}  // namespace cache
}  // namespace dingofs
