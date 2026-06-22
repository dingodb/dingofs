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
 * Created Date: 2026-05-28
 * Author: AI
 */

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "cache/local/cache_store.h"
#include "cache/local/mem_cache.h"
#include "common/block/block_key.h"
#include "common/io_buffer.h"

namespace dingofs {
namespace cache {

class MemCacheTest : public ::testing::Test {
 protected:
  static BlockHandle MakeHandle(uint64_t id, uint32_t index, uint32_t size) {
    return BlockHandle(1, BlockKey(id, index, size));
  }

  static IOBuffer MakeBlock(const std::string& payload) {
    return IOBuffer(payload.data(), payload.size());
  }

  static std::string ReadAll(IOBuffer& buf) {
    std::string out(buf.Size(), '\0');
    buf.CopyTo(out.data());
    return out;
  }

  // Returns `n` handles whose Filename() hashes to the same shard, so tests
  // that need a deterministic per-shard LRU ordering can opt in.
  static std::vector<BlockHandle> SameShardHandles(size_t n, uint32_t size) {
    std::vector<BlockHandle> out;
    size_t target_shard = 0;
    bool target_found = false;
    std::vector<std::vector<BlockHandle>> buckets(MemCache::kShardCount);
    for (uint64_t id = 1; !target_found && id < 100000; ++id) {
      BlockHandle h(1, BlockKey(id, 0, size));
      size_t s = MemCache::ShardIndex(h.Filename());
      buckets[s].push_back(h);
      if (buckets[s].size() >= n) {
        target_shard = s;
        target_found = true;
      }
    }
    CHECK(target_found) << "could not find " << n << " handles in one shard";
    out = std::move(buckets[target_shard]);
    out.resize(n);
    return out;
  }
};

TEST_F(MemCacheTest, StartAndShutdown) {
  MemCache cache({.cache_size_mb = 1});
  EXPECT_FALSE(cache.IsRunning());
  EXPECT_FALSE(cache.Id().empty());

  ASSERT_TRUE(cache.Start([](BlockHandle, size_t, BlockAttr) {}).ok());
  EXPECT_TRUE(cache.IsRunning());

  ASSERT_TRUE(cache.Shutdown().ok());
  EXPECT_FALSE(cache.IsRunning());

  ASSERT_TRUE(cache.Shutdown().ok());
}

TEST_F(MemCacheTest, Stage) {
  MemCache cache({.cache_size_mb = 1});

  size_t uploaded_length = 0;
  ASSERT_TRUE(
      cache
          .Start([&uploaded_length](BlockHandle, size_t length, BlockAttr) {
            uploaded_length = length;
          })
          .ok());

  auto handle = MakeHandle(101, 0, 5);
  ASSERT_TRUE(cache.Stage(handle, MakeBlock("hello")).ok());
  EXPECT_EQ(uploaded_length, 5u);
  EXPECT_TRUE(cache.IsCached(handle));

  {
    IOBuffer buf;
    ASSERT_TRUE(cache.Load(handle, 0, 5, &buf).ok());
    EXPECT_EQ(ReadAll(buf), "hello");
  }

  ASSERT_TRUE(cache.Shutdown().ok());

  {
    auto status = cache.Stage(handle, MakeBlock("x"));
    EXPECT_TRUE(status.IsCacheDown());
  }
}

TEST_F(MemCacheTest, RemoveStage) {
  MemCache cache({.cache_size_mb = 1});
  ASSERT_TRUE(cache.Start([](BlockHandle, size_t, BlockAttr) {}).ok());

  auto handle = MakeHandle(202, 0, 5);
  ASSERT_TRUE(cache.Stage(handle, MakeBlock("world")).ok());
  ASSERT_TRUE(cache.IsCached(handle));

  ASSERT_TRUE(cache.RemoveStage(handle).ok());
  EXPECT_TRUE(cache.IsCached(handle));

  IOBuffer buf;
  ASSERT_TRUE(cache.Load(handle, 0, 5, &buf).ok());
  EXPECT_EQ(ReadAll(buf), "world");
}

TEST_F(MemCacheTest, CacheAndLoad) {
  MemCache cache({.cache_size_mb = 1});
  ASSERT_TRUE(cache.Start([](BlockHandle, size_t, BlockAttr) {}).ok());

  auto handle = MakeHandle(303, 0, 11);
  ASSERT_TRUE(cache.Cache(handle, MakeBlock("hello world")).ok());

  {
    IOBuffer buf;
    ASSERT_TRUE(cache.Load(handle, 0, 11, &buf).ok());
    EXPECT_EQ(ReadAll(buf), "hello world");
  }

  {
    IOBuffer buf;
    ASSERT_TRUE(cache.Load(handle, 6, 5, &buf).ok());
    EXPECT_EQ(ReadAll(buf), "world");
  }

  {
    IOBuffer buf;
    ASSERT_TRUE(cache.Load(handle, 0, 100, &buf).ok());
    EXPECT_EQ(ReadAll(buf), "hello world");
  }

  {
    IOBuffer buf;
    auto status = cache.Load(MakeHandle(999, 0, 1), 0, 1, &buf);
    EXPECT_TRUE(status.IsNotFound());
  }

  ASSERT_TRUE(cache.Cache(handle, MakeBlock("HELLO WORLD")).ok());
  {
    IOBuffer buf;
    ASSERT_TRUE(cache.Load(handle, 0, 11, &buf).ok());
    EXPECT_EQ(ReadAll(buf), "HELLO WORLD");
  }
}

TEST_F(MemCacheTest, LRUEvictionWithinShard) {
  MemCache cache({.cache_size_mb = 0});  // every shard has capacity 0
  ASSERT_TRUE(cache.Start([](BlockHandle, size_t, BlockAttr) {}).ok());

  auto handles = SameShardHandles(2, 4);
  ASSERT_TRUE(cache.Cache(handles[0], MakeBlock("AAAA")).ok());
  ASSERT_TRUE(cache.Cache(handles[1], MakeBlock("BBBB")).ok());

  EXPECT_FALSE(cache.IsCached(handles[0]));
  EXPECT_TRUE(cache.IsCached(handles[1]));
}

TEST_F(MemCacheTest, LRUOrderingPromoteOnLoad) {
  // Shard capacity = 4MiB / 32 = 128KiB. With three 50KiB blocks in the same
  // shard, the first two fit (100KiB) and inserting the third evicts the LRU
  // one.
  MemCache cache({.cache_size_mb = 4});
  ASSERT_TRUE(cache.Start([](BlockHandle, size_t, BlockAttr) {}).ok());

  constexpr uint32_t kBlockSize = 50 * 1024;
  auto handles = SameShardHandles(3, kBlockSize);
  std::string pa(kBlockSize, 'a');
  std::string pb(kBlockSize, 'b');
  std::string pc(kBlockSize, 'c');

  ASSERT_TRUE(cache.Cache(handles[0], MakeBlock(pa)).ok());
  ASSERT_TRUE(cache.Cache(handles[1], MakeBlock(pb)).ok());

  // Touch handle 0 to make handle 1 the LRU victim.
  {
    IOBuffer buf;
    ASSERT_TRUE(cache.Load(handles[0], 0, 1, &buf).ok());
  }

  ASSERT_TRUE(cache.Cache(handles[2], MakeBlock(pc)).ok());

  EXPECT_TRUE(cache.IsCached(handles[0]));
  EXPECT_FALSE(cache.IsCached(handles[1]));
  EXPECT_TRUE(cache.IsCached(handles[2]));
}

TEST_F(MemCacheTest, IsFull) {
  MemCache cache({.cache_size_mb = 1});  // each shard capacity = 32KiB
  ASSERT_TRUE(cache.Start([](BlockHandle, size_t, BlockAttr) {}).ok());

  auto handle = MakeHandle(601, 0, 4);
  EXPECT_FALSE(cache.IsFull(handle));

  std::string huge(32 * 1024, 'x');
  ASSERT_TRUE(cache.Cache(handle, MakeBlock(huge)).ok());
  EXPECT_TRUE(cache.IsFull(handle));
}

TEST_F(MemCacheTest, Dump) {
  MemCache cache({.cache_size_mb = 4});
  ASSERT_TRUE(cache.Start([](BlockHandle, size_t, BlockAttr) {}).ok());

  ASSERT_TRUE(cache.Cache(MakeHandle(701, 0, 3), MakeBlock("abc")).ok());
  ASSERT_TRUE(cache.Cache(MakeHandle(702, 0, 2), MakeBlock("de")).ok());

  Json::Value value;
  EXPECT_TRUE(cache.Dump(value));
  EXPECT_EQ(value["capacity_mb"].asUInt64(), 4u);
  EXPECT_EQ(value["used_bytes"].asUInt64(), 5u);
  EXPECT_EQ(value["block_count"].asUInt64(), 2u);
  EXPECT_EQ(value["shard_count"].asUInt64(), MemCache::kShardCount);
}

}  // namespace cache
}  // namespace dingofs
