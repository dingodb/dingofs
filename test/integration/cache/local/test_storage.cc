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
 * Created Date: 2026-06-22
 * Author: AI
 */

#include "test/integration/cache/local/fixture.h"

namespace dingofs {
namespace cache {
namespace integration {

// Put with writeback stages locally and asynchronously uploads to the backend;
// the backend file eventually holds the exact bytes.
TEST_F(LocalCacheTest, PutWritebackUploadsToBackend) {
  constexpr uint32_t kSize = 256 * 1024;
  auto* cache = client_.cache();
  auto content = PatternFor(1, 0, kSize);
  auto h = MakeHandle(kFsId, 1, 0, kSize);

  ASSERT_TRUE(cache->Put(h, MakeBlock(content), {.writeback = true}).ok());
  ASSERT_TRUE(WaitUntil([&] { return client_.StorageHas(h); }))
      << "block was never uploaded to the backend";
  EXPECT_EQ(client_.ReadStorageFile(h), content);
}

// Put without writeback uploads straight to the backend too.
TEST_F(LocalCacheTest, PutDirectUploadsToBackend) {
  constexpr uint32_t kSize = 128 * 1024;
  auto* cache = client_.cache();
  auto content = PatternFor(2, 0, kSize);
  auto h = MakeHandle(kFsId, 2, 0, kSize);

  ASSERT_TRUE(cache->Put(h, MakeBlock(content), {.writeback = false}).ok());
  ASSERT_TRUE(WaitUntil([&] { return client_.StorageHas(h); }));
  EXPECT_EQ(client_.ReadStorageFile(h), content);
}

// A block present only in the backend (never cached) is served by Range when
// storage reflow is enabled.
TEST_F(LocalCacheTest, RangeMissReflowsFromBackend) {
  constexpr uint32_t kSize = 64 * 1024;
  auto* cache = client_.cache();
  auto content = PatternFor(3, 0, kSize);
  auto h = MakeHandle(kFsId, 3, 0, kSize);

  ASSERT_TRUE(client_.SeedStorage(h, content).ok());
  EXPECT_FALSE(cache->IsCached(h));

  IOBuffer buf = MakeReadBuf(kSize);
  ASSERT_TRUE(
      cache
          ->Range(h, 0, kSize, &buf,
                  {.retrieve_storage = true, .block_whole_length = kSize})
          .ok());
  EXPECT_EQ(ReadAll(buf), content);
}

// A client-side reflow (storage fallback) serves the bytes but does NOT
// implicitly populate the local cache: a following no-reflow read still misses.
// This pins the TierBlockCache contract that local reflow is a pure
// passthrough.
TEST_F(LocalCacheTest, ReflowDoesNotPopulateLocalCache) {
  constexpr uint32_t kSize = 64 * 1024;
  auto* cache = client_.cache();
  auto content = PatternFor(9, 0, kSize);
  auto h = MakeHandle(kFsId, 9, 0, kSize);
  ASSERT_TRUE(client_.SeedStorage(h, content).ok());

  IOBuffer buf = MakeReadBuf(kSize);
  ASSERT_TRUE(
      cache
          ->Range(h, 0, kSize, &buf,
                  {.retrieve_storage = true, .block_whole_length = kSize})
          .ok());
  EXPECT_EQ(ReadAll(buf), content);
  EXPECT_FALSE(cache->IsCached(h));

  IOBuffer buf2 = MakeReadBuf(kSize);
  EXPECT_TRUE(
      cache
          ->Range(h, 0, kSize, &buf2,
                  {.retrieve_storage = false, .block_whole_length = kSize})
          .IsNotFound())
      << "local reflow unexpectedly populated the cache";
}

// Same backend-only block, but reflow disabled -> NotFound (cache-only read).
TEST_F(LocalCacheTest, RangeMissNoReflowReturnsNotFound) {
  constexpr uint32_t kSize = 64 * 1024;
  auto* cache = client_.cache();
  auto h = MakeHandle(kFsId, 4, 0, kSize);
  ASSERT_TRUE(client_.SeedStorage(h, PatternFor(4, 0, kSize)).ok());

  IOBuffer buf = MakeReadBuf(kSize);
  EXPECT_TRUE(
      cache
          ->Range(h, 0, kSize, &buf,
                  {.retrieve_storage = false, .block_whole_length = kSize})
          .IsNotFound());
}

}  // namespace integration
}  // namespace cache
}  // namespace dingofs
