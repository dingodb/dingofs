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

#include "test/integration/cache/deploy/fixture.h"

namespace dingofs {
namespace cache {
namespace integration {

// Put several blocks through the local on-disk cache (writeback uploads them to
// the local-file backend), then read each back and byte-compare.
TEST_F(LocalCacheTest, PutThenRangeRoundTrips) {
  constexpr uint32_t kSize = 4u * 1024 * 1024;
  auto* cache = client_.cache();

  for (uint64_t id = 1; id <= 4; ++id) {
    auto h = MakeHandle(kFsId, id, 0, kSize);
    ASSERT_TRUE(cache->Put(h, MakeBlock(PatternFor(id, 0, kSize)),
                           {.writeback = true})
                    .ok())
        << "put id=" << id;
  }

  for (uint64_t id = 1; id <= 4; ++id) {
    auto h = MakeHandle(kFsId, id, 0, kSize);
    IOBuffer buf = MakeReadBuf(kSize);
    ASSERT_TRUE(cache->Range(h, 0, kSize, &buf,
                             {.retrieve_storage = true,
                              .block_whole_length = kSize})
                    .ok())
        << "range id=" << id;
    EXPECT_EQ(ReadAll(buf), PatternFor(id, 0, kSize)) << "mismatch id=" << id;
  }
}

// Blocks of the same id but different index map to distinct store keys and are
// independently retrievable.
TEST_F(LocalCacheTest, MultiIndexBlocksAreDistinct) {
  constexpr uint32_t kSize = 64 * 1024;
  auto* cache = client_.cache();
  for (uint32_t index = 0; index < 5; ++index) {
    auto h = MakeHandle(kFsId, 7, index, kSize);
    ASSERT_TRUE(
        cache->Put(h, MakeBlock(PatternFor(7, index, kSize)), {.writeback = true})
            .ok());
  }
  for (uint32_t index = 0; index < 5; ++index) {
    auto h = MakeHandle(kFsId, 7, index, kSize);
    IOBuffer buf = MakeReadBuf(kSize);
    ASSERT_TRUE(cache->Range(h, 0, kSize, &buf,
                             {.retrieve_storage = true, .block_whole_length = kSize})
                    .ok());
    EXPECT_EQ(ReadAll(buf), PatternFor(7, index, kSize)) << "index=" << index;
  }
}

// Blocks of several different sizes (incl. small and unaligned) round-trip.
TEST_F(LocalCacheTest, VariousBlockSizes) {
  auto* cache = client_.cache();
  const std::vector<uint32_t> sizes = {1, 4096, 64 * 1024, 1000003,
                                       4u * 1024 * 1024};
  uint64_t id = 100;
  for (uint32_t size : sizes) {
    auto h = MakeHandle(kFsId, id, 0, size);
    ASSERT_TRUE(
        cache->Put(h, MakeBlock(PatternFor(id, 0, size)), {.writeback = true})
            .ok())
        << "put size=" << size;
    IOBuffer buf = MakeReadBuf(size);
    ASSERT_TRUE(cache->Range(h, 0, size, &buf,
                             {.retrieve_storage = true, .block_whole_length = size})
                    .ok())
        << "range size=" << size;
    EXPECT_EQ(ReadAll(buf), PatternFor(id, 0, size)) << "size=" << size;
    ++id;
  }
}

// Reading several sub-ranges of one block returns exactly the requested slices.
TEST_F(LocalCacheTest, SubRangesAtOffsets) {
  constexpr uint32_t kSize = 4u * 1024 * 1024;
  auto* cache = client_.cache();
  auto content = PatternFor(200, 0, kSize);
  auto h = MakeHandle(kFsId, 200, 0, kSize);
  ASSERT_TRUE(cache->Put(h, MakeBlock(content), {.writeback = true}).ok());

  struct {
    off_t off;
    size_t len;
  } cases[] = {{0, 1}, {0, 4096}, {1024, 64 * 1024}, {kSize - 10, 10},
               {123456, 654321}};
  for (auto c : cases) {
    IOBuffer buf = MakeReadBuf(c.len);
    ASSERT_TRUE(cache->Range(h, c.off, c.len, &buf,
                             {.retrieve_storage = true, .block_whole_length = kSize})
                    .ok())
        << "off=" << c.off << " len=" << c.len;
    EXPECT_EQ(ReadAll(buf), content.substr(c.off, c.len))
        << "off=" << c.off << " len=" << c.len;
  }
}

// Cache() populates the local cache; a Range with retrieve_storage=false then
// succeeds only because the block is served from cache (not the backend).
TEST_F(LocalCacheTest, CacheServesWithoutStorageReflow) {
  constexpr uint32_t kSize = 1u * 1024 * 1024;
  auto* cache = client_.cache();
  auto content = PatternFor(42, 0, kSize);
  auto h = MakeHandle(kFsId, 42, 0, kSize);

  ASSERT_TRUE(cache->Cache(h, MakeBlock(content)).ok());
  EXPECT_TRUE(cache->IsCached(h));

  IOBuffer buf = MakeReadBuf(kSize);
  ASSERT_TRUE(cache->Range(h, 0, kSize, &buf,
                           {.retrieve_storage = false, .block_whole_length = kSize})
                  .ok())
      << "cached block was not served locally";
  EXPECT_EQ(ReadAll(buf), content);
}

// An uncached block read with reflow disabled is a NotFound, not a backend hit.
TEST_F(LocalCacheTest, UncachedRangeNoReflowReturnsNotFound) {
  constexpr uint32_t kSize = 4096;
  auto* cache = client_.cache();
  auto h = MakeHandle(kFsId, 404, 0, kSize);
  IOBuffer buf = MakeReadBuf(kSize);
  EXPECT_TRUE(cache->Range(h, 0, kSize, &buf,
                           {.retrieve_storage = false, .block_whole_length = kSize})
                  .IsNotFound());
}

}  // namespace integration
}  // namespace cache
}  // namespace dingofs
