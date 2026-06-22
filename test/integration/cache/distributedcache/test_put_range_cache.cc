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

#include <vector>

#include "test/integration/cache/deploy/fixture.h"

namespace dingofs {
namespace cache {
namespace integration {

// Cache a block onto the remote node, then read it back over the network with
// storage reflow disabled -- a successful, byte-correct read proves the data
// was served from the remote cache (a genuine remote cache hit).
TEST_F(DistributedCacheTest, CacheThenRemoteRangeHits) {
  constexpr uint32_t kSize = 1u * 1024 * 1024;
  auto* cache = client_.cache();
  auto content = PatternFor(7, 0, kSize);
  auto h = MakeHandle(fs_id_, 7, 0, kSize);

  // Retry until the client's upstream has discovered the freshly-joined node.
  ASSERT_TRUE(WaitUntil([&] {
    return cache->Cache(h, MakeBlock(content), {.tier = CacheTier::kRemote})
        .ok();
  })) << "remote Cache never succeeded";

  // The node caches asynchronously; poll the no-reflow Range until the block
  // becomes retrievable from the remote cache (a successful read with
  // retrieve_storage=false proves it came from the cache, not the backend).
  IOBuffer buf;
  ASSERT_TRUE(WaitUntil([&] {
    buf = MakeReadBuf(kSize);
    return cache
        ->Range(h, 0, kSize, &buf,
                {.retrieve_storage = false,
                 .block_whole_length = kSize,
                 .tier = CacheTier::kRemote})
        .ok();
  })) << "remote cache hit never materialized";
  EXPECT_EQ(ReadAll(buf), content);
}

// Put a block to the remote node with writeback (node stages + uploads to the
// shared backend), then read it back over the network end to end.
TEST_F(DistributedCacheTest, PutThenRemoteRangeRoundTrips) {
  constexpr uint32_t kSize = 4u * 1024 * 1024;
  auto* cache = client_.cache();
  auto content = PatternFor(11, 0, kSize);
  auto h = MakeHandle(fs_id_, 11, 0, kSize);

  ASSERT_TRUE(WaitUntil([&] {
    return cache
        ->Put(h, MakeBlock(content),
              {.writeback = true, .tier = CacheTier::kRemote})
        .ok();
  })) << "remote Put never succeeded";

  IOBuffer buf = MakeReadBuf(kSize);
  ASSERT_TRUE(cache->Range(h, 0, kSize, &buf,
                           {.retrieve_storage = true,
                            .block_whole_length = kSize,
                            .tier = CacheTier::kRemote})
                  .ok());
  EXPECT_EQ(ReadAll(buf), content);
}

// Prefetch warms the remote cache; a subsequent no-reflow Range then hits.
TEST_F(DistributedCacheTest, PrefetchWarmsRemoteCache) {
  constexpr uint32_t kSize = 1u * 1024 * 1024;
  auto* cache = client_.cache();
  auto content = PatternFor(23, 0, kSize);
  auto h = MakeHandle(fs_id_, 23, 0, kSize);

  // Seed the shared backend only (do NOT cache on the node), so the no-reflow
  // hit below can only come from Prefetch having warmed the node's cache.
  ASSERT_TRUE(client_.SeedStorage(h, content).ok());

  ASSERT_TRUE(WaitUntil([&] {
    return cache->Prefetch(h, kSize, {.tier = CacheTier::kRemote}).ok();
  })) << "remote Prefetch never succeeded";

  // The node prefetches asynchronously; poll the no-reflow Range until the
  // block becomes retrievable from the remote cache (proves Prefetch warmed it,
  // since the block was never Put/Cached onto the node).
  IOBuffer buf;
  ASSERT_TRUE(WaitUntil([&] {
    buf = MakeReadBuf(kSize);
    return cache
        ->Range(h, 0, kSize, &buf,
                {.retrieve_storage = false,
                 .block_whole_length = kSize,
                 .tier = CacheTier::kRemote})
        .ok();
  })) << "prefetched block never became a remote cache hit";
  EXPECT_EQ(ReadAll(buf), content);
}

// Reading several sub-ranges of a remote block returns exactly the requested
// slices. Covers the node's range branches: a partial read under the
// part-block threshold, one over it (whole-block fetch + slice), and one with
// block_whole_length unset.
TEST_F(DistributedCacheTest, RemoteSubRangesAtOffsets) {
  constexpr uint32_t kSize = 1u * 1024 * 1024;
  auto* cache = client_.cache();
  auto content = PatternFor(60, 0, kSize);
  auto h = MakeHandle(fs_id_, 60, 0, kSize);
  PutRemote(cache, h, content);

  struct {
    off_t off;
    size_t len;
    size_t whole;
  } cases[] = {
      {0, 4096, kSize},            // small partial read
      {1024, 64 * 1024, kSize},    // small partial read at offset
      {123456, 512 * 1024, kSize}, // large read -> whole-block fetch + slice
      {kSize - 10, 10, kSize},     // tail slice
      {2048, 4096, 0},             // block_whole_length unset
  };
  for (auto c : cases) {
    IOBuffer buf = MakeReadBuf(c.len);
    ASSERT_TRUE(WaitUntil([&] {
      buf = MakeReadBuf(c.len);
      return cache
          ->Range(h, c.off, c.len, &buf,
                  {.retrieve_storage = true,
                   .block_whole_length = c.whole,
                   .tier = CacheTier::kRemote})
          .ok();
    })) << "off=" << c.off << " len=" << c.len;
    EXPECT_EQ(ReadAll(buf), content.substr(c.off, c.len))
        << "off=" << c.off << " len=" << c.len << " whole=" << c.whole;
  }
}

// Blocks of several sizes (incl. tiny and unaligned) round-trip over the
// network.
TEST_F(DistributedCacheTest, RemoteVariousBlockSizes) {
  auto* cache = client_.cache();
  const std::vector<uint32_t> sizes = {1, 4096, 64 * 1024, 1000003};
  uint64_t id = 70;
  for (uint32_t size : sizes) {
    auto h = MakeHandle(fs_id_, id, 0, size);
    PutRemote(cache, h, PatternFor(id, 0, size));
    IOBuffer buf = MakeReadBuf(size);
    ASSERT_TRUE(cache->Range(h, 0, size, &buf,
                             {.retrieve_storage = true,
                              .block_whole_length = size,
                              .tier = CacheTier::kRemote})
                    .ok())
        << "size=" << size;
    EXPECT_EQ(ReadAll(buf), PatternFor(id, 0, size)) << "size=" << size;
    ++id;
  }
}

// A remote Put with writeback disabled uploads straight to the shared backend
// (the client bypasses the node's stage path for direct writes).
TEST_F(DistributedCacheTest, RemotePutDirectUploadsToBackend) {
  constexpr uint32_t kSize = 128 * 1024;
  auto* cache = client_.cache();
  auto content = PatternFor(80, 0, kSize);
  auto h = MakeHandle(fs_id_, 80, 0, kSize);

  ASSERT_TRUE(WaitUntil([&] {
    return cache
        ->Put(h, MakeBlock(content),
              {.writeback = false, .tier = CacheTier::kRemote})
        .ok();
  }));
  ASSERT_TRUE(WaitUntil([&] { return client_.StorageHas(h); }))
      << "direct remote Put never reached the backend";
  EXPECT_EQ(client_.ReadStorageFile(h), content);
}

}  // namespace integration
}  // namespace cache
}  // namespace dingofs
