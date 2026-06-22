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

// A block present only in the shared backend (never cached on the node) is
// served by a remote Range with reflow enabled -- the node fetches it from
// storage on the cache miss.
TEST_F(DistributedCacheTest, RemoteRangeReflowsFromBackend) {
  constexpr uint32_t kSize = 256 * 1024;
  auto* cache = client_.cache();
  auto content = PatternFor(31, 0, kSize);
  auto h = MakeHandle(fs_id_, 31, 0, kSize);
  ASSERT_TRUE(client_.SeedStorage(h, content).ok());

  IOBuffer buf;
  ASSERT_TRUE(WaitUntil([&] {
    buf = MakeReadBuf(kSize);
    return cache
        ->Range(h, 0, kSize, &buf,
                {.retrieve_storage = true,
                 .block_whole_length = kSize,
                 .tier = CacheTier::kRemote})
        .ok();
  })) << "remote reflow from backend failed";
  EXPECT_EQ(ReadAll(buf), content);
}

// A block neither seeded in the backend nor cached on the node, read with
// reflow disabled, returns NotFound. The single-node fixture means every key
// hashes to the one peer, so a live round-trip on a probe block establishes the
// remote cache path is reachable before we assert the genuine miss.
TEST_F(DistributedCacheTest, RemoteRangeMissNoReflowReturnsNotFound) {
  constexpr uint32_t kSize = 64 * 1024;
  auto* cache = client_.cache();

  // Probe: a full writeback Put + no-reflow read round-trip proves the no-reflow
  // remote path itself works (not just reflow), so a later NotFound is genuine.
  auto probe = MakeHandle(fs_id_, 32, 0, kSize);
  auto probe_content = PatternFor(32, 0, kSize);
  ASSERT_TRUE(WaitUntil([&] {
    return cache
        ->Put(probe, MakeBlock(probe_content),
              {.writeback = true, .tier = CacheTier::kRemote})
        .ok();
  }));
  ASSERT_TRUE(WaitUntil([&] {
    IOBuffer buf = MakeReadBuf(kSize);
    return cache
        ->Range(probe, 0, kSize, &buf,
                {.retrieve_storage = false,
                 .block_whole_length = kSize,
                 .tier = CacheTier::kRemote})
        .ok();
  })) << "remote no-reflow path never became live";

  auto miss = MakeHandle(fs_id_, 33, 0, kSize);  // never seeded, never cached
  IOBuffer buf = MakeReadBuf(kSize);
  EXPECT_TRUE(cache
                  ->Range(miss, 0, kSize, &buf,
                          {.retrieve_storage = false,
                           .block_whole_length = kSize,
                           .tier = CacheTier::kRemote})
                  .IsNotFound());
}

// A whole-block reflow (read length over the part-block threshold) makes the
// node cache the fetched block: a later no-reflow read then hits the node's
// cache rather than touching the backend.
TEST_F(DistributedCacheTest, RemoteReflowCachesWholeBlock) {
  constexpr uint32_t kSize = 256 * 1024;  // over the 128KiB part-block threshold
  auto* cache = client_.cache();
  auto content = PatternFor(34, 0, kSize);
  auto h = MakeHandle(fs_id_, 34, 0, kSize);
  ASSERT_TRUE(client_.SeedStorage(h, content).ok());

  // Reflow the whole block from the backend through the node.
  ASSERT_TRUE(WaitUntil([&] {
    IOBuffer buf = MakeReadBuf(kSize);
    return cache
        ->Range(h, 0, kSize, &buf,
                {.retrieve_storage = true,
                 .block_whole_length = kSize,
                 .tier = CacheTier::kRemote})
        .ok();
  }));

  // The node caches the reflowed block asynchronously; a no-reflow read then
  // hits it.
  IOBuffer buf;
  ASSERT_TRUE(WaitUntil([&] {
    buf = MakeReadBuf(kSize);
    return cache
        ->Range(h, 0, kSize, &buf,
                {.retrieve_storage = false,
                 .block_whole_length = kSize,
                 .tier = CacheTier::kRemote})
        .ok();
  })) << "node never cached the reflowed whole block";
  EXPECT_EQ(ReadAll(buf), content);
}

// With a tiny node cache budget, writing far more than fits still serves every
// block correctly -- blocks evicted from the node reflow from the backend.
TEST_F(DistributedSmallCacheTest, RemoteEvictionStillServesViaReflow) {
  constexpr uint32_t kSize = 4u * 1024 * 1024;  // node budget is 8MiB
  constexpr uint64_t kBlocks = 8;
  auto* cache = client_.cache();

  for (uint64_t id = 1; id <= kBlocks; ++id) {
    PutRemote(cache, MakeHandle(fs_id_, id, 0, kSize), PatternFor(id, 0, kSize));
  }
  for (uint64_t id = 1; id <= kBlocks; ++id) {
    auto h = MakeHandle(fs_id_, id, 0, kSize);
    IOBuffer buf = MakeReadBuf(kSize);
    ASSERT_TRUE(cache->Range(h, 0, kSize, &buf,
                             {.retrieve_storage = true,
                              .block_whole_length = kSize,
                              .tier = CacheTier::kRemote})
                    .ok())
        << "id=" << id;
    EXPECT_EQ(ReadAll(buf), PatternFor(id, 0, kSize)) << "id=" << id;
  }
}

}  // namespace integration
}  // namespace cache
}  // namespace dingofs
