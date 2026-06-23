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

#include "test/integration/cache/distributed/fixture.h"

namespace dingofs {
namespace cache {
namespace integration {

// With two nodes in the group, the client (consistent hashing) writes many
// blocks across them and reads every one back correctly.
TEST_F(DistributedCacheTest, TwoNodesServeAllBlocks) {
  // Bring up a second node in the same group; the client re-syncs members
  // periodically and starts hashing keys across both.
  const auto id2 = utils::GenerateUUID();
  CacheNode node2;
  ASSERT_TRUE(node2.Start(workdir_ + "/node2", mds_.Addr(), kGroup, id2).ok());
  ASSERT_TRUE(WaitMemberOnline(mds_client_.get(), kGroup, id2));
  ASSERT_TRUE(WaitMemberOnline(mds_client_.get(), kGroup, node_id_));

  constexpr uint32_t kSize = 128 * 1024;
  constexpr uint64_t kBlocks = 12;
  auto* cache = client_.cache();

  for (uint64_t id = 1; id <= kBlocks; ++id) {
    PutRemote(cache, MakeHandle(fs_id_, id, 0, kSize),
              PatternFor(id, 0, kSize));
  }
  for (uint64_t id = 1; id <= kBlocks; ++id) {
    auto h = MakeHandle(fs_id_, id, 0, kSize);
    IOBuffer buf = MakeReadBuf(kSize);
    ASSERT_TRUE(cache
                    ->Range(h, 0, kSize, &buf,
                            {.retrieve_storage = true,
                             .block_whole_length = kSize,
                             .tier = CacheTier::kRemote})
                    .ok())
        << "id=" << id;
    EXPECT_EQ(ReadAll(buf), PatternFor(id, 0, kSize)) << "id=" << id;
  }
}

// A node joining after the client is already running is picked up by the
// periodic member sync, and operations keep succeeding.
TEST_F(DistributedCacheTest, MembershipPicksUpNewNode) {
  auto* cache = client_.cache();
  // Works with the single node from the fixture.
  PutRemote(cache, MakeHandle(fs_id_, 1, 0, 64 * 1024),
            PatternFor(1, 0, 64 * 1024));

  // Add a node, wait for the client to observe the larger ring.
  const auto id2 = utils::GenerateUUID();
  CacheNode node2;
  ASSERT_TRUE(node2.Start(workdir_ + "/node2", mds_.Addr(), kGroup, id2).ok());
  ASSERT_TRUE(WaitMemberOnline(mds_client_.get(), kGroup, id2));

  // New writes spanning the (now two-node) ring still round-trip.
  constexpr uint32_t kSize = 64 * 1024;
  for (uint64_t id = 100; id < 110; ++id) {
    auto h = MakeHandle(fs_id_, id, 0, kSize);
    PutRemote(cache, h, PatternFor(id, 0, kSize));
    IOBuffer buf = MakeReadBuf(kSize);
    ASSERT_TRUE(cache
                    ->Range(h, 0, kSize, &buf,
                            {.retrieve_storage = true,
                             .block_whole_length = kSize,
                             .tier = CacheTier::kRemote})
                    .ok());
    EXPECT_EQ(ReadAll(buf), PatternFor(id, 0, kSize)) << "id=" << id;
  }
}

// With two nodes serving (and every block also uploaded to the backend),
// stopping one node does not lose data: reflow-enabled reads of every block
// still succeed -- blocks that lived on the downed node reflow from the
// backend.
TEST_F(DistributedCacheTest, DataAvailableAfterNodeLeaves) {
  const auto id2 = utils::GenerateUUID();
  CacheNode node2;
  ASSERT_TRUE(node2.Start(workdir_ + "/node2", mds_.Addr(), kGroup, id2).ok());
  ASSERT_TRUE(WaitMemberOnline(mds_client_.get(), kGroup, id2));
  ASSERT_TRUE(WaitMemberOnline(mds_client_.get(), kGroup, node_id_));

  constexpr uint32_t kSize = 128 * 1024;
  constexpr uint64_t kBlocks = 8;
  auto* cache = client_.cache();
  for (uint64_t id = 1; id <= kBlocks; ++id) {
    PutRemote(cache, MakeHandle(fs_id_, id, 0, kSize),
              PatternFor(id, 0, kSize));
  }

  // Take node2 down and wait until the client sees the smaller ring.
  node2.Stop();
  ASSERT_TRUE(WaitMemberNotOnline(mds_client_.get(), kGroup, id2));

  for (uint64_t id = 1; id <= kBlocks; ++id) {
    auto h = MakeHandle(fs_id_, id, 0, kSize);
    IOBuffer buf = MakeReadBuf(kSize);
    ASSERT_TRUE(WaitUntil([&] {
      buf = MakeReadBuf(kSize);
      return cache
          ->Range(h, 0, kSize, &buf,
                  {.retrieve_storage = true,
                   .block_whole_length = kSize,
                   .tier = CacheTier::kRemote})
          .ok();
    })) << "id="
        << id;
    EXPECT_EQ(ReadAll(buf), PatternFor(id, 0, kSize)) << "id=" << id;
  }
}

}  // namespace integration
}  // namespace cache
}  // namespace dingofs
