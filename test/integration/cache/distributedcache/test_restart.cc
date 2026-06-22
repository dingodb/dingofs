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

// Blocks staged on a cache node survive a node restart: stopping the node and
// relaunching it on the same cache dir + id recovers the on-disk blocks via the
// node's DiskCacheLoader, so a no-reflow read hits again without the backend.
TEST_F(DistributedCacheTest, NodeRestartRecoversCachedBlocks) {
  constexpr uint32_t kSize = 256 * 1024;
  constexpr uint64_t kBlocks = 3;
  auto* cache = client_.cache();

  for (uint64_t id = 1; id <= kBlocks; ++id) {
    PutRemote(cache, MakeHandle(fs_id_, id, 0, kSize), PatternFor(id, 0, kSize));
  }
  // Confirm each is a genuine remote cache hit before the restart.
  for (uint64_t id = 1; id <= kBlocks; ++id) {
    auto h = MakeHandle(fs_id_, id, 0, kSize);
    ASSERT_TRUE(WaitUntil([&] {
      IOBuffer buf = MakeReadBuf(kSize);
      return cache
          ->Range(h, 0, kSize, &buf,
                  {.retrieve_storage = false,
                   .block_whole_length = kSize,
                   .tier = CacheTier::kRemote})
          .ok();
    })) << "id=" << id << " not cached on node before restart";
  }

  // Restart the node on the same workdir (same cache dir), id, and port. Reusing
  // the port matters: the MDS keeps a left member locked, and a relocated
  // (different port) same-id rejoin is rejected with ELOCKED until it ages out.
  const int port = node_.Port();
  node_.Stop();
  ASSERT_TRUE(WaitMemberNotOnline(mds_client_.get(), kGroup, node_id_));
  ASSERT_TRUE(node_.Start(workdir_ + "/node", mds_.Addr(), kGroup, node_id_,
                          /*cache_size_mb=*/1024, /*fixed_port=*/port)
                  .ok());
  ASSERT_TRUE(WaitMemberOnline(mds_client_.get(), kGroup, node_id_));

  // The loader recovers blocks asynchronously; poll the no-reflow read.
  for (uint64_t id = 1; id <= kBlocks; ++id) {
    auto h = MakeHandle(fs_id_, id, 0, kSize);
    IOBuffer buf;
    ASSERT_TRUE(WaitUntil([&] {
      buf = MakeReadBuf(kSize);
      return cache
          ->Range(h, 0, kSize, &buf,
                  {.retrieve_storage = false,
                   .block_whole_length = kSize,
                   .tier = CacheTier::kRemote})
          .ok();
    })) << "id=" << id << " not recovered after node restart";
    EXPECT_EQ(ReadAll(buf), PatternFor(id, 0, kSize)) << "id=" << id;
  }
}

}  // namespace integration
}  // namespace cache
}  // namespace dingofs
