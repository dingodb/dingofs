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

// AsyncPut to the remote node, then AsyncRange back over the network, with the
// callbacks firing and the bytes round-tripping.
TEST_F(DistributedCacheTest, AsyncRemotePutThenRange) {
  constexpr uint32_t kSize = 256 * 1024;
  auto* cache = client_.cache();
  auto content = PatternFor(50, 0, kSize);
  auto h = MakeHandle(fs_id_, 50, 0, kSize);

  // Retry the first remote op until the upstream has discovered the node.
  ASSERT_TRUE(WaitUntil([&] {
    return AwaitAsync([&](AsyncCallback cb) {
             cache->AsyncPut(h, MakeBlock(content), cb,
                             {.writeback = true, .tier = CacheTier::kRemote});
           })
        .ok();
  }));

  IOBuffer buf = MakeReadBuf(kSize);
  ASSERT_TRUE(AwaitAsync([&](AsyncCallback cb) {
                cache->AsyncRange(h, 0, kSize, &buf, cb,
                                  {.retrieve_storage = true,
                                   .block_whole_length = kSize,
                                   .tier = CacheTier::kRemote});
              }).ok());
  EXPECT_EQ(ReadAll(buf), content);
}

// AsyncCache to the remote node, then AsyncRange (no reflow) hits once the node
// has cached it.
TEST_F(DistributedCacheTest, AsyncRemoteCacheThenRange) {
  constexpr uint32_t kSize = 256 * 1024;
  auto* cache = client_.cache();
  auto content = PatternFor(51, 0, kSize);
  auto h = MakeHandle(fs_id_, 51, 0, kSize);

  ASSERT_TRUE(WaitUntil([&] {
    return AwaitAsync([&](AsyncCallback cb) {
             cache->AsyncCache(h, MakeBlock(content), cb,
                               {.tier = CacheTier::kRemote});
           })
        .ok();
  }));

  // The node caches asynchronously; poll the no-reflow read until it hits.
  IOBuffer buf;
  ASSERT_TRUE(WaitUntil([&] {
    buf = MakeReadBuf(kSize);
    return AwaitAsync([&](AsyncCallback cb) {
             cache->AsyncRange(h, 0, kSize, &buf, cb,
                               {.retrieve_storage = false,
                                .block_whole_length = kSize,
                                .tier = CacheTier::kRemote});
           })
        .ok();
  })) << "async remote cache hit never materialized";
  EXPECT_EQ(ReadAll(buf), content);
}

// AsyncPrefetch warms the remote cache from the backend; a no-reflow read then
// hits.
TEST_F(DistributedCacheTest, AsyncRemotePrefetchWarmsCache) {
  constexpr uint32_t kSize = 256 * 1024;
  auto* cache = client_.cache();
  auto content = PatternFor(52, 0, kSize);
  auto h = MakeHandle(fs_id_, 52, 0, kSize);
  ASSERT_TRUE(client_.SeedStorage(h, content).ok());

  ASSERT_TRUE(WaitUntil([&] {
    return AwaitAsync([&](AsyncCallback cb) {
             cache->AsyncPrefetch(h, kSize, cb, {.tier = CacheTier::kRemote});
           })
        .ok();
  }));

  IOBuffer buf;
  ASSERT_TRUE(WaitUntil([&] {
    buf = MakeReadBuf(kSize);
    return cache
        ->Range(h, 0, kSize, &buf,
                {.retrieve_storage = false,
                 .block_whole_length = kSize,
                 .tier = CacheTier::kRemote})
        .ok();
  })) << "async-prefetched block never became a remote cache hit";
  EXPECT_EQ(ReadAll(buf), content);
}

}  // namespace integration
}  // namespace cache
}  // namespace dingofs
