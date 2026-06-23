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

// AsyncPut then AsyncRange: callbacks fire and the bytes round-trip.
TEST_F(LocalCacheTest, AsyncPutThenAsyncRange) {
  constexpr uint32_t kSize = 256 * 1024;
  auto* cache = client_.cache();
  auto content = PatternFor(1, 0, kSize);
  auto h = MakeHandle(kFsId, 1, 0, kSize);

  ASSERT_TRUE(AwaitAsync([&](AsyncCallback cb) {
                cache->AsyncPut(h, MakeBlock(content), cb, {.writeback = true});
              }).ok());

  IOBuffer buf = MakeReadBuf(kSize);
  ASSERT_TRUE(AwaitAsync([&](AsyncCallback cb) {
                cache->AsyncRange(
                    h, 0, kSize, &buf, cb,
                    {.retrieve_storage = true, .block_whole_length = kSize});
              }).ok());
  EXPECT_EQ(ReadAll(buf), content);
}

// AsyncCache then AsyncRange (no reflow) hits the freshly cached block.
TEST_F(LocalCacheTest, AsyncCacheThenAsyncRange) {
  constexpr uint32_t kSize = 64 * 1024;
  auto* cache = client_.cache();
  auto content = PatternFor(2, 0, kSize);
  auto h = MakeHandle(kFsId, 2, 0, kSize);

  ASSERT_TRUE(AwaitAsync([&](AsyncCallback cb) {
                cache->AsyncCache(h, MakeBlock(content), cb);
              }).ok());

  IOBuffer buf = MakeReadBuf(kSize);
  ASSERT_TRUE(AwaitAsync([&](AsyncCallback cb) {
                cache->AsyncRange(
                    h, 0, kSize, &buf, cb,
                    {.retrieve_storage = false, .block_whole_length = kSize});
              }).ok());
  EXPECT_EQ(ReadAll(buf), content);
}

// AsyncPrefetch warms the cache from the backend.
TEST_F(LocalCacheTest, AsyncPrefetchFromBackend) {
  constexpr uint32_t kSize = 64 * 1024;
  auto* cache = client_.cache();
  auto h = MakeHandle(kFsId, 3, 0, kSize);
  ASSERT_TRUE(client_.SeedStorage(h, PatternFor(3, 0, kSize)).ok());

  ASSERT_TRUE(AwaitAsync([&](AsyncCallback cb) {
                cache->AsyncPrefetch(h, kSize, cb);
              }).ok());
  EXPECT_TRUE(WaitUntil([&] { return cache->IsCached(h); }));
}

}  // namespace integration
}  // namespace cache
}  // namespace dingofs
