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

// Prefetch pulls a backend-only block into the cache, after which a no-reflow
// Range serves it and the bytes match.
TEST_F(LocalCacheTest, PrefetchFromBackendThenServedLocally) {
  constexpr uint32_t kSize = 256 * 1024;
  auto* cache = client_.cache();
  auto content = PatternFor(1, 0, kSize);
  auto h = MakeHandle(kFsId, 1, 0, kSize);
  ASSERT_TRUE(client_.SeedStorage(h, content).ok());

  ASSERT_TRUE(cache->Prefetch(h, kSize).ok());
  ASSERT_TRUE(WaitUntil([&] { return cache->IsCached(h); }));

  IOBuffer buf = MakeReadBuf(kSize);
  ASSERT_TRUE(
      cache
          ->Range(h, 0, kSize, &buf,
                  {.retrieve_storage = false, .block_whole_length = kSize})
          .ok());
  EXPECT_EQ(ReadAll(buf), content);
}

// Prefetching an already-cached block is a no-op success.
TEST_F(LocalCacheTest, PrefetchAlreadyCachedIsOk) {
  constexpr uint32_t kSize = 64 * 1024;
  auto* cache = client_.cache();
  auto h = MakeHandle(kFsId, 2, 0, kSize);
  ASSERT_TRUE(cache->Cache(h, MakeBlock(PatternFor(2, 0, kSize))).ok());
  EXPECT_TRUE(cache->Prefetch(h, kSize).ok());
}

// Prefetching a block absent from the backend fails.
TEST_F(LocalCacheTest, PrefetchMissingBackendFails) {
  constexpr uint32_t kSize = 64 * 1024;
  auto* cache = client_.cache();
  auto h = MakeHandle(kFsId, 3, 0, kSize);
  EXPECT_FALSE(cache->Prefetch(h, kSize).ok());
  EXPECT_FALSE(cache->IsCached(h));
}

}  // namespace integration
}  // namespace cache
}  // namespace dingofs
