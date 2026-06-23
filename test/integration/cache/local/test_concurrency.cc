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

#include <atomic>
#include <thread>
#include <vector>

#include "test/integration/cache/local/fixture.h"

namespace dingofs {
namespace cache {
namespace integration {

// Many threads concurrently Cache+Range disjoint blocks through one local
// cache, stressing the disk-cache inflight trackers and LRU under contention.
// No crash, and every block reads back byte-correct. Uses Cache() (no upload)
// so the run stays within the test slab-pool budget.
TEST_F(LocalCacheTest, ConcurrentCacheRange) {
  constexpr uint32_t kSize = 64 * 1024;
  constexpr int kThreads = 4;
  constexpr uint64_t kPerThread = 4;
  auto* cache = client_.cache();

  std::atomic<int> failures{0};
  std::vector<std::thread> workers;
  for (int t = 0; t < kThreads; ++t) {
    workers.emplace_back([&, t] {
      for (uint64_t i = 0; i < kPerThread; ++i) {
        uint64_t id = static_cast<uint64_t>(t) * kPerThread + i + 1;
        auto h = MakeHandle(kFsId, id, 0, kSize);
        auto content = PatternFor(id, 0, kSize);
        if (!cache->Cache(h, MakeBlock(content)).ok()) {
          ++failures;
          continue;
        }
        if (!WaitUntil([&] {
              IOBuffer buf = MakeReadBuf(kSize);
              return cache
                         ->Range(h, 0, kSize, &buf,
                                 {.retrieve_storage = false,
                                  .block_whole_length = kSize})
                         .ok() &&
                     ReadAll(buf) == content;
            })) {
          ++failures;
        }
      }
    });
  }
  for (auto& w : workers) w.join();
  ASSERT_EQ(failures.load(), 0);

  // Every block survives and reads back correctly after the storm settles.
  for (uint64_t id = 1; id <= kThreads * kPerThread; ++id) {
    auto h = MakeHandle(kFsId, id, 0, kSize);
    IOBuffer buf = MakeReadBuf(kSize);
    ASSERT_TRUE(
        cache
            ->Range(h, 0, kSize, &buf,
                    {.retrieve_storage = false, .block_whole_length = kSize})
            .ok())
        << "id=" << id;
    EXPECT_EQ(ReadAll(buf), PatternFor(id, 0, kSize)) << "id=" << id;
  }
}

}  // namespace integration
}  // namespace cache
}  // namespace dingofs
