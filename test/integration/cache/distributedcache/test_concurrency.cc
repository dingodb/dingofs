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

#include "test/integration/cache/deploy/fixture.h"

namespace dingofs {
namespace cache {
namespace integration {

// Many threads concurrently Put+Range disjoint blocks against one remote node,
// stressing the upstream / peer connection pool. No crash, and every block
// reads back byte-correct over the network.
TEST_F(DistributedCacheTest, ConcurrentRemotePutRange) {
  constexpr uint32_t kSize = 64 * 1024;
  constexpr int kThreads = 4;
  constexpr uint64_t kPerThread = 3;
  auto* cache = client_.cache();

  // Warm up one remote op so all worker threads start after the upstream has
  // discovered the node (avoids every thread racing the first member sync).
  PutRemote(cache, MakeHandle(fs_id_, 1, 0, kSize), PatternFor(1, 0, kSize));

  std::atomic<int> failures{0};
  std::vector<std::thread> workers;
  for (int t = 0; t < kThreads; ++t) {
    workers.emplace_back([&, t] {
      for (uint64_t i = 0; i < kPerThread; ++i) {
        uint64_t id = static_cast<uint64_t>(t) * kPerThread + i + 100;
        auto h = MakeHandle(fs_id_, id, 0, kSize);
        auto content = PatternFor(id, 0, kSize);
        if (!cache
                 ->Put(h, MakeBlock(content),
                       {.writeback = true, .tier = CacheTier::kRemote})
                 .ok()) {
          ++failures;
          continue;
        }
        IOBuffer buf = MakeReadBuf(kSize);
        if (!cache
                 ->Range(h, 0, kSize, &buf,
                         {.retrieve_storage = true,
                          .block_whole_length = kSize,
                          .tier = CacheTier::kRemote})
                 .ok() ||
            ReadAll(buf) != content) {
          ++failures;
        }
      }
    });
  }
  for (auto& w : workers) w.join();
  ASSERT_EQ(failures.load(), 0);
}

}  // namespace integration
}  // namespace cache
}  // namespace dingofs
