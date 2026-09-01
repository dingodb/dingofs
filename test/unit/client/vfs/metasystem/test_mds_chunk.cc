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

#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <vector>

#include "client/vfs/metasystem/mds/chunk.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {
namespace test {

using namespace std::chrono_literals;

TEST(ReadChunkCacheTest, DeleteByInoRemovesOnlyTargetInode) {
  ReadChunkCache cache;
  ChunkEntry first;
  first.set_index(0);
  ChunkEntry second;
  second.set_index(1);

  cache.Put(1, first);
  cache.Put(1, second);
  cache.Put(2, first);
  cache.DeleteByIno(1);

  ChunkEntry out;
  EXPECT_FALSE(cache.Get(1, 0, out));
  EXPECT_FALSE(cache.Get(1, 1, out));
  EXPECT_TRUE(cache.Get(2, 0, out));
}

TEST(ChunkSetTest, FlushGuardDefersConcurrentWrites) {
  auto chunk_set = ChunkSet::New(100, 1 << 20);
  auto flush_guard = chunk_set->AcquireFlushGuard();

  std::promise<void> started;
  auto write = std::async(std::launch::async, [&]() {
    started.set_value();
    chunk_set->Append(
        0, {Slice{.id = 1, .size = 4096, .off = 0, .len = 4096, .pos = 0}});
  });

  started.get_future().wait();
  EXPECT_EQ(write.wait_for(20ms), std::future_status::timeout);
  EXPECT_FALSE(chunk_set->HasStage());

  flush_guard.unlock();

  EXPECT_EQ(write.wait_for(1s), std::future_status::ready);
  write.get();
  EXPECT_TRUE(chunk_set->HasStage());
}

TEST(ChunkSetTest, FlushGuardDefersWriteLengthUpdates) {
  auto chunk_set = ChunkSet::New(100, 1 << 20);
  auto flush_guard = chunk_set->AcquireFlushGuard();

  std::promise<void> started;
  auto write = std::async(std::launch::async, [&]() {
    started.set_value();
    chunk_set->SetLastWriteLength(0, 4096);
  });

  started.get_future().wait();
  EXPECT_EQ(write.wait_for(20ms), std::future_status::timeout);
  EXPECT_EQ(chunk_set->GetLastWriteLength(), 0);

  flush_guard.unlock();

  EXPECT_EQ(write.wait_for(1s), std::future_status::ready);
  write.get();
  EXPECT_EQ(chunk_set->GetLastWriteLength(), 4096);
}

}  // namespace test
}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs
