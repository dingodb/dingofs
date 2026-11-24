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
 * Created Date: 2026-09-24
 * Author: AI
 */

#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

#include "blockcache/common/block_handle.h"
#include "blockcache/common/route.h"
#include "blockcache/core/runtime/smp.h"
#include "blockcache/utils/hash.h"
#include "test/unit/blockcache/base/runtime.h"

namespace dingofs {
namespace blockcache {

constexpr uint64_t kFsId = 10001;
constexpr uint32_t kBlockSize = 4 * 1024 * 1024;
constexpr unsigned kShardCounts[] = {1, 2, 3, 7, 8, 64};

static BlockHandle Handle(uint64_t id, uint32_t index = 0) {
  return {.fs_id = kFsId, .id = id, .index = index, .size = kBlockSize};
}

TEST(RouteTest, GetRouteKey) {
  const BlockHandle handle = Handle(1001, 3);
  EXPECT_EQ(GetRouteKey(handle), HashInt(1001));

  {  // every block of a slice routes alike: only the slice id counts
    EXPECT_EQ(GetRouteKey(Handle(1001, 0)), GetRouteKey(handle));
    EXPECT_EQ(GetRouteKey(BlockHandle{
                  .fs_id = 10002, .id = 1001, .index = 9, .size = 4096}),
              GetRouteKey(handle));
    EXPECT_NE(GetRouteKey(Handle(1002, 3)), GetRouteKey(handle));
  }
}

TEST(RouteTest, GetShard) {
  for (const unsigned shards : kShardCounts) {
    EXPECT_EQ(GetShard(0, shards), 0u);
    EXPECT_EQ(GetShard(UINT64_MAX, shards), shards - 1);

    // the key space is cut into `shards` ranges, in order
    unsigned last = 0;
    for (int i = 0; i < 4096; i++) {
      const unsigned shard = GetShard((UINT64_MAX / 4096) * i, shards);
      EXPECT_LT(shard, shards);
      EXPECT_GE(shard, last);
      last = shard;
    }
  }

  {  // ... and the ranges are equal: a boundary every 2^64 / shards keys
    EXPECT_EQ(GetShard(UINT64_MAX / 2, 2), 0u);
    EXPECT_EQ(GetShard(UINT64_MAX / 2 + 1, 2), 1u);
    EXPECT_EQ(GetShard((uint64_t{1} << 62) - 1, 4), 0u);
    EXPECT_EQ(GetShard(uint64_t{1} << 62, 4), 1u);
    EXPECT_EQ(GetShard(uint64_t{3} << 62, 4), 3u);
  }
}

TEST(RouteTest, GetOwnerShard) {
  for (const unsigned shards : kShardCounts) {
    for (uint64_t id = 1001; id < 1001 + 32; id++) {
      EXPECT_EQ(GetOwnerShard(Handle(id), shards),
                GetShard(HashInt(id), shards));
      EXPECT_LT(GetOwnerShard(Handle(id), shards), shards);
    }
  }

  {  // consecutive slice ids are spread evenly, not handed out in runs
    constexpr uint32_t kOwners = 4;
    constexpr uint64_t kSlices = 10000;
    std::vector<uint64_t> owned(kOwners, 0);
    for (uint64_t id = 1001; id < 1001 + kSlices; id++) {
      owned[GetOwnerShard(Handle(id), kOwners)]++;
    }
    for (uint32_t owner = 0; owner < kOwners; owner++) {
      EXPECT_GT(owned[owner], kSlices / kOwners * 8 / 10) << owner;
      EXPECT_LT(owned[owner], kSlices / kOwners * 12 / 10) << owner;
    }
  }

  {  // without a count, it divides by the runtime's shard count
    ScopedRuntime runtime;
    const unsigned shards = ShardCount();
    ASSERT_GE(shards, 2u) << "with one shard every block maps to 0";
    for (uint64_t id = 1001; id < 1001 + 32; id++) {
      EXPECT_EQ(GetOwnerShard(Handle(id)), GetOwnerShard(Handle(id), shards));
    }
  }
}

TEST(RouteTest, GetShardStartKey) {
  {  // a lone shard (or none) starts at key 0
    EXPECT_EQ(GetShardStartKey(0, 0), 0u);
    EXPECT_EQ(GetShardStartKey(0, 1), 0u);
  }

  {  // exactly the first key of the shard's range
    EXPECT_EQ(GetShardStartKey(0, 2), 0u);
    EXPECT_EQ(GetShardStartKey(1, 2), uint64_t{1} << 63);
    EXPECT_EQ(GetShardStartKey(3, 4), uint64_t{3} << 62);
  }

  {  // rounded up where `shards` does not divide 2^64
    EXPECT_EQ(GetShardStartKey(1, 3), UINT64_MAX / 3 + 1);
    EXPECT_EQ(GetShardStartKey(2, 3), UINT64_MAX / 3 * 2 + 1);
  }

  {  // so it routes to its shard, and the key before it to the shard before
    for (const unsigned shards : kShardCounts) {
      for (unsigned shard = 0; shard < shards; shard++) {
        const uint64_t start = GetShardStartKey(shard, shards);
        EXPECT_EQ(GetShard(start, shards), shard) << shard << " of " << shards;
        if (shard > 0) {
          EXPECT_EQ(GetShard(start - 1, shards), shard - 1)
              << shard << " of " << shards;
        }
      }
    }
  }
}

}  // namespace blockcache
}  // namespace dingofs
