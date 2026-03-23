/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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

#include <vector>

#include "client/vfs/data/flat/flat_chunk.h"
#include "client/vfs/data/flat/flat_file.h"
#include "client/vfs/vfs_meta.h"
#include "test/unit/client/vfs/test_common.h"

namespace dingofs {
namespace client {
namespace vfs {

// ─── FlatFileChunk ───────────────────────────────────────────────────────────

// An empty slice list means there is no data in the chunk; GenBlockReadReqs
// should return an empty result (the zero/hole path produces no block reqs).
TEST(FlatFileChunkTest, EmptySlices_NoBlockReqs) {
  constexpr uint64_t kChunkSize = 67108864;  // 64 MiB
  constexpr uint64_t kBlockSize = 4194304;   // 4 MiB

  FlatFileChunk chunk(/*fs_id=*/1, /*ino=*/10, /*index=*/0, kChunkSize,
                      kBlockSize, /*chunk_slices=*/{});

  auto reqs = chunk.GenBlockReadReqs();
  EXPECT_TRUE(reqs.empty());
}

// A single non-zero slice that covers exactly one block should produce
// exactly one BlockReadReq.
TEST(FlatFileChunkTest, SingleSlice_OneBlock) {
  constexpr uint64_t kChunkSize = 67108864;  // 64 MiB
  constexpr uint64_t kBlockSize = 4194304;   // 4 MiB

  // One slice at file offset 0, length == block_size
  Slice s = test::MakeSlice(/*id=*/1, /*offset=*/0, /*length=*/kBlockSize);

  FlatFileChunk chunk(1, 10, 0, kChunkSize, kBlockSize, {s});
  auto reqs = chunk.GenBlockReadReqs();

  ASSERT_EQ(reqs.size(), 1u);
  EXPECT_EQ(reqs[0].file_offset, 0);
  EXPECT_EQ(reqs[0].len, static_cast<int64_t>(kBlockSize));
}

// A slice spanning two blocks should yield two BlockReadReqs.
TEST(FlatFileChunkTest, SingleSlice_TwoBlocks) {
  constexpr uint64_t kChunkSize = 67108864;
  constexpr uint64_t kBlockSize = 4194304;

  // Slice covers the first two blocks exactly
  Slice s = test::MakeSlice(2, 0, kBlockSize * 2);

  FlatFileChunk chunk(1, 11, 0, kChunkSize, kBlockSize, {s});
  auto reqs = chunk.GenBlockReadReqs();

  ASSERT_EQ(reqs.size(), 2u);
  EXPECT_EQ(reqs[0].file_offset, 0);
  EXPECT_EQ(reqs[1].file_offset, static_cast<int64_t>(kBlockSize));
}

// A zero-flag slice should not contribute any BlockReadReq (it is treated as a
// hole and skipped by FlatFileChunk::GenBlockReadReqs).
TEST(FlatFileChunkTest, ZeroSlice_NoBlockReqs) {
  constexpr uint64_t kChunkSize = 67108864;
  constexpr uint64_t kBlockSize = 4194304;

  Slice s = test::MakeSlice(3, 0, kBlockSize, /*is_zero=*/true);

  FlatFileChunk chunk(1, 12, 0, kChunkSize, kBlockSize, {s});
  auto reqs = chunk.GenBlockReadReqs();

  EXPECT_TRUE(reqs.empty());
}

// Two non-overlapping slices that together fill two blocks → two BlockReadReqs.
TEST(FlatFileChunkTest, MultiSlice_NonOverlapping_Boundaries) {
  constexpr uint64_t kChunkSize = 67108864;
  constexpr uint64_t kBlockSize = 4194304;

  std::vector<Slice> slices = {
      test::MakeSlice(10, 0, kBlockSize),
      test::MakeSlice(11, kBlockSize, kBlockSize),
  };

  FlatFileChunk chunk(1, 13, 0, kChunkSize, kBlockSize, slices);
  auto reqs = chunk.GenBlockReadReqs();

  ASSERT_EQ(reqs.size(), 2u);
  EXPECT_EQ(reqs[0].file_offset, 0);
  EXPECT_EQ(reqs[1].file_offset, static_cast<int64_t>(kBlockSize));
}

// ─── FlatFile ────────────────────────────────────────────────────────────────

TEST(FlatFileTest, Accessors) {
  FlatFile ff(/*fs_id=*/5, /*ino=*/50, /*chunk_size=*/67108864,
              /*block_size=*/4194304);
  EXPECT_EQ(ff.GetFsId(), 5u);
  EXPECT_EQ(ff.GetIno(), 50u);
  EXPECT_EQ(ff.GetChunkSize(), 67108864);
  EXPECT_EQ(ff.GetBlockSize(), 4194304);
}

// A FlatFile with no chunks filled should produce no block requests.
TEST(FlatFileTest, NoChunks_NoBlockReqs) {
  FlatFile ff(1, 20, 67108864, 4194304);
  auto reqs = ff.GenBlockReadReqs();
  EXPECT_TRUE(reqs.empty());
}

// Fill two separate chunks with one block each; expect two BlockReadReqs in
// ascending chunk order.
TEST(FlatFileTest, MultiChunk_OrderedBlockReqs) {
  constexpr int64_t kChunkSize = 67108864;
  constexpr int64_t kBlockSize = 4194304;

  FlatFile ff(1, 30, kChunkSize, kBlockSize);

  // Chunk 0: one slice at file offset 0
  ff.FillChunk(0, {test::MakeSlice(100, 0, kBlockSize)});
  // Chunk 1: one slice at file offset chunk_size
  ff.FillChunk(1,
               {test::MakeSlice(101, static_cast<uint64_t>(kChunkSize),
                                static_cast<uint64_t>(kBlockSize))});

  auto reqs = ff.GenBlockReadReqs();
  ASSERT_EQ(reqs.size(), 2u);
  EXPECT_LT(reqs[0].file_offset, reqs[1].file_offset);
}

// FillChunk with an empty slice list is valid (represents a hole chunk).
TEST(FlatFileTest, EmptyChunk_NoBlockReqs) {
  FlatFile ff(1, 40, 67108864, 4194304);
  ff.FillChunk(0, {});
  auto reqs = ff.GenBlockReadReqs();
  EXPECT_TRUE(reqs.empty());
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
