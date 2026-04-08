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

  Slice s = test::MakeSlice(/*id=*/0, /*pos=*/0, /*size=*/kBlockSize);

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
  ff.FillChunk(1, {test::MakeSlice(101, 0, kBlockSize)});

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

// ─── End-to-end: verify BlockKey fields (index, size, offset_in_block) ──────

// Helper to check all BlockReadReq fields
static void CheckReq(const BlockReadReq& req, uint64_t slice_id,
                     int32_t block_index, int32_t block_size,
                     int64_t file_offset, int32_t offset_in_block,
                     int32_t len) {
  ASSERT_TRUE(req.key.has_value()) << "Expected non-hole block";
  EXPECT_EQ(req.key->id, slice_id);
  EXPECT_EQ(req.key->index, block_index);
  EXPECT_EQ(req.key->size, block_size);
  EXPECT_EQ(req.file_offset, file_offset);
  EXPECT_EQ(req.offset_in_block, offset_in_block);
  EXPECT_EQ(req.len, len);
}

// Single slice covering 9MB at chunk pos=0. block_size=4MB.
// Expected: 3 blocks with slice-relative indices 0,1,2.
// Last block size = 1MB (self-describing).
TEST(FlatFileChunkE2ETest, SliceRelative_ThreeBlocks_LastSmaller) {
  constexpr int32_t kChunkSize = 64 * 1024 * 1024;
  constexpr int32_t kBlockSize = 4 * 1024 * 1024;
  constexpr int32_t k1MB = 1024 * 1024;

  // slice: id=1001, pos=0, size=9MB, off=0, len=9MB
  Slice s{.id = 1001, .pos = 0, .size = 9 * k1MB, .off = 0, .len = 9 * k1MB};

  FlatFileChunk chunk(1, 10, /*index=*/0, kChunkSize, kBlockSize, {s});
  auto reqs = chunk.GenBlockReadReqs();

  ASSERT_EQ(reqs.size(), 3u);
  CheckReq(reqs[0], 1001, 0, 4 * k1MB, 0, 0, 4 * k1MB);
  CheckReq(reqs[1], 1001, 1, 4 * k1MB, 4 * k1MB, 0, 4 * k1MB);
  CheckReq(reqs[2], 1001, 2, 1 * k1MB, 8 * k1MB, 0, 1 * k1MB);
}

// Slice at non-zero chunk pos. block_index must still start from 0.
// Design doc scenario: slice pos=5MB, size=9MB.
TEST(FlatFileChunkE2ETest, SliceRelative_NonZeroPos_IndexFromZero) {
  constexpr int32_t kChunkSize = 64 * 1024 * 1024;
  constexpr int32_t kBlockSize = 4 * 1024 * 1024;
  constexpr int32_t k1MB = 1024 * 1024;

  Slice s{.id = 1001,
          .pos = 5 * k1MB,
          .size = 9 * k1MB,
          .off = 0,
          .len = 9 * k1MB};

  FlatFileChunk chunk(1, 10, /*index=*/0, kChunkSize, kBlockSize, {s});
  auto reqs = chunk.GenBlockReadReqs();

  ASSERT_EQ(reqs.size(), 3u);
  // block[0]: 4MB, file_offset=5MB, offset_in_block=0
  CheckReq(reqs[0], 1001, 0, 4 * k1MB, 5 * k1MB, 0, 4 * k1MB);
  // block[1]: 4MB, file_offset=9MB, offset_in_block=0
  CheckReq(reqs[1], 1001, 1, 4 * k1MB, 9 * k1MB, 0, 4 * k1MB);
  // block[2]: 1MB (tail), file_offset=13MB, offset_in_block=0
  CheckReq(reqs[2], 1001, 2, 1 * k1MB, 13 * k1MB, 0, 1 * k1MB);
}

// CopyFileRange scenario: off!=0. slice references existing physical data.
// slice: id=100, pos=0, size=12MB, off=8MB, len=4MB
// Logical range: [0, 4MB). Physical range: [8MB, 12MB) → block[2].
TEST(FlatFileChunkE2ETest, CopyFileRange_OffNonZero) {
  constexpr int32_t kChunkSize = 64 * 1024 * 1024;
  constexpr int32_t kBlockSize = 4 * 1024 * 1024;
  constexpr int32_t k1MB = 1024 * 1024;

  Slice s{.id = 100,
          .pos = 0,
          .size = 12 * k1MB,
          .off = 8 * k1MB,
          .len = 4 * k1MB};

  FlatFileChunk chunk(1, 10, /*index=*/0, kChunkSize, kBlockSize, {s});
  auto reqs = chunk.GenBlockReadReqs();

  ASSERT_EQ(reqs.size(), 1u);
  // physical_offset = 8MB, block_index = 8MB/4MB = 2, offset_in_block = 0
  CheckReq(reqs[0], 100, 2, 4 * k1MB, 0, 0, 4 * k1MB);
}

// CopyFileRange crossing multiple blocks: off=3MB, len=6MB, size=12MB.
// Physical range: [3MB, 9MB) → crosses block[0], block[1], block[2].
TEST(FlatFileChunkE2ETest, CopyFileRange_CrossThreeBlocks) {
  constexpr int32_t kChunkSize = 64 * 1024 * 1024;
  constexpr int32_t kBlockSize = 4 * 1024 * 1024;
  constexpr int32_t k1MB = 1024 * 1024;

  Slice s{.id = 300,
          .pos = 0,
          .size = 12 * k1MB,
          .off = 3 * k1MB,
          .len = 6 * k1MB};

  FlatFileChunk chunk(1, 10, /*index=*/0, kChunkSize, kBlockSize, {s});
  auto reqs = chunk.GenBlockReadReqs();

  ASSERT_EQ(reqs.size(), 3u);
  // block[0]: physical [3MB,4MB), offset_in_block=3MB, len=1MB
  CheckReq(reqs[0], 300, 0, 4 * k1MB, 0, 3 * k1MB, 1 * k1MB);
  // block[1]: physical [4MB,8MB), offset_in_block=0, len=4MB
  CheckReq(reqs[1], 300, 1, 4 * k1MB, 1 * k1MB, 0, 4 * k1MB);
  // block[2]: physical [8MB,9MB), offset_in_block=0, len=1MB
  CheckReq(reqs[2], 300, 2, 4 * k1MB, 5 * k1MB, 0, 1 * k1MB);
}

// Zero slice (id=0) should produce no BlockReadReqs.
TEST(FlatFileChunkE2ETest, ZeroSlice_Hole) {
  constexpr int32_t kChunkSize = 64 * 1024 * 1024;
  constexpr int32_t kBlockSize = 4 * 1024 * 1024;

  Slice s{.id = 0, .pos = 0, .size = kBlockSize, .off = 0, .len = kBlockSize};

  FlatFileChunk chunk(1, 10, /*index=*/0, kChunkSize, kBlockSize, {s});
  auto reqs = chunk.GenBlockReadReqs();

  EXPECT_TRUE(reqs.empty());
}

// Multi-chunk FlatFile: verify chunk_start offsets are correct.
// Chunk 0 has slice id=100, chunk 1 has slice id=200.
// Block indices should be slice-relative (0) for both chunks.
TEST(FlatFileE2ETest, MultiChunk_BlockKeysCorrect) {
  constexpr int32_t kChunkSize = 64 * 1024 * 1024;
  constexpr int32_t kBlockSize = 4 * 1024 * 1024;

  FlatFile ff(1, 30, kChunkSize, kBlockSize);

  // Chunk 0: one block
  ff.FillChunk(0, {Slice{.id = 100, .pos = 0, .size = kBlockSize, .off = 0,
                         .len = kBlockSize}});
  // Chunk 1: one block
  ff.FillChunk(1, {Slice{.id = 200, .pos = 0, .size = kBlockSize, .off = 0,
                         .len = kBlockSize}});

  auto reqs = ff.GenBlockReadReqs();
  ASSERT_EQ(reqs.size(), 2u);

  // Chunk 0: file_offset=0, block_index=0
  CheckReq(reqs[0], 100, 0, kBlockSize, 0, 0, kBlockSize);
  // Chunk 1: file_offset=chunk_size, block_index=0 (slice-relative!)
  CheckReq(reqs[1], 200, 0, kBlockSize, kChunkSize, 0, kBlockSize);
}

// Two slices in same chunk, non-overlapping. Verify both get correct keys.
TEST(FlatFileChunkE2ETest, TwoSlices_IndependentBlockKeys) {
  constexpr int32_t kChunkSize = 64 * 1024 * 1024;
  constexpr int32_t kBlockSize = 4 * 1024 * 1024;

  std::vector<Slice> slices = {
      Slice{.id = 10, .pos = 0, .size = kBlockSize, .off = 0,
            .len = kBlockSize},
      Slice{.id = 11, .pos = kBlockSize, .size = kBlockSize, .off = 0,
            .len = kBlockSize},
  };

  FlatFileChunk chunk(1, 13, 0, kChunkSize, kBlockSize, slices);
  auto reqs = chunk.GenBlockReadReqs();

  ASSERT_EQ(reqs.size(), 2u);
  // First slice: id=10, block_index=0
  CheckReq(reqs[0], 10, 0, kBlockSize, 0, 0, kBlockSize);
  // Second slice: id=11, block_index=0 (each slice starts from 0!)
  CheckReq(reqs[1], 11, 0, kBlockSize, kBlockSize, 0, kBlockSize);
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
