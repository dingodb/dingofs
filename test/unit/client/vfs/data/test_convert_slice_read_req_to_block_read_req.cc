
// Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <cstdint>
#include <optional>
#include <vector>

#include "client/vfs/data/common/common.h"
#include "client/vfs/data/common/data_utils.h"
#include "client/vfs/data/test_data_utils_common.h"
#include "common/block/block_key.h"

namespace dingofs {
namespace client {
namespace vfs {

// Constants for readability (in bytes)
static constexpr int32_t MB = 1024 * 1024;

// Helper to verify a BlockReadReq against expected values.
// key_id:           expected BlockKey.id (slice id)
// key_index:        expected BlockKey.index (slice-relative block index)
// key_size:         expected BlockKey.size (actual block size in bytes)
// file_offset:      expected file_offset in the BlockReadReq
// block_offset:  expected offset within the block object
// len:              expected read length
static void CheckBlockReadReq(const BlockReadReq& req, uint64_t key_id,
                               uint32_t key_index, uint32_t key_size,
                               int64_t file_offset, int32_t block_offset,
                               int32_t len) {
  ASSERT_TRUE(req.key.has_value()) << "BlockReadReq.key should not be nullopt";
  EXPECT_EQ(req.key->id, key_id);
  EXPECT_EQ(req.key->index, key_index);
  EXPECT_EQ(req.key->size, key_size);
  EXPECT_EQ(req.file_offset, file_offset);
  EXPECT_EQ(req.block_offset, block_offset);
  EXPECT_EQ(req.len, len);
}

// Helper function to create a SliceReadReq object
static SliceReadReq CreateSliceReadReq(int64_t file_offset, int64_t read_len,
                                       const Slice& slice) {
  return SliceReadReq{
      .file_offset = file_offset,
      .len = read_len,
      .slice = slice,
  };
}

// ============================================================
// off=0 basic scenarios
// ============================================================

// 1. Single block, full read
// slice{pos=0, size=4MB, off=0, len=4MB}, read entire slice
// Expected: 1 req, key(index=0, size=4MB), block_offset=0
TEST(ConvertSliceReadReqToBlockReadReqsTest, SingleBlock_FullRead) {
  Slice slice = CreateSlice(/*id=*/1, /*pos=*/0, /*size=*/4 * MB);
  SliceReadReq slice_req = CreateSliceReadReq(0, 4 * MB, slice);

  uint32_t fs_id = 1;
  uint64_t ino = 2;
  int32_t chunk_size = 64 * MB;
  int32_t block_size = 4 * MB;

  auto reqs = ConvertSliceReadReqToBlockReadReqs(slice_req, fs_id, ino,
                                                  chunk_size, block_size, 0);

  ASSERT_EQ(reqs.size(), 1u);
  CheckBlockReadReq(reqs[0], /*key_id=*/1, /*key_index=*/0,
                    /*key_size=*/4 * MB, /*file_offset=*/0,
                    /*block_offset=*/0, /*len=*/4 * MB);
}

// 2. Single block, partial read
// slice{pos=0, size=4MB, off=0, len=4MB}, read file_offset=1MB, len=1MB
// Expected: 1 req, key(index=0, size=4MB), block_offset=1MB
TEST(ConvertSliceReadReqToBlockReadReqsTest, SingleBlock_PartialRead) {
  Slice slice = CreateSlice(/*id=*/1, /*pos=*/0, /*size=*/4 * MB);
  SliceReadReq slice_req = CreateSliceReadReq(1 * MB, 1 * MB, slice);

  uint32_t fs_id = 1;
  uint64_t ino = 2;
  int32_t chunk_size = 64 * MB;
  int32_t block_size = 4 * MB;

  auto reqs = ConvertSliceReadReqToBlockReadReqs(slice_req, fs_id, ino,
                                                  chunk_size, block_size, 0);

  ASSERT_EQ(reqs.size(), 1u);
  CheckBlockReadReq(reqs[0], 1, 0, 4 * MB, 1 * MB, 1 * MB, 1 * MB);
}

// 3. Single block, small slice (size < block_size)
// slice{pos=0, size=1MB, off=0, len=1MB}, block_size=4MB
// Expected: 1 req, key(index=0, size=1MB) -- self-describing actual size
TEST(ConvertSliceReadReqToBlockReadReqsTest, SingleBlock_SmallSlice) {
  Slice slice = CreateSlice(/*id=*/1, /*pos=*/0, /*size=*/1 * MB);
  SliceReadReq slice_req = CreateSliceReadReq(0, 1 * MB, slice);

  uint32_t fs_id = 1;
  uint64_t ino = 2;
  int32_t chunk_size = 64 * MB;
  int32_t block_size = 4 * MB;

  auto reqs = ConvertSliceReadReqToBlockReadReqs(slice_req, fs_id, ino,
                                                  chunk_size, block_size, 0);

  ASSERT_EQ(reqs.size(), 1u);
  CheckBlockReadReq(reqs[0], 1, 0, 1 * MB, 0, 0, 1 * MB);
}

// ============================================================
// off=0 cross-block scenarios
// ============================================================

// 4. Cross two blocks
// slice{pos=0, size=8MB, off=0, len=8MB}, read from 2MB len=4MB
// Expected: 2 reqs crossing block[0] and block[1]
TEST(ConvertSliceReadReqToBlockReadReqsTest, CrossBlock_TwoBlocks) {
  Slice slice = CreateSlice(/*id=*/1, /*pos=*/0, /*size=*/8 * MB);
  SliceReadReq slice_req = CreateSliceReadReq(2 * MB, 4 * MB, slice);

  uint32_t fs_id = 1;
  uint64_t ino = 2;
  int32_t chunk_size = 64 * MB;
  int32_t block_size = 4 * MB;

  auto reqs = ConvertSliceReadReqToBlockReadReqs(slice_req, fs_id, ino,
                                                  chunk_size, block_size, 0);

  ASSERT_EQ(reqs.size(), 2u);
  // block[0]: physical 2MB..4MB, block_offset=2MB, len=2MB
  CheckBlockReadReq(reqs[0], 1, 0, 4 * MB, 2 * MB, 2 * MB, 2 * MB);
  // block[1]: physical 4MB..8MB, block_offset=0, len=2MB
  CheckBlockReadReq(reqs[1], 1, 1, 4 * MB, 4 * MB, 0, 2 * MB);
}

// 5. Design doc section 5.2 example: cross blocks with non-zero pos
// slice{id=1001, pos=5MB, size=9MB, off=0, len=9MB}
// read from file_offset=7MB (chunk_start=0), len=5MB
// physical_offset = 0 + (7MB - 5MB) = 2MB -> block[0]
// Expected: 2 reqs
TEST(ConvertSliceReadReqToBlockReadReqsTest,
     CrossBlock_ThreeBlocks_DesignDoc) {
  Slice slice = CreateSlice(/*id=*/1001, /*pos=*/5 * MB, /*size=*/9 * MB);
  // file_offset = chunk_start + pos + read_within_slice
  // chunk_start = 0, slice_offset = 0 + 5MB = 5MB
  // read from file_offset=7MB, len=5MB -> covers [7MB, 12MB)
  SliceReadReq slice_req = CreateSliceReadReq(7 * MB, 5 * MB, slice);

  uint32_t fs_id = 1;
  uint64_t ino = 2;
  int32_t chunk_size = 64 * MB;
  int32_t block_size = 4 * MB;

  auto reqs = ConvertSliceReadReqToBlockReadReqs(slice_req, fs_id, ino,
                                                  chunk_size, block_size, 0);

  ASSERT_EQ(reqs.size(), 2u);
  // block[0]: physical_offset=2MB, block_aligned=0..4MB
  //   file range: 5MB + (0-0) = 5MB to 5MB + (4MB-0) = 9MB
  //   read from 7MB, len = 9MB - 7MB = 2MB
  //   block_offset = 2MB % 4MB = 2MB
  CheckBlockReadReq(reqs[0], 1001, 0, 4 * MB, 7 * MB, 2 * MB, 2 * MB);
  // block[1]: physical_offset=4MB, block_aligned=4MB..8MB
  //   file range: 5MB + (4MB-0) = 9MB to 5MB + (8MB-0) = 13MB
  //   read from 9MB, len = min(3MB, 13MB-9MB) = 3MB
  //   block_offset = 4MB % 4MB = 0
  CheckBlockReadReq(reqs[1], 1001, 1, 4 * MB, 9 * MB, 0, 3 * MB);
}

// 6. Read entire slice spanning 3 blocks
// slice{pos=0, size=9MB, off=0, len=9MB}, read all 9MB
// Expected: 3 reqs, last block key.size=1MB
TEST(ConvertSliceReadReqToBlockReadReqsTest, CrossBlock_EntireSlice) {
  Slice slice = CreateSlice(/*id=*/1, /*pos=*/0, /*size=*/9 * MB);
  SliceReadReq slice_req = CreateSliceReadReq(0, 9 * MB, slice);

  uint32_t fs_id = 1;
  uint64_t ino = 2;
  int32_t chunk_size = 64 * MB;
  int32_t block_size = 4 * MB;

  auto reqs = ConvertSliceReadReqToBlockReadReqs(slice_req, fs_id, ino,
                                                  chunk_size, block_size, 0);

  ASSERT_EQ(reqs.size(), 3u);
  // block[0]: 0..4MB, full block
  CheckBlockReadReq(reqs[0], 1, 0, 4 * MB, 0, 0, 4 * MB);
  // block[1]: 4MB..8MB, full block
  CheckBlockReadReq(reqs[1], 1, 1, 4 * MB, 4 * MB, 0, 4 * MB);
  // block[2]: 8MB..9MB, actual size=1MB (self-describing)
  CheckBlockReadReq(reqs[2], 1, 2, 1 * MB, 8 * MB, 0, 1 * MB);
}

// ============================================================
// CopyFileRange (off != 0) scenarios
// ============================================================

// 7. Design doc section 5.3: CopyFileRange single block
// Source slice: {id=100, size=12MB}
// Target slice: {id=100, pos=0, size=12MB, off=8MB, len=4MB}
// read file_offset=0, len=4MB
// physical_offset = 8MB + (0 - 0) = 8MB -> block_index=2
// Expected: 1 req, key(index=2, size=4MB), block_offset=0
TEST(ConvertSliceReadReqToBlockReadReqsTest, CopyFileRange_SingleBlock) {
  Slice slice = CreateSlice(/*id=*/100, /*pos=*/0, /*size=*/12 * MB,
                            /*off=*/8 * MB, /*len=*/4 * MB);
  SliceReadReq slice_req = CreateSliceReadReq(0, 4 * MB, slice);

  uint32_t fs_id = 1;
  uint64_t ino = 2;
  int32_t chunk_size = 64 * MB;
  int32_t block_size = 4 * MB;

  auto reqs = ConvertSliceReadReqToBlockReadReqs(slice_req, fs_id, ino,
                                                  chunk_size, block_size, 0);

  ASSERT_EQ(reqs.size(), 1u);
  CheckBlockReadReq(reqs[0], 100, 2, 4 * MB, 0, 0, 4 * MB);
}

// 8. CopyFileRange partial block
// slice{id=200, pos=0, size=12MB, off=6MB, len=2MB}
// read file_offset=0, len=2MB
// physical_offset = 6MB + (0 - 0) = 6MB -> block_index=1, block_offset=2MB
// block_aligned_start=4MB, block_aligned_end=min(8MB, 12MB)=8MB
// file_block_start = 0 + (4MB - 6MB) = -2MB -> clamped to 0
// file_block_end = 0 + (8MB - 6MB) = 2MB
// Expected: 1 req
TEST(ConvertSliceReadReqToBlockReadReqsTest, CopyFileRange_PartialBlock) {
  Slice slice = CreateSlice(/*id=*/200, /*pos=*/0, /*size=*/12 * MB,
                            /*off=*/6 * MB, /*len=*/2 * MB);
  SliceReadReq slice_req = CreateSliceReadReq(0, 2 * MB, slice);

  uint32_t fs_id = 1;
  uint64_t ino = 2;
  int32_t chunk_size = 64 * MB;
  int32_t block_size = 4 * MB;

  auto reqs = ConvertSliceReadReqToBlockReadReqs(slice_req, fs_id, ino,
                                                  chunk_size, block_size, 0);

  ASSERT_EQ(reqs.size(), 1u);
  // physical_offset=6MB, block_index=6MB/4MB=1, block_offset=6MB%4MB=2MB
  CheckBlockReadReq(reqs[0], 200, 1, 4 * MB, 0, 2 * MB, 2 * MB);
}

// 9. CopyFileRange crossing three blocks
// slice{id=300, pos=0, size=12MB, off=3MB, len=6MB}
// read file_offset=0, len=6MB
// physical start: 3MB, physical end: 3MB + 6MB = 9MB
// block[0]: physical 0..4MB -> read 3MB..4MB (1MB), block_offset=3MB
// block[1]: physical 4MB..8MB -> read 4MB..8MB (4MB), block_offset=0
// block[2]: physical 8MB..12MB -> read 8MB..9MB (1MB), block_offset=0
// Expected: 3 reqs
TEST(ConvertSliceReadReqToBlockReadReqsTest, CopyFileRange_CrossThreeBlocks) {
  Slice slice = CreateSlice(/*id=*/300, /*pos=*/0, /*size=*/12 * MB,
                            /*off=*/3 * MB, /*len=*/6 * MB);
  SliceReadReq slice_req = CreateSliceReadReq(0, 6 * MB, slice);

  uint32_t fs_id = 1;
  uint64_t ino = 2;
  int32_t chunk_size = 64 * MB;
  int32_t block_size = 4 * MB;

  auto reqs = ConvertSliceReadReqToBlockReadReqs(slice_req, fs_id, ino,
                                                  chunk_size, block_size, 0);

  ASSERT_EQ(reqs.size(), 3u);
  // block[0]: physical_offset=3MB, index=0, block_offset=3MB
  //   file_block_end = 0 + (4MB - 3MB) = 1MB
  //   len = min(6MB, 1MB - 0) = 1MB
  CheckBlockReadReq(reqs[0], 300, 0, 4 * MB, 0, 3 * MB, 1 * MB);
  // block[1]: physical_offset=4MB, index=1, block_offset=0
  //   file range: 0 + (4MB - 3MB) = 1MB to 0 + (8MB - 3MB) = 5MB
  //   read from 1MB, len = min(5MB, 5MB - 1MB) = 4MB
  CheckBlockReadReq(reqs[1], 300, 1, 4 * MB, 1 * MB, 0, 4 * MB);
  // block[2]: physical_offset=8MB, index=2, block_offset=0
  //   file range: 0 + (8MB - 3MB) = 5MB to 0 + (12MB - 3MB) = 9MB
  //   but clamped to slice_offset + slice.len = 0 + 6MB = 6MB
  //   read from 5MB, len = min(1MB, 6MB - 5MB) = 1MB
  CheckBlockReadReq(reqs[2], 300, 2, 4 * MB, 5 * MB, 0, 1 * MB);
}

// 10. CopyFileRange large offset spanning 4 blocks
// slice{id=400, pos=0, size=20MB, off=6MB, len=10MB}
// read file_offset=0, len=10MB
// physical range: 6MB..16MB
// block[1]: 4MB..8MB -> read 6MB..8MB (2MB), block_offset=2MB
// block[2]: 8MB..12MB -> read 8MB..12MB (4MB)
// block[3]: 12MB..16MB -> read 12MB..16MB (4MB)
// Wait -- physical 6MB..16MB covers blocks 1,2,3 = 3 blocks.
// Actually: 2MB + 4MB + 4MB = 10MB. That's 3 reqs, not 4.
// Let me use off=5MB, len=10MB to get 4 blocks.
// physical range: 5MB..15MB
// block[1]: 4MB..8MB -> read 5MB..8MB (3MB), block_offset=1MB
// block[2]: 8MB..12MB -> 4MB
// block[3]: 12MB..16MB -> read 12MB..15MB (3MB)
// Still 3. For 4 reqs: off=3MB, len=10MB -> physical 3MB..13MB
// block[0]: 0..4MB -> 3MB..4MB (1MB)
// block[1]: 4..8MB -> 4MB (full)
// block[2]: 8..12MB -> 4MB (full)
// block[3]: 12..16MB -> 12..13MB (1MB)
// That's 4 reqs.
TEST(ConvertSliceReadReqToBlockReadReqsTest, CopyFileRange_LargeOffset) {
  Slice slice = CreateSlice(/*id=*/400, /*pos=*/0, /*size=*/20 * MB,
                            /*off=*/3 * MB, /*len=*/10 * MB);
  SliceReadReq slice_req = CreateSliceReadReq(0, 10 * MB, slice);

  uint32_t fs_id = 1;
  uint64_t ino = 2;
  int32_t chunk_size = 64 * MB;
  int32_t block_size = 4 * MB;

  auto reqs = ConvertSliceReadReqToBlockReadReqs(slice_req, fs_id, ino,
                                                  chunk_size, block_size, 0);

  ASSERT_EQ(reqs.size(), 4u);
  // block[0]: physical 3MB, index=0, block_offset=3MB, len=1MB
  CheckBlockReadReq(reqs[0], 400, 0, 4 * MB, 0, 3 * MB, 1 * MB);
  // block[1]: physical 4MB, index=1, block_offset=0, len=4MB
  CheckBlockReadReq(reqs[1], 400, 1, 4 * MB, 1 * MB, 0, 4 * MB);
  // block[2]: physical 8MB, index=2, block_offset=0, len=4MB
  CheckBlockReadReq(reqs[2], 400, 2, 4 * MB, 5 * MB, 0, 4 * MB);
  // block[3]: physical 12MB, index=3, block_offset=0, len=1MB
  CheckBlockReadReq(reqs[3], 400, 3, 4 * MB, 9 * MB, 0, 1 * MB);
}

// ============================================================
// Boundary conditions
// ============================================================

// 11. Last block smaller than block_size
// slice{pos=0, size=9MB, off=0, len=9MB}, read last 1MB
// read file_offset=8MB, len=1MB
// physical_offset = 8MB -> block_index=2, key.size=min(9MB-8MB, 4MB)=1MB
TEST(ConvertSliceReadReqToBlockReadReqsTest, LastBlock_SmallerThanBlockSize) {
  Slice slice = CreateSlice(/*id=*/1, /*pos=*/0, /*size=*/9 * MB);
  SliceReadReq slice_req = CreateSliceReadReq(8 * MB, 1 * MB, slice);

  uint32_t fs_id = 1;
  uint64_t ino = 2;
  int32_t chunk_size = 64 * MB;
  int32_t block_size = 4 * MB;

  auto reqs = ConvertSliceReadReqToBlockReadReqs(slice_req, fs_id, ino,
                                                  chunk_size, block_size, 0);

  ASSERT_EQ(reqs.size(), 1u);
  // block[2]: actual_size = min(9MB - 2*4MB, 4MB) = 1MB
  CheckBlockReadReq(reqs[0], 1, 2, 1 * MB, 8 * MB, 0, 1 * MB);
}

// 12. Single byte at block boundary
// slice{pos=0, size=64, off=0, len=64}, block_size=32
// read file_offset=32, len=1
// physical_offset=32 -> block_index=1, block_offset=0
TEST(ConvertSliceReadReqToBlockReadReqsTest, SingleByte_AtBlockBoundary) {
  Slice slice = CreateSlice(/*id=*/1, /*pos=*/0, /*size=*/64);
  SliceReadReq slice_req = CreateSliceReadReq(32, 1, slice);

  uint32_t fs_id = 1;
  uint64_t ino = 2;
  int32_t chunk_size = 256;
  int32_t block_size = 32;

  auto reqs = ConvertSliceReadReqToBlockReadReqs(slice_req, fs_id, ino,
                                                  chunk_size, block_size, 0);

  ASSERT_EQ(reqs.size(), 1u);
  // block[1]: key.size=32 (full block), block_offset=0
  CheckBlockReadReq(reqs[0], 1, 1, 32, 32, 0, 1);
}

// 13. Single byte before block boundary
// slice{pos=0, size=64, off=0, len=64}, block_size=32
// read file_offset=31, len=1
// physical_offset=31 -> block_index=0, block_offset=31
TEST(ConvertSliceReadReqToBlockReadReqsTest, SingleByte_BeforeBlockBoundary) {
  Slice slice = CreateSlice(/*id=*/1, /*pos=*/0, /*size=*/64);
  SliceReadReq slice_req = CreateSliceReadReq(31, 1, slice);

  uint32_t fs_id = 1;
  uint64_t ino = 2;
  int32_t chunk_size = 256;
  int32_t block_size = 32;

  auto reqs = ConvertSliceReadReqToBlockReadReqs(slice_req, fs_id, ino,
                                                  chunk_size, block_size, 0);

  ASSERT_EQ(reqs.size(), 1u);
  // block[0]: key.size=32, block_offset=31
  CheckBlockReadReq(reqs[0], 1, 0, 32, 31, 31, 1);
}

// 14. Non-zero chunk_start -- block_index is still slice-relative
// chunk_start=64MB, slice{pos=0, size=8MB, off=0, len=8MB}
// slice_offset = 64MB + 0 = 64MB
// read file_offset=66MB (= 64MB + 2MB), len=4MB
// physical_offset = 0 + (66MB - 64MB) = 2MB -> block_index=0
// This confirms block_index is slice-relative, not chunk-relative.
TEST(ConvertSliceReadReqToBlockReadReqsTest, NonZeroChunkStart) {
  Slice slice = CreateSlice(/*id=*/1, /*pos=*/0, /*size=*/8 * MB);
  int64_t chunk_start = 64 * MB;
  int64_t slice_offset = chunk_start + slice.pos;  // 64MB
  SliceReadReq slice_req =
      CreateSliceReadReq(slice_offset + 2 * MB, 4 * MB, slice);

  uint32_t fs_id = 1;
  uint64_t ino = 2;
  int32_t chunk_size = 64 * MB;
  int32_t block_size = 4 * MB;

  auto reqs = ConvertSliceReadReqToBlockReadReqs(
      slice_req, fs_id, ino, chunk_size, block_size, chunk_start);

  ASSERT_EQ(reqs.size(), 2u);
  // block[0]: physical 2MB, index=0, block_offset=2MB, len=2MB
  CheckBlockReadReq(reqs[0], 1, 0, 4 * MB, slice_offset + 2 * MB, 2 * MB,
                    2 * MB);
  // block[1]: physical 4MB, index=1, block_offset=0, len=2MB
  CheckBlockReadReq(reqs[1], 1, 1, 4 * MB, slice_offset + 4 * MB, 0, 2 * MB);
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
