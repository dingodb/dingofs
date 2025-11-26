
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

namespace dingofs {
namespace client {
namespace vfs {

#define CHECK_BLOCK_BOJ_EQUAL(block_obj, expected_block)              \
  do {                                                                \
    EXPECT_EQ((block_obj).file_offset, (expected_block).file_offset); \
    EXPECT_EQ((block_obj).block_len, (expected_block).block_len);     \
    EXPECT_EQ((block_obj).zero, (expected_block).zero);               \
    EXPECT_EQ((block_obj).version, (expected_block).version);         \
    EXPECT_EQ((block_obj).slice_id, (expected_block).slice_id);       \
    EXPECT_EQ((block_obj).index, (expected_block).index);             \
  } while (0)

// Helper function to create a SliceReadReq object
static SliceReadReq CreateSliceReadReq(uint64_t file_offset, uint64_t read_len,
                                       const Slice& slice) {
  return SliceReadReq{
      .file_offset = file_offset,
      .len = read_len,
      .slice = slice,
  };
}

TEST(ConvertSliceReadReqToBlockReadReqsTest, NormalCase) {
  bool is_zero = false;
  uint64_t compaction = 1;
  Slice slice = CreateSlice(0, 128, 1, is_zero, compaction);

  SliceReadReq slice_req = CreateSliceReadReq(32, 64, slice);

  uint64_t fs_id = 1;
  uint64_t ino = 2;
  uint64_t chunk_size = 128;
  uint64_t block_size = 32;

  auto block_read_reqs = ConvertSliceReadReqToBlockReadReqs(
      slice_req, fs_id, ino, chunk_size, block_size);

  ASSERT_EQ(block_read_reqs.size(), 2);

  uint64_t next_block_index = (slice_req.file_offset % chunk_size) / block_size;

  {
    BlockDesc expected_block{
        .file_offset = 32,
        .block_len = 32,
        .zero = is_zero,
        .version = compaction,
        .slice_id = slice_req.slice->id,
        .index = next_block_index,
    };

    EXPECT_EQ(block_read_reqs[0].block_offset, 0);
    EXPECT_EQ(block_read_reqs[0].len, 32);
    CHECK_BLOCK_BOJ_EQUAL(block_read_reqs[0].block, expected_block);

    next_block_index += 1;
  }

  {
    BlockDesc expected_block{
        .file_offset = 64,
        .block_len = 32,
        .zero = is_zero,
        .version = compaction,
        .slice_id = slice_req.slice->id,
        .index = next_block_index,
    };

    EXPECT_EQ(block_read_reqs[1].block_offset, 0);
    EXPECT_EQ(block_read_reqs[1].len, 32);
    CHECK_BLOCK_BOJ_EQUAL(block_read_reqs[1].block, expected_block);

    next_block_index += 1;
  }
}

TEST(ConvertSliceReadReqToBlockReadReqsTest, TwoBlockRead) {
  bool is_zero = false;
  uint64_t compaction = 1;
  Slice slice = CreateSlice(0, 64, 1, is_zero, compaction);

  SliceReadReq slice_req = CreateSliceReadReq(16, 32, slice);

  uint64_t fs_id = 1;
  uint64_t ino = 2;
  uint64_t chunk_size = 64;
  uint64_t block_size = 32;

  auto block_read_reqs = ConvertSliceReadReqToBlockReadReqs(
      slice_req, fs_id, ino, chunk_size, block_size);

  ASSERT_EQ(block_read_reqs.size(), 2);

  uint64_t next_block_index = (slice_req.file_offset % chunk_size) / block_size;

  {
    BlockDesc expected_block{
        .file_offset = 0,
        .block_len = 32,
        .zero = is_zero,
        .version = compaction,
        .slice_id = slice_req.slice->id,
        .index = next_block_index,
    };

    EXPECT_EQ(block_read_reqs[0].block_offset, 16);
    EXPECT_EQ(block_read_reqs[0].len, 16);
    CHECK_BLOCK_BOJ_EQUAL(block_read_reqs[0].block, expected_block);

    next_block_index += 1;
  }

  {
    BlockDesc expected_block{
        .file_offset = 32,
        .block_len = 32,
        .zero = is_zero,
        .version = compaction,
        .slice_id = slice_req.slice->id,
        .index = next_block_index,
    };

    EXPECT_EQ(block_read_reqs[1].block_offset, 0);
    EXPECT_EQ(block_read_reqs[1].len, 16);
    CHECK_BLOCK_BOJ_EQUAL(block_read_reqs[1].block, expected_block);

    next_block_index += 1;
  }
}

TEST(ConvertSliceReadReqToBlockReadReqsTest, ZeroDataSlice) {
  bool is_zero = true;
  uint64_t compaction = 1;
  Slice slice = CreateSlice(0, 128, 1, is_zero, compaction);

  SliceReadReq slice_req = CreateSliceReadReq(32, 64, slice);

  uint64_t fs_id = 1;
  uint64_t ino = 2;
  uint64_t chunk_size = 128;
  uint64_t block_size = 32;

  auto block_read_reqs = ConvertSliceReadReqToBlockReadReqs(
      slice_req, fs_id, ino, chunk_size, block_size);

  ASSERT_EQ(block_read_reqs.size(), 2);

  uint64_t next_block_index = (slice_req.file_offset % chunk_size) / block_size;

  {
    BlockDesc expected_block{
        .file_offset = 32,
        .block_len = 32,
        .zero = is_zero,
        .version = compaction,
        .slice_id = slice_req.slice->id,
        .index = next_block_index,
    };

    EXPECT_EQ(block_read_reqs[0].block_offset, 0);
    EXPECT_EQ(block_read_reqs[0].len, 32);
    CHECK_BLOCK_BOJ_EQUAL(block_read_reqs[0].block, expected_block);

    next_block_index += 1;
  }

  {
    BlockDesc expected_block{
        .file_offset = 64,
        .block_len = 32,
        .zero = is_zero,
        .version = compaction,
        .slice_id = slice_req.slice->id,
        .index = next_block_index,
    };

    EXPECT_EQ(block_read_reqs[1].block_offset, 0);
    EXPECT_EQ(block_read_reqs[1].len, 32);
    CHECK_BLOCK_BOJ_EQUAL(block_read_reqs[1].block, expected_block);

    next_block_index += 1;
  }
}

TEST(ConvertSliceReadReqToBlockReadReqsTest, ReadAcrossMultipleBlocks) {
  bool is_zero = false;
  uint64_t compaction = 1;
  Slice slice = CreateSlice(0, 256, 1, is_zero, compaction);

  SliceReadReq slice_req = CreateSliceReadReq(64, 128, slice);

  uint64_t fs_id = 1;
  uint64_t ino = 2;
  uint64_t chunk_size = 512;
  uint64_t block_size = 32;

  uint64_t block_num_in_chunk = chunk_size / block_size;

  auto block_read_reqs = ConvertSliceReadReqToBlockReadReqs(
      slice_req, fs_id, ino, chunk_size, block_size);

  ASSERT_EQ(block_read_reqs.size(), 4);

  uint64_t next_block_index = (slice_req.file_offset % chunk_size) / block_size;

  {
    BlockDesc expected_block{
        .file_offset = 64,
        .block_len = 32,
        .zero = is_zero,
        .version = compaction,
        .slice_id = slice_req.slice->id,
        .index = next_block_index,
    };

    EXPECT_EQ(block_read_reqs[0].block_offset, 0);
    EXPECT_EQ(block_read_reqs[0].len, 32);
    CHECK_BLOCK_BOJ_EQUAL(block_read_reqs[0].block, expected_block);

    next_block_index = (next_block_index + 1) % block_num_in_chunk;
  }

  {
    BlockDesc expected_block{
        .file_offset = 96,
        .block_len = 32,
        .zero = is_zero,
        .version = compaction,
        .slice_id = slice_req.slice->id,
        .index = next_block_index,
    };

    EXPECT_EQ(block_read_reqs[1].block_offset, 0);
    EXPECT_EQ(block_read_reqs[1].len, 32);
    CHECK_BLOCK_BOJ_EQUAL(block_read_reqs[1].block, expected_block);

    next_block_index = (next_block_index + 1) % block_num_in_chunk;
  }

  {
    BlockDesc expected_block{
        .file_offset = 128,
        .block_len = 32,
        .zero = is_zero,
        .version = compaction,
        .slice_id = slice_req.slice->id,
        .index = next_block_index,
    };

    EXPECT_EQ(block_read_reqs[2].block_offset, 0);
    EXPECT_EQ(block_read_reqs[2].len, 32);
    CHECK_BLOCK_BOJ_EQUAL(block_read_reqs[2].block, expected_block);

    next_block_index = (next_block_index + 1) % block_num_in_chunk;
  }

  {
    BlockDesc expected_block{
        .file_offset = 160,
        .block_len = 32,
        .zero = is_zero,
        .version = compaction,
        .slice_id = slice_req.slice->id,
        .index = next_block_index,
    };

    EXPECT_EQ(block_read_reqs[3].block_offset, 0);
    EXPECT_EQ(block_read_reqs[3].len, 32);
    CHECK_BLOCK_BOJ_EQUAL(block_read_reqs[3].block, expected_block);

    next_block_index = (next_block_index + 1) % block_num_in_chunk;
  }
}

TEST(ConvertSliceReadReqToBlockReadReqsTest, NonAlignedBlockBoundaries) {
  bool is_zero = false;
  uint64_t compaction = 2;
  uint64_t chunk_size = 256;
  uint64_t block_size = 64;

  Slice slice = CreateSlice(10, 200, 1, is_zero, compaction);   // [10-210]
  SliceReadReq slice_req = CreateSliceReadReq(35, 120, slice);  // [35-155]

  uint64_t fs_id = 1;
  uint64_t ino = 2;

  auto block_read_reqs = ConvertSliceReadReqToBlockReadReqs(
      slice_req, fs_id, ino, chunk_size, block_size);

  ASSERT_EQ(block_read_reqs.size(), 3);

  uint64_t next_block_index = (slice_req.file_offset % chunk_size) / block_size;

  {
    BlockDesc expected_block{
        .file_offset = 10,
        .block_len = 54,
        .zero = is_zero,
        .version = compaction,
        .slice_id = slice_req.slice->id,
        .index = next_block_index,
    };

    EXPECT_EQ(block_read_reqs[0].block_offset, 25);
    EXPECT_EQ(block_read_reqs[0].len, 54 - 25);
    CHECK_BLOCK_BOJ_EQUAL(block_read_reqs[0].block, expected_block);

    next_block_index += 1;
  }

  {
    BlockDesc expected_block{
        .file_offset = 64,
        .block_len = 64,
        .zero = is_zero,
        .version = compaction,
        .slice_id = slice_req.slice->id,
        .index = next_block_index,
    };

    EXPECT_EQ(block_read_reqs[1].block_offset, 0);
    EXPECT_EQ(block_read_reqs[1].len, 64);
    CHECK_BLOCK_BOJ_EQUAL(block_read_reqs[1].block, expected_block);

    next_block_index += 1;
  }

  {
    BlockDesc expected_block{
        .file_offset = 128,
        .block_len = 64,
        .zero = is_zero,
        .version = compaction,
        .slice_id = slice_req.slice->id,
        .index = next_block_index,
    };

    EXPECT_EQ(block_read_reqs[2].block_offset, 0);
    EXPECT_EQ(block_read_reqs[2].len, 155 - 128);
    CHECK_BLOCK_BOJ_EQUAL(block_read_reqs[2].block, expected_block);

    next_block_index += 1;
  }
}

TEST(ConvertSliceReadReqToBlockReadReqsTest, InvalidSlice) {
  SliceReadReq slice_req{
      .file_offset = 32,
      .len = 64,
      .slice = std::nullopt,  // No slice provided
  };

  uint64_t fs_id = 1;
  uint64_t ino = 2;
  uint64_t chunk_size = 128;
  uint64_t block_size = 32;

  EXPECT_DEATH(ConvertSliceReadReqToBlockReadReqs(slice_req, fs_id, ino,
                                                  chunk_size, block_size),
               "Illegal slice_req");
}

TEST(ConvertSliceReadReqToBlockReadReqsTest, InvalidReadOffset) {
  Slice slice = CreateSlice(32, 128, 1, false, 1);
  SliceReadReq slice_req = CreateSliceReadReq(0, 64, slice);

  uint64_t fs_id = 1;
  uint64_t ino = 2;
  uint64_t chunk_size = 128;
  uint64_t block_size = 32;

  EXPECT_DEATH(ConvertSliceReadReqToBlockReadReqs(slice_req, fs_id, ino,
                                                  chunk_size, block_size),
               "Illegal slice_req_offset");
}

TEST(ConvertSliceReadReqToBlockReadReqsTest, ReadLengthExceedsSlice) {
  bool is_zero = false;
  uint64_t compaction = 1;
  Slice slice = CreateSlice(0, 64, 1, is_zero, compaction);
  SliceReadReq slice_req = CreateSliceReadReq(32, 64, slice);

  uint64_t fs_id = 1;
  uint64_t ino = 2;
  uint64_t chunk_size = 64;
  uint64_t block_size = 32;

  EXPECT_DEATH(ConvertSliceReadReqToBlockReadReqs(slice_req, fs_id, ino,
                                                  chunk_size, block_size),
               "Illegal cur_block_start");
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
