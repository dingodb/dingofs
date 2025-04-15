
// Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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

#include "client/vfs_old/service/flat_file.h"

#include <cstdint>

#include "gtest/gtest.h"

namespace dingofs {
namespace client {

using dingofs::pb::metaserver::S3ChunkInfo;

class FlatFileChunkTest : public ::testing::Test {
 protected:
  FlatFileChunk chunk;

  void SetUp() override {}

  void TearDown() override {}
};

TEST_F(FlatFileChunkTest, InsertSliceWithNoOverlapOne) {
  FlatFileSlice slice1 = {0, 10, 1};
  FlatFileSlice slice2 = {10, 10, 2};

  chunk.InsertChunkInfo(slice1);
  chunk.InsertChunkInfo(slice2);

  const std::map<uint64_t, FlatFileSlice>& slice_map =
      chunk.GetFileOffsetSlice();

  ASSERT_EQ(slice_map.size(), 2);

  ASSERT_EQ(slice_map.at(0).file_offset, 0);
  ASSERT_EQ(slice_map.at(0).len, 10);
  ASSERT_EQ(slice_map.at(0).chunk_id, 1);

  ASSERT_EQ(slice_map.at(10).file_offset, 10);
  ASSERT_EQ(slice_map.at(10).len, 10);
  ASSERT_EQ(slice_map.at(10).chunk_id, 2);
}

TEST_F(FlatFileChunkTest, InsertSliceWithNoOverlapTwo) {
  FlatFileSlice slice1 = {0, 10, 1};
  FlatFileSlice slice2 = {20, 10, 2};

  chunk.InsertChunkInfo(slice1);
  chunk.InsertChunkInfo(slice2);

  const std::map<uint64_t, FlatFileSlice>& slice_map =
      chunk.GetFileOffsetSlice();

  ASSERT_EQ(slice_map.size(), 2);

  ASSERT_EQ(slice_map.at(0).file_offset, 0);
  ASSERT_EQ(slice_map.at(0).len, 10);
  ASSERT_EQ(slice_map.at(0).chunk_id, 1);

  ASSERT_EQ(slice_map.at(20).file_offset, 20);
  ASSERT_EQ(slice_map.at(20).len, 10);
  ASSERT_EQ(slice_map.at(20).chunk_id, 2);
}

TEST_F(FlatFileChunkTest, InsertOverlappingSlice) {
  FlatFileSlice slice1 = {0, 10, 1};
  FlatFileSlice slice2 = {5, 10, 2};

  chunk.InsertChunkInfo(slice1);
  chunk.InsertChunkInfo(slice2);

  const std::map<uint64_t, FlatFileSlice>& slice_map =
      chunk.GetFileOffsetSlice();

  ASSERT_EQ(slice_map.size(), 2);
  ASSERT_EQ(slice_map.at(0).file_offset, 0);
  ASSERT_EQ(slice_map.at(0).len, 5);
  ASSERT_EQ(slice_map.at(0).chunk_id, 1);

  ASSERT_EQ(slice_map.at(5).file_offset, 5);
  ASSERT_EQ(slice_map.at(5).len, 10);
  ASSERT_EQ(slice_map.at(5).chunk_id, 2);
}

TEST_F(FlatFileChunkTest, InsertFullyOverlappingSlice) {
  FlatFileSlice slice1 = {0, 20, 1};
  FlatFileSlice slice2 = {5, 10, 2};

  chunk.InsertChunkInfo(slice1);
  chunk.InsertChunkInfo(slice2);

  const std::map<uint64_t, FlatFileSlice>& slice_map =
      chunk.GetFileOffsetSlice();

  ASSERT_EQ(slice_map.size(), 3);

  ASSERT_EQ(slice_map.at(0).file_offset, 0);
  ASSERT_EQ(slice_map.at(0).len, 5);
  ASSERT_EQ(slice_map.at(0).chunk_id, 1);

  ASSERT_EQ(slice_map.at(5).file_offset, 5);
  ASSERT_EQ(slice_map.at(5).len, 10);
  ASSERT_EQ(slice_map.at(5).chunk_id, 2);

  ASSERT_EQ(slice_map.at(15).file_offset, 15);
  ASSERT_EQ(slice_map.at(15).len, 5);
  ASSERT_EQ(slice_map.at(15).chunk_id, 1);
}

class S3ChunkHolerTest : public ::testing::Test {
 protected:
  void SetUp() override {}

  void TearDown() override {}
};

TEST_F(S3ChunkHolerTest, Init) {
  {
    S3ChunkInfo chunk_info;
    chunk_info.set_chunkid(1);
    chunk_info.set_offset(0);
    chunk_info.set_len(8);
    chunk_info.set_zero(false);
    chunk_info.set_compaction(0);

    uint64_t chunk_size = 8;
    uint64_t block_size = 4;
    S3ChunkHoler holer(chunk_info, chunk_size, block_size);

    const auto& offset_to_block = holer.GetOffsetToBlock();

    ASSERT_EQ(offset_to_block.size(), 2);

    auto it = offset_to_block.find(0);
    ASSERT_NE(it, offset_to_block.end());
    EXPECT_EQ(it->second.file_offset, 0);
    EXPECT_EQ(it->second.obj_len, 4);
    EXPECT_EQ(it->second.chunk_id, 1);
    EXPECT_EQ(it->second.block_index, 0);

    it = offset_to_block.find(4);
    ASSERT_NE(it, offset_to_block.end());
    EXPECT_EQ(it->second.file_offset, 4);
    EXPECT_EQ(it->second.obj_len, 4);
    EXPECT_EQ(it->second.chunk_id, 1);
    EXPECT_EQ(it->second.block_index, 1);
  }

  {
    S3ChunkInfo chunk_info;
    chunk_info.set_chunkid(1);
    chunk_info.set_offset(1);
    chunk_info.set_len(5);
    chunk_info.set_zero(false);
    chunk_info.set_compaction(0);

    uint64_t chunk_size = 8;
    uint64_t block_size = 4;
    S3ChunkHoler holer(chunk_info, chunk_size, block_size);

    const auto& offset_to_block = holer.GetOffsetToBlock();

    ASSERT_EQ(offset_to_block.size(), 2);

    auto it = offset_to_block.find(1);
    ASSERT_NE(it, offset_to_block.end());
    EXPECT_EQ(it->second.file_offset, 1);
    EXPECT_EQ(it->second.obj_len, 3);
    EXPECT_EQ(it->second.chunk_id, 1);
    EXPECT_EQ(it->second.block_index, 0);

    it = offset_to_block.find(4);
    ASSERT_NE(it, offset_to_block.end());
    EXPECT_EQ(it->second.file_offset, 4);
    EXPECT_EQ(it->second.obj_len, 2);
    EXPECT_EQ(it->second.chunk_id, 1);
    EXPECT_EQ(it->second.block_index, 1);
  }

  {
    S3ChunkInfo chunk_info;
    chunk_info.set_chunkid(1);
    chunk_info.set_offset(5);
    chunk_info.set_len(5);
    chunk_info.set_zero(false);
    chunk_info.set_compaction(0);

    uint64_t chunk_size = 8;
    uint64_t block_size = 4;
    S3ChunkHoler holer(chunk_info, chunk_size, block_size);

    LOG(INFO) << holer.ToString();

    const auto& offset_to_block = holer.GetOffsetToBlock();

    ASSERT_EQ(offset_to_block.size(), 2);

    auto it = offset_to_block.find(5);
    ASSERT_NE(it, offset_to_block.end());
    EXPECT_EQ(it->second.file_offset, 5);
    EXPECT_EQ(it->second.obj_len, 3);
    EXPECT_EQ(it->second.chunk_id, 1);
    EXPECT_EQ(it->second.block_index, 0);

    it = offset_to_block.find(8);
    ASSERT_NE(it, offset_to_block.end());
    EXPECT_EQ(it->second.file_offset, 8);
    EXPECT_EQ(it->second.obj_len, 2);
    EXPECT_EQ(it->second.chunk_id, 1);
    EXPECT_EQ(it->second.block_index, 1);
  }
}

TEST_F(S3ChunkHolerTest, GetBlockObjOne) {
  uint64_t chunk_id = 1;

  S3ChunkInfo chunk_info;
  chunk_info.set_chunkid(chunk_id);
  chunk_info.set_offset(0);
  chunk_info.set_len(8);
  chunk_info.set_zero(false);
  chunk_info.set_compaction(0);

  uint64_t chunk_size = 8;
  uint64_t block_size = 4;
  S3ChunkHoler holer(chunk_info, chunk_size, block_size);

  FlatFileSlice slice = {0, 8, chunk_id};  //  file_offset, len, chunk_id
  std::vector<BlockObj> block_objs = holer.GetBlockObj(slice);

  EXPECT_EQ(block_objs.size(), 2);

  EXPECT_EQ(block_objs[0].file_offset, 0);
  EXPECT_EQ(block_objs[0].obj_len, 4);
  EXPECT_EQ(block_objs[0].chunk_id, 1);
  EXPECT_EQ(block_objs[0].block_index, 0);

  EXPECT_EQ(block_objs[1].file_offset, 4);
  EXPECT_EQ(block_objs[1].obj_len, 4);
  EXPECT_EQ(block_objs[1].chunk_id, 1);
  EXPECT_EQ(block_objs[1].block_index, 1);
}

TEST_F(S3ChunkHolerTest, GetBlockObjTwo) {
  uint64_t chunk_id = 1;

  S3ChunkInfo chunk_info;
  chunk_info.set_chunkid(chunk_id);
  chunk_info.set_offset(0);
  chunk_info.set_len(8);
  chunk_info.set_zero(false);
  chunk_info.set_compaction(0);

  uint64_t chunk_size = 8;
  uint64_t block_size = 4;
  S3ChunkHoler holer(chunk_info, chunk_size, block_size);

  LOG(INFO) << holer.ToString();

  FlatFileSlice slice = {2, 6, chunk_id};  // file_offset, len, chunk_id
  std::vector<BlockObj> block_objs = holer.GetBlockObj(slice);

  EXPECT_EQ(block_objs.size(), 2);

  EXPECT_EQ(block_objs[0].file_offset, 0);
  EXPECT_EQ(block_objs[0].obj_len, 4);
  EXPECT_EQ(block_objs[0].chunk_id, 1);
  EXPECT_EQ(block_objs[0].block_index, 0);

  EXPECT_EQ(block_objs[1].file_offset, 4);
  EXPECT_EQ(block_objs[1].obj_len, 4);
  EXPECT_EQ(block_objs[1].chunk_id, 1);
  EXPECT_EQ(block_objs[1].block_index, 1);
}

TEST_F(S3ChunkHolerTest, GetBlockObjThree) {
  uint64_t chunk_id = 1;

  S3ChunkInfo chunk_info;
  chunk_info.set_chunkid(chunk_id);
  chunk_info.set_offset(2);
  chunk_info.set_len(13);
  chunk_info.set_zero(false);
  chunk_info.set_compaction(0);

  uint64_t chunk_size = 8;
  uint64_t block_size = 4;
  S3ChunkHoler holer(chunk_info, chunk_size, block_size);

  FlatFileSlice slice = {3, 11, chunk_id};  // file_offset, len, chunk_id
  std::vector<BlockObj> block_objs = holer.GetBlockObj(slice);

  EXPECT_EQ(block_objs.size(), 4);

  EXPECT_EQ(block_objs[0].file_offset, 2);
  EXPECT_EQ(block_objs[0].obj_len, 2);
  EXPECT_EQ(block_objs[0].chunk_id, 1);
  EXPECT_EQ(block_objs[0].block_index, 0);

  EXPECT_EQ(block_objs[1].file_offset, 4);
  EXPECT_EQ(block_objs[1].obj_len, 4);
  EXPECT_EQ(block_objs[1].chunk_id, 1);
  EXPECT_EQ(block_objs[1].block_index, 1);

  EXPECT_EQ(block_objs[2].file_offset, 8);
  EXPECT_EQ(block_objs[2].obj_len, 4);
  EXPECT_EQ(block_objs[2].chunk_id, 1);
  EXPECT_EQ(block_objs[2].block_index, 2);

  EXPECT_EQ(block_objs[3].file_offset, 12);
  EXPECT_EQ(block_objs[3].obj_len, 3);
  EXPECT_EQ(block_objs[3].chunk_id, 1);
  EXPECT_EQ(block_objs[3].block_index, 3);
}

TEST_F(S3ChunkHolerTest, GetBlockObjSliceOne) {
  uint64_t chunk_id = 1;

  S3ChunkInfo chunk_info;
  chunk_info.set_chunkid(chunk_id);
  chunk_info.set_offset(0);
  chunk_info.set_len(8);
  chunk_info.set_zero(false);
  chunk_info.set_compaction(0);

  uint64_t chunk_size = 8;
  uint64_t block_size = 4;
  S3ChunkHoler holer(chunk_info, chunk_size, block_size);

  FlatFileSlice slice = {0, 8, chunk_id};  //  file_offset, len, chunk_id
  std::vector<BlockObjSlice> obj_slices = holer.GetBlockObjSlice(slice);

  EXPECT_EQ(obj_slices.size(), 2);

  EXPECT_EQ(obj_slices[0].file_offset, 0);
  EXPECT_EQ(obj_slices[0].len, 4);
  EXPECT_EQ(obj_slices[0].obj.file_offset, 0);
  EXPECT_EQ(obj_slices[0].obj.obj_len, 4);
  EXPECT_EQ(obj_slices[0].obj.chunk_id, 1);
  EXPECT_EQ(obj_slices[0].obj.block_index, 0);

  EXPECT_EQ(obj_slices[1].file_offset, 4);
  EXPECT_EQ(obj_slices[1].len, 4);
  EXPECT_EQ(obj_slices[1].obj.file_offset, 4);
  EXPECT_EQ(obj_slices[1].obj.obj_len, 4);
  EXPECT_EQ(obj_slices[1].obj.chunk_id, 1);
  EXPECT_EQ(obj_slices[1].obj.block_index, 1);
}

TEST_F(S3ChunkHolerTest, GetBlockObjSliceTwo) {
  uint64_t chunk_id = 1;

  S3ChunkInfo chunk_info;
  chunk_info.set_chunkid(chunk_id);
  chunk_info.set_offset(0);
  chunk_info.set_len(8);
  chunk_info.set_zero(false);
  chunk_info.set_compaction(0);

  uint64_t chunk_size = 8;
  uint64_t block_size = 4;
  S3ChunkHoler holer(chunk_info, chunk_size, block_size);

  FlatFileSlice slice = {2, 6, chunk_id};  // file_offset, len, chunk_id

  std::vector<BlockObjSlice> obj_slices = holer.GetBlockObjSlice(slice);

  EXPECT_EQ(obj_slices.size(), 2);

  EXPECT_EQ(obj_slices[0].file_offset, 2);
  EXPECT_EQ(obj_slices[0].len, 2);
  EXPECT_EQ(obj_slices[0].obj.file_offset, 0);
  EXPECT_EQ(obj_slices[0].obj.obj_len, 4);
  EXPECT_EQ(obj_slices[0].obj.chunk_id, 1);
  EXPECT_EQ(obj_slices[0].obj.block_index, 0);

  EXPECT_EQ(obj_slices[1].file_offset, 4);
  EXPECT_EQ(obj_slices[1].len, 4);
  EXPECT_EQ(obj_slices[1].obj.file_offset, 4);
  EXPECT_EQ(obj_slices[1].obj.obj_len, 4);
  EXPECT_EQ(obj_slices[1].obj.chunk_id, 1);
  EXPECT_EQ(obj_slices[1].obj.block_index, 1);
}

TEST_F(S3ChunkHolerTest, GetBlockObjSliceThree) {
  uint64_t chunk_id = 1;

  S3ChunkInfo chunk_info;
  chunk_info.set_chunkid(chunk_id);
  chunk_info.set_offset(2);
  chunk_info.set_len(13);
  chunk_info.set_zero(false);
  chunk_info.set_compaction(0);

  uint64_t chunk_size = 8;
  uint64_t block_size = 4;
  S3ChunkHoler holer(chunk_info, chunk_size, block_size);

  LOG(INFO) << holer.ToString();

  FlatFileSlice slice = {3, 11, chunk_id};  // file_offset, len, chunk_id

  std::vector<BlockObjSlice> obj_slices = holer.GetBlockObjSlice(slice);

  EXPECT_EQ(obj_slices.size(), 4);

  EXPECT_EQ(obj_slices[0].file_offset, 3);
  EXPECT_EQ(obj_slices[0].len, 1);
  EXPECT_EQ(obj_slices[0].obj.file_offset, 2);
  EXPECT_EQ(obj_slices[0].obj.obj_len, 2);
  EXPECT_EQ(obj_slices[0].obj.chunk_id, 1);
  EXPECT_EQ(obj_slices[0].obj.block_index, 0);

  EXPECT_EQ(obj_slices[1].file_offset, 4);
  EXPECT_EQ(obj_slices[1].len, 4);
  EXPECT_EQ(obj_slices[1].obj.file_offset, 4);
  EXPECT_EQ(obj_slices[1].obj.obj_len, 4);
  EXPECT_EQ(obj_slices[1].obj.chunk_id, 1);
  EXPECT_EQ(obj_slices[1].obj.block_index, 1);

  EXPECT_EQ(obj_slices[2].file_offset, 8);
  EXPECT_EQ(obj_slices[2].len, 4);
  EXPECT_EQ(obj_slices[2].obj.file_offset, 8);
  EXPECT_EQ(obj_slices[2].obj.obj_len, 4);
  EXPECT_EQ(obj_slices[2].obj.chunk_id, 1);
  EXPECT_EQ(obj_slices[2].obj.block_index, 2);

  EXPECT_EQ(obj_slices[3].file_offset, 12);
  EXPECT_EQ(obj_slices[3].len, 2);
  EXPECT_EQ(obj_slices[3].obj.file_offset, 12);
  EXPECT_EQ(obj_slices[3].obj.obj_len, 3);
  EXPECT_EQ(obj_slices[3].obj.chunk_id, 1);
  EXPECT_EQ(obj_slices[3].obj.block_index, 3);
}

}  // namespace client

}  // namespace dingofs

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  FLAGS_minloglevel = google::GLOG_INFO;
  FLAGS_logtostdout = true;
  FLAGS_colorlogtostdout = true;
  FLAGS_logbufsecs = 0;
  FLAGS_v = 12;

  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);

  return RUN_ALL_TESTS();
}