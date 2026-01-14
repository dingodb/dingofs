// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <cstdint>
#include <fstream>
#include <iostream>
#include <ostream>
#include <vector>

#include "gtest/gtest.h"
#include "json/json.h"
#include "mds/common/helper.h"
#include "mds/filesystem/inode.h"
#include "mds/filesystem/store_operation.h"

namespace dingofs {
namespace mds {
namespace unit_test {

static const uint32_t kChunkSize = 64 * 1024 * 1024;
static const uint32_t kBlockSize = 4 * 1024 * 1024;

static FsInfoEntry GenFsInfo() {
  FsInfoEntry fs_info;
  fs_info.set_fs_id(10000);
  fs_info.set_fs_name("test_fs");
  fs_info.set_owner("test_owner");
  fs_info.set_chunk_size(kChunkSize);
  fs_info.set_block_size(kBlockSize);
  fs_info.set_status(pb::mds::FsStatus::NORMAL);

  return fs_info;
}

static SliceEntry GenSlice(uint64_t id, uint32_t offset, uint32_t len) {
  SliceEntry slice;
  slice.set_id(id);
  slice.set_offset(offset);
  slice.set_len(len);
  slice.set_size(len);
  slice.set_zero(false);
  slice.set_compaction_version(0);

  return slice;
}

static ChunkEntry GenChunk(const std::vector<SliceEntry>& slices) {
  ChunkEntry chunk;

  chunk.set_index(0);
  chunk.set_chunk_size(kChunkSize);
  chunk.set_block_size(kBlockSize);
  chunk.set_version(1);

  for (const auto& slice : slices) {
    chunk.add_slices()->CopyFrom(slice);
  }

  return chunk;
}

class OperationTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

static void test_set() {
  std::set<uint64_t> s;

  for (int i = 0; i < 100; ++i) {
    s.insert(dingofs::mds::Helper::GenerateRealRandomInteger(1, 1000000000));
  }

  for (auto it = s.begin(); it != s.end(); ++it) {
    auto next_it = std::next(it);
    if (next_it != s.end()) {
      std::cout << *it << " - " << *next_it << std::endl;
    }
  }
}

static ChunkEntry GenChunkFromJson(const std::string& filepath) {
  std::ifstream file(filepath);

  Json::Value root;
  Json::CharReaderBuilder reader;
  std::string err;
  if (!Json::parseFromStream(reader, file, &root, &err)) {
    std::cerr << fmt::format("parse json fail, path({}) error({}).", filepath,
                             err);
    return {};
  }

  ChunkEntry chunk;

  chunk.set_index(0);
  chunk.set_chunk_size(kChunkSize);
  chunk.set_block_size(kBlockSize);
  chunk.set_version(1);

  for (const auto& chunk_item : root["chunks"]) {
    for (const auto& slice_item : chunk_item["slices"]) {
      SliceEntry slice;
      slice.set_id(slice_item["id"].asUInt64());
      slice.set_offset(slice_item["offset"].asUInt64());
      slice.set_len(slice_item["len"].asUInt64());
      slice.set_size(slice_item["size"].asUInt64());
      slice.set_zero(false);
      slice.set_compaction_version(0);

      chunk.add_slices()->CopyFrom(slice);
    }
  }

  std::cout << fmt::format("load chunk from json file({}), slice num({}).",
                           filepath, chunk.slices_size())
            << std::endl;

  return chunk;
}

void PrintTrashSliceList(const TrashSliceList& trash_slice_list) {
  std::cout << "trash slice list:" << std::endl;
  for (int i = 0; i < trash_slice_list.slices_size(); ++i) {
    const auto& trash_slice = trash_slice_list.slices(i);
    std::cout << fmt::format("  slice_id: {}, is_partial: {}",
                             trash_slice.slice_id(), trash_slice.is_partial())
              << std::endl;
    // for (int j = 0; j < trash_slice.ranges_size(); ++j) {
    //   const auto& range = trash_slice.ranges(j);
    //   std::cout << fmt::format("    range: offset({}), len({}),
    //   compaction_version({})", range.offset(),
    //                            range.len(), range.compaction_version());
    // }
  }
}

// TEST_F(OperationTest, GenTrashSlicesFromJson) {
//   FsInfoEntry fs_info = GenFsInfo();
//   const Ino ino = 1000000001;
//   const uint64_t file_length = 64 * 1024 * 1024;

//   auto chunk = GenChunkFromJson("./20000000056.chunk.json");

//   auto trash_slice_list = CompactChunkOperation::TestGenTrashSlices(
//       fs_info, ino, file_length, chunk);

//   PrintTrashSliceList(trash_slice_list);
// }

// TEST_F(OperationTest, GenTrashSlices) {
//   FsInfoEntry fs_info = GenFsInfo();
//   const Ino ino = 1000000001;
//   const uint64_t file_length = 4 * 1024 * 1024;

//   {
//     std::vector<SliceEntry> slices = {
//         GenSlice(20000, 0, 4 * 1024 * 1024),
//     };
//     ChunkEntry chunk = GenChunk(slices);
//     auto trash_slice_list = CompactChunkOperation::TestGenTrashSlices(
//         fs_info, ino, file_length, chunk);
//     ASSERT_EQ(trash_slice_list.slices_size(), 0);
//   }

//   {
//     std::vector<SliceEntry> slices = {
//         GenSlice(20000, 0, 4 * 1024 * 1024),
//         GenSlice(20001, 0, 4 * 1024 * 1024),
//     };
//     ChunkEntry chunk = GenChunk(slices);
//     auto trash_slice_list = CompactChunkOperation::TestGenTrashSlices(
//         fs_info, ino, file_length, chunk);
//     ASSERT_EQ(trash_slice_list.slices_size(), 1);
//     ASSERT_EQ(trash_slice_list.slices(0).slice_id(), 20000);
//     ASSERT_EQ(trash_slice_list.slices(0).is_partial(), false);
//   }

//   {
//     std::vector<SliceEntry> slices = {
//         GenSlice(20000, 0, 4 * 1024 * 1024),
//         GenSlice(20001, 0, 4 * 1024 * 1024),
//         GenSlice(20002, 0, 1 * 1024 * 1024),
//         GenSlice(20003, 1 * 1024 * 1024, 1 * 1024 * 1024),
//         GenSlice(20004, 2 * 1024 * 1024, 1 * 1024 * 1024),
//         GenSlice(20005, 3 * 1024 * 1024, 1 * 1024 * 1024),
//     };
//     ChunkEntry chunk = GenChunk(slices);
//     auto trash_slice_list = CompactChunkOperation::TestGenTrashSlices(
//         fs_info, ino, file_length, chunk);
//     ASSERT_EQ(trash_slice_list.slices_size(), 2);
//     ASSERT_EQ(trash_slice_list.slices(0).slice_id(), 20000);
//     ASSERT_EQ(trash_slice_list.slices(0).is_partial(), false);
//     ASSERT_EQ(trash_slice_list.slices(1).slice_id(), 20001);
//     ASSERT_EQ(trash_slice_list.slices(1).is_partial(), false);
//   }

//   {
//     std::vector<SliceEntry> slices = {
//         GenSlice(20000, 0, 4 * 1024 * 1024),
//         GenSlice(20001, 0, 4 * 1024 * 1024),
//         GenSlice(20002, 0, 1 * 1024 * 1024),
//         GenSlice(20003, 1 * 1024 * 1024, 1 * 1024 * 1024),
//         GenSlice(20004, 2 * 1024 * 1024, 1 * 1024 * 1024),
//         GenSlice(20005, 3 * 1024 * 1024, 1 * 1024 * 1024),
//         GenSlice(20006, 2 * 1024 * 1024, 2 * 1024 * 1024),
//     };
//     ChunkEntry chunk = GenChunk(slices);
//     auto trash_slice_list = CompactChunkOperation::TestGenTrashSlices(
//         fs_info, ino, file_length, chunk);
//     ASSERT_EQ(trash_slice_list.slices_size(), 4);
//     ASSERT_EQ(trash_slice_list.slices(0).slice_id(), 20000);
//     ASSERT_EQ(trash_slice_list.slices(0).is_partial(), false);
//     ASSERT_EQ(trash_slice_list.slices(1).slice_id(), 20001);
//     ASSERT_EQ(trash_slice_list.slices(1).is_partial(), false);
//     ASSERT_EQ(trash_slice_list.slices(2).slice_id(), 20004);
//     ASSERT_EQ(trash_slice_list.slices(2).is_partial(), false);
//     ASSERT_EQ(trash_slice_list.slices(3).slice_id(), 20005);
//     ASSERT_EQ(trash_slice_list.slices(3).is_partial(), false);
//   }

//   {
//     std::vector<SliceEntry> slices = {
//         GenSlice(20000, 0, 4 * 1024 * 1024),
//         GenSlice(20001, 0, 4 * 1024 * 1024),
//         GenSlice(20002, 0, 1 * 1024 * 1024),
//     };
//     ChunkEntry chunk = GenChunk(slices);
//     auto trash_slice_list = CompactChunkOperation::TestGenTrashSlices(
//         fs_info, ino, file_length, chunk);
//     ASSERT_EQ(trash_slice_list.slices_size(), 1);
//     ASSERT_EQ(trash_slice_list.slices(0).slice_id(), 20000);
//     ASSERT_EQ(trash_slice_list.slices(0).is_partial(), false);
//   }

//   {
//     std::vector<SliceEntry> slices = {
//         GenSlice(20000, 0, 4 * 1024 * 1024),
//         GenSlice(20001, 0, 4 * 1024 * 1024),
//         GenSlice(20002, 0, 4 * 1024 * 1024),
//     };
//     ChunkEntry chunk = GenChunk(slices);
//     auto trash_slice_list = CompactChunkOperation::TestGenTrashSlices(
//         fs_info, ino, file_length, chunk);
//     ASSERT_EQ(trash_slice_list.slices_size(), 2);
//     ASSERT_EQ(trash_slice_list.slices(0).slice_id(), 20000);
//     ASSERT_EQ(trash_slice_list.slices(0).is_partial(), false);
//     ASSERT_EQ(trash_slice_list.slices(1).slice_id(), 20001);
//     ASSERT_EQ(trash_slice_list.slices(1).is_partial(), false);
//   }
// }

}  // namespace unit_test
}  // namespace mds
}  // namespace dingofs
