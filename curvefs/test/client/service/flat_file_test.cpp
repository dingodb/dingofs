
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

#include "curvefs/src/client/service/flat_file.h"

#include "gtest/gtest.h"

namespace curvefs {
namespace client {

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

}  // namespace client

}  // namespace curvefs

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