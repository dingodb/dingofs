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

#include <optional>
#include <vector>

#include "client/vfs/data/common/data_utils.h"
#include "client/vfs/data/test_data_utils_common.h"

namespace dingofs {
namespace client {
namespace vfs {

static void CheckSliceEqual(const Slice& slice, const Slice& expected) {
  EXPECT_EQ(slice.id, expected.id);
  EXPECT_EQ(slice.offset, expected.offset);
  EXPECT_EQ(slice.length, expected.length);
  EXPECT_EQ(slice.compaction, expected.compaction);
  EXPECT_EQ(slice.is_zero, expected.is_zero);
}

TEST(ProcessReadRequestTest, BasicOverlap) {
  std::vector<Slice> slices = {
      CreateSlice(0, 100, 1),
      CreateSlice(100, 100, 2),
      CreateSlice(200, 100, 3),
  };

  FileRange file_range_req = CreateFileRange(50, 150);

  auto results = ProcessReadRequest(slices, file_range_req);

  ASSERT_EQ(results.size(), 2);
  EXPECT_EQ(results[0].file_offset, 50);
  EXPECT_EQ(results[0].len, 50);
  EXPECT_EQ(results[0].slice->id, 1);
  CheckSliceEqual(results[0].slice.value(), slices[0]);

  EXPECT_EQ(results[1].file_offset, 100);
  EXPECT_EQ(results[1].len, 100);
  EXPECT_EQ(results[1].slice->id, 2);
  CheckSliceEqual(results[1].slice.value(), slices[1]);
}

TEST(ProcessReadRequestTest, NoOverlap) {
  std::vector<Slice> slices = {
      CreateSlice(0, 100, 1),
      CreateSlice(200, 100, 2),
  };

  FileRange file_range_req = CreateFileRange(300, 50);

  auto results = ProcessReadRequest(slices, file_range_req);

  ASSERT_EQ(results.size(), 1);
  EXPECT_EQ(results[0].file_offset, 300);
  EXPECT_EQ(results[0].len, 50);
  EXPECT_FALSE(results[0].slice.has_value());
}

TEST(ProcessReadRequestTest, PartialOverlap) {
  std::vector<Slice> slices = {
      CreateSlice(0, 100, 1),
      CreateSlice(100, 100, 2),
  };

  FileRange file_range_req = CreateFileRange(50, 100);

  auto results = ProcessReadRequest(slices, file_range_req);

  ASSERT_EQ(results.size(), 2);
  EXPECT_EQ(results[0].file_offset, 50);
  EXPECT_EQ(results[0].len, 50);
  EXPECT_EQ(results[0].slice->id, 1);
  CheckSliceEqual(results[0].slice.value(), slices[0]);

  EXPECT_EQ(results[1].file_offset, 100);
  EXPECT_EQ(results[1].len, 50);
  EXPECT_EQ(results[1].slice->id, 2);
  CheckSliceEqual(results[1].slice.value(), slices[1]);
}

TEST(ProcessReadRequestTest, CompleteCoverage) {
  std::vector<Slice> slices = {
      CreateSlice(0, 100, 1)  // Covers the entire range
  };
  FileRange req = CreateFileRange(0, 100);

  auto results = ProcessReadRequest(slices, req);

  ASSERT_EQ(results.size(), 1);

  EXPECT_TRUE(results[0].slice.has_value());
  EXPECT_EQ(results[0].slice->id, 1);
  EXPECT_EQ(results[0].file_offset, 0);
  EXPECT_EQ(results[0].len, 100);
  CheckSliceEqual(results[0].slice.value(), slices[0]);
}

TEST(ProcessReadRequestTest, OverlappingSlices) {
  std::vector<Slice> slices = {
      CreateSlice(0, 100, 1),  // Old slice [0-100]
      CreateSlice(50, 100, 2)  // New slice (should take priority) [50-150]
  };
  FileRange req = CreateFileRange(40, 80);  // 40-120

  auto results = ProcessReadRequest(slices, req);

  ASSERT_EQ(results.size(), 2);
  EXPECT_EQ(results[0].file_offset, 40);
  EXPECT_EQ(results[0].len, 10);
  EXPECT_EQ(results[0].slice->id, 1);
  CheckSliceEqual(results[0].slice.value(), slices[0]);

  EXPECT_EQ(results[1].file_offset, 50);
  EXPECT_EQ(results[1].len, 70);
  EXPECT_EQ(results[1].slice->id, 2);
  CheckSliceEqual(results[1].slice.value(), slices[1]);
}

TEST(ProcessReadRequestTest, NonOverlappingSlices) {
  std::vector<Slice> slices = {CreateSlice(0, 50, 1), CreateSlice(100, 50, 2)};

  FileRange req = CreateFileRange(0, 150);

  auto results = ProcessReadRequest(slices, req);

  ASSERT_EQ(results.size(), 3);
  EXPECT_EQ(results[0].file_offset, 0);
  EXPECT_EQ(results[0].len, 50);
  EXPECT_EQ(results[0].slice->id, 1);
  CheckSliceEqual(results[0].slice.value(), slices[0]);

  EXPECT_EQ(results[1].file_offset, 50);
  EXPECT_EQ(results[1].len, 50);
  EXPECT_FALSE(results[1].slice.has_value());

  EXPECT_EQ(results[2].file_offset, 100);
  EXPECT_EQ(results[2].len, 50);
  EXPECT_EQ(results[2].slice->id, 2);
  CheckSliceEqual(results[2].slice.value(), slices[1]);
}

TEST(ProcessReadRequestTest, FragmentedSlices) {
  std::vector<Slice> slices = {CreateSlice(0, 20, 1), CreateSlice(30, 20, 2),
                               CreateSlice(60, 20, 3)};
  FileRange req = CreateFileRange(0, 100);

  auto results = ProcessReadRequest(slices, req);

  ASSERT_EQ(results.size(), 6);

  EXPECT_EQ(results[0].file_offset, 0);
  EXPECT_EQ(results[0].len, 20);
  EXPECT_EQ(results[0].slice->id, 1);
  CheckSliceEqual(results[0].slice.value(), slices[0]);

  EXPECT_EQ(results[1].file_offset, 20);
  EXPECT_EQ(results[1].len, 10);
  EXPECT_FALSE(results[1].slice.has_value());

  EXPECT_EQ(results[2].file_offset, 30);
  EXPECT_EQ(results[2].len, 20);
  EXPECT_EQ(results[2].slice->id, 2);
  CheckSliceEqual(results[2].slice.value(), slices[1]);

  EXPECT_EQ(results[3].file_offset, 50);
  EXPECT_EQ(results[3].len, 10);
  EXPECT_FALSE(results[3].slice.has_value());

  EXPECT_EQ(results[4].file_offset, 60);
  EXPECT_EQ(results[4].len, 20);
  EXPECT_EQ(results[4].slice->id, 3);
  CheckSliceEqual(results[4].slice.value(), slices[2]);

  EXPECT_EQ(results[5].file_offset, 80);
  EXPECT_EQ(results[5].len, 20);
  EXPECT_FALSE(results[5].slice.has_value());
}

TEST(ProcessReadRequestTest, NoCoverage) {
  std::vector<Slice> slices = {CreateSlice(100, 50, 1)};
  FileRange req = CreateFileRange(0, 50);

  auto results = ProcessReadRequest(slices, req);

  ASSERT_EQ(results.size(), 1);
  EXPECT_EQ(results[0].file_offset, 0);
  EXPECT_EQ(results[0].len, 50);
  EXPECT_FALSE(results[0].slice.has_value());
}

TEST(ProcessReadRequestTest, ExactBoundaryAlignment) {
  std::vector<Slice> slices = {CreateSlice(0, 64, 1), CreateSlice(64, 64, 2)};
  FileRange req = CreateFileRange(0, 128);

  auto results = ProcessReadRequest(slices, req);

  ASSERT_EQ(results.size(), 2);
  EXPECT_EQ(results[0].file_offset, 0);
  EXPECT_EQ(results[0].len, 64);
  EXPECT_EQ(results[0].slice->id, 1);
  CheckSliceEqual(results[0].slice.value(), slices[0]);

  EXPECT_EQ(results[1].file_offset, 64);
  EXPECT_EQ(results[1].len, 64);
  EXPECT_EQ(results[1].slice->id, 2);
  CheckSliceEqual(results[1].slice.value(), slices[1]);
}

TEST(ProcessReadRequestTest, ZeroSlices) {
  std::vector<Slice> slices = {
      CreateSlice(0, 100, 1, true)  // Zero data slice
  };
  FileRange req = CreateFileRange(20, 60);

  auto results = ProcessReadRequest(slices, req);

  ASSERT_EQ(results.size(), 1);
  EXPECT_EQ(results[0].file_offset, 20);
  EXPECT_EQ(results[0].len, 60);
  EXPECT_EQ(results[0].slice->id, 1);
  CheckSliceEqual(results[0].slice.value(), slices[0]);
}

TEST(ProcessReadRequestTest, LargeRandomSlices) {
  std::vector<Slice> slices = {
      CreateSlice(0, 1000, 1),    // [0-1000]
      CreateSlice(500, 1000, 2),  // [500-1500]
      CreateSlice(200, 200, 3)    // [200-400]
  };

  FileRange req = CreateFileRange(100, 900);  // [100-1000]

  auto results = ProcessReadRequest(slices, req);
  DumpSliceReadReqs(results);

  ASSERT_EQ(results.size(), 4);

  // [100-200]
  EXPECT_EQ(results[0].file_offset, 100);
  EXPECT_EQ(results[0].len, 100);
  EXPECT_EQ(results[0].slice->id, 1);
  CheckSliceEqual(results[0].slice.value(), slices[0]);

  // [200-400]
  EXPECT_EQ(results[1].file_offset, 200);
  EXPECT_EQ(results[1].len, 200);
  EXPECT_EQ(results[1].slice->id, 3);
  CheckSliceEqual(results[1].slice.value(), slices[2]);

  // [400-500]
  EXPECT_EQ(results[2].file_offset, 400);
  EXPECT_EQ(results[2].len, 100);
  EXPECT_EQ(results[2].slice->id, 1);
  CheckSliceEqual(results[2].slice.value(), slices[0]);

  // [500-1000]
  EXPECT_EQ(results[3].file_offset, 500);
  EXPECT_EQ(results[3].len, 500);
  EXPECT_EQ(results[3].slice->id, 2);
  CheckSliceEqual(results[3].slice.value(), slices[1]);
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
