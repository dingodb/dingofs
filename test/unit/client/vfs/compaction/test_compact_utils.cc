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

#include <vector>

#include "client/vfs/compaction/compact_utils.h"
#include "client/vfs/data/common/common.h"
#include "client/vfs/vfs_meta.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace compaction {

class CompactUtilsTest : public testing::Test {
 protected:
  Slice CreateSlice(uint64_t id, uint64_t offset, uint64_t length) {
    Slice s;
    s.id = id;
    s.offset = offset;
    s.length = length;
    s.size = length;
    return s;
  }
};

TEST_F(CompactUtilsTest, GetCompactFileRange) {
  std::vector<Slice> slices;
  slices.push_back(CreateSlice(1, 100, 100));  // 100-200
  slices.push_back(CreateSlice(2, 150, 150));  // 150-300
  slices.push_back(CreateSlice(3, 50, 50));    // 50-100

  FileRange range = GetSlicesFileRange(absl::MakeSpan(slices));
  EXPECT_EQ(range.offset, 50);
  EXPECT_EQ(range.len, 250);  // 300 - 50 = 250
}

TEST_F(CompactUtilsTest, SliceReadReqsLength) {
  std::vector<SliceReadReq> reqs;
  reqs.push_back({0, 100, std::nullopt});
  reqs.push_back({100, 200, std::nullopt});

  EXPECT_EQ(SliceReadReqsLength(reqs), 300);
}

TEST_F(CompactUtilsTest, SkipEmpty) {
  std::vector<Slice> slices;
  EXPECT_EQ(Skip(slices), 0);
}

TEST_F(CompactUtilsTest, SkipSingleSmallSlice) {
  std::vector<Slice> slices;
  // 10 Bytes < 1MB
  slices.push_back(CreateSlice(1, 0, 10));

  EXPECT_EQ(Skip(slices), 0);
}

TEST_F(CompactUtilsTest, SkipSingleLargeSlice) {
  std::vector<Slice> slices;
  // 2MB > 1MB
  slices.push_back(CreateSlice(1, 0, 2 * 1024 * 1024));

  EXPECT_EQ(Skip(slices), 1);
}

TEST_F(CompactUtilsTest, SkipSmallRatio) {
  std::vector<Slice> slices;
  // Slice 1: 2MB (ok size)
  slices.push_back(CreateSlice(1, 0, 2 * 1024 * 1024));
  // Slice 2: 20MB (continues after)
  // Total len = 22MB.
  // Slice 1 len * 5 = 10MB < 22MB.
  slices.push_back(CreateSlice(2, 2 * 1024 * 1024, 20 * 1024 * 1024));

  // Should skip 0 because first slice is too small relative to total
  EXPECT_EQ(Skip(slices), 0);
}

TEST_F(CompactUtilsTest, SkipLargeRatio) {
  std::vector<Slice> slices;
  // Slice 1: 5MB
  slices.push_back(CreateSlice(1, 0, 5 * 1024 * 1024));
  // Slice 2: 5MB
  // Total len = 10MB.
  // Slice 1 len * 5 = 25MB >= 10MB. Ratio OK.
  slices.push_back(CreateSlice(2, 5 * 1024 * 1024, 5 * 1024 * 1024));

  // Assuming Convert2SliceReadReq preserves non-overlapping slices
  EXPECT_EQ(Skip(slices), 2);
}

TEST_F(CompactUtilsTest, SkipOverlapOverwrite) {
  // Real Convert2SliceReadReq logic is needed for this test to be meaningful.
  // Assuming basic behavior: usually last write wins.
  // But Convert2SliceReadReq takes a list of slices. Usually they are
  // chronologically ordered? If so, later slices in vector overwrite earlier
  // ones.

  std::vector<Slice> slices;
  // Slice 1: 5MB
  slices.push_back(CreateSlice(1, 0, 5 * 1024 * 1024));
  // Slice 2: Overwrites first 1MB of Slice 1
  slices.push_back(CreateSlice(2, 0, 1 * 1024 * 1024));

  // Since Slice 2 overwrites Slice 1 start, the first read req will come from
  // Slice 2 (or part of it). first_req.slice->id (2) != first.id (1) So
  // is_first should be false.

  EXPECT_EQ(Skip(slices), 0);
}

TEST_F(CompactUtilsTest, SkipPartiallyOverwritten) {
  std::vector<Slice> slices;
  // Slice 1: 5MB at offset 0
  slices.push_back(CreateSlice(1, 0, 5 * 1024 * 1024));
  // Slice 2: Overwrites at offset 4MB, len 2MB
  // Range: 0-6MB.
  // Slice 1 is 0-5MB. Slice 2 is 4-6MB.
  // Overlap at 4-5MB.

  // Convert2SliceReadReq result (assuming last wins):
  // 0-4MB: Slice 1 (Trimmed len = 4MB != original 5MB)
  // 4-6MB: Slice 2

  // first_req:
  // offset: 0
  // len: 4MB
  // slice_id: 1

  // Check: first_req.len (4MB) != first.length (5MB)
  // is_first should be false.
  slices.push_back(CreateSlice(2, 4 * 1024 * 1024, 2 * 1024 * 1024));

  EXPECT_EQ(Skip(slices), 0);
}

}  // namespace compaction
}  // namespace vfs
}  // namespace client
}  // namespace dingofs
