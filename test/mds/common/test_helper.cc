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
#include <string>

#include "fmt/core.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "mds/common/helper.h"

namespace dingofs {
namespace mds {
namespace unit_test {

class HelperTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

static IntRange CalBlockIndex(uint64_t block_size, uint64_t chunk_offset,
                              const SliceEntry& slice) {
  IntRange range;
  range.start = (slice.offset() - chunk_offset) / block_size;

  uint64_t end_offset = slice.offset() - chunk_offset + slice.len();
  if (end_offset % block_size == 0) {
    range.end = end_offset / block_size;
  } else {
    range.end = end_offset / block_size + 1;
  }

  return range;
}

TEST_F(HelperTest, CalBlockIndex) {
  const uint64_t block_size = 4194304;  // 4MB
  const uint64_t chunk_offset = 0;

  {
    SliceEntry slice;
    slice.set_offset(0);
    slice.set_len(100);
    auto range = CalBlockIndex(block_size, chunk_offset, slice);
    EXPECT_EQ(0, range.start);
    EXPECT_EQ(1, range.end);
  }

  {
    SliceEntry slice;
    slice.set_offset(0);
    slice.set_len(block_size);
    auto range = CalBlockIndex(block_size, chunk_offset, slice);
    EXPECT_EQ(0, range.start);
    EXPECT_EQ(1, range.end);
  }

  {
    SliceEntry slice;
    slice.set_offset(0);
    slice.set_len(block_size + 1);
    auto range = CalBlockIndex(block_size, chunk_offset, slice);
    EXPECT_EQ(0, range.start);
    EXPECT_EQ(2, range.end);
  }

  {
    SliceEntry slice;
    slice.set_offset(0);
    slice.set_len(block_size * 2);
    auto range = CalBlockIndex(block_size, chunk_offset, slice);
    EXPECT_EQ(0, range.start);
    EXPECT_EQ(2, range.end);
  }

  {
    SliceEntry slice;
    slice.set_offset(0);
    slice.set_len((block_size * 2) + 1234);
    auto range = CalBlockIndex(block_size, chunk_offset, slice);
    EXPECT_EQ(0, range.start);
    EXPECT_EQ(3, range.end);
  }

  {
    SliceEntry slice;
    slice.set_offset(1234);
    slice.set_len(100);
    auto range = CalBlockIndex(block_size, chunk_offset, slice);
    EXPECT_EQ(0, range.start);
    EXPECT_EQ(1, range.end);
  }

  {
    SliceEntry slice;
    slice.set_offset(1234);
    slice.set_len(block_size);
    auto range = CalBlockIndex(block_size, chunk_offset, slice);
    EXPECT_EQ(0, range.start);
    EXPECT_EQ(2, range.end);
  }

  {
    SliceEntry slice;
    slice.set_offset(1234);
    slice.set_len(block_size + 1);
    auto range = CalBlockIndex(block_size, chunk_offset, slice);
    EXPECT_EQ(0, range.start);
    EXPECT_EQ(2, range.end);
  }

  {
    SliceEntry slice;
    slice.set_offset(1234);
    slice.set_len(block_size * 2);
    auto range = CalBlockIndex(block_size, chunk_offset, slice);
    EXPECT_EQ(0, range.start);
    EXPECT_EQ(3, range.end);
  }
  {
    SliceEntry slice;
    slice.set_offset(1234);
    slice.set_len((block_size * 2) + 1234);
    auto range = CalBlockIndex(block_size, chunk_offset, slice);
    EXPECT_EQ(0, range.start);
    EXPECT_EQ(3, range.end);
  }

  {
    SliceEntry slice;
    slice.set_offset(1234);
    slice.set_len((block_size * 2) + 2000);
    auto range = CalBlockIndex(block_size, chunk_offset, slice);
    EXPECT_EQ(0, range.start);
    EXPECT_EQ(3, range.end);
  }
}

}  // namespace unit_test
}  // namespace mds
}  // namespace dingofs