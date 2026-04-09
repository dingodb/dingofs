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

#include <optional>
#include <string>
#include <vector>

#include "client/vfs/data/common/common.h"
#include "client/vfs/service/inode_blocks_service.h"
#include "common/block/block_key.h"

namespace dingofs {
namespace client {
namespace vfs {

TEST(DumpBlockReadReqsTest, SingleBlock_DelimitedFormat) {
  std::vector<BlockReadReq> reqs = {BlockReadReq{
      .file_offset = 0,
      .block_offset = 0,
      .len = 4194304,
      .key = BlockKey(1001, 0, 4194304),
  }};

  std::string out = DumpBlockReadReqs(reqs, /*use_delimiter=*/true);

  EXPECT_NE(out.find("file_offset"), std::string::npos);
  EXPECT_NE(out.find("block_name"), std::string::npos);
  EXPECT_NE(out.find("blocks/0/1/1001_0_4194304"), std::string::npos);
  EXPECT_NE(out.find("4194304"), std::string::npos);
}

TEST(DumpBlockReadReqsTest, HoleBlock_ShowsAsHole) {
  std::vector<BlockReadReq> reqs = {BlockReadReq{
      .file_offset = 0,
      .block_offset = 0,
      .len = 4194304,
      .key = std::nullopt,
  }};

  std::string out = DumpBlockReadReqs(reqs, /*use_delimiter=*/true);

  // Hole should be explicitly labeled, not empty
  EXPECT_NE(out.find("hole"), std::string::npos);
}

TEST(DumpBlockReadReqsTest, MixedHoleAndData) {
  std::vector<BlockReadReq> reqs = {
      BlockReadReq{.file_offset = 0,
                   .block_offset = 0,
                   .len = 4194304,
                   .key = BlockKey(100, 0, 4194304)},
      BlockReadReq{.file_offset = 4194304,
                   .block_offset = 0,
                   .len = 4194304,
                   .key = std::nullopt},
      BlockReadReq{.file_offset = 8388608,
                   .block_offset = 0,
                   .len = 1048576,
                   .key = BlockKey(100, 2, 1048576)},
  };

  std::string out = DumpBlockReadReqs(reqs, /*use_delimiter=*/true);

  EXPECT_NE(out.find("blocks/0/0/100_0_4194304"), std::string::npos);
  EXPECT_NE(out.find("hole"), std::string::npos);
  EXPECT_NE(out.find("blocks/0/0/100_2_1048576"), std::string::npos);
}

TEST(DumpBlockReadReqsTest, EmptyReqs_HeaderOnly) {
  std::vector<BlockReadReq> reqs;
  std::string out = DumpBlockReadReqs(reqs, /*use_delimiter=*/true);

  // Header present, but no data rows
  EXPECT_NE(out.find("file_offset"), std::string::npos);
  EXPECT_EQ(out.find("hole"), std::string::npos);
  EXPECT_EQ(out.find("blocks/"), std::string::npos);
}

TEST(DumpBlockReadReqsTest, FixedWidthFormat_NoDelimiter) {
  std::vector<BlockReadReq> reqs = {BlockReadReq{
      .file_offset = 0,
      .block_offset = 0,
      .len = 4194304,
      .key = BlockKey(1001, 0, 4194304),
  }};

  std::string out = DumpBlockReadReqs(reqs, /*use_delimiter=*/false);

  EXPECT_NE(out.find("blocks/0/1/1001_0_4194304"), std::string::npos);
}

TEST(DumpBlockReadReqsTest, OffsetInBlock_DisplayedCorrectly) {
  std::vector<BlockReadReq> reqs = {BlockReadReq{
      .file_offset = 7340032,      // 7MB
      .block_offset = 3145728,  // 3MB (CopyFileRange scenario)
      .len = 1048576,              // 1MB
      .key = BlockKey(100, 0, 4194304),
  }};

  std::string out = DumpBlockReadReqs(reqs, /*use_delimiter=*/true);

  EXPECT_NE(out.find("3145728"), std::string::npos);  // block_offset
  EXPECT_NE(out.find("7340032"), std::string::npos);  // file_offset
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
