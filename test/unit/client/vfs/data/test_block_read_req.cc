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

#include <optional>

#include "client/vfs/data/common/common.h"
#include "common/block/block_key.h"
#include "common/block/block_utils.h"

namespace dingofs {
namespace client {
namespace vfs {

TEST(BlockReadReqTest, IsHole_NulloptKey_ReturnsTrue) {
  BlockReadReq req{};
  req.key = std::nullopt;
  EXPECT_TRUE(req.IsHole());
}

TEST(BlockReadReqTest, IsHole_ValidKey_ReturnsFalse) {
  BlockReadReq req{};
  req.key = BlockKey(1, 0, 4194304);
  EXPECT_FALSE(req.IsHole());
}

TEST(BlockReadReqTest, FileEnd_CorrectCalculation) {
  BlockReadReq req{};
  req.file_offset = 100;
  req.len = 50;
  EXPECT_EQ(req.FileEnd(), 150);
}

TEST(BlockKeyTest, Filename_WithActualSize) {
  BlockKey key(1001, 2, 1048576);
  EXPECT_EQ(key.Filename(), "1001_2_1048576");
}

TEST(BlockKeyTest, StoreKey_WithActualSize) {
  BlockKey key(1001, 2, 1048576);
  EXPECT_EQ(key.StoreKey(), "blocks/0/1/1001_2_1048576");
}

// ─── EnumerateBlockKeys ─────────────────────────────────────────────────────

TEST(EnumerateBlockKeysTest, Aligned_ThreeFullBlocks) {
  auto keys = dingofs::EnumerateBlockKeys(100, 12 * 1024 * 1024, 4 * 1024 * 1024);
  ASSERT_EQ(keys.size(), 3u);
  EXPECT_EQ(keys[0].index, 0);
  EXPECT_EQ(keys[0].size, 4 * 1024 * 1024);
  EXPECT_EQ(keys[1].index, 1);
  EXPECT_EQ(keys[1].size, 4 * 1024 * 1024);
  EXPECT_EQ(keys[2].index, 2);
  EXPECT_EQ(keys[2].size, 4 * 1024 * 1024);
  for (const auto& k : keys) {
    EXPECT_EQ(k.id, 100u);
  }
}

TEST(EnumerateBlockKeysTest, NotAligned_LastBlockSmaller) {
  auto keys = dingofs::EnumerateBlockKeys(200, 9 * 1024 * 1024, 4 * 1024 * 1024);
  ASSERT_EQ(keys.size(), 3u);
  EXPECT_EQ(keys[0].size, 4 * 1024 * 1024);
  EXPECT_EQ(keys[1].size, 4 * 1024 * 1024);
  EXPECT_EQ(keys[2].size, 1 * 1024 * 1024);  // 9MB - 8MB = 1MB
}

TEST(EnumerateBlockKeysTest, SmallerThanBlockSize_SingleBlock) {
  auto keys = dingofs::EnumerateBlockKeys(300, 100, 4 * 1024 * 1024);
  ASSERT_EQ(keys.size(), 1u);
  EXPECT_EQ(keys[0].index, 0);
  EXPECT_EQ(keys[0].size, 100);
}

TEST(EnumerateBlockKeysTest, ExactlyOneBlock) {
  auto keys = dingofs::EnumerateBlockKeys(400, 4 * 1024 * 1024, 4 * 1024 * 1024);
  ASSERT_EQ(keys.size(), 1u);
  EXPECT_EQ(keys[0].size, 4 * 1024 * 1024);
}

TEST(EnumerateBlockKeysTest, ZeroSize_Empty) {
  auto keys = dingofs::EnumerateBlockKeys(500, 0, 4 * 1024 * 1024);
  EXPECT_TRUE(keys.empty());
}

TEST(EnumerateBlockKeysTest, StoreKey_Correct) {
  auto keys = dingofs::EnumerateBlockKeys(1001, 9 * 1024 * 1024, 4 * 1024 * 1024);
  EXPECT_EQ(keys[0].StoreKey(), "blocks/0/1/1001_0_4194304");
  EXPECT_EQ(keys[1].StoreKey(), "blocks/0/1/1001_1_4194304");
  EXPECT_EQ(keys[2].StoreKey(), "blocks/0/1/1001_2_1048576");
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
