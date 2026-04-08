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

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
