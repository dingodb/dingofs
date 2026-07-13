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

#include "common/block/block_handle.h"
#include "common/block/block_key.h"
#include "common/block/block_utils.h"
#include "common/block/tensor_key.h"

namespace dingofs {

TEST(BlockKeyTest, FilenameAndStoreKeyEncodeIdIndexAndSize) {
  BlockKey key(123456789, 3, 4194304);
  EXPECT_EQ(key.Id(), "123456789");
  EXPECT_EQ(key.Filename(), "123456789_3_4194304");
  EXPECT_EQ(key.StoreKey(), "blocks/123/123456/123456789_3_4194304");
  EXPECT_EQ(key.StoreSize(), 4194304u);
}

TEST(BlockKeyTest, DefaultConstructedIsZeroed) {
  BlockKey key;
  EXPECT_EQ(key.id, 0u);
  EXPECT_EQ(key.index, 0u);
  EXPECT_EQ(key.size, 0u);
}

TEST(BlockUtilsTest, EnumerateBlockKeysSplitsFullBlocksAndRemainder) {
  // 10MB slice split into 4MB blocks -> two full blocks + a 2MB remainder.
  auto keys = EnumerateBlockKeys(/*slice_id=*/7, /*size=*/10 * 1024 * 1024,
                                 /*block_size=*/4 * 1024 * 1024);
  ASSERT_EQ(keys.size(), 3u);
  EXPECT_EQ(keys[0].index, 0u);
  EXPECT_EQ(keys[0].size, 4u * 1024 * 1024);
  EXPECT_EQ(keys[1].index, 1u);
  EXPECT_EQ(keys[1].size, 4u * 1024 * 1024);
  EXPECT_EQ(keys[2].index, 2u);
  EXPECT_EQ(keys[2].size, 2u * 1024 * 1024);
  for (const auto& k : keys) {
    EXPECT_EQ(k.id, 7u);
  }
}

TEST(BlockUtilsTest, EnumerateBlockKeysExactMultipleHasNoRemainder) {
  auto keys = EnumerateBlockKeys(1, 8 * 1024 * 1024, 4 * 1024 * 1024);
  ASSERT_EQ(keys.size(), 2u);
  EXPECT_EQ(keys[1].size, 4u * 1024 * 1024);
}

TEST(BlockUtilsTest, EnumerateBlockKeysOfZeroSizeIsEmpty) {
  EXPECT_TRUE(EnumerateBlockKeys(1, 0, 4 * 1024 * 1024).empty());
}

TEST(TensorKeyTest, FilenameSanitizesUnsafeCharacters) {
  TensorKey key("model/a@b", 2, 1, "abcd1234", "float16");
  EXPECT_EQ(key.Filename(), "model_a_b@2@1@abcd1234@float16");
  EXPECT_EQ(key.Id(), "abcd1234");
  EXPECT_EQ(key.StoreSize(), 0u);
}

TEST(TensorKeyTest, StoreKeyBucketsByChunkHashPrefix) {
  TensorKey key("model", 1, 0, "abcdef0123456789", "bfloat16");
  EXPECT_EQ(key.StoreKey(), "tensor/ab/abcd/model@1@0@abcdef0123456789@bfloat16");
}

TEST(BlockHandleTest, WrapsBlockKeyAndDelegates) {
  BlockKey block_key(42, 0, 1024);
  BlockHandle handle(/*fs_id=*/9, block_key);
  EXPECT_EQ(handle.FsId(), 9u);
  EXPECT_EQ(handle.Id(), block_key.Id());
  EXPECT_EQ(handle.Filename(), block_key.Filename());
  EXPECT_EQ(handle.StoreKey(), block_key.StoreKey());
  EXPECT_EQ(handle.StoreSize(), block_key.StoreSize());
}

TEST(BlockHandleTest, WrapsTensorKeyAndDelegates) {
  TensorKey tensor_key("model", 1, 0, "abcd1234", "float16");
  BlockHandle handle(tensor_key);
  // TensorKey handles carry no explicit fs_id; default-initialized to 0.
  EXPECT_EQ(handle.FsId(), 0u);
  EXPECT_EQ(handle.Id(), tensor_key.Id());
  EXPECT_EQ(handle.Filename(), tensor_key.Filename());
  EXPECT_EQ(handle.StoreKey(), tensor_key.StoreKey());
}

TEST(BlockHandleTest, VisitDispatchesToActiveAlternative) {
  BlockHandle handle(1, BlockKey(1, 0, 1));
  bool saw_block = false;
  handle.Visit([&](const auto& key) {
    using T = std::decay_t<decltype(key)>;
    saw_block = std::is_same_v<T, BlockKey>;
  });
  EXPECT_TRUE(saw_block);
}

}  // namespace dingofs
