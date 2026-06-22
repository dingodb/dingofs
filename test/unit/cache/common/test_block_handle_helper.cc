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

/*
 * Project: DingoFS
 * Created Date: 2026-06-21
 * Author: AI
 */

#include <gtest/gtest.h>

#include <cstdint>

#include "cache/common/block_handle_helper.h"

namespace dingofs {
namespace cache {

class BlockHandleHelperTest : public ::testing::Test {};

TEST_F(BlockHandleHelperTest, ParseBlockKeyFromFilename) {
  {
    BlockKey key;
    ASSERT_TRUE(ParseFromFilename("4098_2_4194304", &key));
    EXPECT_EQ(key.id, 4098u);
    EXPECT_EQ(key.index, 2u);
    EXPECT_EQ(key.size, 4194304u);
  }

  {  // round-trip with the canonical Filename() format
    BlockKey original(123456, 7, 65536);
    BlockKey parsed;
    ASSERT_TRUE(ParseFromFilename(original.Filename(), &parsed));
    EXPECT_EQ(parsed.Filename(), original.Filename());
  }

  {  // malformed filenames are rejected
    BlockKey key;
    EXPECT_FALSE(ParseFromFilename("", &key));
    EXPECT_FALSE(ParseFromFilename("4098_2", &key));
    EXPECT_FALSE(ParseFromFilename("abc", &key));
    EXPECT_FALSE(ParseFromFilename("_2_3", &key));
  }
}

TEST_F(BlockHandleHelperTest, ParseTensorKeyFromFilename) {
  {
    TensorKey key;
    ASSERT_TRUE(ParseFromFilename("llama@8@3@deadbeef@float16", &key));
    EXPECT_EQ(key.model_name, "llama");
    EXPECT_EQ(key.world_size, 8u);
    EXPECT_EQ(key.worker_id, 3u);
    EXPECT_EQ(key.chunk_hash, "deadbeef");
    EXPECT_EQ(key.dtype, "float16");
  }

  {  // too few / too many fields are rejected
    TensorKey key;
    EXPECT_FALSE(ParseFromFilename("a@b@c@d", &key));             // 4 parts
    EXPECT_FALSE(ParseFromFilename("m@8@3@hash@f16@e@x", &key));  // 7 parts
  }

  {  // world_size / worker_id must be valid uint32
    TensorKey key;
    EXPECT_FALSE(ParseFromFilename("m@x@3@hash@f16", &key));    // non-numeric
    EXPECT_FALSE(ParseFromFilename("m@@3@hash@f16", &key));     // empty
    EXPECT_FALSE(ParseFromFilename("m@8@y@hash@f16", &key));    // non-numeric
    EXPECT_FALSE(ParseFromFilename("m@12x@3@hash@f16", &key));  // trailing junk
    EXPECT_FALSE(
        ParseFromFilename("m@99999999999@3@hash@f16", &key));  // > UINT32_MAX
    EXPECT_FALSE(ParseFromFilename("m@999999999999999999999@3@hash@f16",
                                   &key));  // strtoul overflow/errno
  }
}

TEST_F(BlockHandleHelperTest, BlockKeyToFromPB) {
  BlockKey key(4098, 2, 4194304);
  auto pb = ToPB(key);
  EXPECT_EQ(pb.id(), 4098u);
  EXPECT_EQ(pb.index(), 2u);
  EXPECT_EQ(pb.size(), 4194304u);

  auto back = FromPB(pb);
  EXPECT_EQ(back.id, key.id);
  EXPECT_EQ(back.index, key.index);
  EXPECT_EQ(back.size, key.size);
}

TEST_F(BlockHandleHelperTest, TensorKeyToFromPB) {
  TensorKey key("llama", 8, 3, "deadbeef", "float16");
  auto pb = ToPB(key);
  EXPECT_EQ(pb.model_name(), "llama");
  EXPECT_EQ(pb.world_size(), 8u);
  EXPECT_EQ(pb.worker_id(), 3u);
  EXPECT_EQ(pb.chunk_hash(), "deadbeef");
  EXPECT_EQ(pb.dtype(), "float16");

  auto back = FromPB(pb);
  EXPECT_EQ(back.model_name, key.model_name);
  EXPECT_EQ(back.world_size, key.world_size);
  EXPECT_EQ(back.worker_id, key.worker_id);
  EXPECT_EQ(back.chunk_hash, key.chunk_hash);
  EXPECT_EQ(back.dtype, key.dtype);
}

TEST_F(BlockHandleHelperTest, BlockHandleToFromPB) {
  {  // file-block handle carries fs_id + block_key
    BlockHandle handle(7, BlockKey(4098, 2, 4194304));
    auto pb = ToHandlePB(handle);
    ASSERT_TRUE(pb.has_block_key());
    EXPECT_FALSE(pb.has_tensor_key());
    EXPECT_EQ(pb.fs_id(), 7u);

    auto back = FromHandlePB(pb);
    EXPECT_EQ(back.FsId(), 7u);
    EXPECT_EQ(back.Filename(), "4098_2_4194304");
  }

  {  // fs_id of 0 still round-trips (fs_id is optional with explicit presence)
    BlockHandle handle(0, BlockKey(1, 0, 4096));
    auto pb = ToHandlePB(handle);
    ASSERT_TRUE(pb.has_fs_id());
    auto back = FromHandlePB(pb);
    EXPECT_EQ(back.FsId(), 0u);
    EXPECT_EQ(back.Filename(), "1_0_4096");
  }

  {  // tensor handle carries tensor_key and no fs_id semantics
    BlockHandle handle(TensorKey("llama", 8, 3, "deadbeef", "float16"));
    auto pb = ToHandlePB(handle);
    ASSERT_TRUE(pb.has_tensor_key());
    EXPECT_FALSE(pb.has_block_key());

    auto back = FromHandlePB(pb);
    EXPECT_EQ(back.Filename(), "llama@8@3@deadbeef@float16");
  }
}

}  // namespace cache
}  // namespace dingofs
