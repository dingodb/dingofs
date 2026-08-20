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

#include <cstring>
#include <vector>

#include "client/vfs/blockstore/block_store_v2_util.h"

namespace dingofs {
namespace client {
namespace vfs {

TEST(ToV2HandleTest, BlockKey) {
  BlockHandle in(7, BlockKey(123456789, 3, 4194304));

  blockcache::BlockHandle out;
  auto status = ToV2Handle(in, &out);
  ASSERT_TRUE(status.ok());
  EXPECT_EQ(out.fs_id, 7);
  EXPECT_EQ(out.id, 123456789);
  EXPECT_EQ(out.index, 3);
  EXPECT_EQ(out.size, 4194304);
}

TEST(ToV2HandleTest, StoreKeyCompatible) {
  BlockHandle in(1, BlockKey(9876543210, 15, 1048576));

  blockcache::BlockHandle out;
  ASSERT_TRUE(ToV2Handle(in, &out).ok());
  EXPECT_EQ(in.StoreKey(), out.StoreKey());
  EXPECT_EQ(in.Filename(), out.Filename());
}

TEST(ToV2HandleTest, TensorKeyNotSupported) {
  BlockHandle in(TensorKey("model", 8, 1, "abcdef0123", "fp16"));

  blockcache::BlockHandle out;
  auto status = ToV2Handle(in, &out);
  EXPECT_TRUE(status.IsNotSupport());
}

TEST(BuildBufferViewsTest, MergesAdjacentSegments) {
  static char arena[3 * 4096];
  IOBuffer data;
  for (int i = 0; i < 3; ++i) {
    data.AppendUserData(arena + (i * 4096), 4096, [](void*) {});
  }

  std::vector<blockcache::BufferView> views;
  ASSERT_TRUE(BuildBufferViews(data, &views).ok());
  ASSERT_EQ(views.size(), 1);
  EXPECT_EQ(views[0].data, arena);
  EXPECT_EQ(views[0].size, 3 * 4096);
}

TEST(BuildBufferViewsTest, KeepsNonAdjacentSegments) {
  static char arena[4 * 8192];
  IOBuffer data;
  data.AppendUserData(arena, 4096, [](void*) {});
  data.AppendUserData(arena + 8192, 4096, [](void*) {});

  std::vector<blockcache::BufferView> views;
  ASSERT_TRUE(BuildBufferViews(data, &views).ok());
  ASSERT_EQ(views.size(), 2);
  EXPECT_EQ(views[0].data, arena);
  EXPECT_EQ(views[1].data, arena + 8192);
}

TEST(BuildBufferViewsTest, MaxSegmentsOk) {
  static char arena[64 * 8192];
  IOBuffer data;
  for (size_t i = 0; i < blockcache::kMaxBufferViews; ++i) {
    data.AppendUserData(arena + (i * 8192), 4096, [](void*) {});
  }

  std::vector<blockcache::BufferView> views;
  ASSERT_TRUE(BuildBufferViews(data, &views).ok());
  EXPECT_EQ(views.size(), blockcache::kMaxBufferViews);
}

TEST(BuildBufferViewsTest, TooManySegmentsFails) {
  static char arena[65 * 8192];
  IOBuffer data;
  for (size_t i = 0; i < blockcache::kMaxBufferViews + 1; ++i) {
    data.AppendUserData(arena + (i * 8192), 4096, [](void*) {});
  }

  std::vector<blockcache::BufferView> views;
  auto status = BuildBufferViews(data, &views);
  EXPECT_TRUE(status.IsInvalidParam());
}

TEST(BuildBufferViewsTest, Empty) {
  IOBuffer data;
  std::vector<blockcache::BufferView> views;
  ASSERT_TRUE(BuildBufferViews(data, &views).ok());
  EXPECT_TRUE(views.empty());
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
