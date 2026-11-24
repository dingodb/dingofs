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
 * Created Date: 2026-09-24
 * Author: AI
 */

#include <gtest/gtest.h>

#include <cstdint>
#include <sstream>
#include <string>
#include <unordered_set>

#include "blockcache/common/block_handle.h"
#include "blockcache/utils/hash.h"
#include "dingofs/cache.pb.h"

namespace dingofs {
namespace blockcache {

constexpr uint64_t kFsId = 10001;
constexpr uint64_t kSliceId = 1001;
constexpr uint32_t kBlockSize = 4 * 1024 * 1024;

static BlockHandle Handle(uint64_t id, uint32_t index = 0,
                          uint32_t size = kBlockSize) {
  return {.fs_id = kFsId, .id = id, .index = index, .size = size};
}

TEST(BlockHandleTest, Equality) {
  const BlockHandle handle = Handle(kSliceId, 3);
  EXPECT_EQ(handle, Handle(kSliceId, 3));
  EXPECT_FALSE(handle != Handle(kSliceId, 3));

  {  // any of slice, index and size apart makes another block
    EXPECT_NE(handle, Handle(1002, 3));
    EXPECT_NE(handle, Handle(kSliceId, 4));
    EXPECT_NE(handle, Handle(kSliceId, 3, kBlockSize - 1));
  }

  {  // the fs is not part of a block's identity, neither here nor in Hash()
    EXPECT_EQ(
        handle,
        (BlockHandle{
            .fs_id = 10002, .id = kSliceId, .index = 3, .size = kBlockSize}));
  }
}

TEST(BlockHandleTest, FromPbAndToPb) {
  const BlockHandle handle = Handle(kSliceId, 3);

  pb::blockcache::BlockHandle pb;
  handle.ToPb(&pb);
  EXPECT_EQ(pb.fs_id(), kFsId);
  EXPECT_EQ(pb.id(), kSliceId);
  EXPECT_EQ(pb.index(), 3u);
  EXPECT_EQ(pb.size(), kBlockSize);

  const BlockHandle back = BlockHandle::FromPb(pb);
  EXPECT_EQ(back, handle);
  EXPECT_EQ(back.fs_id, kFsId);

  {  // an empty message is the zero handle
    const BlockHandle zero = BlockHandle::FromPb(pb::blockcache::BlockHandle());
    EXPECT_EQ(zero, (BlockHandle{}));
    EXPECT_EQ(zero.fs_id, 0u);
  }
}

TEST(BlockHandleTest, ParseFromFilename) {
  BlockHandle handle = Handle(0);
  {  // "<slice>_<index>_<size>", as Filename() writes it; the fs is not in
     // the name and stays whatever it was
    ASSERT_TRUE(handle.ParseFromFilename("1001_3_4194304"));
    EXPECT_EQ(handle, Handle(kSliceId, 3));
    EXPECT_EQ(handle.fs_id, kFsId);

    ASSERT_TRUE(
        handle.ParseFromFilename("18446744073709551615_4294967295_4294967295"));
    EXPECT_EQ(handle.id, UINT64_MAX);
    EXPECT_EQ(handle.index, UINT32_MAX);
    EXPECT_EQ(handle.size, UINT32_MAX);
  }

  {  // anything else is refused and leaves the handle as it was
    const BlockHandle before = Handle(kSliceId, 7);
    for (const char* name : {"",
                             "1001",
                             "1001_3",
                             "1001_3_",
                             "_3_4194304",
                             "1001__4194304",
                             "abc_3_4194304",
                             "1001_x_4194304",
                             "1001_3_4194304MiB",
                             "+1001_3_4194304",
                             "-1_3_4194304",
                             "1001_3_-1",
                             "99999999999999999999_3_4194304",
                             "1001_4294967296_4194304",
                             "1001_3_4294967296",
                             "1001_3_4194304_",
                             "1001_3_4194304_1",
                             "1001-3-4194304",
                             " 1001_3_4194304",
                             "1001_3_4194304 "}) {
      handle = before;
      EXPECT_FALSE(handle.ParseFromFilename(name)) << "'" << name << "'";
      EXPECT_EQ(handle, before) << "'" << name << "'";
    }
  }
}

TEST(BlockHandleTest, Hash) {
  const BlockHandle handle = Handle(kSliceId, 3);
  EXPECT_EQ(handle.Hash(),
            HashInt(HashInt(kSliceId) ^ ((uint64_t{3} << 32) | kBlockSize)));
  EXPECT_EQ(BlockHandleHash{}(handle), static_cast<size_t>(handle.Hash()));

  {  // slice, index and size are mixed in; the fs is not
    EXPECT_NE(Handle(1002, 3).Hash(), handle.Hash());
    EXPECT_NE(Handle(kSliceId, 4).Hash(), handle.Hash());
    EXPECT_NE(Handle(kSliceId, 3, kBlockSize - 1).Hash(), handle.Hash());
    EXPECT_EQ(
        (BlockHandle{
             .fs_id = 10002, .id = kSliceId, .index = 3, .size = kBlockSize})
            .Hash(),
        handle.Hash());
  }

  {  // the blocks of neighbouring slices do not collide
    std::unordered_set<uint64_t> hashes;
    for (uint64_t id = kSliceId; id < kSliceId + 64; id++) {
      for (uint32_t index = 0; index < 64; index++) {
        hashes.insert(Handle(id, index).Hash());
      }
    }
    EXPECT_EQ(hashes.size(), 64u * 64u);
  }
}

TEST(BlockHandleTest, Filename) {
  EXPECT_EQ(Handle(kSliceId).Filename(), "1001_0_4194304");
  EXPECT_EQ(Handle(1234567890, 3, 65536).Filename(), "1234567890_3_65536");

  {  // what Filename() writes, ParseFromFilename() reads back
    BlockHandle parsed = Handle(0);
    ASSERT_TRUE(
        parsed.ParseFromFilename(Handle(1234567890, 3, 65536).Filename()));
    EXPECT_EQ(parsed, Handle(1234567890, 3, 65536));
  }
}

TEST(BlockHandleTest, StoreKey) {
  // blocks/<slice / 1e6>/<slice / 1e3>/<filename>: two directory levels keep
  // any one listing of the bucket short
  EXPECT_EQ(Handle(999).StoreKey(), "blocks/0/0/999_0_4194304");
  EXPECT_EQ(Handle(kSliceId).StoreKey(), "blocks/0/1/1001_0_4194304");
  EXPECT_EQ(Handle(1234567890, 3).StoreKey(),
            "blocks/1234/1234567/1234567890_3_4194304");
}

TEST(BlockHandleTest, Print) {
  std::ostringstream os;
  os << Handle(kSliceId, 3);
  EXPECT_EQ(os.str(), "BlockHandle{fs_id=10001 id=1001 index=3 size=4194304}");
}

}  // namespace blockcache
}  // namespace dingofs
