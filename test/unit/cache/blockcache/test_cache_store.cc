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
 * Created Date: 2026-02-02
 * Author: AI
 */

#include <gtest/gtest.h>

#include <sstream>

#include "cache/blockcache/cache_store.h"

namespace dingofs {
namespace cache {

class CacheStoreTest : public ::testing::Test {};

TEST_F(CacheStoreTest, BlockKeyDefaultConstructor) {
  BlockKey key;
  EXPECT_EQ(key.fs_id, 0);
  EXPECT_EQ(key.ino, 0);
  EXPECT_EQ(key.id, 0);
  EXPECT_EQ(key.index, 0);
  EXPECT_EQ(key.version, 0);
}

TEST_F(CacheStoreTest, BlockKeyConstructor) {
  BlockKey key(1, 2, 3, 4, 5);
  EXPECT_EQ(key.fs_id, 1);
  EXPECT_EQ(key.ino, 2);
  EXPECT_EQ(key.id, 3);
  EXPECT_EQ(key.index, 4);
  EXPECT_EQ(key.version, 5);
}

TEST_F(CacheStoreTest, BlockKeyFilename) {
  BlockKey key(1, 2, 3, 4, 5);
  EXPECT_EQ(key.Filename(), "1_2_3_4_5");
}

TEST_F(CacheStoreTest, BlockKeyStoreKey) {
  BlockKey key(1, 2, 1000000, 4, 5);
  EXPECT_EQ(key.StoreKey(), "blocks/1/1000/1_2_1000000_4_5");
}

TEST_F(CacheStoreTest, BlockKeyParseFromFilename) {
  BlockKey key;
  EXPECT_TRUE(key.ParseFromFilename("1_2_3_4_5"));
  EXPECT_EQ(key.fs_id, 1);
  EXPECT_EQ(key.ino, 2);
  EXPECT_EQ(key.id, 3);
  EXPECT_EQ(key.index, 4);
  EXPECT_EQ(key.version, 5);
}

TEST_F(CacheStoreTest, BlockKeyParseFromFilenameInvalid) {
  BlockKey key;
  EXPECT_FALSE(key.ParseFromFilename("invalid"));
  EXPECT_FALSE(key.ParseFromFilename("1_2_3"));
  EXPECT_FALSE(key.ParseFromFilename("1_2_3_4_5_6"));
}

TEST_F(CacheStoreTest, BlockKeyToPB) {
  BlockKey key(1, 2, 3, 4, 5);
  auto pb = key.ToPB();
  EXPECT_EQ(pb.fs_id(), 1);
  EXPECT_EQ(pb.ino(), 2);
  EXPECT_EQ(pb.id(), 3);
  EXPECT_EQ(pb.index(), 4);
  EXPECT_EQ(pb.version(), 5);
}

TEST_F(CacheStoreTest, BlockKeyFromPB) {
  pb::cache::BlockKey pb;
  pb.set_fs_id(1);
  pb.set_ino(2);
  pb.set_id(3);
  pb.set_index(4);
  pb.set_version(5);

  BlockKey key(pb);
  EXPECT_EQ(key.fs_id, 1);
  EXPECT_EQ(key.ino, 2);
  EXPECT_EQ(key.id, 3);
  EXPECT_EQ(key.index, 4);
  EXPECT_EQ(key.version, 5);
}

TEST_F(CacheStoreTest, BlockDefaultConstructor) {
  Block block;
  EXPECT_EQ(block.size, 0);
}

TEST_F(CacheStoreTest, BlockConstructorWithData) {
  const char* data = "Hello, World!";
  Block block(data, strlen(data));
  EXPECT_EQ(block.size, strlen(data));
}

TEST_F(CacheStoreTest, BlockAttrDefault) {
  BlockAttr attr;
  EXPECT_EQ(attr.from, BlockAttr::kFromUnknown);
  EXPECT_TRUE(attr.store_id.empty());
}

TEST_F(CacheStoreTest, BlockAttrWithFrom) {
  BlockAttr attr(BlockAttr::kFromWriteback);
  EXPECT_EQ(attr.from, BlockAttr::kFromWriteback);
  EXPECT_TRUE(attr.store_id.empty());
}

TEST_F(CacheStoreTest, BlockAttrWithFromAndStoreId) {
  BlockAttr attr(BlockAttr::kFromReload, "store1");
  EXPECT_EQ(attr.from, BlockAttr::kFromReload);
  EXPECT_EQ(attr.store_id, "store1");
}

TEST_F(CacheStoreTest, BlockFromToString) {
  EXPECT_EQ(BlockFromToString(BlockAttr::kFromWriteback), "writeback");
  EXPECT_EQ(BlockFromToString(BlockAttr::kFromReload), "reload");
  EXPECT_EQ(BlockFromToString(BlockAttr::kFromUnknown), "unknown");
}

TEST_F(CacheStoreTest, BlockAttrStreamOperator) {
  BlockAttr attr(BlockAttr::kFromReload, "store1");
  std::ostringstream oss;
  oss << attr;
  EXPECT_NE(oss.str().find("reload"), std::string::npos);
  EXPECT_NE(oss.str().find("store1"), std::string::npos);
}

TEST_F(CacheStoreTest, BlockKeyEquality) {
  BlockKey key1(1, 2, 3, 4, 5);
  BlockKey key2(1, 2, 3, 4, 5);
  BlockKey key3(1, 2, 3, 4, 6);  // Different version

  EXPECT_EQ(key1.Filename(), key2.Filename());
  EXPECT_NE(key1.Filename(), key3.Filename());
}

TEST_F(CacheStoreTest, BlockKeyLargeValues) {
  BlockKey key(UINT32_MAX, UINT64_MAX, UINT64_MAX, UINT64_MAX, UINT64_MAX);
  EXPECT_EQ(key.fs_id, UINT32_MAX);
  EXPECT_EQ(key.ino, UINT64_MAX);
  EXPECT_EQ(key.id, UINT64_MAX);
  EXPECT_EQ(key.index, UINT64_MAX);
  EXPECT_EQ(key.version, UINT64_MAX);

  // Should still be parseable
  std::string filename = key.Filename();
  BlockKey parsed;
  EXPECT_TRUE(parsed.ParseFromFilename(filename));
  EXPECT_EQ(parsed.fs_id, key.fs_id);
  EXPECT_EQ(parsed.ino, key.ino);
  EXPECT_EQ(parsed.id, key.id);
  EXPECT_EQ(parsed.index, key.index);
  EXPECT_EQ(parsed.version, key.version);
}

TEST_F(CacheStoreTest, BlockKeyFilenameNotEmpty) {
  BlockKey key(1, 2, 3, 4, 5);
  std::string filename = key.Filename();
  EXPECT_FALSE(filename.empty());
}

TEST_F(CacheStoreTest, BlockKeyPBRoundTrip) {
  BlockKey original(100, 200, 300, 400, 500);
  auto pb = original.ToPB();
  BlockKey restored(pb);

  EXPECT_EQ(original.fs_id, restored.fs_id);
  EXPECT_EQ(original.ino, restored.ino);
  EXPECT_EQ(original.id, restored.id);
  EXPECT_EQ(original.index, restored.index);
  EXPECT_EQ(original.version, restored.version);
}

TEST_F(CacheStoreTest, BlockAttrAllFromTypes) {
  EXPECT_EQ(BlockFromToString(BlockAttr::kFromUnknown), "unknown");
  EXPECT_EQ(BlockFromToString(BlockAttr::kFromWriteback), "writeback");
  EXPECT_EQ(BlockFromToString(BlockAttr::kFromReload), "reload");
}

TEST_F(CacheStoreTest, StageOptionDefault) {
  CacheStore::StageOption option;
  // Default values should be set
}

TEST_F(CacheStoreTest, CacheOptionDefault) {
  CacheStore::CacheOption option;
  // Default values should be set
}

TEST_F(CacheStoreTest, LoadOptionDefault) {
  CacheStore::LoadOption option;
  // Default values should be set
}

TEST_F(CacheStoreTest, RemoveStageOptionDefault) {
  CacheStore::RemoveStageOption option;
  // Default values should be set
}

}  // namespace cache
}  // namespace dingofs
