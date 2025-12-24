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

#include <sys/types.h>

#include <cstdint>

#include "dingofs/mds.pb.h"
#include "gtest/gtest.h"
#include "mds/common/type.h"
#include "mds/filesystem/chunk_cache.h"

namespace dingofs {
namespace mds {
namespace unit_test {

const int64_t kFsId = 1000;

static mds::ChunkEntry GenChunkEntry(uint32_t index, uint64_t version) {
  mds::ChunkEntry chunk;
  chunk.set_index(index);
  chunk.set_version(version);
  for (uint32_t i = 0; i < 3; ++i) {
    auto* slice = chunk.add_slices();
    slice->set_id(i);
    slice->set_size(1024 * 1024);
    slice->set_len(slice->size());
    slice->set_offset(i * 1024 * 1024);
  }

  return chunk;
}

class ChunkCacheTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(ChunkCacheTest, PutIf) {
  mds::ChunkCache chunk_cache(kFsId);

  Ino ino = 100000;

  ASSERT_TRUE(chunk_cache.PutIf(ino, GenChunkEntry(0, 1)));
  ASSERT_TRUE(chunk_cache.PutIf(ino, GenChunkEntry(0, 2)));

  ASSERT_TRUE(chunk_cache.PutIf(ino, GenChunkEntry(1, 1)));
  ASSERT_TRUE(chunk_cache.PutIf(ino, GenChunkEntry(1, 2)));

  ASSERT_TRUE(chunk_cache.PutIf(ino, GenChunkEntry(2, 1)));
  ASSERT_TRUE(chunk_cache.PutIf(ino, GenChunkEntry(2, 4)));
  ASSERT_FALSE(chunk_cache.PutIf(ino, GenChunkEntry(2, 3)));

  ASSERT_EQ(chunk_cache.Size(), 3);
  chunk_cache.Clear();
  ASSERT_EQ(chunk_cache.Size(), 0);
}

TEST_F(ChunkCacheTest, Get) {
  mds::ChunkCache chunk_cache(kFsId);

  Ino ino = 100000;

  {
    ASSERT_TRUE(chunk_cache.PutIf(ino, GenChunkEntry(0, 1)));
    ASSERT_TRUE(chunk_cache.PutIf(ino, GenChunkEntry(0, 2)));

    auto chunk = chunk_cache.Get(ino, 0);
    ASSERT_NE(chunk, nullptr);
    ASSERT_EQ(chunk->version(), 2);
  }

  {
    ASSERT_TRUE(chunk_cache.PutIf(ino, GenChunkEntry(1, 1)));
    ASSERT_TRUE(chunk_cache.PutIf(ino, GenChunkEntry(1, 2)));

    auto chunk = chunk_cache.Get(ino, 1);
    ASSERT_NE(chunk, nullptr);
    ASSERT_EQ(chunk->version(), 2);
  }

  {
    ASSERT_TRUE(chunk_cache.PutIf(ino, GenChunkEntry(2, 4)));
    ASSERT_FALSE(chunk_cache.PutIf(ino, GenChunkEntry(2, 3)));

    auto chunk = chunk_cache.Get(ino, 2);
    ASSERT_NE(chunk, nullptr);
    ASSERT_EQ(chunk->version(), 4);
  }

  {
    auto chunks = chunk_cache.Get(ino);
    ASSERT_EQ(chunks.size(), 3);
  }

  {
    auto chunks = chunk_cache.Get(ino + 1);
    ASSERT_EQ(chunks.size(), 0);
  }

  chunk_cache.Clear();
  ASSERT_EQ(chunk_cache.Size(), 0);
}

TEST_F(ChunkCacheTest, Delete) {
  mds::ChunkCache chunk_cache(kFsId);
  Ino ino = 100000;

  {
    ASSERT_TRUE(chunk_cache.PutIf(ino, GenChunkEntry(0, 1)));

    chunk_cache.Delete(ino, 0);
    ASSERT_EQ(nullptr, chunk_cache.Get(ino, 0));
  }

  {
    ASSERT_TRUE(chunk_cache.PutIf(ino, GenChunkEntry(1, 2)));
    chunk_cache.Delete(ino, 1);
    ASSERT_EQ(nullptr, chunk_cache.Get(ino, 1));
  }
}

}  // namespace unit_test
}  // namespace mds
}  // namespace dingofs
