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

#include <string>

#include "client/vfs/data/chunk.h"

namespace dingofs {
namespace client {
namespace vfs {

// UUID() is formatted as "<ino>-<index>"
TEST(ChunkTest, UUID_Format) {
  Chunk chunk(/*fs_id=*/1, /*ino=*/42, /*index=*/3, /*chunk_size=*/67108864,
              /*block_size=*/4194304);
  std::string uuid = chunk.UUID();
  EXPECT_EQ(uuid, "42-3");
}

TEST(ChunkTest, UUID_ContainsInoAndChunkIndex) {
  constexpr uint64_t kIno = 12345;
  constexpr uint64_t kIndex = 7;
  Chunk chunk(1, kIno, kIndex, 67108864, 4194304);
  std::string uuid = chunk.UUID();
  EXPECT_NE(uuid.find("12345"), std::string::npos);
  EXPECT_NE(uuid.find("7"), std::string::npos);
}

TEST(ChunkTest, Boundaries_Correct) {
  constexpr uint64_t kChunkSize = 67108864;  // 64 MiB
  constexpr uint64_t kIndex = 2;
  Chunk chunk(1, 10, kIndex, kChunkSize, 4194304);

  EXPECT_EQ(chunk.chunk_start, kIndex * kChunkSize);
  EXPECT_EQ(chunk.chunk_end, chunk.chunk_start + kChunkSize);
}

TEST(ChunkTest, Boundaries_FirstChunk) {
  Chunk chunk(1, 5, /*index=*/0, /*chunk_size=*/1024, /*block_size=*/256);
  EXPECT_EQ(chunk.chunk_start, 0u);
  EXPECT_EQ(chunk.chunk_end, 1024u);
}

TEST(ChunkTest, ToString_NoCrash) {
  Chunk chunk(2, 99, 4, 67108864, 4194304);
  std::string s = chunk.ToString();
  EXPECT_FALSE(s.empty());
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
