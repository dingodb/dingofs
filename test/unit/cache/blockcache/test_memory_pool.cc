/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2024-09-26
 * Author: Jingli Chen (Wine93)
 */

#include "client/datastream/memory_pool.h"
#include "common/const.h"
#include "gtest/gtest.h"

namespace dingofs {
namespace cache {

using client::datastream::MemoryPool;

class MemoryPoolTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(MemoryPoolTest, Basic) {
  size_t block_size = 4 * kMiB;
  uint64_t num_blocks = 3;
  auto mem_pool = std::make_unique<MemoryPool>();
  auto rc = mem_pool->CreatePool(block_size, num_blocks);
  ASSERT_TRUE(rc);
  ASSERT_EQ(mem_pool->GetFreeBlocks(), 3);

  void* p = mem_pool->Allocate();
  ASSERT_TRUE(p != nullptr);
  memset(p, 0, sizeof(block_size));
  ASSERT_EQ(mem_pool->GetFreeBlocks(), 2);

  mem_pool->DestroyPool();
}

TEST_F(MemoryPoolTest, Allocate) {
  size_t block_size = 4 * kMiB;
  uint64_t num_blocks = 3;
  auto mem_pool = std::make_unique<MemoryPool>();
  auto rc = mem_pool->CreatePool(block_size, num_blocks);
  ASSERT_TRUE(rc);

  for (auto i = 0; i < num_blocks; i++) {
    void* p = mem_pool->Allocate();
    ASSERT_TRUE(p != nullptr);
    memset(p, 0, sizeof(block_size));
  }
  ASSERT_EQ(mem_pool->GetFreeBlocks(), 0);
  ASSERT_TRUE(mem_pool->Allocate() == nullptr);

  mem_pool->DestroyPool();
}

TEST_F(MemoryPoolTest, DeAllocate) {
  size_t block_size = 4 * kMiB;
  uint64_t num_blocks = 3;
  auto mem_pool = std::make_unique<MemoryPool>();
  auto rc = mem_pool->CreatePool(block_size, num_blocks);
  ASSERT_TRUE(rc);

  // allocate it
  std::vector<void*> blocks;
  for (auto i = 0; i < num_blocks; i++) {
    void* p = mem_pool->Allocate();
    ASSERT_TRUE(p != nullptr);
    memset(p, 0, sizeof(block_size));
    blocks.emplace_back(p);
  }
  ASSERT_EQ(mem_pool->GetFreeBlocks(), 0);
  ASSERT_TRUE(mem_pool->Allocate() == nullptr);

  // deallocate it
  for (auto i = 0; i < num_blocks; i++) {
    mem_pool->DeAllocate(blocks[i]);
  }
  ASSERT_EQ(mem_pool->GetFreeBlocks(), 3);

  // allocate again
  for (auto i = 0; i < num_blocks; i++) {
    ASSERT_TRUE(mem_pool->Allocate() != nullptr);
  }
  ASSERT_EQ(mem_pool->GetFreeBlocks(), 0);

  mem_pool->DestroyPool();
}

}  // namespace cache
}  // namespace dingofs
