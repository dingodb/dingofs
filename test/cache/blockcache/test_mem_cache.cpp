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
 * Created Date: 2024-09-04
 * Author: Jingli Chen (Wine93)
 */

#include <sstream>

#include "cache/blockcache/cache_store.h"
#include "cache/blockcache/mem_cache.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

namespace dingofs {
namespace cache {

class MemCacheTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(MemCacheTest, Basic) {
  auto store = std::make_unique<MemCache>();
  BlockKey key;
  Block block(nullptr, 0);
  std::shared_ptr<BlockReader> reader;

  ASSERT_EQ(store->Init(nullptr), Status::OK());
  ASSERT_EQ(store->Shutdown(), Status::OK());
  ASSERT_EQ(store->Stage(key, block, BlockContext(BlockFrom::kCtoFlush)),
            Status::NotSupport(""));
  ASSERT_EQ(store->RemoveStage(key, BlockContext(BlockFrom::kCtoFlush)),
            Status::NotSupport(""));
  ASSERT_EQ(store->Cache(key, block), Status::NotSupport(""));
  ASSERT_EQ(store->Load(key, reader), Status::NotSupport(""));
  ASSERT_FALSE(store->IsCached(key));
  ASSERT_EQ(store->Id(), "memory_cache");
}

}  // namespace cache
}  // namespace dingofs
