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

#include <sstream>
#include <string>

#include "cache/api/block_cache.h"
#include "common/block/block_handle.h"
#include "common/block/block_key.h"
#include "common/io_buffer.h"

namespace dingofs {
namespace cache {

namespace {
BlockHandle MakeHandle() { return BlockHandle(1, BlockKey(1, 0, 4096)); }
}  // namespace

// The base BlockCache is the no-op tier TierBlockCache falls back to: lifecycle
// is a no-op success and every data op is unsupported / disabled.
TEST(BlockCacheBaseTest, LifecycleIsNoop) {
  BlockCache bc;
  EXPECT_TRUE(bc.Start().ok());
  EXPECT_TRUE(bc.Shutdown().ok());
}

TEST(BlockCacheBaseTest, DataOpsUnsupported) {
  BlockCache bc;
  IOBuffer buffer;
  EXPECT_TRUE(bc.Put(MakeHandle(), IOBuffer()).IsNotSupport());
  EXPECT_TRUE(bc.Range(MakeHandle(), 0, 1, &buffer).IsNotSupport());
  EXPECT_TRUE(bc.Cache(MakeHandle(), IOBuffer()).IsNotSupport());
  EXPECT_TRUE(bc.Prefetch(MakeHandle(), 1).IsNotSupport());
}

TEST(BlockCacheBaseTest, AsyncOpsReportUnsupported) {
  BlockCache bc;
  IOBuffer buffer;

  {
    Status got;
    bc.AsyncPut(MakeHandle(), IOBuffer(), [&](Status s) { got = s; });
    EXPECT_TRUE(got.IsNotSupport());
  }
  {
    Status got;
    bc.AsyncRange(MakeHandle(), 0, 1, &buffer, [&](Status s) { got = s; });
    EXPECT_TRUE(got.IsNotSupport());
  }
  {
    Status got;
    bc.AsyncCache(MakeHandle(), IOBuffer(), [&](Status s) { got = s; });
    EXPECT_TRUE(got.IsNotSupport());
  }
  {
    Status got;
    bc.AsyncPrefetch(MakeHandle(), 1, [&](Status s) { got = s; });
    EXPECT_TRUE(got.IsNotSupport());
  }
}

TEST(BlockCacheBaseTest, QueriesDisabled) {
  BlockCache bc;
  EXPECT_FALSE(bc.IsEnabled());
  EXPECT_FALSE(bc.EnableStage());
  EXPECT_FALSE(bc.EnableCache());
  EXPECT_FALSE(bc.IsCached(MakeHandle()));

  Json::Value value;
  EXPECT_TRUE(bc.Dump(value));
}

TEST(BlockCacheBaseTest, StreamOperator) {
  BlockCache bc;
  std::ostringstream oss;
  oss << bc;
  EXPECT_NE(oss.str().find("BlockCache{enable=0"), std::string::npos);
}

}  // namespace cache
}  // namespace dingofs
