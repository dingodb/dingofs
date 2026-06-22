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

#include "cache/local/cache_store.h"

#include <gtest/gtest.h>

#include <sstream>
#include <string>

namespace dingofs {
namespace cache {

TEST(BlockAttrTest, Constructors) {
  {
    BlockAttr attr;  // default
    EXPECT_EQ(attr.from, BlockAttr::kFromUnknown);
    EXPECT_TRUE(attr.store_id.empty());
  }

  {
    BlockAttr attr(BlockAttr::kFromWriteback);
    EXPECT_EQ(attr.from, BlockAttr::kFromWriteback);
    EXPECT_TRUE(attr.store_id.empty());
  }

  {  // a store_id is only valid for reload blocks
    BlockAttr attr(BlockAttr::kFromReload,
                   "950c9813-ea26-4726-96fd-383b0cd22b20");
    EXPECT_EQ(attr.from, BlockAttr::kFromReload);
    EXPECT_EQ(attr.store_id, "950c9813-ea26-4726-96fd-383b0cd22b20");
  }
}

TEST(BlockAttrTest, NonReloadWithStoreIdDies) {
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  // A non-empty store_id is only allowed together with kFromReload.
  EXPECT_DEATH(BlockAttr(BlockAttr::kFromWriteback, "store-1"), "");
}

TEST(BlockAttrTest, BlockFromToString) {
  EXPECT_EQ(BlockFromToString(BlockAttr::kFromWriteback), "writeback");
  EXPECT_EQ(BlockFromToString(BlockAttr::kFromReload), "reload");
  EXPECT_EQ(BlockFromToString(BlockAttr::kFromUnknown), "unknown");
}

TEST(BlockAttrTest, StreamOperator) {
  BlockAttr attr(BlockAttr::kFromReload, "store-7");
  std::ostringstream oss;
  oss << attr;
  EXPECT_NE(oss.str().find("from=reload"), std::string::npos);
  EXPECT_NE(oss.str().find("store_id=store-7"), std::string::npos);
}

}  // namespace cache
}  // namespace dingofs
