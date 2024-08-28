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

#include "curvefs/src/client/blockcache/error.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

#include <sstream>

namespace curvefs {
namespace client {
namespace blockcache {

class ErrorTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(ErrorTest, Basic) {
  ASSERT_EQ(StrErr(BCACHE_ERROR::OK), "OK");
  ASSERT_EQ(StrErr(BCACHE_ERROR::NOT_FOUND), "not found");
  ASSERT_EQ(StrErr(BCACHE_ERROR::IO_ERROR), "IO error");

  // OK
  {
    std::ostringstream oss;
    oss << BCACHE_ERROR::OK;
    ASSERT_EQ(oss.str(), "success");
  }

  // IO error
  {
    std::ostringstream oss;
    oss << BCACHE_ERROR::IO_ERROR;
    ASSERT_EQ(oss.str(), "failed [IO error]");
  }
}

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs
