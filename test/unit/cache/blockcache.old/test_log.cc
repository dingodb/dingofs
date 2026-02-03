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

#include "cache/utils/access_log.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

namespace dingofs {
namespace cache {

using cache::utils::LogGuard;

class LogTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

// Only test core dump
TEST_F(LogTest, Basic) {
  LogGuard log([] { return "hello world"; });
}

}  // namespace cache
}  // namespace dingofs
