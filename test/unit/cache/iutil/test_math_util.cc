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

#include <vector>

#include "cache/iutil/math_util.h"

namespace dingofs {
namespace cache {
namespace iutil {

class MathUtilTest : public ::testing::Test {};

TEST_F(MathUtilTest, NormalizeByGcd) {
  {
    std::vector<uint64_t> nums = {10, 20, 30};
    auto result = NormalizeByGcd(nums);
    EXPECT_EQ(result, (std::vector<uint64_t>{1, 2, 3}));
  }

  {
    std::vector<uint64_t> nums = {6, 12, 18};
    auto result = NormalizeByGcd(nums);
    EXPECT_EQ(result, (std::vector<uint64_t>{1, 2, 3}));
  }

  {
    std::vector<uint64_t> nums = {7, 14, 21};
    auto result = NormalizeByGcd(nums);
    EXPECT_EQ(result, (std::vector<uint64_t>{1, 2, 3}));
  }

  {
    std::vector<uint64_t> nums = {5, 10, 15, 20};
    auto result = NormalizeByGcd(nums);
    EXPECT_EQ(result, (std::vector<uint64_t>{1, 2, 3, 4}));
  }

  {
    std::vector<uint64_t> nums = {100};
    auto result = NormalizeByGcd(nums);
    EXPECT_EQ(result, (std::vector<uint64_t>{1}));
  }

  {
    std::vector<uint64_t> nums = {3, 5, 7};
    auto result = NormalizeByGcd(nums);
    EXPECT_EQ(result, (std::vector<uint64_t>{3, 5, 7}));
  }

  {
    std::vector<uint64_t> nums = {12, 18, 24};
    auto result = NormalizeByGcd(nums);
    EXPECT_EQ(result, (std::vector<uint64_t>{2, 3, 4}));
  }
}

}  // namespace iutil
}  // namespace cache
}  // namespace dingofs
