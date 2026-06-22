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

#include <cstdint>
#include <vector>

#include "cache/iutil/math_util.h"

namespace dingofs {
namespace cache {
namespace iutil {

TEST(MathUtilTest, NormalizeByGcd) {
  using V = std::vector<uint64_t>;

  {
    EXPECT_EQ(NormalizeByGcd({3, 6, 9}), V({1, 2, 3}));
    EXPECT_EQ(NormalizeByGcd({4, 6}), V({2, 3}));
    EXPECT_EQ(NormalizeByGcd({100, 250, 50}), V({2, 5, 1}));
  }

  {  // single element normalizes to 1
    EXPECT_EQ(NormalizeByGcd({6}), V({1}));
    EXPECT_EQ(NormalizeByGcd({1}), V({1}));
  }

  {  // already coprime stays the same
    EXPECT_EQ(NormalizeByGcd({7, 7, 7}), V({1, 1, 1}));
    EXPECT_EQ(NormalizeByGcd({2, 3, 5}), V({2, 3, 5}));
  }

  {  // zeros are kept proportional to the non-zero gcd
    EXPECT_EQ(NormalizeByGcd({0, 5}), V({0, 1}));
    EXPECT_EQ(NormalizeByGcd({0, 4, 8}), V({0, 1, 2}));
  }

  {  // large values fit in uint64_t
    uint64_t big = 1ull << 40;
    EXPECT_EQ(NormalizeByGcd({big, big * 2}), V({1, 2}));
  }
}

TEST(MathUtilTest, NormalizeByGcdDeath) {
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  // gcd stays 0 when there are no non-zero numbers, tripping CHECK_NE.
  EXPECT_DEATH(NormalizeByGcd({}), "");
  EXPECT_DEATH(NormalizeByGcd({0, 0}), "");
}

}  // namespace iutil
}  // namespace cache
}  // namespace dingofs
