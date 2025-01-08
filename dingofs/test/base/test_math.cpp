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

#include "base/math/math.h"
#include "gtest/gtest.h"

namespace dingofs {
namespace base {
namespace math {

class MathTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(MathTest, Unit) {
  ASSERT_EQ(kKiB, 1024);
  ASSERT_EQ(kMiB, 1048576);
  ASSERT_EQ(kGiB, 1073741824);
  ASSERT_EQ(kTiB, 1099511627776);
}

TEST_F(MathTest, Divide) {
  ASSERT_EQ(Divide(10, 2), 5);
  ASSERT_EQ(Divide(3, 2), 1.5);
  ASSERT_EQ(Divide(2, 10), 0.2);
}

}  // namespace math
}  // namespace base
}  // namespace dingofs
