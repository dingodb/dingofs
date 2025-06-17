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

#include "base/string/string.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

namespace dingofs {
namespace base {
namespace string {

class StringTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(StringTest, Str2Int) {
  uint64_t n;

  ASSERT_TRUE(Str2Int("0", &n));
  ASSERT_EQ(n, 0);

  ASSERT_TRUE(Str2Int("65536", &n));
  ASSERT_EQ(n, 65536);

  ASSERT_TRUE(Str2Int("123456789123456789", &n));
  ASSERT_EQ(n, 123456789123456789);

  ASSERT_FALSE(Str2Int("abc", &n));
  ASSERT_FALSE(Str2Int(std::string('1', 30), &n));
}

TEST_F(StringTest, Strs2Ints) {
  uint64_t a, b, c;

  ASSERT_TRUE(Strs2Ints({"0", "1", "2"}, {&a, &b, &c}));
  ASSERT_EQ(a, 0);
  ASSERT_EQ(b, 1);
  ASSERT_EQ(c, 2);

  ASSERT_FALSE(Strs2Ints({"0", "1"}, {&a, &b, &c}));
  ASSERT_FALSE(Strs2Ints({"0", "1", "2"}, {&a, &b}));
  ASSERT_FALSE(Strs2Ints({"0", "1", "a"}, {&a, &b, &c}));
}

}  // namespace string
}  // namespace base
}  // namespace dingofs
