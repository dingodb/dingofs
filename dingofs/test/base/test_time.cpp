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

#include "base/time/time.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

namespace dingofs {
namespace base {
namespace time {

class TimeSpecTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(TimeSpecTest, Basic) {
  // time1 == time2
  ASSERT_EQ(TimeSpec(10, 20), TimeSpec(10, 20));

  // time1 != time2
  ASSERT_NE(TimeSpec(10, 20), TimeSpec(10, 21));
  ASSERT_NE(TimeSpec(10, 20), TimeSpec(11, 20));
  ASSERT_NE(TimeSpec(10, 20), TimeSpec(11, 21));

  // time1 < time2
  ASSERT_LT(TimeSpec(10, 20), TimeSpec(10, 21));
  ASSERT_LT(TimeSpec(10, 20), TimeSpec(11, 20));
  ASSERT_LT(TimeSpec(10, 20), TimeSpec(11, 21));

  // time1 > time2
  ASSERT_GT(TimeSpec(10, 20), TimeSpec(10, 19));
  ASSERT_GT(TimeSpec(10, 20), TimeSpec(9, 21));
  ASSERT_GT(TimeSpec(10, 20), TimeSpec(9, 19));

  // time1 + time2
  ASSERT_EQ(TimeSpec(10, 20) + TimeSpec(10, 20), TimeSpec(20, 40));

  // std::cout << time
  std::ostringstream oss;
  oss << TimeSpec(10, 20);
  ASSERT_EQ(oss.str(), "10.20");

  // time2(time1)
  TimeSpec time1(10, 20);
  TimeSpec time2(time1);
  ASSERT_EQ(time2.seconds, 10);
  ASSERT_EQ(time2.nanoSeconds, 20);
}

}  // namespace time
}  // namespace base
}  // namespace dingofs
