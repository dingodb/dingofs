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

#include <sstream>

#include "cache/iutil/time_util.h"

namespace dingofs {
namespace cache {
namespace iutil {

TEST(TimeUtilTest, TimeSpecConstructor) {
  {
    TimeSpec ts;
    EXPECT_EQ(ts.sec, 0);
    EXPECT_EQ(ts.nsec, 0);
  }

  {
    TimeSpec ts(100);
    EXPECT_EQ(ts.sec, 100);
    EXPECT_EQ(ts.nsec, 0);
  }

  {
    TimeSpec ts(100, 500);
    EXPECT_EQ(ts.sec, 100);
    EXPECT_EQ(ts.nsec, 500);
  }

  {
    TimeSpec ts1(100, 500);
    TimeSpec ts2(ts1);
    EXPECT_EQ(ts2.sec, 100);
    EXPECT_EQ(ts2.nsec, 500);
  }

  {
    TimeSpec ts1(100, 500);
    TimeSpec ts2;
    ts2 = ts1;
    EXPECT_EQ(ts2.sec, 100);
    EXPECT_EQ(ts2.nsec, 500);
  }
}

TEST(TimeUtilTest, TimeSpecAddition) {
  TimeSpec ts1(100, 500);
  TimeSpec ts2(50, 300);
  TimeSpec result = ts1 + ts2;
  EXPECT_EQ(result.sec, 150);
  EXPECT_EQ(result.nsec, 800);
}

TEST(TimeUtilTest, TimeSpecEquality) {
  TimeSpec ts1(100, 500);
  TimeSpec ts2(100, 500);
  TimeSpec ts3(100, 501);
  TimeSpec ts4(101, 500);

  EXPECT_TRUE(ts1 == ts2);
  EXPECT_FALSE(ts1 == ts3);
  EXPECT_FALSE(ts1 == ts4);
}

TEST(TimeUtilTest, TimeSpecInequality) {
  TimeSpec ts1(100, 500);
  TimeSpec ts2(100, 500);
  TimeSpec ts3(100, 501);

  EXPECT_FALSE(ts1 != ts2);
  EXPECT_TRUE(ts1 != ts3);
}

TEST(TimeUtilTest, TimeSpecLessThan) {
  TimeSpec ts1(100, 500);
  TimeSpec ts2(100, 501);
  TimeSpec ts3(101, 0);
  TimeSpec ts4(100, 500);

  EXPECT_TRUE(ts1 < ts2);
  EXPECT_TRUE(ts1 < ts3);
  EXPECT_FALSE(ts1 < ts4);
  EXPECT_FALSE(ts2 < ts1);
}

TEST(TimeUtilTest, TimeSpecGreaterThan) {
  TimeSpec ts1(100, 501);
  TimeSpec ts2(100, 500);
  TimeSpec ts3(99, 999);
  TimeSpec ts4(100, 501);

  EXPECT_TRUE(ts1 > ts2);
  EXPECT_TRUE(ts1 > ts3);
  EXPECT_FALSE(ts1 > ts4);
  EXPECT_FALSE(ts2 > ts1);
}

TEST(TimeUtilTest, TimeSpecStreamOperator) {
  TimeSpec ts(100, 500);
  std::ostringstream oss;
  oss << ts;
  EXPECT_EQ(oss.str(), "100.500");
}

TEST(TimeUtilTest, TimeNow) {
  TimeSpec before = TimeNow();
  TimeSpec after = TimeNow();

  EXPECT_GT(before.sec, 0);
  EXPECT_TRUE(before < after || before == after);
}

}  // namespace iutil
}  // namespace cache
}  // namespace dingofs
