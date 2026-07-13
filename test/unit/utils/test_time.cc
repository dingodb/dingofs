// Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <sstream>
#include <thread>  // NOLINT

#include "utils/time.h"

namespace dingofs {
namespace utils {
namespace unit_test {

TEST(TimeTest, TimestampsAreMonotonicAndConsistentAcrossUnits) {
  uint64_t s = Timestamp();
  uint64_t ms = TimestampMs();
  uint64_t us = TimestampUs();
  uint64_t ns = TimestampNs();

  // coarser units should be consistent with finer ones, modulo the small
  // window between successive calls.
  EXPECT_NEAR(static_cast<double>(ms), static_cast<double>(s) * 1000, 2000);
  EXPECT_NEAR(static_cast<double>(us), static_cast<double>(ms) * 1000, 2000000);
  EXPECT_NEAR(static_cast<double>(ns), static_cast<double>(us) * 1000,
              2000000000);

  uint64_t ns2 = TimestampNs();
  EXPECT_GE(ns2, ns);
}

TEST(TimeTest, FormatTimeProducesExpectedLayout) {
  // 2021-01-01 00:00:00 UTC
  const int64_t ts = 1609459200;
  std::string formatted = FormatTime(ts, "%Y-%m-%d");
  EXPECT_EQ(formatted.size(), 10U);  // YYYY-MM-DD

  std::string default_formatted = FormatTime(ts);
  EXPECT_EQ(default_formatted.size(), 19U);  // YYYY-MM-DD HH:MM:SS
}

TEST(TimeTest, FormatMsTimeAppendsMillisecondSuffix) {
  const int64_t ts_ms = 1609459200123;
  std::string formatted = FormatMsTime(ts_ms);

  // "YYYY-MM-DD HH:MM:SS.mmm"
  EXPECT_NE(formatted.find(".123"), std::string::npos);
}

TEST(TimeTest, FormatNsTimeIncludesNanosecondRemainder) {
  const int64_t ts_ns = 1609459200123456789LL;
  std::string formatted = FormatNsTime(ts_ns);

  EXPECT_NE(formatted.find(".123456789"), std::string::npos);
}

TEST(TimeTest, GetNowFormatMsTimeMatchesIso8601Layout) {
  std::string now = GetNowFormatMsTime();

  // "YYYY-MM-DDTHH:MM:SS.000Z"
  EXPECT_EQ(now.size(), 24U);
  EXPECT_EQ(now[10], 'T');
  EXPECT_EQ(now.substr(20), "000Z");
}

TEST(TimeTest, NowTimeMatchesDefaultFormat) {
  std::string now = NowTime();

  // FormatMsTime always appends ".<ms>" regardless of the format string, so
  // NowTime()'s output is "YYYY-MM-DD HH:MM:SS" (19 chars) plus a variable
  // width millisecond suffix.
  ASSERT_GT(now.size(), 20U);
  EXPECT_EQ(now[4], '-');
  EXPECT_EQ(now[7], '-');
  EXPECT_EQ(now[10], ' ');
  EXPECT_EQ(now[13], ':');
  EXPECT_EQ(now[16], ':');
  EXPECT_EQ(now[19], '.');
}

TEST(TimeTest, TimeSpecComparisonOperators) {
  TimeSpec a(10, 500);
  TimeSpec b(10, 600);
  TimeSpec c(11, 0);
  TimeSpec d(10, 500);

  EXPECT_TRUE(a < b);
  EXPECT_TRUE(b < c);
  EXPECT_TRUE(c > a);
  EXPECT_TRUE(a == d);
  EXPECT_TRUE(a != b);
  EXPECT_FALSE(a == b);
}

TEST(TimeTest, TimeSpecAdditionSumsFields) {
  TimeSpec a(1, 200);
  TimeSpec b(2, 300);

  TimeSpec sum = a + b;
  EXPECT_EQ(sum.seconds, 3U);
  EXPECT_EQ(sum.nanoSeconds, 500U);
}

TEST(TimeTest, TimeSpecStreamOperator) {
  TimeSpec t(5, 10);
  std::ostringstream oss;
  oss << t;
  EXPECT_EQ(oss.str(), "5.10");
}

TEST(TimeTest, TimeNowReturnsPlausibleWallClock) {
  TimeSpec t = TimeNow();
  // Should be after 2020-01-01 and a valid nanosecond fraction.
  EXPECT_GT(t.seconds, 1577836800U);
  EXPECT_LT(t.nanoSeconds, 1000000000U);
}

TEST(TimeTest, DurationTracksElapsedTime) {
  Duration duration;
  EXPECT_GE(duration.StartMs(), 0);

  std::this_thread::sleep_for(std::chrono::milliseconds(20));

  EXPECT_GE(duration.ElapsedMs(), 15);
  EXPECT_GE(duration.ElapsedUs(), duration.ElapsedMs() * 1000 - 5000);
  EXPECT_GE(duration.ElapsedNs(), 0);
  EXPECT_GE(duration.ElapsedS(), 0);
}

TEST(TimeTest, DurationResetRestartsClock) {
  Duration duration;
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  int64_t before_reset = duration.ElapsedMs();
  ASSERT_GE(before_reset, 15);

  duration.Reset();
  EXPECT_LT(duration.ElapsedMs(), before_reset);
}

}  // namespace unit_test
}  // namespace utils
}  // namespace dingofs
