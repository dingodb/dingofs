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

#include "utils/string.h"

namespace dingofs {
namespace utils {
namespace unit_test {

TEST(StringTest, Str2IntUint64) {
  uint64_t num = 0;

  EXPECT_TRUE(Str2Int("12345", &num));
  EXPECT_EQ(num, 12345U);

  EXPECT_FALSE(Str2Int("not_a_number", &num));
  EXPECT_FALSE(Str2Int("999999999999999999999999999999", &num));  // overflow
}

TEST(StringTest, Str2IntUint32) {
  uint32_t num = 0;

  EXPECT_TRUE(Str2Int("42", &num));
  EXPECT_EQ(num, 42U);

  EXPECT_FALSE(Str2Int("abc", &num));
}

TEST(StringTest, Strs2IntsSucceedsWhenSizesMatch) {
  uint64_t a = 0, b = 0;
  std::vector<std::string> strs{"1", "2"};
  std::vector<uint64_t*> nums{&a, &b};

  EXPECT_TRUE(Strs2Ints(strs, nums));
  EXPECT_EQ(a, 1U);
  EXPECT_EQ(b, 2U);
}

TEST(StringTest, Strs2IntsFailsOnSizeMismatch) {
  uint64_t a = 0;
  std::vector<std::string> strs{"1", "2"};
  std::vector<uint64_t*> nums{&a};

  EXPECT_FALSE(Strs2Ints(strs, nums));
}

TEST(StringTest, Strs2IntsFailsOnInvalidElement) {
  uint64_t a = 0, b = 0;
  std::vector<std::string> strs{"1", "bad"};
  std::vector<uint64_t*> nums{&a, &b};

  EXPECT_FALSE(Strs2Ints(strs, nums));
}

TEST(StringTest, TrimSpaceRemovesLeadingAndTrailingWhitespace) {
  EXPECT_EQ(TrimSpace("  hello world  "), "hello world");
  EXPECT_EQ(TrimSpace("\t\nhello\n\t"), "hello");
  EXPECT_EQ(TrimSpace("no_trim"), "no_trim");
  EXPECT_EQ(TrimSpace("   "), "");
  EXPECT_EQ(TrimSpace(""), "");
}

TEST(StringTest, TrimAsciiWhitespaceRemovesAllInternalWhitespace) {
  EXPECT_EQ(TrimAsciiWhitespace("a b\nc\rd"), "abcd");
  EXPECT_EQ(TrimAsciiWhitespace("no_ws"), "no_ws");
  EXPECT_EQ(TrimAsciiWhitespace(""), "");
}

TEST(StringTest, TrimCharsTrimsGivenCharSet) {
  EXPECT_EQ(TrimChars("xxhelloxx", "x"), "hello");
  EXPECT_EQ(TrimChars("--a-b--", "-"), "a-b");
  EXPECT_EQ(TrimChars("xxxx", "x"), "");
}

TEST(StringTest, HexCharToIntConvertsAllValidRanges) {
  EXPECT_EQ(HexCharToInt('0'), 0);
  EXPECT_EQ(HexCharToInt('9'), 9);
  EXPECT_EQ(HexCharToInt('A'), 10);
  EXPECT_EQ(HexCharToInt('F'), 15);
  EXPECT_EQ(HexCharToInt('a'), 10);
  EXPECT_EQ(HexCharToInt('f'), 15);
  EXPECT_EQ(HexCharToInt('g'), -1);
}

TEST(StringTest, HexStringToBufRoundTripsWithBufToHexString) {
  unsigned char buf[4] = {0};

  int n = HexStringToBuf("deadbeef", buf, sizeof(buf));
  ASSERT_EQ(n, 4);
  EXPECT_EQ(buf[0], 0xde);
  EXPECT_EQ(buf[3], 0xef);
  EXPECT_EQ(BufToHexString(buf, 4), "deadbeef");
}

TEST(StringTest, HexStringToBufRejectsOddLength) {
  unsigned char buf[4] = {0};
  EXPECT_EQ(HexStringToBuf("abc", buf, sizeof(buf)), -1);
}

TEST(StringTest, HexStringToBufRejectsBufferTooSmall) {
  unsigned char buf[1] = {0};
  EXPECT_EQ(HexStringToBuf("deadbeef", buf, sizeof(buf)), -1);
}

TEST(StringTest, HexStringToBufRejectsInvalidHexChars) {
  unsigned char buf[4] = {0};
  EXPECT_EQ(HexStringToBuf("zzzz", buf, sizeof(buf)), -1);
}

}  // namespace unit_test
}  // namespace utils
}  // namespace dingofs
