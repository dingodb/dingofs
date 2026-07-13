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

#include "common/string_slice.h"

#include <gtest/gtest.h>

namespace dingofs {

TEST(StringSliceTest, ConstructsFromStdStringAndCString) {
  std::string s = "hello";
  StringSlice from_string(s);
  StringSlice from_cstr("hello");
  EXPECT_EQ(from_string, from_cstr);
  EXPECT_EQ(from_string.ToString(), "hello");
}

TEST(StringSliceTest, EmptySliceHasZeroSize) {
  StringSlice empty;
  EXPECT_TRUE(empty.empty());
  EXPECT_EQ(empty.size(), 0u);
}

TEST(StringSliceTest, NullCStringIsTreatedAsEmpty) {
  StringSlice s(static_cast<const char*>(nullptr));
  EXPECT_TRUE(s.empty());
}

TEST(StringSliceTest, StartsWithAndEndsWith) {
  StringSlice s("hello world");
  EXPECT_TRUE(s.starts_with("hello"));
  EXPECT_TRUE(s.ends_with("world"));
  EXPECT_FALSE(s.starts_with("world"));
}

TEST(StringSliceTest, CompareOrdersLexicographically) {
  EXPECT_LT(StringSlice("abc").compare(StringSlice("abd")), 0);
  EXPECT_GT(StringSlice("abd").compare(StringSlice("abc")), 0);
  EXPECT_EQ(StringSlice("abc").compare(StringSlice("abc")), 0);
  // Shared prefix: the shorter slice sorts first.
  EXPECT_LT(StringSlice("ab").compare(StringSlice("abc")), 0);
}

TEST(StringSliceTest, DifferenceOffsetFindsFirstMismatch) {
  EXPECT_EQ(StringSlice("abcdef").difference_offset(StringSlice("abcXef")), 3u);
  EXPECT_EQ(StringSlice("abc").difference_offset(StringSlice("abc")), 3u);
}

TEST(StringSliceTest, RemovePrefixAndSuffix) {
  StringSlice s("hello world");
  s.remove_prefix(6);
  EXPECT_EQ(s.ToString(), "world");
  s.remove_suffix(2);
  EXPECT_EQ(s.ToString(), "wor");
}

TEST(StringSliceTest, ToStringHexRoundTripsThroughDecodeHex) {
  StringSlice s("Az9");
  std::string hex = s.ToString(/*hex=*/true);
  EXPECT_EQ(hex, "417A39");  // 'A'=0x41 'z'=0x7A '9'=0x39

  StringSlice hex_slice(hex);
  std::string decoded;
  ASSERT_TRUE(hex_slice.DecodeHex(&decoded));
  EXPECT_EQ(decoded, "Az9");
}

TEST(StringSliceTest, DecodeHexAcceptsLowercase) {
  std::string decoded;
  ASSERT_TRUE(StringSlice("61").DecodeHex(&decoded));
  EXPECT_EQ(decoded, "a");
}

TEST(StringSliceTest, DecodeHexRejectsOddLength) {
  std::string decoded;
  EXPECT_FALSE(StringSlice("abc").DecodeHex(&decoded));
}

TEST(StringSliceTest, DecodeHexRejectsInvalidCharacters) {
  std::string decoded;
  EXPECT_FALSE(StringSlice("zz").DecodeHex(&decoded));
  EXPECT_FALSE(StringSlice("gg").DecodeHex(&decoded));
}

TEST(StringSliceTest, DecodeHexOfEmptySliceSucceedsWithEmptyResult) {
  std::string decoded = "leftover";
  ASSERT_TRUE(StringSlice("").DecodeHex(&decoded));
  EXPECT_TRUE(decoded.empty());
}

TEST(StringSliceTest, PartsConcatenatesIntoBuffer) {
  StringSlice parts[] = {StringSlice("foo"), StringSlice("bar")};
  StringSliceParts sp(parts, 2);
  std::string buf;
  StringSlice combined(sp, &buf);
  EXPECT_EQ(combined.ToString(), "foobar");
}

}  // namespace dingofs
