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

#include "common/io_buffer.h"

#include <gtest/gtest.h>

#include <cstring>

namespace dingofs {

TEST(IOBufferTest, ConstructsFromRawDataAndReportsSize) {
  IOBuffer buf("hello", 5);
  EXPECT_EQ(buf.Size(), 5u);
  EXPECT_EQ(buf.Length(), 5u);
}

TEST(IOBufferTest, CopyToExtractsBytes) {
  IOBuffer buf("hello world", 11);
  char dest[11] = {};
  buf.CopyTo(dest, 11, 0);
  EXPECT_EQ(std::string(dest, 11), "hello world");
}

TEST(IOBufferTest, CopyToWithOffsetSkipsLeadingBytes) {
  IOBuffer buf("hello world", 11);
  char dest[5] = {};
  buf.CopyTo(dest, 5, 6);
  EXPECT_EQ(std::string(dest, 5), "world");
}

TEST(IOBufferTest, AppendToConcatenatesIntoAnotherBuffer) {
  IOBuffer a("foo", 3);
  IOBuffer b("bar", 3);
  size_t moved = a.AppendTo(&b);
  EXPECT_EQ(moved, 3u);
  EXPECT_EQ(b.Size(), 6u);
  char dest[6] = {};
  b.CopyTo(dest, 6);
  EXPECT_EQ(std::string(dest, 6), "barfoo");
}

TEST(IOBufferTest, AppendMergesAnotherIOBufferInPlace) {
  IOBuffer a("foo", 3);
  IOBuffer b("bar", 3);
  a.Append(&b);
  EXPECT_EQ(a.Size(), 6u);
  char dest[6] = {};
  a.CopyTo(dest, 6);
  EXPECT_EQ(std::string(dest, 6), "foobar");
}

TEST(IOBufferTest, PopFrontAndPopBackShrinkSize) {
  IOBuffer buf("0123456789", 10);
  buf.PopFront(2);
  buf.PopBack(3);
  EXPECT_EQ(buf.Size(), 5u);
  char dest[5] = {};
  buf.CopyTo(dest, 5);
  EXPECT_EQ(std::string(dest, 5), "23456");
}

TEST(IOBufferTest, CopyConstructorSharesEqualContent) {
  IOBuffer original("payload", 7);
  IOBuffer copy(original);
  EXPECT_EQ(copy.Size(), original.Size());
  char dest[7] = {};
  copy.CopyTo(dest, 7);
  EXPECT_EQ(std::string(dest, 7), "payload");
}

TEST(IOBufferTest, MoveConstructorTransfersContent) {
  IOBuffer original("payload", 7);
  IOBuffer moved(std::move(original));
  EXPECT_EQ(moved.Size(), 7u);
}

TEST(IOBufferTest, FetchReturnsBackingIovecsCoveringAllBytes) {
  IOBuffer buf("abc", 3);
  auto iovecs = buf.Fetch();
  size_t total = 0;
  for (const auto& iov : iovecs) total += iov.iov_len;
  EXPECT_EQ(total, 3u);
}

TEST(IOBufferTest, Fetch1RequiresSingleBackingBlock) {
  IOBuffer buf("abc", 3);
  ASSERT_EQ(buf.BackingBlockNum(), 1u);
  EXPECT_EQ(std::string(buf.Fetch1(), 3), "abc");
}

TEST(IOBufferTest, DescribeReportsSizeAndIsNonEmptyForNonEmptyBuffer) {
  IOBuffer buf("abc", 3);
  std::string desc = buf.Describe();
  EXPECT_NE(desc.find("size: 3"), std::string::npos);
}

TEST(IOBufferTest, DescribeOfEmptyBufferReportsEmptyMarker) {
  IOBuffer buf;
  EXPECT_EQ(buf.Describe(), "IOBuffer[]");
}

TEST(IOBufferTest, AppendUserDataInvokesDeleterOnDestruction) {
  bool deleted = false;
  auto* payload = new char[4];
  std::memcpy(payload, "data", 4);
  {
    IOBuffer buf;
    buf.AppendUserData(payload, 4, [&deleted](void* p) {
      deleted = true;
      delete[] static_cast<char*>(p);
    });
    EXPECT_EQ(buf.Size(), 4u);
  }
  EXPECT_TRUE(deleted);
}

TEST(IOBufferTest, AppendUserDataWithMetaRoundTripsMeta) {
  auto* payload = new char[4];
  std::memcpy(payload, "meta", 4);
  IOBuffer buf;
  buf.AppendUserDataWithMeta(payload, 4, [](void* p) { delete[] static_cast<char*>(p); },
                            0xABCD);
  EXPECT_EQ(buf.GetFirstDataMeta(), 0xABCDu);
}

}  // namespace dingofs
