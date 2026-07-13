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

#include <cstdint>
#include <cstring>

#include "utils/encode.h"

namespace dingofs {
namespace utils {
namespace unit_test {

TEST(EncodeTest, BigEndian64RoundTrip) {
  const uint64_t value = 0x0102030405060708ULL;
  char buf[8];

  EncodeBigEndian64(buf, value);

  // big-endian: most significant byte first
  EXPECT_EQ(static_cast<uint8_t>(buf[0]), 0x01);
  EXPECT_EQ(static_cast<uint8_t>(buf[7]), 0x08);
  EXPECT_EQ(DecodeBigEndian64(buf), value);
}

TEST(EncodeTest, BigEndian32RoundTrip) {
  const uint32_t value = 0x01020304U;
  char buf[4];

  EncodeBigEndian32(buf, value);

  EXPECT_EQ(static_cast<uint8_t>(buf[0]), 0x01);
  EXPECT_EQ(static_cast<uint8_t>(buf[3]), 0x04);
  EXPECT_EQ(DecodeBigEndian32(buf), value);
}

TEST(EncodeTest, LittleEndian64RoundTrip) {
  const uint64_t value = 0x0102030405060708ULL;
  char buf[8];

  EncodeLittleEndian64(buf, value);

  // little-endian: least significant byte first
  EXPECT_EQ(static_cast<uint8_t>(buf[0]), 0x08);
  EXPECT_EQ(static_cast<uint8_t>(buf[7]), 0x01);
  EXPECT_EQ(DecodeLittleEndian64(buf), value);
}

TEST(EncodeTest, LittleEndian32RoundTrip) {
  const uint32_t value = 0x01020304U;
  char buf[4];

  EncodeLittleEndian32(buf, value);

  EXPECT_EQ(static_cast<uint8_t>(buf[0]), 0x04);
  EXPECT_EQ(static_cast<uint8_t>(buf[3]), 0x01);
  EXPECT_EQ(DecodeLittleEndian32(buf), value);
}

TEST(EncodeTest, BigEndianAndLittleEndianAreByteReversed) {
  const uint64_t value = 0x1122334455667788ULL;
  char big[8];
  char little[8];

  EncodeBigEndian64(big, value);
  EncodeLittleEndian64(little, value);

  for (int i = 0; i < 8; ++i) {
    EXPECT_EQ(big[i], little[7 - i]) << "byte index " << i;
  }
}

TEST(EncodeTest, IsLittleEndianMatchesHostByteOrder) {
  // x86_64 test hosts are little-endian; verify via a known 16-bit pattern.
  uint16_t num = 0x0102;
  bool expect_little = reinterpret_cast<uint8_t*>(&num)[0] == 0x02;

  EXPECT_EQ(IsLittleEndian(), expect_little);
}

TEST(EncodeTest, NativeEndian64MatchesHostChoice) {
  const uint64_t value = 0xdeadbeefcafef00dULL;
  char native[8];
  char expected[8];

  EncodeNativeEndian64(native, value);
  if (IsLittleEndian()) {
    EncodeLittleEndian64(expected, value);
  } else {
    EncodeBigEndian64(expected, value);
  }

  EXPECT_EQ(memcmp(native, expected, 8), 0);
  EXPECT_EQ(DecodeNativeEndian64(native), value);
}

TEST(EncodeTest, NativeEndian32MatchesHostChoice) {
  const uint32_t value = 0xcafef00dU;
  char native[4];
  char expected[4];

  EncodeNativeEndian32(native, value);
  if (IsLittleEndian()) {
    EncodeLittleEndian32(expected, value);
  } else {
    EncodeBigEndian32(expected, value);
  }

  EXPECT_EQ(memcmp(native, expected, 4), 0);
  EXPECT_EQ(DecodeNativeEndian32(native), value);
}

TEST(EncodeTest, ZeroAndMaxValues) {
  char buf[8];

  EncodeBigEndian64(buf, 0);
  EXPECT_EQ(DecodeBigEndian64(buf), 0U);

  EncodeBigEndian64(buf, UINT64_MAX);
  EXPECT_EQ(DecodeBigEndian64(buf), UINT64_MAX);

  char buf32[4];
  EncodeLittleEndian32(buf32, 0);
  EXPECT_EQ(DecodeLittleEndian32(buf32), 0U);

  EncodeLittleEndian32(buf32, UINT32_MAX);
  EXPECT_EQ(DecodeLittleEndian32(buf32), UINT32_MAX);
}

}  // namespace unit_test
}  // namespace utils
}  // namespace dingofs
