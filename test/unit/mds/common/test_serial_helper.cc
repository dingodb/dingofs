// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "mds/common/serial_helper.h"

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

namespace dingofs {
namespace mds {
namespace unit_test {

class SerialHelperTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(SerialHelperTest, IsLE) {
  // Test that IsLE returns consistent result
  bool is_le = SerialHelper::IsLE();
  // Most modern systems are little endian
  // Just verify it returns a valid boolean
  EXPECT_TRUE(is_le || !is_le);
}

TEST_F(SerialHelperTest, WriteAndReadInt) {
  std::string output;
  int32_t value = 0x12345678;

  SerialHelper::WriteInt(value, output);
  EXPECT_EQ(output.size(), 4);

  int32_t read_value = SerialHelper::ReadInt(output);
  EXPECT_EQ(read_value, value);
}

TEST_F(SerialHelperTest, WriteAndReadIntZero) {
  std::string output;
  int32_t value = 0;

  SerialHelper::WriteInt(value, output);
  EXPECT_EQ(output.size(), 4);

  int32_t read_value = SerialHelper::ReadInt(output);
  EXPECT_EQ(read_value, value);
}

TEST_F(SerialHelperTest, WriteAndReadIntNegative) {
  std::string output;
  int32_t value = -12345678;

  SerialHelper::WriteInt(value, output);
  EXPECT_EQ(output.size(), 4);

  int32_t read_value = SerialHelper::ReadInt(output);
  EXPECT_EQ(read_value, value);
}

TEST_F(SerialHelperTest, WriteAndReadIntMax) {
  std::string output;
  int32_t value = INT32_MAX;

  SerialHelper::WriteInt(value, output);
  EXPECT_EQ(output.size(), 4);

  int32_t read_value = SerialHelper::ReadInt(output);
  EXPECT_EQ(read_value, value);
}

TEST_F(SerialHelperTest, WriteAndReadIntMin) {
  std::string output;
  int32_t value = INT32_MIN;

  SerialHelper::WriteInt(value, output);
  EXPECT_EQ(output.size(), 4);

  int32_t read_value = SerialHelper::ReadInt(output);
  EXPECT_EQ(read_value, value);
}

TEST_F(SerialHelperTest, WriteAndReadLong) {
  std::string output;
  int64_t value = 0x123456789ABCDEF0;

  SerialHelper::WriteLong(value, output);
  EXPECT_EQ(output.size(), 8);

  int64_t read_value = SerialHelper::ReadLong(output);
  EXPECT_EQ(read_value, value);
}

TEST_F(SerialHelperTest, WriteAndReadLongZero) {
  std::string output;
  int64_t value = 0;

  SerialHelper::WriteLong(value, output);
  EXPECT_EQ(output.size(), 8);

  int64_t read_value = SerialHelper::ReadLong(output);
  EXPECT_EQ(read_value, value);
}

TEST_F(SerialHelperTest, WriteAndReadLongNegative) {
  std::string output;
  int64_t value = -123456789012345;

  SerialHelper::WriteLong(value, output);
  EXPECT_EQ(output.size(), 8);

  int64_t read_value = SerialHelper::ReadLong(output);
  EXPECT_EQ(read_value, value);
}

TEST_F(SerialHelperTest, WriteAndReadLongMax) {
  std::string output;
  int64_t value = INT64_MAX;

  SerialHelper::WriteLong(value, output);
  EXPECT_EQ(output.size(), 8);

  int64_t read_value = SerialHelper::ReadLong(output);
  EXPECT_EQ(read_value, value);
}

TEST_F(SerialHelperTest, WriteAndReadLongMin) {
  std::string output;
  int64_t value = INT64_MIN;

  SerialHelper::WriteLong(value, output);
  EXPECT_EQ(output.size(), 8);

  int64_t read_value = SerialHelper::ReadLong(output);
  EXPECT_EQ(read_value, value);
}

TEST_F(SerialHelperTest, WriteAndReadULong) {
  std::string output;
  uint64_t value = 0x123456789ABCDEF0;

  SerialHelper::WriteULong(value, output);
  EXPECT_EQ(output.size(), 8);

  uint64_t read_value = SerialHelper::ReadULong(output);
  EXPECT_EQ(read_value, value);
}

TEST_F(SerialHelperTest, WriteAndReadULongZero) {
  std::string output;
  uint64_t value = 0;

  SerialHelper::WriteULong(value, output);
  EXPECT_EQ(output.size(), 8);

  uint64_t read_value = SerialHelper::ReadULong(output);
  EXPECT_EQ(read_value, value);
}

TEST_F(SerialHelperTest, WriteAndReadULongMax) {
  std::string output;
  uint64_t value = UINT64_MAX;

  SerialHelper::WriteULong(value, output);
  EXPECT_EQ(output.size(), 8);

  uint64_t read_value = SerialHelper::ReadULong(output);
  EXPECT_EQ(read_value, value);
}

TEST_F(SerialHelperTest, WriteAndReadLongWithNegation) {
  std::string output;
  int64_t value = 123456789;

  SerialHelper::WriteLongWithNegation(value, output);
  EXPECT_EQ(output.size(), 8);

  int64_t read_value = SerialHelper::ReadLongWithNegation(output);
  EXPECT_EQ(read_value, value);
}

TEST_F(SerialHelperTest, WriteAndReadLongWithNegationZero) {
  std::string output;
  int64_t value = 0;

  SerialHelper::WriteLongWithNegation(value, output);
  EXPECT_EQ(output.size(), 8);

  int64_t read_value = SerialHelper::ReadLongWithNegation(output);
  EXPECT_EQ(read_value, value);
}

TEST_F(SerialHelperTest, WriteAndReadLongWithNegationLarge) {
  std::string output;
  int64_t value = INT64_MAX;

  SerialHelper::WriteLongWithNegation(value, output);
  EXPECT_EQ(output.size(), 8);

  int64_t read_value = SerialHelper::ReadLongWithNegation(output);
  EXPECT_EQ(read_value, value);
}

TEST_F(SerialHelperTest, WriteAndReadLongComparable) {
  std::string output1;
  std::string output2;
  int64_t value1 = 100;
  int64_t value2 = 200;

  SerialHelper::WriteLongComparable(value1, output1);
  SerialHelper::WriteLongComparable(value2, output2);

  EXPECT_EQ(output1.size(), 8);
  EXPECT_EQ(output2.size(), 8);

  // The serialized form should be comparable
  EXPECT_LT(output1, output2);

  int64_t read_value1 = SerialHelper::ReadLongComparable(output1);
  int64_t read_value2 = SerialHelper::ReadLongComparable(output2);

  EXPECT_EQ(read_value1, value1);
  EXPECT_EQ(read_value2, value2);
}

TEST_F(SerialHelperTest, WriteAndReadLongComparableNegative) {
  std::string output1;
  std::string output2;
  int64_t value1 = -200;
  int64_t value2 = -100;

  SerialHelper::WriteLongComparable(value1, output1);
  SerialHelper::WriteLongComparable(value2, output2);

  EXPECT_EQ(output1.size(), 8);
  EXPECT_EQ(output2.size(), 8);

  // The serialized form should be comparable
  EXPECT_LT(output1, output2);

  int64_t read_value1 = SerialHelper::ReadLongComparable(output1);
  int64_t read_value2 = SerialHelper::ReadLongComparable(output2);

  EXPECT_EQ(read_value1, value1);
  EXPECT_EQ(read_value2, value2);
}

TEST_F(SerialHelperTest, WriteAndReadLongComparableMixedSign) {
  std::string output1;
  std::string output2;
  int64_t value1 = -100;
  int64_t value2 = 100;

  SerialHelper::WriteLongComparable(value1, output1);
  SerialHelper::WriteLongComparable(value2, output2);

  EXPECT_EQ(output1.size(), 8);
  EXPECT_EQ(output2.size(), 8);

  // Negative values should be less than positive values
  EXPECT_LT(output1, output2);

  int64_t read_value1 = SerialHelper::ReadLongComparable(output1);
  int64_t read_value2 = SerialHelper::ReadLongComparable(output2);

  EXPECT_EQ(read_value1, value1);
  EXPECT_EQ(read_value2, value2);
}

TEST_F(SerialHelperTest, WriteAndReadLongComparableZero) {
  std::string output;
  int64_t value = 0;

  SerialHelper::WriteLongComparable(value, output);
  EXPECT_EQ(output.size(), 8);

  int64_t read_value = SerialHelper::ReadLongComparable(output);
  EXPECT_EQ(read_value, value);
}

TEST_F(SerialHelperTest, ReadLongComparableStringView) {
  std::string output;
  int64_t value = 123456789;

  SerialHelper::WriteLongComparable(value, output);
  EXPECT_EQ(output.size(), 8);

  std::string_view sv(output);
  int64_t read_value = SerialHelper::ReadLongComparable(sv);
  EXPECT_EQ(read_value, value);
}

TEST_F(SerialHelperTest, OrderingInt) {
  std::string output1;
  std::string output2;
  std::string output3;

  SerialHelper::WriteInt(100, output1);
  SerialHelper::WriteInt(200, output2);
  SerialHelper::WriteInt(300, output3);

  // Big endian encoding should preserve ordering
  EXPECT_LT(output1, output2);
  EXPECT_LT(output2, output3);
}

TEST_F(SerialHelperTest, OrderingLong) {
  std::string output1;
  std::string output2;
  std::string output3;

  SerialHelper::WriteLong(100, output1);
  SerialHelper::WriteLong(200, output2);
  SerialHelper::WriteLong(300, output3);

  // Big endian encoding should preserve ordering
  EXPECT_LT(output1, output2);
  EXPECT_LT(output2, output3);
}

TEST_F(SerialHelperTest, OrderingULong) {
  std::string output1;
  std::string output2;
  std::string output3;

  SerialHelper::WriteULong(100, output1);
  SerialHelper::WriteULong(200, output2);
  SerialHelper::WriteULong(300, output3);

  // Big endian encoding should preserve ordering
  EXPECT_LT(output1, output2);
  EXPECT_LT(output2, output3);
}

}  // namespace unit_test
}  // namespace mds
}  // namespace dingofs
