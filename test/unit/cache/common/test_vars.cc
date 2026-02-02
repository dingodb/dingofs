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

#include <gtest/gtest.h>

#include "cache/common/vars.h"

namespace dingofs {
namespace cache {

class VarsTest : public ::testing::Test {};

TEST_F(VarsTest, PerSecondVarCreation) {
  PerSecondVar var("test_op", "count");

  // Initial value should be 0
  EXPECT_EQ(var.total_count.get_value(), 0);
}

TEST_F(VarsTest, PerSecondVarIncrement) {
  PerSecondVar var("test_op2", "items");

  var.total_count << 1;
  EXPECT_EQ(var.total_count.get_value(), 1);

  var.total_count << 5;
  EXPECT_EQ(var.total_count.get_value(), 6);

  var.total_count << 10;
  EXPECT_EQ(var.total_count.get_value(), 16);
}

TEST_F(VarsTest, OpVarCreation) {
  OpVar var("test_cache_op");

  // Initial values should be 0
  EXPECT_EQ(var.op_per_second.total_count.get_value(), 0);
  EXPECT_EQ(var.bandwidth_per_second.total_count.get_value(), 0);
  EXPECT_EQ(var.error_per_second.total_count.get_value(), 0);
  EXPECT_EQ(var.total_latency.get_value(), 0);
}

TEST_F(VarsTest, OpVarRecordOp) {
  OpVar var("test_cache_op2");

  var.op_per_second.total_count << 1;
  EXPECT_EQ(var.op_per_second.total_count.get_value(), 1);

  var.op_per_second.total_count << 1;
  var.op_per_second.total_count << 1;
  EXPECT_EQ(var.op_per_second.total_count.get_value(), 3);
}

TEST_F(VarsTest, OpVarRecordBandwidth) {
  OpVar var("test_cache_op3");

  var.bandwidth_per_second.total_count << 1024;
  EXPECT_EQ(var.bandwidth_per_second.total_count.get_value(), 1024);

  var.bandwidth_per_second.total_count << 2048;
  EXPECT_EQ(var.bandwidth_per_second.total_count.get_value(), 3072);
}

TEST_F(VarsTest, OpVarRecordError) {
  OpVar var("test_cache_op4");

  var.error_per_second.total_count << 1;
  EXPECT_EQ(var.error_per_second.total_count.get_value(), 1);

  var.error_per_second.total_count << 1;
  EXPECT_EQ(var.error_per_second.total_count.get_value(), 2);
}

TEST_F(VarsTest, OpVarRecordLatency) {
  OpVar var("test_cache_op5");

  var.latency << 100;  // 100us
  var.latency << 200;  // 200us
  var.latency << 300;  // 300us

  // The latency recorder should have recorded 3 samples
  EXPECT_EQ(var.latency.count(), 3);
}

TEST_F(VarsTest, OpVarTotalLatency) {
  OpVar var("test_cache_op6");

  var.total_latency << 100;
  var.total_latency << 200;
  var.total_latency << 300;

  EXPECT_EQ(var.total_latency.get_value(), 600);
}

TEST_F(VarsTest, MultipleOpVars) {
  OpVar read_var("test_read_op");
  OpVar write_var("test_write_op");

  read_var.op_per_second.total_count << 10;
  write_var.op_per_second.total_count << 5;

  EXPECT_EQ(read_var.op_per_second.total_count.get_value(), 10);
  EXPECT_EQ(write_var.op_per_second.total_count.get_value(), 5);

  // They should be independent
  read_var.op_per_second.total_count << 5;
  EXPECT_EQ(read_var.op_per_second.total_count.get_value(), 15);
  EXPECT_EQ(write_var.op_per_second.total_count.get_value(), 5);
}

}  // namespace cache
}  // namespace dingofs
