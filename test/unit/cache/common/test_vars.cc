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
 * Created Date: 2026-06-21
 * Author: AI
 */

#include <gtest/gtest.h>

#include "cache/common/vars.h"

namespace dingofs {
namespace cache {

// Each test uses a unique opname so bvar's global registry does not collide.

TEST(VarsTest, PerSecondVarCounts) {
  PerSecondVar var("dingofs_test_vars_persecond", "error");
  EXPECT_EQ(var.total_count.get_value(), 0u);

  var.total_count << 5;
  var.total_count << 3;
  EXPECT_EQ(var.total_count.get_value(), 8u);
}

TEST(VarsTest, OpVarAggregates) {
  OpVar op("dingofs_test_vars_opvar");

  op.op_per_second.total_count << 1;
  op.bandwidth_per_second.total_count << 4096;
  op.error_per_second.total_count << 2;
  op.latency << 100;  // record a 100us latency sample
  op.total_latency << 100;

  EXPECT_EQ(op.op_per_second.total_count.get_value(), 1u);
  EXPECT_EQ(op.bandwidth_per_second.total_count.get_value(), 4096u);
  EXPECT_EQ(op.error_per_second.total_count.get_value(), 2u);
  EXPECT_EQ(op.total_latency.get_value(), 100u);
  EXPECT_EQ(op.latency.count(), 1);
}

}  // namespace cache
}  // namespace dingofs
