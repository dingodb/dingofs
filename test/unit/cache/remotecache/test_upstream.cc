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

#include "cache/remotecache/upstream.h"

namespace dingofs {
namespace cache {

class UpstreamVarsCollectorTest : public ::testing::Test {};

TEST_F(UpstreamVarsCollectorTest, PrefixValue) {
  EXPECT_EQ(UpstreamVarsCollector::prefix, "dingofs_remote_node_group");
}

TEST_F(UpstreamVarsCollectorTest, OpVarsCreation) {
  UpstreamVarsCollector vars;

  // All OpVars should be initialized with 0
  EXPECT_EQ(vars.op_put.op_per_second.total_count.get_value(), 0);
  EXPECT_EQ(vars.op_range.op_per_second.total_count.get_value(), 0);
  EXPECT_EQ(vars.op_cache.op_per_second.total_count.get_value(), 0);
  EXPECT_EQ(vars.op_prefetch.op_per_second.total_count.get_value(), 0);
}

TEST_F(UpstreamVarsCollectorTest, RecordPutOp) {
  UpstreamVarsCollector vars;

  vars.op_put.op_per_second.total_count << 1;
  vars.op_put.bandwidth_per_second.total_count << 1024;

  EXPECT_EQ(vars.op_put.op_per_second.total_count.get_value(), 1);
  EXPECT_EQ(vars.op_put.bandwidth_per_second.total_count.get_value(), 1024);
}

TEST_F(UpstreamVarsCollectorTest, RecordRangeOp) {
  UpstreamVarsCollector vars;

  vars.op_range.op_per_second.total_count << 5;
  vars.op_range.bandwidth_per_second.total_count << 4096;

  EXPECT_EQ(vars.op_range.op_per_second.total_count.get_value(), 5);
  EXPECT_EQ(vars.op_range.bandwidth_per_second.total_count.get_value(), 4096);
}

TEST_F(UpstreamVarsCollectorTest, RecordCacheOp) {
  UpstreamVarsCollector vars;

  vars.op_cache.op_per_second.total_count << 3;
  vars.op_cache.error_per_second.total_count << 1;

  EXPECT_EQ(vars.op_cache.op_per_second.total_count.get_value(), 3);
  EXPECT_EQ(vars.op_cache.error_per_second.total_count.get_value(), 1);
}

TEST_F(UpstreamVarsCollectorTest, RecordPrefetchOp) {
  UpstreamVarsCollector vars;

  vars.op_prefetch.op_per_second.total_count << 10;
  vars.op_prefetch.total_latency << 500;

  EXPECT_EQ(vars.op_prefetch.op_per_second.total_count.get_value(), 10);
  EXPECT_EQ(vars.op_prefetch.total_latency.get_value(), 500);
}

TEST_F(UpstreamVarsCollectorTest, RecordGuardSuccess) {
  UpstreamVarsCollector vars;
  Status status = Status::OK();

  {
    UpstreamVarsRecordGuard guard("Put", 1024, status, &vars);
  }

  EXPECT_EQ(vars.op_put.op_per_second.total_count.get_value(), 1);
  EXPECT_EQ(vars.op_put.bandwidth_per_second.total_count.get_value(), 1024);
  EXPECT_EQ(vars.op_put.error_per_second.total_count.get_value(), 0);
}

TEST_F(UpstreamVarsCollectorTest, RecordGuardError) {
  UpstreamVarsCollector vars;
  Status status = Status::Internal("error");

  {
    UpstreamVarsRecordGuard guard("Range", 2048, status, &vars);
  }

  EXPECT_EQ(vars.op_range.op_per_second.total_count.get_value(), 0);
  EXPECT_EQ(vars.op_range.bandwidth_per_second.total_count.get_value(), 0);
  EXPECT_EQ(vars.op_range.error_per_second.total_count.get_value(), 1);
}

TEST_F(UpstreamVarsCollectorTest, RecordGuardCache) {
  UpstreamVarsCollector vars;
  Status status = Status::OK();

  {
    UpstreamVarsRecordGuard guard("Cache", 4096, status, &vars);
  }

  EXPECT_EQ(vars.op_cache.op_per_second.total_count.get_value(), 1);
  EXPECT_EQ(vars.op_cache.bandwidth_per_second.total_count.get_value(), 4096);
}

TEST_F(UpstreamVarsCollectorTest, RecordGuardPrefetch) {
  UpstreamVarsCollector vars;
  Status status = Status::OK();

  {
    UpstreamVarsRecordGuard guard("Prefetch", 8192, status, &vars);
  }

  EXPECT_EQ(vars.op_prefetch.op_per_second.total_count.get_value(), 1);
  EXPECT_EQ(vars.op_prefetch.bandwidth_per_second.total_count.get_value(),
            8192);
}

TEST_F(UpstreamVarsCollectorTest, MultipleOperations) {
  UpstreamVarsCollector vars;
  Status success = Status::OK();
  Status failure = Status::Internal("error");

  for (int i = 0; i < 10; ++i) {
    UpstreamVarsRecordGuard guard("Put", 1024, success, &vars);
  }

  for (int i = 0; i < 3; ++i) {
    UpstreamVarsRecordGuard guard("Put", 1024, failure, &vars);
  }

  EXPECT_EQ(vars.op_put.op_per_second.total_count.get_value(), 10);
  EXPECT_EQ(vars.op_put.bandwidth_per_second.total_count.get_value(), 10240);
  EXPECT_EQ(vars.op_put.error_per_second.total_count.get_value(), 3);
}

}  // namespace cache
}  // namespace dingofs
