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

#include "mds/common/context.h"

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

namespace dingofs {
namespace mds {
namespace unit_test {

class ContextTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(ContextTest, DefaultConstructor) {
  Context ctx;

  EXPECT_FALSE(ctx.IsBypassCache());
  EXPECT_FALSE(ctx.UseBaseVersion());
  EXPECT_EQ(ctx.GetInodeVersion(), 0);
  EXPECT_EQ(ctx.ClientId(), "");
  EXPECT_EQ(ctx.RequestId(), "");
  EXPECT_EQ(ctx.MethodName(), "");
  EXPECT_TRUE(ctx.GetAncestors().empty());
}

TEST_F(ContextTest, ConstructorWithRequestIdAndMethod) {
  Context ctx("req-123", "GetInode");

  EXPECT_FALSE(ctx.IsBypassCache());
  EXPECT_FALSE(ctx.UseBaseVersion());
  EXPECT_EQ(ctx.GetInodeVersion(), 0);
  EXPECT_EQ(ctx.ClientId(), "");
  EXPECT_EQ(ctx.RequestId(), "req-123");
  EXPECT_EQ(ctx.MethodName(), "GetInode");
  EXPECT_TRUE(ctx.GetAncestors().empty());
}

TEST_F(ContextTest, ConstructorWithEmptyStrings) {
  Context ctx("", "");

  EXPECT_EQ(ctx.RequestId(), "");
  EXPECT_EQ(ctx.MethodName(), "");
}

TEST_F(ContextTest, GetTrace) {
  Context ctx;

  Trace& trace = ctx.GetTrace();

  // Verify trace is usable
  trace.SetHitInode();
  EXPECT_TRUE(trace.GetCache().is_hit_inode);

  trace.RecordElapsedTime("test_point");
  EXPECT_EQ(trace.GetTime().elapsed_times.size(), 1);
}

TEST_F(ContextTest, GetTraceReference) {
  Context ctx("req-1", "TestMethod");

  // Get trace and modify it
  ctx.GetTrace().SetHitDentry();
  ctx.GetTrace().SetHitPartition();

  // Get trace again and verify modifications persist
  auto& cache = ctx.GetTrace().GetCache();
  EXPECT_TRUE(cache.is_hit_dentry);
  EXPECT_TRUE(cache.is_hit_partition);
}

}  // namespace unit_test
}  // namespace mds
}  // namespace dingofs
