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
 * Created Date: 2026-02-02
 * Author: AI
 */

#include <gtest/gtest.h>

#include <string>

#include "cache/common/context.h"

namespace dingofs {
namespace cache {

class ContextTest : public ::testing::Test {};

TEST_F(ContextTest, DefaultConstructor) {
  Context ctx;
  EXPECT_FALSE(ctx.TraceId().empty());
  EXPECT_FALSE(ctx.GetCacheHit());
}

TEST_F(ContextTest, ConstructorWithTraceId) {
  std::string trace_id = "custom-trace-id-12345";
  Context ctx(trace_id);
  EXPECT_EQ(ctx.TraceId(), trace_id);
  EXPECT_FALSE(ctx.GetCacheHit());
}

TEST_F(ContextTest, SetCacheHit) {
  Context ctx;
  EXPECT_FALSE(ctx.GetCacheHit());

  ctx.SetCacheHit(true);
  EXPECT_TRUE(ctx.GetCacheHit());

  ctx.SetCacheHit(false);
  EXPECT_FALSE(ctx.GetCacheHit());
}

TEST_F(ContextTest, NewContext) {
  auto ctx = NewContext();
  EXPECT_NE(ctx, nullptr);
  EXPECT_FALSE(ctx->TraceId().empty());
}

TEST_F(ContextTest, NewContextWithTraceId) {
  std::string trace_id = "test-trace-id";
  auto ctx = NewContext(trace_id);
  EXPECT_NE(ctx, nullptr);
  EXPECT_EQ(ctx->TraceId(), trace_id);
}

TEST_F(ContextTest, UniqueTraceId) {
  Context ctx1;
  Context ctx2;
  EXPECT_NE(ctx1.TraceId(), ctx2.TraceId());
}

}  // namespace cache
}  // namespace dingofs
