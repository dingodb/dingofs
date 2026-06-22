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

#include <algorithm>
#include <cctype>

#include "cache/common/context.h"

namespace dingofs {
namespace cache {

TEST(ContextTest, DefaultTraceId) {
  Context ctx;

  std::string trace_id = ctx.TraceId();
  EXPECT_FALSE(trace_id.empty());
  // generated from cpuwide_time_ns(), so it must be all digits
  EXPECT_TRUE(std::all_of(trace_id.begin(), trace_id.end(),
                          [](char c) { return std::isdigit(c); }));
  EXPECT_FALSE(ctx.GetCacheHit());
}

TEST(ContextTest, ExplicitTraceId) {
  Context ctx("550e8400-e29b-41d4-a716-446655440000");
  EXPECT_EQ(ctx.TraceId(), "550e8400-e29b-41d4-a716-446655440000");
}

TEST(ContextTest, CacheHitFlag) {
  Context ctx;
  EXPECT_FALSE(ctx.GetCacheHit());

  ctx.SetCacheHit(true);
  EXPECT_TRUE(ctx.GetCacheHit());

  ctx.SetCacheHit(false);
  EXPECT_FALSE(ctx.GetCacheHit());
}

TEST(ContextTest, Factory) {
  {
    auto ctx = NewContext();
    ASSERT_NE(ctx, nullptr);
    EXPECT_FALSE(ctx->TraceId().empty());
  }

  {
    auto ctx = NewContext("my-trace-id");
    ASSERT_NE(ctx, nullptr);
    EXPECT_EQ(ctx->TraceId(), "my-trace-id");
  }
}

}  // namespace cache
}  // namespace dingofs
