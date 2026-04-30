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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "common/blockaccess/accesser_common.h"
#include "common/blockaccess/prefix_block_accesser.h"
#include "test/unit/common/blockaccess/mock/mock_accesser.h"

namespace dingofs {
namespace blockaccess {

using ::testing::_;
using ::testing::Invoke;

// Regression: PrefixBlockAccesser must NOT mutate context->origin_key on
// AsyncPut/AsyncGet. Callers reuse the same context across retries; if the
// wrapper accumulated prefix on each call (the original bug), the storage
// key would grow as "myfs/myfs/myfs/.../blocks/..." and eventually exceed
// path/key limits — hanging IO under MDS-mode S3 backends and aborting the
// dingo-client process under file backend (fs::create_directories throw).
//
// Invariant under test: every retry must hit the inner backend with the
// SAME key string, and ctx->origin_key must remain unchanged after any
// number of calls.
TEST(PrefixBlockAccesserTest, AsyncPutRetryDoesNotAccumulatePrefix) {
  auto mock_inner = std::make_unique<MockBlockAccesser>();
  std::vector<std::string> seen_keys;

  EXPECT_CALL(*mock_inner, AsyncPut(_, _))
      .WillRepeatedly(Invoke([&](const std::string& key,
                                 std::shared_ptr<PutObjectAsyncContext> ctx) {
        seen_keys.push_back(key);
        ctx->status = Status::OK();
        ctx->cb(ctx);
      }));

  PrefixBlockAccesser wrapper("myfs", std::move(mock_inner));

  const std::string logical_key = "blocks/100/100/100_0_4096";

  static const char kData[] = "data";
  auto ctx = std::make_shared<PutObjectAsyncContext>(logical_key);
  ctx->buffer = kData;
  ctx->buffer_size = sizeof(kData);
  ctx->cb = [](const PutObjectAsyncContextSPtr&) {};

  // Simulate first call + 4 retries — caller passes the SAME ctx and
  // SAME logical key each time (mirroring storage_client.cc PutBlockTask).
  for (int i = 0; i < 5; ++i) {
    wrapper.AsyncPut(logical_key, ctx);
  }

  // Every retry must hit the inner backend with the IDENTICAL prefixed key.
  // Pre-fix: seen_keys[0]="myfs/blocks/...",
  // seen_keys[1]="myfs/myfs/blocks/...", etc.
  ASSERT_EQ(seen_keys.size(), 5u);
  const std::string expected_prefixed = "myfs/" + logical_key;
  for (size_t i = 0; i < seen_keys.size(); ++i) {
    EXPECT_EQ(seen_keys[i], expected_prefixed)
        << "iteration " << i
        << " — prefix accumulated; the wrapper mutated something it should "
           "not, or computed prefix from a stale source.";
  }

  // ctx->origin_key must equal the caller's logical key throughout — the
  // wrapper never writes back to it (it is `const` for compile-time
  // enforcement, but this assertion documents the contract for readers).
  EXPECT_EQ(ctx->origin_key, logical_key);
}

TEST(PrefixBlockAccesserTest, AsyncGetRetryDoesNotAccumulatePrefix) {
  auto mock_inner = std::make_unique<MockBlockAccesser>();
  std::vector<std::string> seen_keys;

  EXPECT_CALL(*mock_inner, AsyncGet(_, _))
      .WillRepeatedly(Invoke([&](const std::string& key,
                                 std::shared_ptr<GetObjectAsyncContext> ctx) {
        seen_keys.push_back(key);
        ctx->status = Status::OK();
        ctx->actual_len = ctx->len;
        ctx->cb(ctx);
      }));

  PrefixBlockAccesser wrapper("fsA", std::move(mock_inner));

  const std::string logical_key = "blocks/200/200/200_0_4096";

  std::vector<char> buf(4096);
  auto ctx = std::make_shared<GetObjectAsyncContext>(logical_key);
  ctx->buf = buf.data();
  ctx->offset = 0;
  ctx->len = buf.size();
  ctx->cb = [](const GetObjectAsyncContextSPtr&) {};

  for (int i = 0; i < 5; ++i) {
    wrapper.AsyncGet(logical_key, ctx);
  }

  ASSERT_EQ(seen_keys.size(), 5u);
  const std::string expected_prefixed = "fsA/" + logical_key;
  for (size_t i = 0; i < seen_keys.size(); ++i) {
    EXPECT_EQ(seen_keys[i], expected_prefixed)
        << "iteration " << i << " — Get prefix accumulated.";
  }
  EXPECT_EQ(ctx->origin_key, logical_key);
}

// Sanity: synchronous Put/Get also passes through the prefix exactly once.
TEST(PrefixBlockAccesserTest, SyncPutPrefixesKeyOnce) {
  auto mock_inner = std::make_unique<MockBlockAccesser>();
  std::string seen_key;

  EXPECT_CALL(*mock_inner, Put(_, ::testing::A<const char*>(), _))
      .WillRepeatedly(Invoke([&](const std::string& key, const char*, size_t) {
        seen_key = key;
        return Status::OK();
      }));

  PrefixBlockAccesser wrapper("fs", std::move(mock_inner));
  static const char kData[] = "hello";
  ASSERT_TRUE(wrapper.Put("a/b/c", kData, sizeof(kData)).ok());
  EXPECT_EQ(seen_key, "fs/a/b/c");
}

}  // namespace blockaccess
}  // namespace dingofs
