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

#include "common/blockaccess/fake/fake_accesser.h"

#include <gtest/gtest.h>

#include <future>

namespace dingofs {
namespace blockaccess {

class FakeAccesserTest : public ::testing::Test {
 protected:
  FakeAccesser accesser_;
};

TEST_F(FakeAccesserTest, InitFailsWhenAlreadyStarted) {
  EXPECT_TRUE(accesser_.Init());
  EXPECT_FALSE(accesser_.Init());
  EXPECT_TRUE(accesser_.Destroy());
}

TEST_F(FakeAccesserTest, DestroyIsIdempotentWhenNotStarted) {
  EXPECT_TRUE(accesser_.Destroy());
  EXPECT_TRUE(accesser_.Destroy());
}

TEST_F(FakeAccesserTest, ContainerAlwaysExists) {
  EXPECT_TRUE(accesser_.ContainerExist());
}

TEST_F(FakeAccesserTest, PutAlwaysSucceeds) {
  const char data[] = "payload";
  EXPECT_TRUE(accesser_.Put("key", data, sizeof(data)).ok());
}

TEST_F(FakeAccesserTest, GetFillsFixedSizeBuffer) {
  std::string data;
  ASSERT_TRUE(accesser_.Get("key", &data).ok());
  EXPECT_EQ(data.size(), 4 * 1024 * 1024);
}

TEST_F(FakeAccesserTest, RangeReadSucceedsWithoutTouchingKeyOrOffset) {
  std::vector<char> buffer(128, 'x');
  EXPECT_TRUE(accesser_.Range("key", /*offset=*/64, buffer.size(),
                              buffer.data())
                  .ok());
}

TEST_F(FakeAccesserTest, BlockExistAlwaysTrue) {
  EXPECT_TRUE(accesser_.BlockExist("any-key"));
}

TEST_F(FakeAccesserTest, DeleteAlwaysSucceeds) {
  EXPECT_TRUE(accesser_.Delete("key").ok());
}

TEST_F(FakeAccesserTest, BatchDeleteAlwaysSucceeds) {
  EXPECT_TRUE(accesser_.BatchDelete({"a", "b", "c"}).ok());
}

TEST_F(FakeAccesserTest, AsyncPutInvokesCallbackWithOkStatus) {
  const char data[] = "payload";
  auto context = std::make_shared<PutObjectAsyncContext>("key");
  context->buffer = data;
  context->buffer_size = sizeof(data);

  std::promise<Status> promise;
  auto future = promise.get_future();
  context->cb = [&promise](const PutObjectAsyncContextSPtr& ctx) {
    promise.set_value(ctx->status);
  };

  accesser_.AsyncPut("key", context);
  EXPECT_TRUE(future.get().ok());
}

TEST_F(FakeAccesserTest, AsyncGetInvokesCallbackWithActualLength) {
  auto context = std::make_shared<GetObjectAsyncContext>("key");
  std::vector<char> buffer(256, 0);
  context->buf = buffer.data();
  context->len = buffer.size();

  std::promise<size_t> promise;
  auto future = promise.get_future();
  context->cb = [&promise](const GetObjectAsyncContextSPtr& ctx) {
    promise.set_value(ctx->actual_len);
  };

  accesser_.AsyncGet("key", context);
  EXPECT_EQ(future.get(), buffer.size());
}

TEST_F(FakeAccesserTest, AsyncDeleteInvokesCallbackWithOkStatus) {
  auto context = std::make_shared<DeleteObjectAsyncContext>("key");

  std::promise<Status> promise;
  auto future = promise.get_future();
  context->cb = [&promise](const DeleteObjectAsyncContextSPtr& ctx) {
    promise.set_value(ctx->status);
  };

  accesser_.AsyncDelete("key", context);
  EXPECT_TRUE(future.get().ok());
}

TEST_F(FakeAccesserTest, AsyncBatchDeleteInvokesCallbackWithOkStatus) {
  auto context =
      std::make_shared<BatchDeleteObjectAsyncContext>(std::list<std::string>{
          "a", "b"});

  std::promise<Status> promise;
  auto future = promise.get_future();
  context->cb = [&promise](const BatchDeleteObjectAsyncContextSPtr& ctx) {
    promise.set_value(ctx->status);
  };

  accesser_.AsyncBatchDelete({"a", "b"}, context);
  EXPECT_TRUE(future.get().ok());
}

}  // namespace blockaccess
}  // namespace dingofs
