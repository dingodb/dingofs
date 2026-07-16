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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "cache/common/storage_client.h"
#include "common/block/block_handle.h"
#include "common/block/block_key.h"
#include "common/blockaccess/accesser_common.h"
#include "common/io_buffer.h"
#include "test/unit/common/blockaccess/mock/mock_accesser.h"

namespace dingofs {
namespace cache {

using blockaccess::GetObjectAsyncContext;
using blockaccess::MockBlockAccesser;
using blockaccess::PutObjectAsyncContext;
using ::testing::_;
using ::testing::Invoke;

DECLARE_int64(storage_upload_retry_timeout_s);

class StorageClientTest : public ::testing::Test {
 protected:
  static BlockHandle Handle(uint64_t id) {
    return BlockHandle(1, BlockKey(id, 0, 4194304));
  }
  static IOBuffer Buf(const std::string& s) {
    return IOBuffer(s.data(), s.size());
  }

  MockBlockAccesser accesser_;
};

TEST_F(StorageClientTest, StartAndShutdownIdempotent) {
  StorageClient client(&accesser_);
  EXPECT_TRUE(client.Start().ok());
  EXPECT_TRUE(client.Start().ok());
  EXPECT_TRUE(client.Shutdown().ok());
  EXPECT_TRUE(client.Shutdown().ok());
}

TEST_F(StorageClientTest, PutSuccess) {
  StorageClient client(&accesser_);
  ASSERT_TRUE(client.Start().ok());

  EXPECT_CALL(accesser_, AsyncPut(_, _))
      .WillOnce(Invoke(
          [](const std::string&, std::shared_ptr<PutObjectAsyncContext> ctx) {
            ctx->status = Status::OK();
            ctx->cb(ctx);
          }));

  EXPECT_TRUE(client.Put(Handle(100), Buf("hello")).ok());
  ASSERT_TRUE(client.Shutdown().ok());
}

TEST_F(StorageClientTest, PutPreservesSegmentedIOBuffer) {
  StorageClient client(&accesser_);
  ASSERT_TRUE(client.Start().ok());

  std::string first = "hello ";
  std::string second = "segments";
  IOBuffer buffer;
  buffer.AppendUserData(first.data(), first.size(), [](void*) {});
  buffer.AppendUserData(second.data(), second.size(), [](void*) {});
  ASSERT_EQ(buffer.BackingBlockNum(), 2);

  EXPECT_CALL(accesser_, AsyncPut(_, _))
      .WillOnce(Invoke(
          [&](const std::string&, std::shared_ptr<PutObjectAsyncContext> ctx) {
            EXPECT_EQ(ctx->payload.SegmentCount(), 2);
            EXPECT_EQ(ctx->payload.Size(), first.size() + second.size());
            EXPECT_EQ(std::string(ctx->payload.Segments()[0].data,
                                  ctx->payload.Segments()[0].size),
                      first);
            EXPECT_EQ(std::string(ctx->payload.Segments()[1].data,
                                  ctx->payload.Segments()[1].size),
                      second);
            ctx->status = Status::OK();
            ctx->cb(ctx);
          }));

  EXPECT_TRUE(client.Put(Handle(103), buffer).ok());
  ASSERT_TRUE(client.Shutdown().ok());
}

TEST_F(StorageClientTest, PutRetriesThenSucceeds) {
  StorageClient client(&accesser_);
  ASSERT_TRUE(client.Start().ok());

  int calls = 0;
  EXPECT_CALL(accesser_, AsyncPut(_, _))
      .Times(2)
      .WillRepeatedly(
          Invoke([&calls](const std::string&,
                          std::shared_ptr<PutObjectAsyncContext> ctx) {
            ctx->status =
                (++calls == 1) ? Status::IoError("transient") : Status::OK();
            ctx->cb(ctx);
          }));

  EXPECT_TRUE(client.Put(Handle(101), Buf("data")).ok());
  EXPECT_EQ(calls, 2);
  ASSERT_TRUE(client.Shutdown().ok());
}

TEST_F(StorageClientTest, PutRetriesOutOfMemoryThenSucceeds) {
  StorageClient client(&accesser_);
  ASSERT_TRUE(client.Start().ok());

  int calls = 0;
  EXPECT_CALL(accesser_, AsyncPut(_, _))
      .Times(2)
      .WillRepeatedly(Invoke([&calls](
                                 const std::string&,
                                 std::shared_ptr<PutObjectAsyncContext> ctx) {
        ctx->status = (++calls == 1)
                          ? Status::OutOfMemory("transient allocation failure")
                          : Status::OK();
        ctx->cb(ctx);
      }));

  EXPECT_TRUE(client.Put(Handle(104), Buf("data")).ok());
  EXPECT_EQ(calls, 2);
  ASSERT_TRUE(client.Shutdown().ok());
}

TEST_F(StorageClientTest, PutFailsWhenRetryWindowElapsed) {
  // Disable the retry window so a persistent failure completes immediately.
  auto saved = FLAGS_storage_upload_retry_timeout_s;
  FLAGS_storage_upload_retry_timeout_s = 0;

  StorageClient client(&accesser_);
  ASSERT_TRUE(client.Start().ok());

  EXPECT_CALL(accesser_, AsyncPut(_, _))
      .WillOnce(Invoke(
          [](const std::string&, std::shared_ptr<PutObjectAsyncContext> ctx) {
            ctx->status = Status::IoError("permanent");
            ctx->cb(ctx);
          }));

  EXPECT_TRUE(client.Put(Handle(102), Buf("x")).IsIoError());
  ASSERT_TRUE(client.Shutdown().ok());

  FLAGS_storage_upload_retry_timeout_s = saved;
}

TEST_F(StorageClientTest, PutRetryAfterShutdownReturnsIoError) {
  StorageClient client(&accesser_);
  ASSERT_TRUE(client.Start().ok());

  // Capture the in-flight async context and defer its callback, so the failure
  // fires after Shutdown has stopped the retry queue. This reproduces the race
  // where a late S3 callback resubmits to a dead queue, which used to abort the
  // process; it must now converge to an IO error instead.
  std::mutex mu;
  std::condition_variable cv;
  std::shared_ptr<PutObjectAsyncContext> inflight;
  EXPECT_CALL(accesser_, AsyncPut(_, _))
      .WillOnce(Invoke(
          [&](const std::string&, std::shared_ptr<PutObjectAsyncContext> ctx) {
            std::lock_guard<std::mutex> lk(mu);
            inflight = ctx;
            cv.notify_one();
          }));

  // Put blocks until the task completes, so drive it from a separate thread.
  Status put_status;
  std::thread putter([&] { put_status = client.Put(Handle(300), Buf("x")); });

  {
    std::unique_lock<std::mutex> lk(mu);
    cv.wait(lk, [&] { return inflight != nullptr; });
  }

  ASSERT_TRUE(client.Shutdown().ok());

  inflight->status = Status::IoError("late failure after shutdown");
  inflight->cb(inflight);

  putter.join();
  EXPECT_TRUE(put_status.IsIoError());
}

TEST_F(StorageClientTest, RangeSuccess) {
  StorageClient client(&accesser_);
  ASSERT_TRUE(client.Start().ok());

  const size_t length = 5;
  EXPECT_CALL(accesser_, AsyncGet(_, _))
      .WillOnce(Invoke([length](const std::string&,
                                std::shared_ptr<GetObjectAsyncContext> ctx) {
        ctx->actual_len = length;  // must equal requested length
        ctx->status = Status::OK();
        ctx->cb(ctx);
      }));

  // Range requires the caller to pre-size the output buffer to a single block.
  std::string storage(length, '\0');
  IOBuffer buffer(storage.data(), storage.size());
  EXPECT_TRUE(client.Range(Handle(200), 0, length, &buffer).ok());
  ASSERT_TRUE(client.Shutdown().ok());
}

TEST_F(StorageClientTest, RangeRetriesThenSucceeds) {
  StorageClient client(&accesser_);
  ASSERT_TRUE(client.Start().ok());

  const size_t length = 5;
  int calls = 0;
  EXPECT_CALL(accesser_, AsyncGet(_, _))
      .Times(2)
      .WillRepeatedly(Invoke([&calls, length](
                                 const std::string&,
                                 std::shared_ptr<GetObjectAsyncContext> ctx) {
        if (++calls == 1) {
          ctx->status = Status::IoError("transient");  // non-NotFound -> retry
        } else {
          ctx->actual_len = length;
          ctx->status = Status::OK();
        }
        ctx->cb(ctx);
      }));

  std::string storage(length, '\0');
  IOBuffer buffer(storage.data(), storage.size());
  EXPECT_TRUE(client.Range(Handle(202), 0, length, &buffer).ok());
  EXPECT_EQ(calls, 2);
  ASSERT_TRUE(client.Shutdown().ok());
}

TEST_F(StorageClientTest, RangeNotFoundIsNotRetried) {
  StorageClient client(&accesser_);
  ASSERT_TRUE(client.Start().ok());

  EXPECT_CALL(accesser_, AsyncGet(_, _))
      .WillOnce(Invoke(
          [](const std::string&, std::shared_ptr<GetObjectAsyncContext> ctx) {
            ctx->status = Status::NotFound("no such object");
            ctx->cb(ctx);
          }));

  std::string storage(4096, '\0');
  IOBuffer buffer(storage.data(), storage.size());
  EXPECT_TRUE(client.Range(Handle(201), 0, 4096, &buffer).IsNotFound());
  ASSERT_TRUE(client.Shutdown().ok());
}

TEST_F(StorageClientTest, RangeRetryAfterShutdownReturnsIoError) {
  StorageClient client(&accesser_);
  ASSERT_TRUE(client.Start().ok());

  // Same late-callback race as the Put case, exercised on the download retry
  // queue: submitting a retry to the stopped queue must return an IO error
  // rather than crashing.
  std::mutex mu;
  std::condition_variable cv;
  std::shared_ptr<GetObjectAsyncContext> inflight;
  EXPECT_CALL(accesser_, AsyncGet(_, _))
      .WillOnce(Invoke(
          [&](const std::string&, std::shared_ptr<GetObjectAsyncContext> ctx) {
            std::lock_guard<std::mutex> lk(mu);
            inflight = ctx;
            cv.notify_one();
          }));

  const size_t length = 5;
  std::string storage(length, '\0');
  IOBuffer buffer(storage.data(), storage.size());
  Status range_status;
  std::thread ranger(
      [&] { range_status = client.Range(Handle(301), 0, length, &buffer); });

  {
    std::unique_lock<std::mutex> lk(mu);
    cv.wait(lk, [&] { return inflight != nullptr; });
  }

  ASSERT_TRUE(client.Shutdown().ok());

  inflight->status = Status::IoError("late failure after shutdown");
  inflight->cb(inflight);

  ranger.join();
  EXPECT_TRUE(range_status.IsIoError());
}

}  // namespace cache
}  // namespace dingofs
