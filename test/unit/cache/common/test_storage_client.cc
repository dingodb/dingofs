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
 * Created Date: 2026-07-19
 * Author: AI
 */

#include <butil/time.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstring>
#include <thread>

#include "cache/common/storage_client.h"
#include "common/options/cache.h"
#include "mock/mock_block_accesser.h"

namespace dingofs {
namespace cache {

using ::testing::_;
using ::testing::Invoke;

using blockaccess::GetObjectAsyncContext;
using blockaccess::MockBlockAccesser;
using blockaccess::PutObjectAsyncContext;

class StorageClientTest : public ::testing::Test {
 protected:
  void SetUp() override {
    saved_upload_max_tries_ = FLAGS_storage_upload_max_tries;
    saved_download_max_tries_ = FLAGS_storage_download_max_tries;
    saved_upload_backoff_base_ms_ = FLAGS_storage_upload_retry_backoff_base_ms;
    saved_download_backoff_base_ms_ =
        FLAGS_storage_download_retry_backoff_base_ms;

    // keep retries fast by default, individual cases override as needed
    FLAGS_storage_upload_retry_backoff_base_ms = 1;
    FLAGS_storage_download_retry_backoff_base_ms = 1;

    storage_client_ = std::make_unique<StorageClient>(&mock_accesser_);
    ASSERT_TRUE(storage_client_->Start().ok());
  }

  void TearDown() override {
    storage_client_->Shutdown();

    FLAGS_storage_upload_max_tries = saved_upload_max_tries_;
    FLAGS_storage_download_max_tries = saved_download_max_tries_;
    FLAGS_storage_upload_retry_backoff_base_ms = saved_upload_backoff_base_ms_;
    FLAGS_storage_download_retry_backoff_base_ms =
        saved_download_backoff_base_ms_;
  }

  static BlockKey TestBlockKey() { return BlockKey(1, 100, 2000, 0, 1); }

  MockBlockAccesser mock_accesser_;
  StorageClientUPtr storage_client_;

  uint32_t saved_upload_max_tries_;
  uint32_t saved_download_max_tries_;
  uint32_t saved_upload_backoff_base_ms_;
  uint32_t saved_download_backoff_base_ms_;
};

TEST_F(StorageClientTest, PutSuccess) {
  int calls = 0;
  EXPECT_CALL(mock_accesser_, AsyncPut(_))
      .WillOnce(Invoke([&](std::shared_ptr<PutObjectAsyncContext> ctx) {
        calls++;
        ctx->status = Status::OK();
        ctx->cb(ctx);
      }));

  Block block("hello world", 11);
  auto status = storage_client_->Put(NewContext(), TestBlockKey(), &block);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(calls, 1);
}

TEST_F(StorageClientTest, PutBoundedRetries) {
  FLAGS_storage_upload_max_tries = 3;

  int calls = 0;
  EXPECT_CALL(mock_accesser_, AsyncPut(_))
      .Times(3)
      .WillRepeatedly(Invoke([&](std::shared_ptr<PutObjectAsyncContext> ctx) {
        calls++;
        ctx->status = Status::IoError("inject io error");
        ctx->cb(ctx);
      }));

  Block block("hello world", 11);
  auto status = storage_client_->Put(NewContext(), TestBlockKey(), &block);
  ASSERT_TRUE(status.IsIoError());
  ASSERT_EQ(calls, 3);
}

TEST_F(StorageClientTest, PutRetryOptionOverridesFlag) {
  FLAGS_storage_upload_max_tries = 10;

  int calls = 0;
  EXPECT_CALL(mock_accesser_, AsyncPut(_))
      .Times(3)
      .WillRepeatedly(Invoke([&](std::shared_ptr<PutObjectAsyncContext> ctx) {
        calls++;
        ctx->status = Status::IoError("inject io error");
        ctx->cb(ctx);
      }));

  Block block("hello world", 11);
  auto status = storage_client_->Put(NewContext(), TestBlockKey(), &block,
                                     {.max_tries = 3});
  ASSERT_TRUE(status.IsIoError());
  ASSERT_EQ(calls, 3);
}

TEST_F(StorageClientTest, PutNoRetryOnUnretriableError) {
  {  // not support: e.g. overwrite on an EC pool
    EXPECT_CALL(mock_accesser_, AsyncPut(_))
        .WillOnce(Invoke([&](std::shared_ptr<PutObjectAsyncContext> ctx) {
          ctx->status = Status::NotSupport("inject not support");
          ctx->cb(ctx);
        }));

    Block block("hello world", 11);
    auto status = storage_client_->Put(NewContext(), TestBlockKey(), &block);
    ASSERT_TRUE(status.IsNotSupport());
  }

  {  // not found
    EXPECT_CALL(mock_accesser_, AsyncPut(_))
        .WillOnce(Invoke([&](std::shared_ptr<PutObjectAsyncContext> ctx) {
          ctx->status = Status::NotFound("inject not found");
          ctx->cb(ctx);
        }));

    Block block("hello world", 11);
    auto status = storage_client_->Put(NewContext(), TestBlockKey(), &block);
    ASSERT_TRUE(status.IsNotFound());
  }
}

TEST_F(StorageClientTest, RangeSuccessAfterRetries) {
  const std::string data = "0123456789";

  int calls = 0;
  EXPECT_CALL(mock_accesser_, AsyncGet(_))
      .Times(3)
      .WillRepeatedly(Invoke([&](std::shared_ptr<GetObjectAsyncContext> ctx) {
        calls++;
        if (calls < 3) {
          ctx->status = Status::IoError("inject io error");
        } else {
          std::memcpy(ctx->buf, data.data(), data.size());
          ctx->actual_len = data.size();
          ctx->status = Status::OK();
        }
        ctx->cb(ctx);
      }));

  IOBuffer buffer;
  auto status = storage_client_->Range(NewContext(), TestBlockKey(), 0,
                                       data.size(), &buffer);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(calls, 3);
  ASSERT_EQ(buffer.Size(), data.size());
  ASSERT_EQ(std::string(buffer.Fetch1(), buffer.Size()), data);
}

TEST_F(StorageClientTest, RangeBoundedRetries) {
  FLAGS_storage_download_max_tries = 3;

  int calls = 0;
  EXPECT_CALL(mock_accesser_, AsyncGet(_))
      .Times(3)
      .WillRepeatedly(Invoke([&](std::shared_ptr<GetObjectAsyncContext> ctx) {
        calls++;
        ctx->status = Status::IoError("inject io error");
        ctx->cb(ctx);
      }));

  IOBuffer buffer;
  auto status =
      storage_client_->Range(NewContext(), TestBlockKey(), 0, 4096, &buffer);
  ASSERT_TRUE(status.IsIoError());
  ASSERT_EQ(calls, 3);
}

TEST_F(StorageClientTest, RangeShortObjectReturnsError) {
  // e.g. a ranged GET straddling the end of a truncated object: 206 with
  // fewer bytes and OK status; must fail gracefully without retry instead
  // of aborting the process
  int calls = 0;
  EXPECT_CALL(mock_accesser_, AsyncGet(_))
      .WillOnce(Invoke([&](std::shared_ptr<GetObjectAsyncContext> ctx) {
        calls++;
        std::memset(ctx->buf, 'x', 1024);
        ctx->actual_len = 1024;  // shorter than the requested 4096
        ctx->status = Status::OK();
        ctx->cb(ctx);
      }));

  IOBuffer buffer;
  auto status =
      storage_client_->Range(NewContext(), TestBlockKey(), 0, 4096, &buffer);
  ASSERT_TRUE(status.IsInternal());
  ASSERT_EQ(calls, 1);
}

TEST_F(StorageClientTest, RangeNoRetryOnNotFound) {
  EXPECT_CALL(mock_accesser_, AsyncGet(_))
      .WillOnce(Invoke([&](std::shared_ptr<GetObjectAsyncContext> ctx) {
        ctx->status = Status::NotFound("inject not found");
        ctx->cb(ctx);
      }));

  IOBuffer buffer;
  auto status =
      storage_client_->Range(NewContext(), TestBlockKey(), 0, 4096, &buffer);
  ASSERT_TRUE(status.IsNotFound());
}

TEST_F(StorageClientTest, BackoffFormula) {
  FLAGS_storage_upload_retry_backoff_base_ms = 1000;
  FLAGS_storage_download_retry_backoff_base_ms = 300;

  {  // upload: min(base * tried * tried, 60s)
    ASSERT_EQ(UploadRetryBackoffMs(1), 1000);
    ASSERT_EQ(UploadRetryBackoffMs(2), 4000);
    ASSERT_EQ(UploadRetryBackoffMs(3), 9000);
    ASSERT_EQ(UploadRetryBackoffMs(8), 60000);   // 64s capped to 60s
    ASSERT_EQ(UploadRetryBackoffMs(100), 60000);
  }

  {  // download: min(base * tried, 10s)
    ASSERT_EQ(DownloadRetryBackoffMs(1), 300);
    ASSERT_EQ(DownloadRetryBackoffMs(2), 600);
    ASSERT_EQ(DownloadRetryBackoffMs(9), 2700);
    ASSERT_EQ(DownloadRetryBackoffMs(100), 10000);  // 30s capped to 10s
  }
}

TEST_F(StorageClientTest, BackoffApplied) {
  FLAGS_storage_download_max_tries = 3;
  FLAGS_storage_download_retry_backoff_base_ms = 50;

  EXPECT_CALL(mock_accesser_, AsyncGet(_))
      .Times(3)
      .WillRepeatedly(Invoke([&](std::shared_ptr<GetObjectAsyncContext> ctx) {
        ctx->status = Status::IoError("inject io error");
        ctx->cb(ctx);
      }));

  auto start_us = butil::gettimeofday_us();
  IOBuffer buffer;
  auto status =
      storage_client_->Range(NewContext(), TestBlockKey(), 0, 4096, &buffer);
  auto elapsed_ms = (butil::gettimeofday_us() - start_us) / 1000;

  ASSERT_TRUE(status.IsIoError());
  ASSERT_GE(elapsed_ms, 150);  // backoff 50ms + 100ms between the 3 tries
}

TEST_F(StorageClientTest, ShutdownAbortsBackoff) {
  FLAGS_storage_upload_max_tries = 3;
  FLAGS_storage_upload_retry_backoff_base_ms = 10000;  // 10s per backoff

  EXPECT_CALL(mock_accesser_, AsyncPut(_))
      .WillOnce(Invoke([&](std::shared_ptr<PutObjectAsyncContext> ctx) {
        ctx->status = Status::IoError("inject io error");
        ctx->cb(ctx);
      }));

  auto start_us = butil::gettimeofday_us();
  std::thread shutdown_thread([&]() {
    bthread_usleep(200 * 1000);
    storage_client_->Shutdown();
  });

  Block block("hello world", 11);
  auto status = storage_client_->Put(NewContext(), TestBlockKey(), &block);
  auto elapsed_ms = (butil::gettimeofday_us() - start_us) / 1000;

  shutdown_thread.join();
  ASSERT_TRUE(status.IsAbort());
  ASSERT_LT(elapsed_ms, 2000);  // way below the 10s backoff
}

TEST_F(StorageClientTest, RejectAfterShutdown) {
  storage_client_->Shutdown();

  Block block("hello world", 11);
  auto status = storage_client_->Put(NewContext(), TestBlockKey(), &block);
  ASSERT_TRUE(status.IsAbort());

  IOBuffer buffer;
  status =
      storage_client_->Range(NewContext(), TestBlockKey(), 0, 4096, &buffer);
  ASSERT_TRUE(status.IsAbort());
}

}  // namespace cache
}  // namespace dingofs
