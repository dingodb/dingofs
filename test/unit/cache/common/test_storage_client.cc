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

#include <butil/time.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstring>
#include <memory>
#include <string>
#include <thread>

#include "cache/common/storage_client.h"
#include "common/block/block_handle.h"
#include "common/block/block_key.h"
#include "common/blockaccess/accesser_common.h"
#include "common/io_buffer.h"
#include "common/options/cache.h"
#include "test/unit/common/blockaccess/mock/mock_accesser.h"

namespace dingofs {
namespace cache {

using blockaccess::GetObjectAsyncContext;
using blockaccess::MockBlockAccesser;
using blockaccess::PutObjectAsyncContext;
using ::testing::_;
using ::testing::Invoke;

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

    client_ = std::make_unique<StorageClient>(&accesser_);
    ASSERT_TRUE(client_->Start().ok());
  }

  void TearDown() override {
    client_->Shutdown();

    FLAGS_storage_upload_max_tries = saved_upload_max_tries_;
    FLAGS_storage_download_max_tries = saved_download_max_tries_;
    FLAGS_storage_upload_retry_backoff_base_ms = saved_upload_backoff_base_ms_;
    FLAGS_storage_download_retry_backoff_base_ms =
        saved_download_backoff_base_ms_;
  }

  static BlockHandle Handle(uint64_t id) {
    return BlockHandle(1, BlockKey(id, 0, 4194304));
  }
  static IOBuffer Buf(const std::string& s) {
    return IOBuffer(s.data(), s.size());
  }
  // Range requires the caller to pre-size the output buffer to a single
  // backing block (IOBuffer(data, size) may split across tls iobuf blocks).
  static IOBuffer PreAlloc(std::string* storage, size_t length) {
    storage->assign(length, '\0');
    IOBuffer buffer;
    buffer.AppendUserData(storage->data(), storage->size(), [](void*) {});
    return buffer;
  }

  MockBlockAccesser accesser_;
  StorageClientUPtr client_;

  uint32_t saved_upload_max_tries_;
  uint32_t saved_download_max_tries_;
  uint32_t saved_upload_backoff_base_ms_;
  uint32_t saved_download_backoff_base_ms_;
};

TEST_F(StorageClientTest, StartAndShutdownIdempotent) {
  EXPECT_TRUE(client_->Start().ok());
  EXPECT_TRUE(client_->Shutdown().ok());
  EXPECT_TRUE(client_->Shutdown().ok());
}

TEST_F(StorageClientTest, PutSuccess) {
  int calls = 0;
  EXPECT_CALL(accesser_, AsyncPut(_, _))
      .WillOnce(Invoke(
          [&](const std::string&, std::shared_ptr<PutObjectAsyncContext> ctx) {
            calls++;
            ctx->status = Status::OK();
            ctx->cb(ctx);
          }));

  EXPECT_TRUE(client_->Put(Handle(100), Buf("hello")).ok());
  EXPECT_EQ(calls, 1);
}

TEST_F(StorageClientTest, PutPreservesSegmentedIOBuffer) {
  std::string first = "hello ";
  std::string second = "segments";
  IOBuffer buffer;
  buffer.AppendUserData(first.data(), first.size(), [](void*) {});
  buffer.AppendUserData(second.data(), second.size(), [](void*) {});
  ASSERT_EQ(buffer.BackingBlockNum(), 2);

  // Fail the first attempt: the rebuilt payload on retry must still carry
  // the original segments untouched (zero-copy survives the retry loop).
  int calls = 0;
  EXPECT_CALL(accesser_, AsyncPut(_, _))
      .Times(2)
      .WillRepeatedly(Invoke(
          [&](const std::string&, std::shared_ptr<PutObjectAsyncContext> ctx) {
            EXPECT_EQ(ctx->payload.SegmentCount(), 2);
            EXPECT_EQ(ctx->payload.Size(), first.size() + second.size());
            EXPECT_EQ(std::string(ctx->payload.Segments()[0].data,
                                  ctx->payload.Segments()[0].size),
                      first);
            EXPECT_EQ(std::string(ctx->payload.Segments()[1].data,
                                  ctx->payload.Segments()[1].size),
                      second);
            ctx->status =
                (++calls == 1) ? Status::IoError("transient") : Status::OK();
            ctx->cb(ctx);
          }));

  EXPECT_TRUE(client_->Put(Handle(103), buffer).ok());
  EXPECT_EQ(calls, 2);
}

TEST_F(StorageClientTest, PutRetriesThenSucceeds) {
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

  EXPECT_TRUE(client_->Put(Handle(101), Buf("data")).ok());
  EXPECT_EQ(calls, 2);
}

TEST_F(StorageClientTest, PutRetriesOutOfMemoryThenSucceeds) {
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

  EXPECT_TRUE(client_->Put(Handle(104), Buf("data")).ok());
  EXPECT_EQ(calls, 2);
}

TEST_F(StorageClientTest, PutBoundedRetries) {
  FLAGS_storage_upload_max_tries = 3;

  int calls = 0;
  EXPECT_CALL(accesser_, AsyncPut(_, _))
      .Times(3)
      .WillRepeatedly(Invoke(
          [&](const std::string&, std::shared_ptr<PutObjectAsyncContext> ctx) {
            calls++;
            ctx->status = Status::IoError("inject io error");
            ctx->cb(ctx);
          }));

  EXPECT_TRUE(client_->Put(Handle(105), Buf("data")).IsIoError());
  EXPECT_EQ(calls, 3);
}

TEST_F(StorageClientTest, PutRetryOptionOverridesFlag) {
  FLAGS_storage_upload_max_tries = 10;

  int calls = 0;
  EXPECT_CALL(accesser_, AsyncPut(_, _))
      .Times(3)
      .WillRepeatedly(Invoke(
          [&](const std::string&, std::shared_ptr<PutObjectAsyncContext> ctx) {
            calls++;
            ctx->status = Status::IoError("inject io error");
            ctx->cb(ctx);
          }));

  EXPECT_TRUE(
      client_->Put(Handle(106), Buf("data"), {.max_tries = 3}).IsIoError());
  EXPECT_EQ(calls, 3);
}

TEST_F(StorageClientTest, PutNoRetryOnUnretriableError) {
  {  // not support: e.g. overwrite on an EC pool
    EXPECT_CALL(accesser_, AsyncPut(_, _))
        .WillOnce(Invoke(
            [](const std::string&, std::shared_ptr<PutObjectAsyncContext> ctx) {
              ctx->status = Status::NotSupport("inject not support");
              ctx->cb(ctx);
            }));

    EXPECT_TRUE(client_->Put(Handle(107), Buf("data")).IsNotSupport());
  }

  {  // not found
    EXPECT_CALL(accesser_, AsyncPut(_, _))
        .WillOnce(Invoke(
            [](const std::string&, std::shared_ptr<PutObjectAsyncContext> ctx) {
              ctx->status = Status::NotFound("inject not found");
              ctx->cb(ctx);
            }));

    EXPECT_TRUE(client_->Put(Handle(108), Buf("data")).IsNotFound());
  }
}

TEST_F(StorageClientTest, RangeSuccess) {
  const size_t length = 5;
  EXPECT_CALL(accesser_, AsyncGet(_, _))
      .WillOnce(Invoke([length](const std::string&,
                                std::shared_ptr<GetObjectAsyncContext> ctx) {
        ctx->actual_len = length;  // must equal requested length
        ctx->status = Status::OK();
        ctx->cb(ctx);
      }));

  std::string storage;
  IOBuffer buffer = PreAlloc(&storage, length);
  EXPECT_TRUE(client_->Range(Handle(200), 0, length, &buffer).ok());
}

TEST_F(StorageClientTest, RangeSuccessAfterRetries) {
  const std::string data = "0123456789";

  int calls = 0;
  EXPECT_CALL(accesser_, AsyncGet(_, _))
      .Times(3)
      .WillRepeatedly(Invoke(
          [&](const std::string&, std::shared_ptr<GetObjectAsyncContext> ctx) {
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

  std::string storage;
  IOBuffer buffer = PreAlloc(&storage, data.size());
  auto status = client_->Range(Handle(202), 0, data.size(), &buffer);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(calls, 3);
  ASSERT_EQ(std::string(buffer.Fetch1(), buffer.Size()), data);
}

TEST_F(StorageClientTest, RangeBoundedRetries) {
  FLAGS_storage_download_max_tries = 3;

  int calls = 0;
  EXPECT_CALL(accesser_, AsyncGet(_, _))
      .Times(3)
      .WillRepeatedly(Invoke(
          [&](const std::string&, std::shared_ptr<GetObjectAsyncContext> ctx) {
            calls++;
            ctx->status = Status::IoError("inject io error");
            ctx->cb(ctx);
          }));

  std::string storage;
  IOBuffer buffer = PreAlloc(&storage, 4096);
  EXPECT_TRUE(client_->Range(Handle(203), 0, 4096, &buffer).IsIoError());
  EXPECT_EQ(calls, 3);
}

TEST_F(StorageClientTest, RangeShortObjectReturnsError) {
  // e.g. a ranged GET straddling the end of a truncated object: 206 with
  // fewer bytes and OK status; must fail gracefully without retry instead
  // of aborting the process
  int calls = 0;
  EXPECT_CALL(accesser_, AsyncGet(_, _))
      .WillOnce(Invoke(
          [&](const std::string&, std::shared_ptr<GetObjectAsyncContext> ctx) {
            calls++;
            std::memset(ctx->buf, 'x', 1024);
            ctx->actual_len = 1024;  // shorter than the requested 4096
            ctx->status = Status::OK();
            ctx->cb(ctx);
          }));

  std::string storage;
  IOBuffer buffer = PreAlloc(&storage, 4096);
  EXPECT_TRUE(client_->Range(Handle(204), 0, 4096, &buffer).IsInternal());
  EXPECT_EQ(calls, 1);
}

TEST_F(StorageClientTest, RangeNotFoundIsNotRetried) {
  EXPECT_CALL(accesser_, AsyncGet(_, _))
      .WillOnce(Invoke(
          [](const std::string&, std::shared_ptr<GetObjectAsyncContext> ctx) {
            ctx->status = Status::NotFound("no such object");
            ctx->cb(ctx);
          }));

  std::string storage;
  IOBuffer buffer = PreAlloc(&storage, 4096);
  EXPECT_TRUE(client_->Range(Handle(201), 0, 4096, &buffer).IsNotFound());
}

TEST_F(StorageClientTest, BackoffFormula) {
  FLAGS_storage_upload_retry_backoff_base_ms = 1000;
  FLAGS_storage_download_retry_backoff_base_ms = 300;

  {  // upload: min(base * tried * tried, 60s)
    ASSERT_EQ(UploadRetryBackoffMs(1), 1000);
    ASSERT_EQ(UploadRetryBackoffMs(2), 4000);
    ASSERT_EQ(UploadRetryBackoffMs(3), 9000);
    ASSERT_EQ(UploadRetryBackoffMs(8), 60000);  // 64s capped to 60s
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

  EXPECT_CALL(accesser_, AsyncGet(_, _))
      .Times(3)
      .WillRepeatedly(Invoke(
          [](const std::string&, std::shared_ptr<GetObjectAsyncContext> ctx) {
            ctx->status = Status::IoError("inject io error");
            ctx->cb(ctx);
          }));

  auto start_us = butil::gettimeofday_us();
  std::string storage;
  IOBuffer buffer = PreAlloc(&storage, 4096);
  auto status = client_->Range(Handle(205), 0, 4096, &buffer);
  auto elapsed_ms = (butil::gettimeofday_us() - start_us) / 1000;

  ASSERT_TRUE(status.IsIoError());
  ASSERT_GE(elapsed_ms, 150);  // backoff 50ms + 100ms between the 3 tries
}

TEST_F(StorageClientTest, ShutdownAbortsBackoff) {
  FLAGS_storage_upload_max_tries = 3;
  FLAGS_storage_upload_retry_backoff_base_ms = 10000;  // 10s per backoff

  EXPECT_CALL(accesser_, AsyncPut(_, _))
      .WillOnce(Invoke(
          [](const std::string&, std::shared_ptr<PutObjectAsyncContext> ctx) {
            ctx->status = Status::IoError("inject io error");
            ctx->cb(ctx);
          }));

  auto start_us = butil::gettimeofday_us();
  std::thread shutdown_thread([&]() {
    bthread_usleep(200 * 1000);
    client_->Shutdown();
  });

  auto status = client_->Put(Handle(300), Buf("data"));
  auto elapsed_ms = (butil::gettimeofday_us() - start_us) / 1000;

  shutdown_thread.join();
  ASSERT_TRUE(status.IsAbort());
  ASSERT_LT(elapsed_ms, 2000);  // way below the 10s backoff
}

TEST_F(StorageClientTest, RejectAfterShutdown) {
  client_->Shutdown();

  EXPECT_TRUE(client_->Put(Handle(301), Buf("data")).IsAbort());

  std::string storage;
  IOBuffer buffer = PreAlloc(&storage, 4096);
  EXPECT_TRUE(client_->Range(Handle(302), 0, 4096, &buffer).IsAbort());
}

}  // namespace cache
}  // namespace dingofs
