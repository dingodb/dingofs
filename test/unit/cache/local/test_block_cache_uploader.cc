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

#include <atomic>
#include <functional>
#include <memory>
#include <string>

#include "cache/common/mock/mock_storage_client_pool.h"
#include "cache/common/storage_client.h"
#include "cache/local/block_cache_uploader.h"
#include "cache/local/cache_store.h"
#include "cache/local/mock/mock_cache_store.h"
#include "common/block/block_handle.h"
#include "common/block/block_key.h"
#include "common/blockaccess/accesser_common.h"
#include "common/io_buffer.h"
#include "common/options/cache.h"
#include "test/unit/common/blockaccess/mock/mock_accesser.h"

namespace dingofs {
namespace cache {

using blockaccess::MockBlockAccesser;
using blockaccess::PutObjectAsyncContext;
using ::testing::_;
using ::testing::Invoke;
using ::testing::NiceMock;
using ::testing::Return;

class BlockCacheUploaderTest : public ::testing::Test {
 protected:
  void SetUp() override {
    saved_max_inflights_ = FLAGS_upload_stage_max_inflights;
    saved_max_tries_ = FLAGS_upload_stage_max_tries;
    saved_retry_delay_s_ = FLAGS_upload_stage_retry_delay_s;
  }

  void TearDown() override {
    if (uploader_ != nullptr) {
      uploader_->Shutdown();
    }
    if (storage_client_ != nullptr) {
      storage_client_->Shutdown();
    }

    FLAGS_upload_stage_max_inflights = saved_max_inflights_;
    FLAGS_upload_stage_max_tries = saved_max_tries_;
    FLAGS_upload_stage_retry_delay_s = saved_retry_delay_s_;
  }

  // called after each case tunes the flags: the inflight tracker snapshots
  // FLAGS_upload_stage_max_inflights in the constructor
  void Init() {
    store_ = std::make_shared<NiceMock<MockCacheStore>>();
    ON_CALL(*store_, IsRunning()).WillByDefault(Return(true));
    // Load must hand back a single-block buffer (StorageClient::Put calls
    // IOBuffer::Fetch1, which requires exactly one backing block).
    ON_CALL(*store_, Load(_, _, _, _, _))
        .WillByDefault(Invoke([](BlockHandle, off_t, size_t length,
                                 IOBuffer* buffer, CacheStore::LoadOption) {
          std::string data(length, 'x');
          *buffer = IOBuffer(data.data(), data.size());
          return Status::OK();
        }));
    ON_CALL(*store_, RemoveStage(_, _)).WillByDefault(Return(Status::OK()));

    storage_client_ = std::make_unique<StorageClient>(&accesser_);
    ASSERT_TRUE(storage_client_->Start().ok());

    auto pool = std::make_shared<NiceMock<MockStorageClientPool>>();
    ON_CALL(*pool, GetStorageClient(_, _))
        .WillByDefault(Invoke([this](uint32_t, StorageClient** out) {
          *out = storage_client_.get();
          return Status::OK();
        }));

    uploader_ = std::make_unique<BlockCacheUploader>(store_, pool);
    uploader_->Start();
  }

  static BlockHandle Handle(uint64_t id) {
    return BlockHandle(1, BlockKey(id, 0, 4194304));
  }

  static StageBlock TestStageBlock(uint64_t id = 100) {
    return StageBlock(Handle(id), 4096, BlockAttr(BlockAttr::kFromWriteback));
  }

  static bool WaitUntil(const std::function<bool()>& cond,
                        uint64_t timeout_ms) {
    auto deadline_us = butil::gettimeofday_us() + (timeout_ms * 1000);
    while (butil::gettimeofday_us() < deadline_us) {
      if (cond()) {
        return true;
      }
      bthread_usleep(10 * 1000);
    }
    return cond();
  }

  MockBlockAccesser accesser_;
  StorageClientUPtr storage_client_;
  std::shared_ptr<NiceMock<MockCacheStore>> store_;
  BlockCacheUploaderUPtr uploader_;

  uint32_t saved_max_inflights_;
  uint32_t saved_max_tries_;
  uint32_t saved_retry_delay_s_;
};

TEST_F(BlockCacheUploaderTest, UploadSuccessRemovesStage) {
  Init();

  std::atomic<int> puts(0), removes(0);
  EXPECT_CALL(accesser_, AsyncPut(_, _))
      .WillOnce(Invoke(
          [&](const std::string&, std::shared_ptr<PutObjectAsyncContext> ctx) {
            puts++;
            ctx->status = Status::OK();
            ctx->cb(ctx);
          }));
  EXPECT_CALL(*store_, RemoveStage(_, _))
      .WillOnce(Invoke([&](BlockHandle, CacheStore::RemoveStageOption) {
        removes++;
        return Status::OK();
      }));

  uploader_->EnterUploadQueue(TestStageBlock());
  ASSERT_TRUE(WaitUntil([&]() { return puts == 1 && removes == 1; }, 3000));
}

TEST_F(BlockCacheUploaderTest, SlowRequeueAfterFailure) {
  FLAGS_upload_stage_max_tries = 1;
  FLAGS_upload_stage_retry_delay_s = 1;
  Init();

  std::atomic<int> puts(0);
  std::atomic<int64_t> first_put_us(0), second_put_us(0);
  EXPECT_CALL(accesser_, AsyncPut(_, _))
      .WillRepeatedly(Invoke(
          [&](const std::string&, std::shared_ptr<PutObjectAsyncContext> ctx) {
            int n = ++puts;
            if (n == 1) {
              first_put_us = butil::gettimeofday_us();
            } else if (n == 2) {
              second_put_us = butil::gettimeofday_us();
            }
            ctx->status = Status::IoError("inject io error");
            ctx->cb(ctx);
          }));

  uploader_->EnterUploadQueue(TestStageBlock());
  ASSERT_TRUE(WaitUntil([&]() { return puts >= 2; }, 5000));
  ASSERT_GE(second_put_us - first_put_us, 900 * 1000);  // ~1s slow cycle
}

TEST_F(BlockCacheUploaderTest, ReleaseInflightSlotBeforeSlowRequeue) {
  FLAGS_upload_stage_max_inflights = 1;
  FLAGS_upload_stage_max_tries = 1;
  FLAGS_upload_stage_retry_delay_s = 5;
  Init();

  const auto poison = TestStageBlock(100);
  const auto normal = TestStageBlock(200);
  const auto poison_key = poison.handle.StoreKey();

  std::atomic<int> poison_puts(0), normal_puts(0);
  EXPECT_CALL(accesser_, AsyncPut(_, _))
      .WillRepeatedly(Invoke([&](const std::string& key,
                                 std::shared_ptr<PutObjectAsyncContext> ctx) {
        if (key == poison_key) {
          poison_puts++;
          ctx->status = Status::IoError("inject io error");
        } else {
          normal_puts++;
          ctx->status = Status::OK();
        }
        ctx->cb(ctx);
      }));

  uploader_->EnterUploadQueue(poison);
  ASSERT_TRUE(WaitUntil([&]() { return poison_puts >= 1; }, 3000));

  // the poison block is waiting for its 5s slow requeue now, the only
  // inflight slot must be free for the normal block
  uploader_->EnterUploadQueue(normal);
  ASSERT_TRUE(WaitUntil([&]() { return normal_puts >= 1; }, 2000));
}

TEST_F(BlockCacheUploaderTest, RequeueOnStoragePutNotFound) {
  FLAGS_upload_stage_max_tries = 1;
  FLAGS_upload_stage_retry_delay_s = 1;
  Init();

  // storage 404 (e.g. missing bucket) must NOT be treated like the
  // stage-file-deleted NotFound: the block stays on the slow retry cycle
  // and the stage file is kept. Shared-ptr captures: the retry cycle can
  // fire the mock after the test body returns.
  auto puts = std::make_shared<std::atomic<int>>(0);
  EXPECT_CALL(accesser_, AsyncPut(_, _))
      .WillRepeatedly(Invoke([puts](const std::string&,
                                    std::shared_ptr<PutObjectAsyncContext>
                                        ctx) {
        (*puts)++;
        ctx->status = Status::NotFound("inject no such bucket");
        ctx->cb(ctx);
      }));
  EXPECT_CALL(*store_, RemoveStage(_, _)).Times(0);

  uploader_->EnterUploadQueue(TestStageBlock());
  ASSERT_TRUE(WaitUntil([&]() { return *puts >= 2; }, 5000));
}

TEST_F(BlockCacheUploaderTest, SkipDuplicateInflightUpload) {
  Init();

  std::atomic<int> puts(0), removes(0);
  std::atomic<bool> release(false);
  EXPECT_CALL(accesser_, AsyncPut(_, _))
      .WillRepeatedly(Invoke(
          [&](const std::string&, std::shared_ptr<PutObjectAsyncContext> ctx) {
            puts++;
            while (!release) {
              bthread_usleep(10 * 1000);
            }
            ctx->status = Status::OK();
            ctx->cb(ctx);
          }));
  EXPECT_CALL(*store_, RemoveStage(_, _))
      .WillRepeatedly(Invoke([&](BlockHandle, CacheStore::RemoveStageOption) {
        removes++;
        return Status::OK();
      }));

  const auto sblock = TestStageBlock(100);
  uploader_->EnterUploadQueue(sblock);
  ASSERT_TRUE(WaitUntil([&]() { return puts == 1; }, 3000));

  // same block again while the first upload is inflight: must be skipped,
  // a duplicate upload bthread would corrupt the inflight accounting
  uploader_->EnterUploadQueue(sblock);
  bthread_usleep(300 * 1000);
  ASSERT_EQ(puts, 1);

  release = true;
  ASSERT_TRUE(WaitUntil([&]() { return removes == 1; }, 3000));
  bthread_usleep(300 * 1000);
  ASSERT_EQ(puts, 1);
}

TEST_F(BlockCacheUploaderTest, EnterUploadQueueAfterShutdown) {
  Init();

  EXPECT_CALL(accesser_, AsyncPut(_, _)).Times(0);

  uploader_->Shutdown();
  uploader_->EnterUploadQueue(TestStageBlock());  // no crash, silently kept

  bthread_usleep(300 * 1000);
}

TEST_F(BlockCacheUploaderTest, LoadNotFoundSkipsUpload) {
  Init();

  // A block that vanished before upload must not be put, have its stage
  // removed, or be re-enqueued.
  std::atomic<int> loads(0);
  EXPECT_CALL(*store_, Load(_, _, _, _, _))
      .WillRepeatedly(Invoke(
          [&](BlockHandle, off_t, size_t, IOBuffer*, CacheStore::LoadOption) {
            loads++;
            return Status::NotFound("inject not found");
          }));
  EXPECT_CALL(accesser_, AsyncPut(_, _)).Times(0);
  EXPECT_CALL(*store_, RemoveStage(_, _)).Times(0);

  uploader_->EnterUploadQueue(TestStageBlock());
  ASSERT_TRUE(WaitUntil([&]() { return loads == 1; }, 3000));

  bthread_usleep(500 * 1000);  // no retry should happen
  ASSERT_EQ(loads, 1);
}

TEST_F(BlockCacheUploaderTest, CacheDownIsNotRetried) {
  Init();

  // Cache went down mid-upload: DoUpload returns CacheDown, OnComplete logs
  // and gives up (no re-queue, no storage put, no RemoveStage).
  std::atomic<int> loads(0);
  EXPECT_CALL(*store_, Load(_, _, _, _, _))
      .WillRepeatedly(Invoke(
          [&](BlockHandle, off_t, size_t, IOBuffer*, CacheStore::LoadOption) {
            loads++;
            return Status::CacheDown("cache down");
          }));
  EXPECT_CALL(accesser_, AsyncPut(_, _)).Times(0);
  EXPECT_CALL(*store_, RemoveStage(_, _)).Times(0);

  uploader_->EnterUploadQueue(TestStageBlock());
  ASSERT_TRUE(WaitUntil([&]() { return loads == 1; }, 3000));

  bthread_usleep(500 * 1000);  // no retry should happen
  ASSERT_EQ(loads, 1);
}

TEST_F(BlockCacheUploaderTest, ShutdownDuringSlowRequeueReturnsPromptly) {
  FLAGS_upload_stage_max_tries = 1;
  FLAGS_upload_stage_retry_delay_s = 60;
  Init();

  std::atomic<int> puts(0);
  EXPECT_CALL(accesser_, AsyncPut(_, _))
      .WillRepeatedly(Invoke(
          [&](const std::string&, std::shared_ptr<PutObjectAsyncContext> ctx) {
            puts++;
            ctx->status = Status::IoError("inject io error");
            ctx->cb(ctx);
          }));

  uploader_->EnterUploadQueue(TestStageBlock());
  ASSERT_TRUE(WaitUntil([&]() { return puts >= 1; }, 3000));
  bthread_usleep(200 * 1000);  // let it enter the slow requeue wait

  auto start_us = butil::gettimeofday_us();
  uploader_->Shutdown();
  auto elapsed_ms = (butil::gettimeofday_us() - start_us) / 1000;
  ASSERT_LT(elapsed_ms, 3000);  // way below the 60s slow cycle
}

}  // namespace cache
}  // namespace dingofs
