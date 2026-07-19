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

#include <atomic>
#include <cstring>
#include <memory>

#include "cache/blockcache/block_cache_uploader.h"
#include "cache/common/storage_client.h"
#include "cache/common/storage_client_pool.h"
#include "cache/iutil/string_util.h"
#include "common/options/cache.h"
#include "mock/mock_cache_store.h"
#include "cache/common/mock/mock_block_accesser.h"

namespace dingofs {
namespace cache {

using ::testing::_;
using ::testing::Invoke;
using ::testing::NiceMock;
using ::testing::Return;

using blockaccess::MockBlockAccesser;
using blockaccess::PutObjectAsyncContext;

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
    ON_CALL(*store_, Load(_, _, _, _, _, _))
        .WillByDefault(Invoke([](ContextSPtr, const BlockKey&, off_t,
                                 size_t length, IOBuffer* buffer,
                                 CacheStore::LoadOption) {
          char* data = new char[length];
          std::memset(data, 'x', length);
          buffer->AppendUserData(data, length, iutil::DeleteBuffer);
          return Status::OK();
        }));
    ON_CALL(*store_, RemoveStage(_, _, _))
        .WillByDefault(Return(Status::OK()));

    storage_client_ = std::make_unique<StorageClient>(&mock_accesser_);
    ASSERT_TRUE(storage_client_->Start().ok());

    uploader_ = std::make_unique<BlockCacheUploader>(
        store_,
        std::make_shared<SingletonStorageClient>(storage_client_.get()));
    uploader_->Start();
  }

  static StageBlock TestStageBlock(uint64_t ino = 100) {
    return StageBlock(NewContext(), BlockKey(1, ino, 2000, 0, 1), 4096,
                      BlockAttr(BlockAttr::kFromWriteback));
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

  MockBlockAccesser mock_accesser_;
  StorageClientUPtr storage_client_;
  std::shared_ptr<NiceMock<MockCacheStore>> store_;
  BlockCacheUploaderUPtr uploader_;

  uint32_t saved_max_inflights_;
  uint32_t saved_max_tries_;
  uint32_t saved_retry_delay_s_;
};

TEST_F(BlockCacheUploaderTest, UploadAndRemoveStage) {
  Init();

  std::atomic<int> puts(0), removes(0);
  EXPECT_CALL(mock_accesser_, AsyncPut(_))
      .WillOnce(Invoke([&](std::shared_ptr<PutObjectAsyncContext> ctx) {
        puts++;
        ctx->status = Status::OK();
        ctx->cb(ctx);
      }));
  EXPECT_CALL(*store_, RemoveStage(_, _, _))
      .WillOnce(Invoke([&](ContextSPtr, const BlockKey&,
                           CacheStore::RemoveStageOption) {
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
  EXPECT_CALL(mock_accesser_, AsyncPut(_))
      .WillRepeatedly(Invoke([&](std::shared_ptr<PutObjectAsyncContext> ctx) {
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
  const auto poison_key = poison.key.StoreKey();

  std::atomic<int> poison_puts(0), normal_puts(0);
  EXPECT_CALL(mock_accesser_, AsyncPut(_))
      .WillRepeatedly(Invoke([&](std::shared_ptr<PutObjectAsyncContext> ctx) {
        if (ctx->key == poison_key) {
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

TEST_F(BlockCacheUploaderTest, NoRequeueOnStoreNotFound) {
  Init();

  std::atomic<int> loads(0);
  EXPECT_CALL(*store_, Load(_, _, _, _, _, _))
      .WillRepeatedly(Invoke([&](ContextSPtr, const BlockKey&, off_t, size_t,
                                 IOBuffer*, CacheStore::LoadOption) {
        loads++;
        return Status::NotFound("inject not found");
      }));
  EXPECT_CALL(mock_accesser_, AsyncPut(_)).Times(0);

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
  EXPECT_CALL(mock_accesser_, AsyncPut(_))
      .WillRepeatedly(Invoke([&](std::shared_ptr<PutObjectAsyncContext> ctx) {
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
