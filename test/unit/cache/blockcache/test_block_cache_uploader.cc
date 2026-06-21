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

#include "cache/blockcache/block_cache_uploader.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>

#include "cache/blockcache/cache_store.h"
#include "cache/blockcache/mock/mock_cache_store.h"
#include "cache/common/mock/mock_storage_client_pool.h"
#include "cache/common/storage_client.h"
#include "common/block/block_handle.h"
#include "common/block/block_key.h"
#include "common/blockaccess/accesser_common.h"
#include "common/io_buffer.h"
#include "test/unit/common/blockaccess/mock/mock_accesser.h"

namespace dingofs {
namespace cache {

using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::NiceMock;
using ::testing::Return;
using blockaccess::MockBlockAccesser;
using blockaccess::PutObjectAsyncContext;

class BlockCacheUploaderTest : public ::testing::Test {
 protected:
  static BlockHandle Handle(uint64_t id) {
    return BlockHandle(1, BlockKey(id, 0, 4194304));
  }

  template <typename Pred>
  static bool WaitUntil(Pred pred, int timeout_ms = 5000) {
    for (int waited = 0; waited < timeout_ms; waited += 10) {
      if (pred()) return true;
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return pred();
  }
};

TEST_F(BlockCacheUploaderTest, UploadSuccessRemovesStage) {
  auto store = std::make_shared<NiceMock<MockCacheStore>>();
  ON_CALL(*store, IsRunning()).WillByDefault(Return(true));
  // Load must hand back a single-block buffer (StorageClient::Put calls
  // IOBuffer::Fetch1, which requires exactly one backing block).
  ON_CALL(*store, Load(_, _, _, _, _))
      .WillByDefault(Invoke([](BlockHandle, off_t, size_t length,
                               IOBuffer* buffer, CacheStore::LoadOption) {
        std::string data(length, 'x');
        *buffer = IOBuffer(data.data(), data.size());
        return Status::OK();
      }));

  std::atomic<bool> removed{false};
  EXPECT_CALL(*store, RemoveStage(_, _))
      .WillOnce(DoAll(Invoke([&removed](BlockHandle, CacheStore::RemoveStageOption) {
                        removed = true;
                      }),
                      Return(Status::OK())));

  MockBlockAccesser accesser;
  EXPECT_CALL(accesser, AsyncPut(_, _))
      .WillOnce(Invoke([](const std::string&,
                          std::shared_ptr<PutObjectAsyncContext> ctx) {
        ctx->status = Status::OK();
        ctx->cb(ctx);
      }));
  StorageClient storage_client(&accesser);
  ASSERT_TRUE(storage_client.Start().ok());

  auto pool = std::make_shared<NiceMock<MockStorageClientPool>>();
  ON_CALL(*pool, GetStorageClient(_, _))
      .WillByDefault(Invoke([&storage_client](uint32_t, StorageClient** out) {
        *out = &storage_client;
        return Status::OK();
      }));

  BlockCacheUploader uploader(store, pool);
  uploader.Start();
  uploader.EnterUploadQueue(
      StageBlock(Handle(100), 5, BlockAttr(BlockAttr::kFromWriteback)));

  EXPECT_TRUE(WaitUntil([&removed]() { return removed.load(); }));

  uploader.Shutdown();
  ASSERT_TRUE(storage_client.Shutdown().ok());
}

TEST_F(BlockCacheUploaderTest, LoadNotFoundSkipsUpload) {
  auto store = std::make_shared<NiceMock<MockCacheStore>>();
  ON_CALL(*store, IsRunning()).WillByDefault(Return(true));
  ON_CALL(*store, Load(_, _, _, _, _))
      .WillByDefault(Return(Status::NotFound("deleted")));
  // A block that vanished before upload must not be put or have its stage
  // removed.
  EXPECT_CALL(*store, RemoveStage(_, _)).Times(0);

  MockBlockAccesser accesser;
  EXPECT_CALL(accesser, AsyncPut(_, _)).Times(0);
  StorageClient storage_client(&accesser);
  ASSERT_TRUE(storage_client.Start().ok());

  auto pool = std::make_shared<NiceMock<MockStorageClientPool>>();
  ON_CALL(*pool, GetStorageClient(_, _))
      .WillByDefault(Invoke([&storage_client](uint32_t, StorageClient** out) {
        *out = &storage_client;
        return Status::OK();
      }));

  BlockCacheUploader uploader(store, pool);
  uploader.Start();
  uploader.EnterUploadQueue(
      StageBlock(Handle(101), 5, BlockAttr(BlockAttr::kFromWriteback)));

  // Give the worker time to process the (no-op) upload, then shut down so the
  // Times(0) expectations are verified.
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  uploader.Shutdown();
  ASSERT_TRUE(storage_client.Shutdown().ok());
}

TEST_F(BlockCacheUploaderTest, CacheDownIsNotRetried) {
  auto store = std::make_shared<NiceMock<MockCacheStore>>();
  ON_CALL(*store, IsRunning()).WillByDefault(Return(true));
  // Cache went down mid-upload: DoUpload returns CacheDown, OnComplete logs and
  // gives up (no re-queue, no storage put, no RemoveStage).
  ON_CALL(*store, Load(_, _, _, _, _))
      .WillByDefault(Return(Status::CacheDown("cache down")));
  EXPECT_CALL(*store, RemoveStage(_, _)).Times(0);

  MockBlockAccesser accesser;
  EXPECT_CALL(accesser, AsyncPut(_, _)).Times(0);
  StorageClient storage_client(&accesser);
  ASSERT_TRUE(storage_client.Start().ok());

  auto pool = std::make_shared<NiceMock<MockStorageClientPool>>();
  ON_CALL(*pool, GetStorageClient(_, _))
      .WillByDefault(Invoke([&storage_client](uint32_t, StorageClient** out) {
        *out = &storage_client;
        return Status::OK();
      }));

  BlockCacheUploader uploader(store, pool);
  uploader.Start();
  uploader.EnterUploadQueue(
      StageBlock(Handle(102), 5, BlockAttr(BlockAttr::kFromWriteback)));

  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  uploader.Shutdown();
  ASSERT_TRUE(storage_client.Shutdown().ok());
}

}  // namespace cache
}  // namespace dingofs
