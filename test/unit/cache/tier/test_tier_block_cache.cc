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

#include "cache/tier/tier_block_cache.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
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

using ::testing::_;
using ::testing::Invoke;
using blockaccess::GetObjectAsyncContext;
using blockaccess::MockBlockAccesser;
using blockaccess::PutObjectAsyncContext;

DECLARE_string(cache_store);
DECLARE_string(cache_group);

// With no disk store and no cache group, both tiers are no-op base BlockCaches,
// so TierBlockCache degrades to a pure pass-through to the storage client. That
// is the configuration we can exercise without RDMA/disk hardware.
class TierBlockCacheTest : public ::testing::Test {
 protected:
  void SetUp() override {
    saved_store_ = FLAGS_cache_store;
    saved_group_ = FLAGS_cache_group;
    FLAGS_cache_store = "none";
    FLAGS_cache_group = "";

    tier_ = std::make_unique<TierBlockCache>(
        std::make_unique<StorageClient>(&accesser_));
    ASSERT_TRUE(tier_->Start().ok());
  }

  void TearDown() override {
    tier_->Shutdown();
    FLAGS_cache_store = saved_store_;
    FLAGS_cache_group = saved_group_;
  }

  static BlockHandle Handle(uint64_t id) {
    return BlockHandle(1, BlockKey(id, 0, 4194304));
  }
  static IOBuffer Buf(const std::string& s) {
    return IOBuffer(s.data(), s.size());
  }

  template <typename Pred>
  static bool WaitUntil(Pred pred, int timeout_ms = 3000) {
    for (int waited = 0; waited < timeout_ms; waited += 10) {
      if (pred()) return true;
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return pred();
  }

  std::string saved_store_;
  std::string saved_group_;
  MockBlockAccesser accesser_;
  std::unique_ptr<TierBlockCache> tier_;
};

TEST_F(TierBlockCacheTest, CachesDisabled) {
  EXPECT_FALSE(tier_->IsEnabled());
  EXPECT_FALSE(tier_->EnableStage());
  EXPECT_FALSE(tier_->EnableCache());
  EXPECT_FALSE(tier_->IsCached(Handle(1)));
}

TEST_F(TierBlockCacheTest, PutFallsThroughToStorage) {
  EXPECT_CALL(accesser_, AsyncPut(_, _))
      .WillOnce(Invoke([](const std::string&,
                          std::shared_ptr<PutObjectAsyncContext> ctx) {
        ctx->status = Status::OK();
        ctx->cb(ctx);
      }));

  EXPECT_TRUE(tier_->Put(Handle(100), Buf("hello")).ok());
}

TEST_F(TierBlockCacheTest, PutWritebackFallsBackToStorageWhenNoLayer) {
  EXPECT_CALL(accesser_, AsyncPut(_, _))
      .WillOnce(Invoke([](const std::string&,
                          std::shared_ptr<PutObjectAsyncContext> ctx) {
        ctx->status = Status::OK();
        ctx->cb(ctx);
      }));

  PutOption option;
  option.writeback = true;
  EXPECT_TRUE(tier_->Put(Handle(101), Buf("hello"), option).ok());
}

TEST_F(TierBlockCacheTest, RangeFallsThroughToStorage) {
  const size_t length = 5;
  EXPECT_CALL(accesser_, AsyncGet(_, _))
      .WillOnce(Invoke([length](const std::string&,
                                std::shared_ptr<GetObjectAsyncContext> ctx) {
        ctx->actual_len = length;
        ctx->status = Status::OK();
        ctx->cb(ctx);
      }));

  std::string storage(length, '\0');
  IOBuffer buffer(storage.data(), storage.size());
  EXPECT_TRUE(tier_->Range(Handle(200), 0, length, &buffer).ok());
}

TEST_F(TierBlockCacheTest, RangeReturnsNotFoundWhenStorageDisabled) {
  std::string storage(5, '\0');
  IOBuffer buffer(storage.data(), storage.size());
  RangeOption option;
  option.retrieve_storage = false;

  EXPECT_TRUE(tier_->Range(Handle(201), 0, storage.size(), &buffer, option)
                  .IsNotFound());
}

TEST_F(TierBlockCacheTest, CacheWithoutLayerReturnsNotFound) {
  EXPECT_TRUE(tier_->Cache(Handle(300), Buf("x")).IsNotFound());
}

TEST_F(TierBlockCacheTest, PrefetchWithoutLayerReturnsNotFound) {
  EXPECT_TRUE(tier_->Prefetch(Handle(350), 4096).IsNotFound());
}

TEST_F(TierBlockCacheTest, AsyncPutInvokesCallback) {
  EXPECT_CALL(accesser_, AsyncPut(_, _))
      .WillOnce(Invoke([](const std::string&,
                          std::shared_ptr<PutObjectAsyncContext> ctx) {
        ctx->status = Status::OK();
        ctx->cb(ctx);
      }));

  std::atomic<bool> done{false};
  Status result;
  tier_->AsyncPut(Handle(400), Buf("async"), [&](Status s) {
    result = s;
    done.store(true);
  });

  ASSERT_TRUE(WaitUntil([&done]() { return done.load(); }));
  EXPECT_TRUE(result.ok());
}

TEST_F(TierBlockCacheTest, AsyncRangeInvokesCallback) {
  const size_t length = 5;
  EXPECT_CALL(accesser_, AsyncGet(_, _))
      .WillOnce(Invoke([length](const std::string&,
                                std::shared_ptr<GetObjectAsyncContext> ctx) {
        ctx->actual_len = length;
        ctx->status = Status::OK();
        ctx->cb(ctx);
      }));

  std::string storage(length, '\0');
  IOBuffer buffer(storage.data(), storage.size());
  std::atomic<bool> done{false};
  Status result;
  tier_->AsyncRange(Handle(401), 0, length, &buffer, [&](Status s) {
    result = s;
    done.store(true);
  });

  ASSERT_TRUE(WaitUntil([&done]() { return done.load(); }));
  EXPECT_TRUE(result.ok());
}

TEST_F(TierBlockCacheTest, AsyncCacheInvokesCallback) {
  std::atomic<bool> done{false};
  Status result;
  tier_->AsyncCache(Handle(402), Buf("async"), [&](Status s) {
    result = s;
    done.store(true);
  });

  ASSERT_TRUE(WaitUntil([&done]() { return done.load(); }));
  EXPECT_TRUE(result.IsNotFound());
}

TEST_F(TierBlockCacheTest, AsyncPrefetchInvokesCallback) {
  std::atomic<bool> done{false};
  Status result;
  tier_->AsyncPrefetch(Handle(403), 4096, [&](Status s) {
    result = s;
    done.store(true);
  });

  ASSERT_TRUE(WaitUntil([&done]() { return done.load(); }));
  EXPECT_TRUE(result.IsNotFound());
}

}  // namespace cache
}  // namespace dingofs
