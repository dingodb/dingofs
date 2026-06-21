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

#include "cache/blockcache/block_cache_impl.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <thread>

#include "cache/common/mock/mock_storage_client_pool.h"
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
using ::testing::NiceMock;
using blockaccess::GetObjectAsyncContext;
using blockaccess::MockBlockAccesser;
using blockaccess::PutObjectAsyncContext;

DECLARE_string(cache_store);
DECLARE_bool(enable_stage);
DECLARE_bool(enable_cache);

// Exercises BlockCacheImpl backed by an in-memory MemCache (FLAGS_cache_store =
// "memory"), so no disk/RDMA hardware is required. A real StorageClient over a
// mock BlockAccesser serves the background uploader and the storage-fetch path.
class BlockCacheImplTest : public ::testing::Test {
 protected:
  void SetUp() override {
    saved_store_ = FLAGS_cache_store;
    FLAGS_cache_store = "memory";

    // Default storage IO succeeds immediately so the background upload worker
    // never blocks in StorageClient::Put()/Range()::Wait().
    ON_CALL(accesser_, AsyncPut(_, _))
        .WillByDefault(Invoke([](const std::string&,
                                 std::shared_ptr<PutObjectAsyncContext> ctx) {
          ctx->status = Status::OK();
          ctx->cb(ctx);
        }));
    ON_CALL(accesser_, AsyncGet(_, _))
        .WillByDefault(Invoke([](const std::string&,
                                 std::shared_ptr<GetObjectAsyncContext> ctx) {
          ctx->actual_len = ctx->len;  // must equal requested length
          ctx->status = Status::OK();
          ctx->cb(ctx);
        }));

    storage_client_ = std::make_unique<StorageClient>(&accesser_);
    ASSERT_TRUE(storage_client_->Start().ok());

    pool_ = std::make_shared<NiceMock<MockStorageClientPool>>();
    ON_CALL(*pool_, GetStorageClient(_, _))
        .WillByDefault(Invoke([this](uint32_t, StorageClient** out) {
          *out = storage_client_.get();
          return Status::OK();
        }));

    block_cache_ = std::make_unique<BlockCacheImpl>(pool_);
    ASSERT_TRUE(block_cache_->Start().ok());
  }

  void TearDown() override {
    block_cache_->Shutdown();
    storage_client_->Shutdown();
    FLAGS_cache_store = saved_store_;
  }

  static BlockHandle Handle(uint64_t id) {
    return BlockHandle(1, BlockKey(id, 0, 4194304));
  }
  static IOBuffer Buf(const std::string& s) {
    return IOBuffer(s.data(), s.size());
  }
  static std::string ReadAll(IOBuffer& buf) {
    std::string out(buf.Size(), '\0');
    buf.CopyTo(out.data());
    return out;
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
  NiceMock<MockBlockAccesser> accesser_;
  std::unique_ptr<StorageClient> storage_client_;
  std::shared_ptr<NiceMock<MockStorageClientPool>> pool_;
  std::unique_ptr<BlockCacheImpl> block_cache_;
};

TEST_F(BlockCacheImplTest, EnableFlags) {
  EXPECT_TRUE(block_cache_->IsEnabled());
  EXPECT_TRUE(block_cache_->EnableStage());
  EXPECT_TRUE(block_cache_->EnableCache());

  {
    FLAGS_enable_stage = false;
    EXPECT_FALSE(block_cache_->EnableStage());
    FLAGS_enable_stage = true;
  }
  {
    FLAGS_enable_cache = false;
    EXPECT_FALSE(block_cache_->EnableCache());
    FLAGS_enable_cache = true;
  }
  {
    FLAGS_cache_store = "none";
    EXPECT_FALSE(block_cache_->IsEnabled());
    EXPECT_FALSE(block_cache_->EnableStage());
    FLAGS_cache_store = "memory";
  }
}

TEST_F(BlockCacheImplTest, PutCachesAndRangeReadsBack) {
  ASSERT_TRUE(block_cache_->Put(Handle(1), Buf("hello")).ok());
  EXPECT_TRUE(block_cache_->IsCached(Handle(1)));

  IOBuffer buffer;
  ASSERT_TRUE(block_cache_->Range(Handle(1), 0, 5, &buffer).ok());
  EXPECT_EQ(ReadAll(buffer), "hello");
}

TEST_F(BlockCacheImplTest, CacheStoresAndRangeReadsBack) {
  ASSERT_TRUE(block_cache_->Cache(Handle(2), Buf("world")).ok());
  EXPECT_TRUE(block_cache_->IsCached(Handle(2)));

  IOBuffer buffer;
  ASSERT_TRUE(block_cache_->Range(Handle(2), 0, 5, &buffer).ok());
  EXPECT_EQ(ReadAll(buffer), "world");
}

TEST_F(BlockCacheImplTest, RangeMissReturnsNotFound) {
  // BlockCacheImpl::Range is cache-only; a miss is NotFound, not a storage read.
  IOBuffer buffer;
  EXPECT_TRUE(block_cache_->Range(Handle(999), 0, 1, &buffer).IsNotFound());
}

TEST_F(BlockCacheImplTest, PrefetchAlreadyCachedReturnsOk) {
  ASSERT_TRUE(block_cache_->Cache(Handle(3), Buf("data")).ok());
  EXPECT_TRUE(block_cache_->Prefetch(Handle(3), 4).ok());
}

TEST_F(BlockCacheImplTest, PrefetchFetchesFromStorageAndCaches) {
  // Regression #2: Prefetch used to hand StorageRange an empty IOBuffer, which
  // tripped IOBuffer::Fetch1()'s single-backing-block CHECK and crashed.
  auto handle = Handle(20);
  EXPECT_FALSE(block_cache_->IsCached(handle));

  ASSERT_TRUE(block_cache_->Prefetch(handle, 4096).ok());
  EXPECT_TRUE(block_cache_->IsCached(handle));
}

TEST_F(BlockCacheImplTest, AsyncCacheInvokesCallback) {
  std::atomic<bool> done{false};
  Status result;
  block_cache_->AsyncCache(Handle(10), Buf("async"), [&](Status s) {
    result = s;
    done.store(true);
  });

  ASSERT_TRUE(WaitUntil([&done]() { return done.load(); }));
  EXPECT_TRUE(result.ok());
  EXPECT_TRUE(block_cache_->IsCached(Handle(10)));
}

TEST_F(BlockCacheImplTest, AsyncRangeInvokesCallback) {
  ASSERT_TRUE(block_cache_->Cache(Handle(11), Buf("range")).ok());

  std::atomic<bool> done{false};
  Status result;
  IOBuffer buffer;
  block_cache_->AsyncRange(Handle(11), 0, 5, &buffer, [&](Status s) {
    result = s;
    done.store(true);
  });

  ASSERT_TRUE(WaitUntil([&done]() { return done.load(); }));
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(ReadAll(buffer), "range");
}

TEST_F(BlockCacheImplTest, PutWriteback) {
  PutOption option;
  option.writeback = true;
  ASSERT_TRUE(block_cache_->Put(Handle(12), Buf("wb"), option).ok());
  EXPECT_TRUE(block_cache_->IsCached(Handle(12)));
}

TEST_F(BlockCacheImplTest, AsyncPutInvokesCallback) {
  std::atomic<bool> done{false};
  Status result;
  block_cache_->AsyncPut(Handle(13), Buf("aput"), [&](Status s) {
    result = s;
    done.store(true);
  });

  ASSERT_TRUE(WaitUntil([&done]() { return done.load(); }));
  EXPECT_TRUE(result.ok());
  EXPECT_TRUE(block_cache_->IsCached(Handle(13)));
}

TEST_F(BlockCacheImplTest, AsyncPrefetchInvokesCallback) {
  auto handle = Handle(14);
  std::atomic<bool> done{false};
  Status result;
  block_cache_->AsyncPrefetch(handle, 4096, [&](Status s) {
    result = s;
    done.store(true);
  });

  ASSERT_TRUE(WaitUntil([&done]() { return done.load(); }));
  EXPECT_TRUE(result.ok());
  EXPECT_TRUE(block_cache_->IsCached(handle));
}

TEST_F(BlockCacheImplTest, RangeStorageFetchFailurePropagates) {
  // Prefetch surfaces a storage failure (GetStorageClient ok but Range fails).
  ::testing::Mock::VerifyAndClearExpectations(&accesser_);
  EXPECT_CALL(accesser_, AsyncGet(::testing::_, ::testing::_))
      .WillRepeatedly(::testing::Invoke(
          [](const std::string&,
             std::shared_ptr<blockaccess::GetObjectAsyncContext> ctx) {
            ctx->status = Status::NotFound("missing");
            ctx->cb(ctx);
          }));

  EXPECT_TRUE(block_cache_->Prefetch(Handle(15), 4096).IsNotFound());
  EXPECT_FALSE(block_cache_->IsCached(Handle(15)));
}

}  // namespace cache
}  // namespace dingofs
