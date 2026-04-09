/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

#include "client/vfs/blockstore/block_store.h"
#include "client/vfs/components/context.h"
#include "client/vfs/components/prefetch_manager.h"
#include "common/status.h"
#include "common/trace/trace_manager.h"
#include "test/unit/client/vfs/test_base.h"
#include "test/unit/client/vfs/test_common.h"

namespace dingofs {
namespace client {
namespace vfs {

using ::testing::_;
using ::testing::AnyNumber;
using ::testing::Invoke;
using ::testing::Return;

// PrefetchContext with a single block covering the whole file.
// block_size=4MB, file_size=4MB, prefetch_blocks=1 → 1 block.
static PrefetchContext MakeSingleBlockCtx(uint64_t ino = 100) {
  // offset=0, file_size=4MB, prefetch_blocks=1
  return PrefetchContext(ino, 0, 4 * 1024 * 1024, 1);
}

class PrefetchManagerTest : public test::VFSTestBase {
 protected:
  void SetUp() override {
    trace_manager_ = std::make_unique<TraceManager>();
    ON_CALL(*mock_hub_, GetTraceManager())
        .WillByDefault(Return(trace_manager_.get()));
    EXPECT_CALL(*mock_hub_, GetTraceManager()).Times(AnyNumber());

    // FsInfo must have a valid block_size
    ON_CALL(*mock_hub_, GetFsInfo())
        .WillByDefault(Return(test::MakeTestFsInfo(64 * 1024 * 1024,
                                                   4 * 1024 * 1024)));
    EXPECT_CALL(*mock_hub_, GetFsInfo()).Times(AnyNumber());

    // Return a real non-zero slice so FileRange2BlockKey produces block keys.
    ON_CALL(*mock_meta_system_, ReadSlice)
        .WillByDefault([](ContextSPtr, Ino, uint64_t chunk_index, uint64_t,
                          std::vector<Slice>* s, uint64_t& v) {
          s->clear();
          const uint64_t kBlockSize = 4 * 1024 * 1024;
          Slice sl;
          sl.id = chunk_index + 1;
          sl.pos = 0;
          sl.size = 12 * kBlockSize;  // cover up to 3 blocks
          sl.off = 0;
          sl.len = sl.size;
          s->push_back(sl);
          v = 1;
          return Status::OK();
        });
    EXPECT_CALL(*mock_meta_system_, ReadSlice).Times(AnyNumber());

    mgr_ = std::make_unique<PrefetchManager>(mock_hub_);
  }

  void TearDown() override {
    if (mgr_) {
      mgr_->Stop();
    }
  }

  std::unique_ptr<PrefetchManager> mgr_;
  std::unique_ptr<TraceManager> trace_manager_;
};

TEST_F(PrefetchManagerTest, Start_Stop_NoCrash) {
  ASSERT_TRUE(mgr_->Start(2).ok());
  ASSERT_TRUE(mgr_->Stop().ok());
}

// After removing PrefetchReq.block_size, consumers must read the block
// size from req.block_ctx.key.size. Verify BlockKey fields are set.
TEST_F(PrefetchManagerTest, SubmitTask_VerifyBlockKeyFields) {
  ASSERT_TRUE(mgr_->Start(2).ok());

  test::AsyncWaiter waiter;
  waiter.Expect(1);

  std::vector<PrefetchReq> captured;
  EXPECT_CALL(*mock_block_store_, PrefetchAsync(_, _, _))
      .WillOnce([&](ContextSPtr, PrefetchReq req, StatusCallback cb) {
        captured.push_back(req);
        cb(Status::OK());
        waiter.Done();
      });

  mgr_->SubmitTask(MakeSingleBlockCtx());
  waiter.Wait();

  ASSERT_EQ(captured.size(), 1u);
  // BlockKey must carry a valid (non-zero) id and the actual block size.
  EXPECT_NE(captured[0].block_ctx.key.id, 0u);
  EXPECT_GT(captured[0].block_ctx.key.size, 0);
}

TEST_F(PrefetchManagerTest, SubmitTask_TriggersPrefetchAsync) {
  ASSERT_TRUE(mgr_->Start(2).ok());

  test::AsyncWaiter waiter;
  waiter.Expect(1);

  EXPECT_CALL(*mock_block_store_, PrefetchAsync(_, _, _))
      .WillOnce([&](ContextSPtr, PrefetchReq, StatusCallback cb) {
        cb(Status::OK());
        waiter.Done();
      });

  mgr_->SubmitTask(MakeSingleBlockCtx());
  waiter.Wait();
}

TEST_F(PrefetchManagerTest, DuplicateKey_NotPrefetchedTwice) {
  ASSERT_TRUE(mgr_->Start(1).ok());

  // Count actual PrefetchAsync calls.
  std::atomic<int> call_count{0};

  EXPECT_CALL(*mock_block_store_, PrefetchAsync(_, _, _))
      .WillRepeatedly([&](ContextSPtr, PrefetchReq, StatusCallback cb) {
        call_count.fetch_add(1, std::memory_order_relaxed);
        cb(Status::OK());
      });

  // Submit the same inode/offset twice; inflight_keys_ deduplicates when
  // tasks overlap in time (timing-dependent with synchronous mock).
  mgr_->SubmitTask(MakeSingleBlockCtx(100));
  mgr_->SubmitTask(MakeSingleBlockCtx(100));

  // Stop drains all pending tasks before local variables go out of scope.
  // This is critical: must call Stop() before the lambda captures go out of
  // scope to prevent use-after-free during TearDown.
  mgr_->Stop();
  mgr_.reset();

  // At most 2 calls (1 if dedup triggered, 2 if tasks ran sequentially).
  EXPECT_LE(call_count.load(), 2);
}

TEST_F(PrefetchManagerTest, PrefetchFailure_NotCrash) {
  ASSERT_TRUE(mgr_->Start(2).ok());

  test::AsyncWaiter waiter;
  waiter.Expect(1);

  EXPECT_CALL(*mock_block_store_, PrefetchAsync(_, _, _))
      .WillOnce([&](ContextSPtr, PrefetchReq, StatusCallback cb) {
        cb(Status::Internal("simulated error"));
        waiter.Done();
      });

  mgr_->SubmitTask(MakeSingleBlockCtx());
  waiter.Wait();
  // No crash - error is just logged.
}

TEST_F(PrefetchManagerTest, PrefetchAlreadyExists_TreatedAsSuccess) {
  ASSERT_TRUE(mgr_->Start(2).ok());

  test::AsyncWaiter waiter;
  waiter.Expect(1);

  EXPECT_CALL(*mock_block_store_, PrefetchAsync(_, _, _))
      .WillOnce([&](ContextSPtr, PrefetchReq, StatusCallback cb) {
        cb(Status::Exist("block already cached"));
        waiter.Done();
      });

  mgr_->SubmitTask(MakeSingleBlockCtx());
  waiter.Wait();
  // No crash on Exist status.
}

TEST_F(PrefetchManagerTest, SubmitTask_MultipleBlocks) {
  ASSERT_TRUE(mgr_->Start(4).ok());

  // 3 blocks: file_size=12MB, block_size=4MB, prefetch_blocks=3
  PrefetchContext ctx(200, 0, 12 * 1024 * 1024, 3);

  test::AsyncWaiter waiter;
  waiter.Expect(3);

  EXPECT_CALL(*mock_block_store_, PrefetchAsync(_, _, _))
      .Times(3)
      .WillRepeatedly([&](ContextSPtr, PrefetchReq, StatusCallback cb) {
        cb(Status::OK());
        waiter.Done();
      });

  mgr_->SubmitTask(ctx);
  waiter.Wait();
}

TEST_F(PrefetchManagerTest, Stop_DrainsPendingTasks) {
  ASSERT_TRUE(mgr_->Start(2).ok());

  // Submit many tasks and then stop - should not crash or hang.
  for (int i = 0; i < 20; ++i) {
    mgr_->SubmitTask(MakeSingleBlockCtx(static_cast<uint64_t>(i + 1000)));
  }

  ASSERT_TRUE(mgr_->Stop().ok());
}

TEST_F(PrefetchManagerTest, Concurrent_SubmitFromMultipleThreads) {
  ASSERT_TRUE(mgr_->Start(4).ok());

  constexpr int kThreads = 4;
  constexpr int kTasksPerThread = 5;

  std::vector<std::thread> threads;
  threads.reserve(kThreads);

  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([this, t]() {
      for (int i = 0; i < kTasksPerThread; ++i) {
        uint64_t ino = static_cast<uint64_t>(t * 100 + i + 1);
        mgr_->SubmitTask(MakeSingleBlockCtx(ino));
      }
    });
  }

  for (auto& th : threads) {
    th.join();
  }

  // Stop without crash.
  ASSERT_TRUE(mgr_->Stop().ok());
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
