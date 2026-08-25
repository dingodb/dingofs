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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <memory>
#include <mutex>
#include <random>
#include <thread>
#include <vector>

#include "client/vfs/data/reader/chunk_read_op.h"
#include "client/vfs/data/reader/chunk_reader.h"
#include "common/trace/trace_manager.h"
#include "test/unit/client/vfs/test_base.h"

namespace dingofs {
namespace client {
namespace vfs {

using ::testing::_;
using ::testing::AnyNumber;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;

namespace {

constexpr uint64_t kTestIno = 300;
constexpr uint64_t kTestFh = 3;

// One captured block-store callback: the release side of a deferred read.
struct PendingCallback {
  StatusCallback cb;
  RangeReq req;
};

// Callback registry shared between the RangeAsync mock and the test body.
// Fill() runs inline in the mock (dispatch thread); Release() runs on test
// threads in arbitrary order.
struct CallbackRegistry {
  std::mutex mtx;
  std::vector<PendingCallback> pending;
  int range_calls{0};

  void Capture(StatusCallback cb, RangeReq req) {
    std::lock_guard<std::mutex> lk(mtx);
    pending.push_back(PendingCallback{std::move(cb), req});
    ++range_calls;
  }
};

}  // namespace

class ChunkReadOpTest : public test::VFSTestBase {
 protected:
  void SetUp() override {
    trace_manager_ = std::make_unique<TraceManager>();
    ON_CALL(*mock_hub_, GetTraceManager())
        .WillByDefault(Return(trace_manager_.get()));
    EXPECT_CALL(*mock_hub_, GetTraceManager()).Times(AnyNumber());
  }

  // Slices covering [0, len) with one data slice; ReadSlice returns it.
  void SetOneSliceCovering(int64_t len) {
    std::vector<Slice> slices{Slice{.id = 1,
                                    .size = static_cast<int32_t>(len),
                                    .off = 0,
                                    .len = static_cast<int32_t>(len),
                                    .pos = 0}};
    ON_CALL(*mock_meta_system_, ReadSlice(_, kTestIno, _, _, _, _))
        .WillByDefault(DoAll(SetArgPointee<4>(slices), Return(Status::OK())));
    EXPECT_CALL(*mock_meta_system_, ReadSlice(_, kTestIno, _, _, _, _))
        .Times(AnyNumber());
  }

  static ChunkReq MakeReq(int64_t offset, int64_t len) {
    return ChunkReq(kTestIno, /*index=*/0, offset,
                    FileRange{.offset = offset, .len = len});
  }

  // The same slice layout SetOneSliceCovering publishes via ReadSlice; tests
  // that call StartChunkRead directly pass it as the slices argument.
  static std::vector<Slice> OneSliceCovering(int64_t len) {
    return {Slice{.id = 1,
                  .size = static_cast<int32_t>(len),
                  .off = 0,
                  .len = static_cast<int32_t>(len),
                  .pos = 0}};
  }

  std::unique_ptr<TraceManager> trace_manager_;
};

// Inline completion: no slice metadata -> single hole block, filled with
// zeros inline; final callback runs before StartChunkRead returns.
TEST_F(ChunkReadOpTest, InlineCompletion_AllHoles) {
  // ReadSlice default: empty slices -> the whole range is one hole.
  const int64_t kLen = 128 * 1024;
  std::vector<uint8_t> buf(kLen, 0xAB);  // poison: hole must overwrite

  int cb_count = 0;
  Status final_status;
  StartChunkRead(ctx_, mock_hub_, MakeReq(0, kLen), /*slices=*/{},
                 ReadBufView{buf.data(), 0, (size_t)kLen}, [&](Status s) {
                   final_status = s;
                   ++cb_count;
                 });

  // Everything completed inline.
  EXPECT_EQ(cb_count, 1);
  EXPECT_TRUE(final_status.ok());
  for (uint8_t v : buf) {
    EXPECT_EQ(v, 0) << "hole block must zero-fill the window";
  }
}

// 14.2 inline completion: BlockStore invokes the callback synchronously
// (test_base default) before RangeAsync returns.
TEST_F(ChunkReadOpTest, InlineCompletion_SyncStore) {
  const int64_t kLen = 8 * 1024 * 1024;  // 2 blocks @ 4MB block_size
  SetOneSliceCovering(kLen);
  std::vector<uint8_t> buf(kLen, 0xFF);

  int cb_count = 0;
  StartChunkRead(ctx_, mock_hub_, MakeReq(0, kLen), OneSliceCovering(kLen),
                 ReadBufView{buf.data(), 0, (size_t)kLen}, [&](Status s) {
                   ++cb_count;
                   EXPECT_TRUE(s.ok());
                 });

  EXPECT_EQ(cb_count, 1);
  // test_base's default RangeAsync zero-fills the window.
  for (uint8_t v : buf) {
    EXPECT_EQ(v, 0);
  }
}

// 14.1 multi-block out-of-order completion from multiple threads; final
// callback exactly once; each sub-window holds its own pattern.
TEST_F(ChunkReadOpTest, AsyncOutOfOrder_MultiBlocks) {
  const int64_t kLen = 16 * 1024 * 1024;  // 4 blocks @ 4MB
  SetOneSliceCovering(kLen);

  auto registry = std::make_shared<CallbackRegistry>();
  ON_CALL(*mock_block_store_, RangeAsync)
      .WillByDefault(
          Invoke([registry](ContextSPtr, RangeReq req, StatusCallback cb) {
            // RangeReq::offset is the offset inside each block (zero for these
            // aligned full-block requests), not the block ordinal. Dispatch is
            // serial, so assign the pattern from capture order under the
            // registry lock.
            std::lock_guard<std::mutex> lk(registry->mtx);
            const uint8_t pattern = 1 + registry->range_calls;
            std::memset(req.dst.data(), pattern, req.length);
            registry->pending.push_back(
                PendingCallback{std::move(cb), std::move(req)});
            ++registry->range_calls;
          }));

  std::vector<uint8_t> buf(kLen, 0);

  test::AsyncWaiter waiter;
  waiter.Expect(1);
  StartChunkRead(ctx_, mock_hub_, MakeReq(0, kLen), OneSliceCovering(kLen),
                 ReadBufView{buf.data(), 0, (size_t)kLen}, [&](Status s) {
                   EXPECT_TRUE(s.ok());
                   waiter.Done();
                 });

  ASSERT_EQ(registry->range_calls, 4);

  // Release callbacks from several threads in shuffled order.
  {
    std::vector<PendingCallback> cbs;
    {
      std::lock_guard<std::mutex> lk(registry->mtx);
      cbs = std::move(registry->pending);
    }
    std::shuffle(cbs.begin(), cbs.end(), std::mt19937(42));

    std::vector<std::thread> threads;
    for (auto& pc : cbs) {
      threads.emplace_back([cb = std::move(pc.cb)]() { cb(Status::OK()); });
    }
    for (auto& t : threads) {
      t.join();
    }
  }

  waiter.Wait();

  // Each 4MB window carries its own pattern -> no cross-window corruption.
  for (int b = 0; b < 4; ++b) {
    const uint8_t expect = 1 + b;
    const uint8_t* window = buf.data() + b * 4 * 1024 * 1024;
    for (size_t i = 0; i < 4 * 1024 * 1024; ++i) {
      ASSERT_EQ(window[i], expect) << "window " << b << " corrupted at +" << i;
    }
  }
}

// Deterministically covers the ownership handoff that replaces the original
// ReaderSharedState UAF: a non-final callback has published completion but its
// callback object is still on the return path when the final callback runs.
// The callback's shared_ptr must keep the operation alive until it returns.
TEST_F(ChunkReadOpTest, OperationLivesUntilNonFinalCallbackReturns) {
  const int64_t kLen = 8 * 1024 * 1024;  // 2 blocks
  SetOneSliceCovering(kLen);

  auto registry = std::make_shared<CallbackRegistry>();
  ON_CALL(*mock_block_store_, RangeAsync)
      .WillByDefault(
          Invoke([registry](ContextSPtr, RangeReq req, StatusCallback cb) {
            registry->Capture(std::move(cb), req);
          }));

  std::vector<uint8_t> buf(kLen, 0);
  std::atomic<int> final_count{0};
  Status final_status;

  auto op = std::make_shared<detail::ChunkReadOp>(
      mock_hub_, MakeReq(0, kLen), [&](Status status) {
        final_status = std::move(status);
        final_count.fetch_add(1, std::memory_order_relaxed);
      });
  std::weak_ptr<detail::ChunkReadOp> weak_op = op;
  op->Run(ctx_, OneSliceCovering(kLen),
          ReadBufView{buf.data(), 0, static_cast<size_t>(kLen)});
  op.reset();  // only BlockStore callbacks own the operation now

  std::vector<PendingCallback> callbacks;
  {
    std::lock_guard<std::mutex> lock(registry->mtx);
    callbacks = std::move(registry->pending);
  }
  ASSERT_EQ(callbacks.size(), 2);

  std::mutex gate_mutex;
  std::condition_variable gate_cv;
  bool non_final_returned = false;
  bool release_non_final = false;

  std::thread non_final([cb = std::move(callbacks[0].cb), &gate_mutex, &gate_cv,
                         &non_final_returned, &release_non_final]() mutable {
    cb(Status::OK());  // remaining: 2 -> 1
    std::unique_lock<std::mutex> lock(gate_mutex);
    non_final_returned = true;
    gate_cv.notify_all();
    gate_cv.wait(lock, [&release_non_final] { return release_non_final; });
  });

  {
    std::unique_lock<std::mutex> lock(gate_mutex);
    if (!gate_cv.wait_for(lock, std::chrono::seconds(5), [&non_final_returned] {
          return non_final_returned;
        })) {
      release_non_final = true;
      lock.unlock();
      gate_cv.notify_all();
      non_final.join();
      FAIL() << "non-final callback did not reach the return-path gate";
      return;
    }
  }

  StatusCallback final_cb = std::move(callbacks[1].cb);
  final_cb(Status::OK());  // remaining: 1 -> 0, runs Finish
  final_cb = nullptr;      // release the final callback's owning reference

  EXPECT_EQ(final_count.load(std::memory_order_relaxed), 1);
  EXPECT_TRUE(final_status.ok());
  EXPECT_FALSE(weak_op.expired())
      << "non-final callback must still own the operation";

  {
    std::lock_guard<std::mutex> lock(gate_mutex);
    release_non_final = true;
  }
  gate_cv.notify_all();
  non_final.join();

  EXPECT_TRUE(weak_op.expired());
}

// Error priority: NotFound (block 1) loses to Internal (block 0); the
// aggregate converts to EIO naming the higher-priority error.
TEST_F(ChunkReadOpTest, ErrorPriority_NonNotFoundWins) {
  const int64_t kLen = 8 * 1024 * 1024;  // 2 blocks
  SetOneSliceCovering(kLen);

  auto registry = std::make_shared<CallbackRegistry>();
  ON_CALL(*mock_block_store_, RangeAsync)
      .WillByDefault(
          Invoke([registry](ContextSPtr, RangeReq req, StatusCallback cb) {
            registry->Capture(std::move(cb), req);
          }));

  std::vector<uint8_t> buf(kLen, 0);

  test::AsyncWaiter waiter;
  waiter.Expect(1);
  StartChunkRead(ctx_, mock_hub_, MakeReq(0, kLen), OneSliceCovering(kLen),
                 ReadBufView{buf.data(), 0, (size_t)kLen}, [&](Status s) {
                   EXPECT_FALSE(s.ok());
                   EXPECT_TRUE(s.IsIoError()) << s.ToString();
                   // The Internal error must have survived the NotFound.
                   EXPECT_NE(s.ToString().find("Internal"), std::string::npos)
                       << s.ToString();
                   waiter.Done();
                 });

  ASSERT_EQ(registry->range_calls, 2);

  std::vector<PendingCallback> cbs;
  {
    std::lock_guard<std::mutex> lk(registry->mtx);
    cbs = std::move(registry->pending);
  }
  // Record block 1's NotFound first, then block 0's Internal error.
  cbs[1].cb(Status::NotFound("block missing"));
  cbs[0].cb(Status::Internal("disk on fire"));

  waiter.Wait();
}

// 14.5 exactly-once: a duplicate block callback must fail fast (CHECK) rather
// than silently double-finalize.
TEST_F(ChunkReadOpTest, DuplicateCallback_FailsFast) {
  const int64_t kLen = 4 * 1024 * 1024;  // 1 block
  SetOneSliceCovering(kLen);

  StatusCallback captured;
  ON_CALL(*mock_block_store_, RangeAsync)
      .WillByDefault(
          Invoke([&captured](ContextSPtr, RangeReq, StatusCallback cb) {
            captured = std::move(cb);
          }));

  std::vector<uint8_t> buf(kLen, 0);

  test::AsyncWaiter waiter;
  waiter.Expect(1);
  StartChunkRead(ctx_, mock_hub_, MakeReq(0, kLen), OneSliceCovering(kLen),
                 ReadBufView{buf.data(), 0, (size_t)kLen},
                 [&](Status) { waiter.Done(); });
  ASSERT_NE(captured, nullptr);
  captured(Status::OK());  // legitimate first completion
  waiter.Wait();

  // Re-exec mode avoids unsafe fork-only death tests after VFSTestBase has
  // started its executor threads.
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  // Second invocation of the same block callback: contract violation.
  EXPECT_DEATH(captured(Status::OK()), "duplicate block read callback");
}

// Integration via ChunkReader: GetSlices failure short-circuits to the
// callback with the meta error; no BlockStore call is made.
TEST_F(ChunkReadOpTest, ChunkReader_MetaFailure_ShortCircuit) {
  ON_CALL(*mock_meta_system_, ReadSlice(_, kTestIno, _, _, _, _))
      .WillByDefault(Return(Status::Internal("meta down")));
  EXPECT_CALL(*mock_meta_system_, ReadSlice(_, kTestIno, _, _, _, _))
      .Times(AnyNumber());

  EXPECT_CALL(*mock_block_store_, RangeAsync).Times(0);

  const int64_t kLen = 128 * 1024;
  std::vector<uint8_t> buf(kLen, 0);

  ChunkReader reader(mock_hub_, kTestFh, MakeReq(0, kLen));
  Status final_status;
  reader.ReadAsync(ctx_, ReadBufView{buf.data(), 0, (size_t)kLen},
                   [&](Status s) { final_status = s; });

  EXPECT_FALSE(final_status.ok());
  EXPECT_TRUE(final_status.IsInternal());
}

// ChunkReader is now a stack object: it must be safely destructible right
// after ReadAsync returns while the operation is still in flight.
TEST_F(ChunkReadOpTest, ChunkReader_DiesWhileOpInFlight) {
  const int64_t kLen = 4 * 1024 * 1024;
  SetOneSliceCovering(kLen);

  auto registry = std::make_shared<CallbackRegistry>();
  ON_CALL(*mock_block_store_, RangeAsync)
      .WillByDefault(
          Invoke([registry](ContextSPtr, RangeReq req, StatusCallback cb) {
            registry->Capture(std::move(cb), req);
          }));

  std::vector<uint8_t> buf(kLen, 0);
  test::AsyncWaiter waiter;
  waiter.Expect(1);

  {
    ChunkReader reader(mock_hub_, kTestFh, MakeReq(0, kLen));
    reader.ReadAsync(ctx_, ReadBufView{buf.data(), 0, (size_t)kLen},
                     [&](Status s) {
                       EXPECT_TRUE(s.ok());
                       waiter.Done();
                     });
  }  // reader destroyed while the block read is still in flight

  std::vector<PendingCallback> cbs;
  {
    std::lock_guard<std::mutex> lk(registry->mtx);
    cbs = std::move(registry->pending);
  }
  for (auto& pc : cbs) {
    pc.cb(Status::OK());
  }
  waiter.Wait();
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
