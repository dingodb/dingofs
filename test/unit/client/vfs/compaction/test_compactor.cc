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

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "client/vfs/compaction/compactor_impl.h"
#include "client/vfs/vfs_meta.h"
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

class CompactorTest : public dingofs::client::vfs::test::VFSTestBase {
 protected:
  void SetUp() override {
    // Wire TraceManager (concrete, tracing disabled by default in tests).
    trace_manager_ = std::make_unique<TraceManager>();
    ON_CALL(*mock_hub_, GetTraceManager())
        .WillByDefault(Return(trace_manager_.get()));
    EXPECT_CALL(*mock_hub_, GetTraceManager()).Times(AnyNumber());

    compactor_ = std::make_unique<CompactorImpl>(mock_hub_);
    ASSERT_TRUE(compactor_->Start().ok());
  }

  void TearDown() override { compactor_->Stop(); }

  // Build a zero-filled slice of the given length at a given file offset.
  // The slice uses is_zero=true so no actual block reads are performed for
  // the data path (ChunkReqReader skips zero blocks).
  Slice MakeZeroSlice(uint64_t id, uint64_t offset, uint64_t length) {
    return dingofs::client::vfs::test::MakeSlice(id, offset, length,
                                                 /*is_zero=*/true);
  }

  std::unique_ptr<TraceManager> trace_manager_;
  std::unique_ptr<CompactorImpl> compactor_;
};

// 1. Basic Start/Stop lifecycle — no crash.
TEST_F(CompactorTest, Start_Stop_NoCrash) {
  // compactor_ already started in SetUp; TearDown calls Stop().
  SUCCEED();
}

// 2. Stop is idempotent.
TEST_F(CompactorTest, Stop_Idempotent) {
  Status s1 = compactor_->Stop();
  EXPECT_TRUE(s1.ok());
  Status s2 = compactor_->Stop();
  EXPECT_TRUE(s2.ok());
}

// 3. Compact with empty slices fires a CHECK (death test).
//    We verify this contract by checking the behaviour on an empty vector.
//    Because the implementation uses CHECK(!slices.empty()), calling Compact
//    with an empty slice list is a programming error — document it with a
//    EXPECT_DEATH to make the contract explicit.
TEST_F(CompactorTest, Compact_EmptySlices_AbortContract) {
  std::vector<Slice> empty;
  std::vector<Slice> out;
  // The implementation uses CHECK(!slices.empty()) — this kills the process.
  EXPECT_DEATH(compactor_->Compact(ctx_, 1, 0, empty, out), "");
}

// 4. Compact with a single zero-slice: the skip logic may skip it entirely.
//    The result should be OK and output slices must not be larger than input.
TEST_F(CompactorTest, Compact_SingleZeroSlice_SkippedOrOk) {
  // A single zero slice occupies less than 1 MB -> Skip() returns 0 ->
  // to_compact is empty -> Compact returns OK with empty out_slices.
  std::vector<Slice> slices = {MakeZeroSlice(1, 0, 4096)};
  std::vector<Slice> out;
  Status s = compactor_->Compact(ctx_, 100, 0, slices, out);
  EXPECT_TRUE(s.ok());
  // out_slices should have at most as many slices as the input.
  EXPECT_LE(out.size(), slices.size());
}

// 5. Compact with a single large zero-slice (> 1 MB threshold): Skip()
//    considers the first slice "large enough" to skip, so to_compact becomes
//    empty and Compact returns OK with no output work done.
TEST_F(CompactorTest, Compact_SingleLargeZeroSlice_SkippedBySkipLogic) {
  // 2 MB > 1 MB threshold -> Skip returns 1 -> to_compact is empty.
  std::vector<Slice> slices = {MakeZeroSlice(1, 0, 2 * 1024 * 1024)};
  std::vector<Slice> out;
  Status s = compactor_->Compact(ctx_, 100, 0, slices, out);
  EXPECT_TRUE(s.ok());
  // All slices skipped: out_slices is empty (nothing to compact).
  EXPECT_TRUE(out.empty());
}

// 6. Compact_AfterStop returns a Stop error.
TEST_F(CompactorTest, Compact_AfterStop_ReturnsStopError) {
  compactor_->Stop();
  std::vector<Slice> slices = {MakeZeroSlice(1, 0, 4096)};
  std::vector<Slice> out;
  Status s = compactor_->Compact(ctx_, 100, 0, slices, out);
  EXPECT_FALSE(s.ok());
}

// 7. ForceCompact_AfterStop returns a Stop error.
TEST_F(CompactorTest, ForceCompact_AfterStop_ReturnsStopError) {
  compactor_->Stop();
  std::vector<Slice> slices = {MakeZeroSlice(1, 0, 4096)};
  std::vector<Slice> out;
  Status s = compactor_->ForceCompact(ctx_, 100, 0, slices, out);
  EXPECT_FALSE(s.ok());
}

// 8. BlockStore RangeAsync failure propagates as a Compact error.
//    We use a non-zero slice so that ChunkReqReader actually issues a
//    RangeAsync call. A single non-zero data slice whose length is large
//    enough to pass the Skip() threshold forces DoCompact to be called,
//    and if RangeAsync returns an error the Compact call must return an error.
TEST_F(CompactorTest, Compact_BlockStore_ReadFail_ReturnsError) {
  // Override the default RangeAsync behaviour to return an error.
  ON_CALL(*mock_block_store_, RangeAsync)
      .WillByDefault(
          [](ContextSPtr, RangeReq, StatusCallback cb) {
            cb(Status::IoError("simulated read failure"));
          });
  EXPECT_CALL(*mock_block_store_, RangeAsync).Times(AnyNumber());

  // A single non-zero slice of 4 MB: Skip() would skip it (single large
  // slice), so use ForceCompact to bypass the skip logic and force DoCompact.
  std::vector<Slice> slices = {
      dingofs::client::vfs::test::MakeSlice(1, 0, 4 * 1024 * 1024,
                                            /*is_zero=*/false)};
  std::vector<Slice> out;
  Status s = compactor_->ForceCompact(ctx_, 100, 0, slices, out);
  EXPECT_FALSE(s.ok());
}

// 9. Stop waits for in-flight compactions to finish.
//    We simulate a slow compact by making RangeAsync sleep before calling
//    the callback.  Stop() must block until the in-flight op completes.
TEST_F(CompactorTest, Stop_WaitsForInflight) {
  std::mutex m;
  std::condition_variable cv;
  bool compact_started = false;
  bool range_done = false;

  ON_CALL(*mock_block_store_, RangeAsync)
      .WillByDefault([&](ContextSPtr, RangeReq req, StatusCallback cb) {
        {
          std::lock_guard<std::mutex> lk(m);
          compact_started = true;
        }
        cv.notify_all();
        // Simulate slow IO.
        std::this_thread::sleep_for(std::chrono::milliseconds(60));
        {
          std::lock_guard<std::mutex> lk(m);
          range_done = true;
        }
        // Fill output buffer so io_buffer.Size() == block_req.len.
        if (req.data && req.length > 0) {
          char* buf = new char[req.length]();
          req.data->AppendUserData(
              buf, req.length,
              [](void* p) { delete[] static_cast<char*>(p); });
        }
        cb(Status::OK());
      });
  EXPECT_CALL(*mock_block_store_, RangeAsync).Times(AnyNumber());
  EXPECT_CALL(*mock_block_store_, PutAsync).Times(AnyNumber());

  // Use a non-zero 4 MB slice with ForceCompact to drive DoCompact.
  std::vector<Slice> slices = {
      dingofs::client::vfs::test::MakeSlice(2, 0, 4 * 1024 * 1024,
                                            /*is_zero=*/false)};
  std::vector<Slice> out;

  std::thread compact_thread([&]() {
    compactor_->ForceCompact(ctx_, 200, 0, slices, out);
  });

  // Wait until the compact is inside RangeAsync (in-flight).
  {
    std::unique_lock<std::mutex> lk(m);
    cv.wait(lk, [&] { return compact_started; });
  }

  // Stop must block until the in-flight compact finishes.
  compactor_->Stop();

  // After Stop() returns the in-flight work must have completed.
  {
    std::lock_guard<std::mutex> lk(m);
    EXPECT_TRUE(range_done);
  }

  compact_thread.join();
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
