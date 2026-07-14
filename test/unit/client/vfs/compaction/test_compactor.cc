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
#include <cstring>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "client/vfs/compaction/compactor_impl.h"
#include "client/vfs/metasystem/mds/compact.h"
#include "client/vfs/metasystem/mds/mds_client.h"
#include "client/vfs/vfs_meta.h"
#include "common/status.h"
#include "common/trace/trace_manager.h"
#include "mds/filesystem/fs_info.h"
#include "test/unit/client/vfs/mock/mock_compactor.h"
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

  // Build a zero-filled slice (id=0 means hole in new design).
  Slice MakeZeroSlice(int32_t pos, int32_t size) {
    return dingofs::client::vfs::test::MakeSlice(/*id=*/0, pos, size);
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
  std::vector<Slice> slices = {MakeZeroSlice(0, 4096)};
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
  std::vector<Slice> slices = {MakeZeroSlice(0, 2 * 1024 * 1024)};
  std::vector<Slice> out;
  Status s = compactor_->Compact(ctx_, 100, 0, slices, out);
  EXPECT_TRUE(s.ok());
  // All slices skipped: out_slices is empty (nothing to compact).
  EXPECT_TRUE(out.empty());
}

TEST_F(CompactorTest, Compact_DuplicateLargeHoles_RemainSparse) {
  constexpr int32_t kHoleSize = 2 * 1024 * 1024;
  std::vector<Slice> slices = {
      MakeZeroSlice(0, kHoleSize),
      MakeZeroSlice(0, kHoleSize),
  };
  std::vector<Slice> out;

  EXPECT_CALL(*mock_block_store_, RangeAsync).Times(0);
  EXPECT_CALL(*mock_block_store_, PutAsync).Times(0);

  Status s = compactor_->Compact(ctx_, 100, 0, slices, out);

  EXPECT_TRUE(s.ok()) << s.ToString();
  EXPECT_TRUE(out.empty());
}

TEST(CompactChunkTaskTest, EmptyCompactionResultDoesNotCallMDS) {
  constexpr Ino kIno = 100;
  constexpr uint32_t kChunkIndex = 0;
  constexpr int32_t kHoleSize = 2 * 1024 * 1024;

  mds::ChunkEntry chunk_entry;
  chunk_entry.set_index(kChunkIndex);
  chunk_entry.set_version(1);
  for (int i = 0; i < 99; ++i) {
    auto* slice = chunk_entry.add_slices();
    slice->set_id(0);
    slice->set_pos(0);
    slice->set_len(kHoleSize);
  }
  auto chunk = meta::Chunk::New(kIno, chunk_entry, "test");
  meta::InodeSPtr inode;

  mds::FsInfoEntry fs_info_entry;
  mds::FsInfo fs_info(fs_info_entry);
  meta::RPC rpc{butil::EndPoint()};
  TraceManager trace_manager;
  meta::MDSClient mds_client(ClientId(), fs_info, std::move(rpc),
                             trace_manager);

  test::MockCompactor compactor;
  EXPECT_CALL(compactor, Compact(_, kIno, kChunkIndex, _, _))
      .WillOnce([](ContextSPtr, Ino, int64_t, const std::vector<Slice>&,
                   std::vector<Slice>&) { return Status::OK(); });

  meta::CompactProcessor compact_processor;
  auto task = meta::CompactChunkTask::New(kIno, inode, chunk, mds_client,
                                          compactor, compact_processor);
  task->Run();

  EXPECT_TRUE(task->GetStatus().IsNotFit()) << task->GetStatus().ToString();
}

// 6. Compact_AfterStop returns a Stop error.
TEST_F(CompactorTest, Compact_AfterStop_ReturnsStopError) {
  compactor_->Stop();
  std::vector<Slice> slices = {MakeZeroSlice(0, 4096)};
  std::vector<Slice> out;
  Status s = compactor_->Compact(ctx_, 100, 0, slices, out);
  EXPECT_FALSE(s.ok());
}

// 7. ForceCompact_AfterStop returns a Stop error.
TEST_F(CompactorTest, ForceCompact_AfterStop_ReturnsStopError) {
  compactor_->Stop();
  std::vector<Slice> slices = {MakeZeroSlice(0, 4096)};
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
      .WillByDefault([](ContextSPtr, RangeReq, StatusCallback cb) {
        cb(Status::IoError("simulated read failure"));
      });
  EXPECT_CALL(*mock_block_store_, RangeAsync).Times(AnyNumber());

  // A single non-zero slice of 4 MB: Skip() would skip it (single large
  // slice), so use ForceCompact to bypass the skip logic and force DoCompact.
  std::vector<Slice> slices = {
      dingofs::client::vfs::test::MakeSlice(1, 0, 4 * 1024 * 1024)};
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
        // Fill the request slot window in place.
        if (req.dst.base != nullptr && req.length > 0) {
          std::memset(req.dst.data(), 0, req.length);
        }
        cb(Status::OK());
      });
  EXPECT_CALL(*mock_block_store_, RangeAsync).Times(AnyNumber());
  EXPECT_CALL(*mock_block_store_, PutAsync).Times(AnyNumber());

  // Use a non-zero 4 MB slice with ForceCompact to drive DoCompact.
  std::vector<Slice> slices = {
      dingofs::client::vfs::test::MakeSlice(2, 0, 4 * 1024 * 1024)};
  std::vector<Slice> out;

  std::thread compact_thread(
      [&]() { compactor_->ForceCompact(ctx_, 200, 0, slices, out); });

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

// 10. Stop closes admission before waiting for existing work to drain.
TEST_F(CompactorTest, Stop_RejectsNewWorkWhileDraining) {
  std::mutex m;
  std::condition_variable cv;
  bool compact_started = false;
  bool release_range = false;

  ON_CALL(*mock_block_store_, RangeAsync)
      .WillByDefault([&](ContextSPtr, RangeReq req, StatusCallback cb) {
        {
          std::unique_lock<std::mutex> lk(m);
          compact_started = true;
          cv.notify_all();
          cv.wait(lk, [&] { return release_range; });
        }
        if (req.dst.base != nullptr && req.length > 0) {
          std::memset(req.dst.data(), 0, req.length);
        }
        cb(Status::OK());
      });
  EXPECT_CALL(*mock_block_store_, RangeAsync).Times(AnyNumber());
  EXPECT_CALL(*mock_block_store_, PutAsync).Times(AnyNumber());

  std::vector<Slice> inflight_slices = {
      dingofs::client::vfs::test::MakeSlice(3, 0, 4 * 1024 * 1024)};
  std::vector<Slice> inflight_out;
  std::thread compact_thread([&]() {
    compactor_->ForceCompact(ctx_, 201, 0, inflight_slices, inflight_out);
  });

  {
    std::unique_lock<std::mutex> lk(m);
    cv.wait(lk, [&] { return compact_started; });
  }

  std::thread stop_thread([&]() { compactor_->Stop(); });

  // A large zero slice is skipped without I/O when admitted, so retrying is
  // cheap and avoids relying on scheduling sleeps to observe Stop's state.
  std::vector<Slice> new_slices = {MakeZeroSlice(0, 2 * 1024 * 1024)};
  Status new_status;
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(2);
  do {
    std::vector<Slice> out;
    new_status = compactor_->Compact(ctx_, 202, 0, new_slices, out);
    std::this_thread::yield();
  } while (new_status.ok() && std::chrono::steady_clock::now() < deadline);
  const bool rejected_while_draining = new_status.IsStop();

  {
    std::lock_guard<std::mutex> lk(m);
    release_range = true;
  }
  cv.notify_all();

  compact_thread.join();
  stop_thread.join();

  EXPECT_TRUE(rejected_while_draining) << new_status.ToString();
}

// 11. Repeatedly drive compaction through the shared-owned SliceWriter path.
// This guards its async FlushAsync lifetime and end-to-end commit result.
TEST_F(CompactorTest, RegressionHeapAllocSliceWriter_RepeatedDoCompact_Stable) {
  for (int i = 0; i < 20; ++i) {
    // 4 MB non-zero slice; ForceCompact bypasses Skip() logic and forces
    // DoCompact -> SliceWriter heap alloc + FlushAsync.
    std::vector<Slice> slices = {dingofs::client::vfs::test::MakeSlice(
        /*id=*/100 + i, /*pos=*/0, /*len=*/4 * 1024 * 1024)};
    std::vector<Slice> out;
    Status s = compactor_->ForceCompact(ctx_,
                                        /*ino=*/300 + i,
                                        /*chunk_index=*/0, slices, out);
    ASSERT_TRUE(s.ok()) << "iter=" << i << " status=" << s.ToString();
    ASSERT_EQ(out.size(), 1u) << "iter=" << i;
  }
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
