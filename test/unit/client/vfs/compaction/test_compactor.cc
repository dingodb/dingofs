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

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "client/vfs/compaction/compactor_impl.h"
#include "client/vfs/metasystem/mds/compact.h"
#include "client/vfs/metasystem/mds/mds_client.h"
#include "client/vfs/vfs_meta.h"
#include "common/block/block_key.h"
#include "common/status.h"
#include "common/trace/trace_manager.h"
#include "mds/filesystem/fs_info.h"
#include "test/unit/client/vfs/mock/mock_compactor.h"
#include "test/unit/client/vfs/test_base.h"
#include "test/unit/client/vfs/test_common.h"
#include "test/unit/common/blockaccess/mock/mock_accesser.h"

namespace dingofs {
namespace client {
namespace vfs {

DECLARE_uint32(vfs_compact_cleanup_batch_size);

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

// 12. CleanupUncommittedSlices reconstructs the exact dense block layout of a
// slice: full blocks plus a partial tail, keyed under the slice id.
TEST_F(CompactorTest, CleanupUncommittedSlices_EnumeratesDenseBlockKeys) {
  blockaccess::MockBlockAccesser accesser;
  ON_CALL(*mock_hub_, GetBlockAccesser()).WillByDefault(Return(&accesser));
  EXPECT_CALL(*mock_hub_, GetBlockAccesser()).Times(AnyNumber());

  std::list<std::string> deleted;
  EXPECT_CALL(accesser, BatchDelete(_))
      .WillOnce([&](const std::list<std::string>& keys) {
        deleted = keys;
        return Status::OK();
      });

  // 10 MB slice with 4 MB blocks: two full blocks + one 2 MB tail. The hole
  // slice (id=0) must contribute nothing.
  constexpr int32_t kSize = 10 * 1024 * 1024;
  std::vector<Slice> slices = {
      dingofs::client::vfs::test::MakeSlice(/*id=*/1001, /*pos=*/0, kSize),
      MakeZeroSlice(0, 4096)};
  Status s = compactor_->CleanupUncommittedSlices(ctx_, slices);
  EXPECT_TRUE(s.ok()) << s.ToString();

  std::list<std::string> expected = {
      BlockKey(1001, 0, 4 * 1024 * 1024).StoreKey(),
      BlockKey(1001, 1, 4 * 1024 * 1024).StoreKey(),
      BlockKey(1001, 2, 2 * 1024 * 1024).StoreKey()};
  EXPECT_EQ(deleted, expected);
}

// 13. Hole-only input never touches the accesser.
TEST_F(CompactorTest, CleanupUncommittedSlices_HolesOnly_NoDelete) {
  blockaccess::MockBlockAccesser accesser;
  ON_CALL(*mock_hub_, GetBlockAccesser()).WillByDefault(Return(&accesser));
  EXPECT_CALL(*mock_hub_, GetBlockAccesser()).Times(AnyNumber());
  EXPECT_CALL(accesser, BatchDelete(_)).Times(0);

  std::vector<Slice> slices = {MakeZeroSlice(0, 4096)};
  EXPECT_TRUE(compactor_->CleanupUncommittedSlices(ctx_, slices).ok());
}

// 14. CleanupUncommittedSlices after Stop is rejected like other entry points.
TEST_F(CompactorTest, CleanupUncommittedSlices_AfterStop_ReturnsStopError) {
  compactor_->Stop();
  std::vector<Slice> slices = {
      dingofs::client::vfs::test::MakeSlice(1, 0, 4096)};
  Status s = compactor_->CleanupUncommittedSlices(ctx_, slices);
  EXPECT_TRUE(s.IsStop()) << s.ToString();
}

// 15. A failed flush waits for all block callbacks before reclaiming the
// never-committed slice. Cleanup failure must not replace the flush error.
TEST_F(CompactorTest,
       ForceCompact_PartialUploadFailure_CleansUpAfterAllCallbacks) {
  std::atomic<int> upload_callbacks{0};
  ON_CALL(*mock_block_store_, PutAsync)
      .WillByDefault([&](ContextSPtr, PutReq, StatusCallback cb) {
        int completed = upload_callbacks.fetch_add(1) + 1;
        if (completed == 2) {
          cb(Status::IoError("simulated upload failure"));
        } else {
          cb(Status::OK());
        }
      });
  EXPECT_CALL(*mock_block_store_, PutAsync).Times(3);

  blockaccess::MockBlockAccesser accesser;
  ON_CALL(*mock_hub_, GetBlockAccesser()).WillByDefault(Return(&accesser));
  EXPECT_CALL(*mock_hub_, GetBlockAccesser()).Times(AnyNumber());

  std::list<std::string> deleted;
  EXPECT_CALL(accesser, BatchDelete(_))
      .WillOnce([&](const std::list<std::string>& keys) {
        EXPECT_EQ(upload_callbacks.load(), 3);
        deleted = keys;
        return Status::IoError("simulated cleanup failure");
      });

  constexpr int32_t kSize = 10 * 1024 * 1024;
  std::vector<Slice> slices = {
      dingofs::client::vfs::test::MakeSlice(1, 0, kSize)};
  std::vector<Slice> out;
  Status s = compactor_->ForceCompact(ctx_, 400, 0, slices, out);

  EXPECT_TRUE(s.IsIoError()) << s.ToString();
  EXPECT_NE(s.ToString().find("simulated upload failure"), std::string::npos);
  EXPECT_TRUE(out.empty());
  EXPECT_EQ(deleted.size(), 3u);
}

// 16. The cleanup batch loop splits keys by vfs_compact_cleanup_batch_size
// and keeps deleting the remaining batches after one fails.
TEST_F(CompactorTest, CleanupUncommittedSlices_SplitsBatches_KeepsGoingOnFail) {
  const uint32_t saved_batch_size = FLAGS_vfs_compact_cleanup_batch_size;
  FLAGS_vfs_compact_cleanup_batch_size = 1;

  blockaccess::MockBlockAccesser accesser;
  ON_CALL(*mock_hub_, GetBlockAccesser()).WillByDefault(Return(&accesser));
  EXPECT_CALL(*mock_hub_, GetBlockAccesser()).Times(AnyNumber());

  int calls = 0;
  EXPECT_CALL(accesser, BatchDelete(_))
      .Times(3)
      .WillRepeatedly([&](const std::list<std::string>& keys) {
        EXPECT_EQ(keys.size(), 1u);
        return ++calls == 2 ? Status::IoError("simulated delete failure")
                            : Status::OK();
      });

  // 10 MB slice with 4 MB blocks: 3 keys -> 3 single-key batches.
  constexpr int32_t kSize = 10 * 1024 * 1024;
  std::vector<Slice> slices = {
      dingofs::client::vfs::test::MakeSlice(/*id=*/1002, /*pos=*/0, kSize)};
  Status s = compactor_->CleanupUncommittedSlices(ctx_, slices);

  EXPECT_TRUE(s.IsIoError()) << s.ToString();
  EXPECT_EQ(calls, 3);

  FLAGS_vfs_compact_cleanup_batch_size = saved_batch_size;
}

// ---------------------------------------------------------------------------
// Data-correctness coverage. The mock BlockStore serves a position-sensitive
// pattern per (slice_id, slice-internal offset) and captures every uploaded
// block, so tests can assert the compacted bytes equal the newest-wins
// composition of the input slices — the compactor's actual job, which the
// zero-filled mocks above never verify.
// ---------------------------------------------------------------------------

namespace {

constexpr int64_t kTestBlockSize = 4 * 1024 * 1024;

uint8_t PatternAt(uint64_t slice_id, int64_t off) {
  return static_cast<uint8_t>((slice_id * 131 + off * 7) & 0xFF);
}

// BlockKey::Filename() is "id_index_size".
void ParseBlockFilename(const std::string& filename, uint64_t* id,
                        uint32_t* index) {
  size_t p1 = filename.find('_');
  size_t p2 = filename.find('_', p1 + 1);
  CHECK(p1 != std::string::npos && p2 != std::string::npos) << filename;
  *id = std::stoull(filename.substr(0, p1));
  *index = std::stoul(filename.substr(p1 + 1, p2 - p1 - 1));
}

struct CapturedWrites {
  std::mutex mu;
  // (slice_id, block_index) -> bytes
  std::map<std::pair<uint64_t, uint32_t>, std::string> blocks;
  std::map<uint64_t, size_t> reads;
  size_t duplicate_uploads{0};

  // Concatenate the blocks of one slice in index order.
  std::string SliceBytes(uint64_t slice_id) {
    std::lock_guard<std::mutex> lk(mu);
    std::string out;
    for (const auto& [key, bytes] : blocks) {
      if (key.first == slice_id) out += bytes;
    }
    return out;
  }

  size_t ReadCount(uint64_t slice_id) {
    std::lock_guard<std::mutex> lk(mu);
    return reads[slice_id];
  }

  size_t DuplicateUploads() {
    std::lock_guard<std::mutex> lk(mu);
    return duplicate_uploads;
  }

  size_t BlockCount(uint64_t slice_id) {
    std::lock_guard<std::mutex> lk(mu);
    return std::count_if(blocks.begin(), blocks.end(),
                         [slice_id](const auto& entry) {
                           return entry.first.first == slice_id;
                         });
  }
};

// RangeAsync fills the requested window with the owning slice's pattern;
// PutAsync records the uploaded bytes keyed by (slice_id, block_index).
void InstallPatternBlockStore(test::MockBlockStore* bs,
                              std::shared_ptr<CapturedWrites> captured) {
  ON_CALL(*bs, RangeAsync)
      .WillByDefault([captured](ContextSPtr, RangeReq req, StatusCallback cb) {
        uint64_t slice_id = 0;
        uint32_t block_index = 0;
        ParseBlockFilename(req.handle.Filename(), &slice_id, &block_index);
        int64_t base =
            static_cast<int64_t>(block_index) * kTestBlockSize + req.offset;
        for (int64_t i = 0; i < req.length; ++i) {
          req.dst.data()[i] = PatternAt(slice_id, base + i);
        }
        {
          std::lock_guard<std::mutex> lk(captured->mu);
          captured->reads[slice_id]++;
        }
        cb(Status::OK());
      });
  ON_CALL(*bs, PutAsync)
      .WillByDefault([captured](ContextSPtr, PutReq req, StatusCallback cb) {
        uint64_t slice_id = 0;
        uint32_t block_index = 0;
        ParseBlockFilename(req.handle.Filename(), &slice_id, &block_index);
        std::string bytes(req.data.Length(), '\0');
        req.data.CopyTo(bytes.data(), bytes.size(), 0);
        {
          std::lock_guard<std::mutex> lk(captured->mu);
          auto [it, inserted] = captured->blocks.emplace(
              std::make_pair(slice_id, block_index), std::move(bytes));
          if (!inserted) {
            captured->duplicate_uploads++;
          }
        }
        cb(Status::OK());
      });
}

}  // namespace

// 19. The compacted slice must be the newest-wins composition of the inputs.
// S2 (newer) overlaps the second half of S1 (older); every output byte is
// checked against the pattern of the slice that must win at that offset.
TEST_F(CompactorTest, ForceCompact_OverlappingSlices_NewestWinsBytes) {
  auto captured = std::make_shared<CapturedWrites>();
  InstallPatternBlockStore(mock_block_store_, captured);

  constexpr int64_t kMB = 1024 * 1024;
  // S1 [0, 2M) id=10 older; S2 [1M, 3M) id=11 newer (later in vector).
  std::vector<Slice> slices = {
      dingofs::client::vfs::test::MakeSlice(/*id=*/10, /*pos=*/0, 2 * kMB),
      dingofs::client::vfs::test::MakeSlice(/*id=*/11, /*pos=*/kMB, 2 * kMB)};

  std::vector<Slice> out;
  Status s = compactor_->ForceCompact(ctx_, /*ino=*/500, /*chunk_index=*/0,
                                      slices, out);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(out.size(), 1u);

  const Slice& compacted = out[0];
  EXPECT_NE(compacted.id, 10u);
  EXPECT_NE(compacted.id, 11u);
  EXPECT_EQ(compacted.pos, 0);
  EXPECT_EQ(compacted.len, 3 * kMB);
  EXPECT_EQ(compacted.size, 3 * kMB);
  EXPECT_EQ(compacted.off, 0);

  std::string written = captured->SliceBytes(compacted.id);
  ASSERT_EQ(written.size(), static_cast<size_t>(3 * kMB));
  EXPECT_EQ(captured->DuplicateUploads(), 0u);
  for (int64_t off = 0; off < 3 * kMB; ++off) {
    uint8_t expected = (off < kMB)
                           ? PatternAt(10, off)         // S1-only region
                           : PatternAt(11, off - kMB);  // S2 wins overlap+tail
    ASSERT_EQ(static_cast<uint8_t>(written[off]), expected)
        << "composition mismatch at offset " << off;
  }
}

// 20. Compact() output contract: the skipped prefix must be preserved
// verbatim (same ids, same geometry, same order) ahead of exactly one fresh
// slice covering the compacted tail. Downstream (MDS CompactChunk, failure
// cleanup) depends on this mixed-ownership layout.
TEST_F(CompactorTest, Compact_SkipPrefixPreservedVerbatimInOutput) {
  auto captured = std::make_shared<CapturedWrites>();
  InstallPatternBlockStore(mock_block_store_, captured);

  constexpr int64_t kMB = 1024 * 1024;
  // S0 is 16 MiB, first, unoverlapped, and ≥1/5 of the 16.75 MiB span: Skip()
  // keeps it. Three 256 KiB tail slices get compacted.
  std::vector<Slice> slices = {
      dingofs::client::vfs::test::MakeSlice(/*id=*/20, /*pos=*/0, 16 * kMB),
      dingofs::client::vfs::test::MakeSlice(/*id=*/21, /*pos=*/16 * kMB,
                                            256 * 1024),
      dingofs::client::vfs::test::MakeSlice(/*id=*/22,
                                            /*pos=*/16 * kMB + 256 * 1024,
                                            256 * 1024),
      dingofs::client::vfs::test::MakeSlice(/*id=*/23,
                                            /*pos=*/16 * kMB + 512 * 1024,
                                            256 * 1024)};

  std::vector<Slice> out;
  Status s =
      compactor_->Compact(ctx_, /*ino=*/501, /*chunk_index=*/0, slices, out);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(out.size(), 2u);

  // Preserved prefix: bit-identical to the input slice.
  EXPECT_EQ(out[0].id, 20u);
  EXPECT_EQ(out[0].pos, slices[0].pos);
  EXPECT_EQ(out[0].len, slices[0].len);
  EXPECT_EQ(out[0].off, slices[0].off);
  EXPECT_EQ(out[0].size, slices[0].size);
  // The skipped slice must not be re-uploaded.
  EXPECT_TRUE(captured->SliceBytes(20).empty());
  EXPECT_EQ(captured->ReadCount(20), 0u);

  // Fresh tail: new id covering exactly the compacted range.
  EXPECT_EQ(out[1].pos, 16 * kMB);
  EXPECT_EQ(out[1].len, 3 * 256 * 1024);
  for (const auto& in : slices) {
    EXPECT_NE(out[1].id, in.id);
  }
}

// 21. Non-origin coordinates: a mid-chunk range in a non-zero chunk must
// survive the chunk_start/offset_in_chunk arithmetic byte-for-byte.
TEST_F(CompactorTest, ForceCompact_NonZeroChunkAndOffset_BytesPreserved) {
  auto captured = std::make_shared<CapturedWrites>();
  InstallPatternBlockStore(mock_block_store_, captured);

  constexpr int64_t kMB = 1024 * 1024;
  constexpr int64_t kChunkIndex = 3;
  // Single slice [1M, 1.5M) inside chunk 3.
  std::vector<Slice> slices = {dingofs::client::vfs::test::MakeSlice(
      /*id=*/30, /*pos=*/kMB, 512 * 1024)};

  std::vector<Slice> out;
  Status s =
      compactor_->ForceCompact(ctx_, /*ino=*/502, kChunkIndex, slices, out);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(out.size(), 1u);
  EXPECT_EQ(out[0].pos, kMB);
  EXPECT_EQ(out[0].len, 512 * 1024);

  std::string written = captured->SliceBytes(out[0].id);
  ASSERT_EQ(written.size(), static_cast<size_t>(512 * 1024));
  for (int64_t off = 0; off < 512 * 1024; ++off) {
    ASSERT_EQ(static_cast<uint8_t>(written[off]), PatternAt(30, off))
        << "coordinate arithmetic shifted data at offset " << off;
  }
}

// 22. A write failure (page pool exhausted) must abort the compaction with an
// error, upload nothing, and trigger no cleanup — nothing was ever uploaded.
TEST_F(CompactorTest, ForceCompact_WritePoolExhausted_FailsWithoutUpload) {
  // 64 KiB pool cannot back a 4 MiB block: SliceWriter::Write fails in the
  // reserve phase and rolls back before any upload is submitted.
  WriteMemPool tiny_pool(64 * 1024, 4096);
  ON_CALL(*mock_hub_, GetWriteMemPool()).WillByDefault(Return(&tiny_pool));

  blockaccess::MockBlockAccesser accesser;
  ON_CALL(*mock_hub_, GetBlockAccesser()).WillByDefault(Return(&accesser));
  EXPECT_CALL(*mock_hub_, GetBlockAccesser()).Times(AnyNumber());
  EXPECT_CALL(accesser, BatchDelete(_)).Times(0);
  EXPECT_CALL(*mock_block_store_, PutAsync).Times(0);

  std::vector<Slice> slices = {dingofs::client::vfs::test::MakeSlice(
      /*id=*/40, /*pos=*/0, 4 * 1024 * 1024)};
  std::vector<Slice> out;
  Status s = compactor_->ForceCompact(ctx_, /*ino=*/503, /*chunk_index=*/0,
                                      slices, out);
  EXPECT_TRUE(s.IsNoSpace()) << s.ToString();
  EXPECT_TRUE(out.empty());
  EXPECT_EQ(tiny_pool.GetUsedBytes(), 0);
}

// 23. Holes participate in newest-wins composition as zero-filled ranges and
// must never issue a block read of their own.
TEST_F(CompactorTest, ForceCompact_HoleOverData_ProducesZeros) {
  auto captured = std::make_shared<CapturedWrites>();
  InstallPatternBlockStore(mock_block_store_, captured);

  constexpr int64_t kMB = 1024 * 1024;
  std::vector<Slice> slices = {
      dingofs::client::vfs::test::MakeSlice(/*id=*/50, /*pos=*/0, 2 * kMB),
      MakeZeroSlice(/*pos=*/kMB, /*size=*/kMB)};

  std::vector<Slice> out;
  Status s = compactor_->ForceCompact(ctx_, /*ino=*/504, /*chunk_index=*/0,
                                      slices, out);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(out.size(), 1u);

  std::string written = captured->SliceBytes(out[0].id);
  ASSERT_EQ(written.size(), static_cast<size_t>(2 * kMB));
  for (int64_t off = 0; off < 2 * kMB; ++off) {
    const uint8_t expected = off < kMB ? PatternAt(50, off) : 0;
    ASSERT_EQ(static_cast<uint8_t>(written[off]), expected)
        << "hole composition mismatch at offset " << off;
  }
  EXPECT_EQ(captured->ReadCount(0), 0u);
  EXPECT_EQ(captured->DuplicateUploads(), 0u);
}

// 24. Slice::off is a physical offset inside the source slice. Compaction
// must read from that offset while writing a dense output starting at off=0.
TEST_F(CompactorTest, ForceCompact_NonZeroSliceOff_ReadsPhysicalSubrange) {
  auto captured = std::make_shared<CapturedWrites>();
  InstallPatternBlockStore(mock_block_store_, captured);

  constexpr int64_t kMB = 1024 * 1024;
  std::vector<Slice> slices = {
      dingofs::client::vfs::test::MakeSlice(/*id=*/60, /*pos=*/2 * kMB,
                                            /*size=*/3 * kMB,
                                            /*off=*/kMB, /*len=*/kMB)};

  std::vector<Slice> out;
  Status s = compactor_->ForceCompact(ctx_, /*ino=*/505, /*chunk_index=*/0,
                                      slices, out);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(out.size(), 1u);
  EXPECT_EQ(out[0].pos, 2 * kMB);
  EXPECT_EQ(out[0].off, 0);
  EXPECT_EQ(out[0].len, kMB);

  std::string written = captured->SliceBytes(out[0].id);
  ASSERT_EQ(written.size(), static_cast<size_t>(kMB));
  for (int64_t off = 0; off < kMB; ++off) {
    ASSERT_EQ(static_cast<uint8_t>(written[off]), PatternAt(60, kMB + off))
        << "slice physical offset ignored at output offset " << off;
  }
}

// 25. A result spanning block boundaries must upload every block exactly once
// and preserve byte order across the boundary.
TEST_F(CompactorTest, ForceCompact_CrossBlock_UploadsEachBlockOnce) {
  auto captured = std::make_shared<CapturedWrites>();
  InstallPatternBlockStore(mock_block_store_, captured);

  constexpr int64_t kMB = 1024 * 1024;
  constexpr int64_t kSize = 5 * kMB;
  std::vector<Slice> slices = {
      dingofs::client::vfs::test::MakeSlice(/*id=*/70, /*pos=*/0, kSize)};

  std::vector<Slice> out;
  Status s = compactor_->ForceCompact(ctx_, /*ino=*/506, /*chunk_index=*/0,
                                      slices, out);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(out.size(), 1u);
  EXPECT_EQ(captured->BlockCount(out[0].id), 2u);
  EXPECT_EQ(captured->DuplicateUploads(), 0u);

  std::string written = captured->SliceBytes(out[0].id);
  ASSERT_EQ(written.size(), static_cast<size_t>(kSize));
  for (int64_t off = 0; off < kSize; ++off) {
    ASSERT_EQ(static_cast<uint8_t>(written[off]), PatternAt(70, off))
        << "cross-block byte mismatch at offset " << off;
  }
}

// 26. On success both entry points replace, rather than append to or retain,
// caller-provided output. This also defines the all-skipped result as empty.
TEST_F(CompactorTest, SuccessfulCalls_ReplacePrepopulatedOutput) {
  const Slice stale =
      dingofs::client::vfs::test::MakeSlice(/*id=*/999, /*pos=*/7, 4096);

  std::vector<Slice> force_out = {stale};
  std::vector<Slice> small = {
      dingofs::client::vfs::test::MakeSlice(/*id=*/80, /*pos=*/0, 4096)};
  ASSERT_TRUE(
      compactor_
          ->ForceCompact(ctx_, /*ino=*/507, /*chunk_index=*/0, small, force_out)
          .ok());
  ASSERT_EQ(force_out.size(), 1u);
  EXPECT_NE(force_out[0].id, stale.id);

  std::vector<Slice> skipped_out = {stale};
  std::vector<Slice> all_skipped = {MakeZeroSlice(0, 2 * 1024 * 1024)};
  ASSERT_TRUE(compactor_
                  ->Compact(ctx_, /*ino=*/508, /*chunk_index=*/0, all_skipped,
                            skipped_out)
                  .ok());
  EXPECT_TRUE(skipped_out.empty());
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
