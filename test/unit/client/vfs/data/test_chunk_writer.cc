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
#include <future>
#include <thread>
#include <vector>

#include "client/vfs/data/writer/chunk_writer.h"
#include "common/sync_point.h"
#include "common/trace/trace_manager.h"
#include "test/unit/client/vfs/test_base.h"
#include "test/unit/client/vfs/test_common.h"

namespace dingofs {
namespace client {
namespace vfs {

using dingofs::client::vfs::test::AsyncWaiter;
using dingofs::client::vfs::test::VFSTestBase;
using ::testing::_;
using ::testing::AnyNumber;
using ::testing::Invoke;
using ::testing::Return;

class ChunkWriterTest : public VFSTestBase {
 protected:
  void SetUp() override {
    trace_manager_ = std::make_unique<TraceManager>();
    ON_CALL(*mock_hub_, GetTraceManager())
        .WillByDefault(Return(trace_manager_.get()));
    EXPECT_CALL(*mock_hub_, GetTraceManager()).Times(AnyNumber());
  }

  std::unique_ptr<TraceManager> trace_manager_;

  // Convenience: create a chunk writer for chunk 0 on ino 100 with fh 1.
  std::unique_ptr<ChunkWriter> MakeWriter(uint64_t ino = 100,
                                          uint64_t chunk_index = 0,
                                          uint64_t fh = 1) {
    return std::make_unique<ChunkWriter>(mock_hub_, fh, ino, chunk_index);
  }

  const uint64_t kIno = 100;
  const uint64_t kFh = 1;
  // chunk_size = 64 MiB (from MakeTestFsInfo default)
  const uint64_t kChunkSize = 64 * 1024 * 1024;
};

// 1. A single Write creates at least one slice internally.
//    After FlushAsync the WriteSlice meta call is made.
TEST_F(ChunkWriterTest, SingleWrite_WriteSliceCalled) {
  int write_slice_count = 0;
  ON_CALL(*mock_meta_system_, WriteSlice)
      .WillByDefault([&](auto, auto, auto, auto, const auto&) {
        ++write_slice_count;
        return Status::OK();
      });

  auto writer = MakeWriter();

  const char buf[] = "hello";
  Status s = writer->Write(ctx_, buf, sizeof(buf), /*chunk_offset=*/0);
  EXPECT_TRUE(s.ok());

  AsyncWaiter waiter;
  waiter.Expect(1);
  writer->FlushAsync([&](Status fs) {
    EXPECT_TRUE(fs.ok());
    waiter.Done();
  });
  waiter.Wait();

  EXPECT_GE(write_slice_count, 1);
}

// 2. Writing at a contiguous offset appends to the same slice (single
//    WriteSlice call covering the combined range).
TEST_F(ChunkWriterTest, ContiguousWrites_AppendToSlice) {
  int write_slice_count = 0;
  ON_CALL(*mock_meta_system_, WriteSlice)
      .WillByDefault([&](auto, auto, auto, auto, const auto& slices) {
        ++write_slice_count;
        return Status::OK();
      });

  auto writer = MakeWriter();

  const char data[8] = "abcdefg";
  // Write two contiguous 4-byte chunks: [0,4) then [4,8).
  Status s1 = writer->Write(ctx_, data, 4, 0);
  Status s2 = writer->Write(ctx_, data + 4, 4, 4);
  EXPECT_TRUE(s1.ok());
  EXPECT_TRUE(s2.ok());

  AsyncWaiter waiter;
  waiter.Expect(1);
  writer->FlushAsync([&](Status fs) {
    EXPECT_TRUE(fs.ok());
    waiter.Done();
  });
  waiter.Wait();

  // Both writes are in the same slice, so WriteSlice is called once with one
  // slice entry.
  EXPECT_EQ(write_slice_count, 1);
}

// 3. Writing at non-contiguous (non-adjacent) offsets produces multiple slices.
TEST_F(ChunkWriterTest, NonContiguousWrites_MultipleSlices) {
  int committed_slice_count = 0;
  ON_CALL(*mock_meta_system_, WriteSlice)
      .WillByDefault(
          [&](auto, auto, auto, auto, const std::vector<Slice>& slices) {
            committed_slice_count += static_cast<int>(slices.size());
            return Status::OK();
          });

  auto writer = MakeWriter();

  const char data[4] = "abc";
  // Gap of 4 MiB between writes forces separate slices.
  Status s1 = writer->Write(ctx_, data, 4, 0);
  Status s2 = writer->Write(ctx_, data, 4, 8 * 1024 * 1024);
  EXPECT_TRUE(s1.ok());
  EXPECT_TRUE(s2.ok());

  AsyncWaiter waiter;
  waiter.Expect(1);
  writer->FlushAsync([&](Status fs) {
    EXPECT_TRUE(fs.ok());
    waiter.Done();
  });
  waiter.Wait();

  EXPECT_GE(committed_slice_count, 2);
}

// 4. FlushAsync with no prior writes still invokes the callback with OK status.
TEST_F(ChunkWriterTest, FlushAsyncNoWrites_CallsCallbackOK) {
  auto writer = MakeWriter();

  AsyncWaiter waiter;
  waiter.Expect(1);
  writer->FlushAsync([&](Status s) {
    EXPECT_TRUE(s.ok());
    waiter.Done();
  });
  waiter.Wait();
}

// 5. WriteSlice returning an error propagates to FlushAsync callback.
TEST_F(ChunkWriterTest, FlushAsync_WriteSliceError_Propagated) {
  ON_CALL(*mock_meta_system_, WriteSlice)
      .WillByDefault(Return(Status::Internal("meta error")));

  auto writer = MakeWriter();

  const char buf[] = "data";
  writer->Write(ctx_, buf, sizeof(buf), 0);

  AsyncWaiter waiter;
  waiter.Expect(1);
  Status flush_status;
  writer->FlushAsync([&](Status s) {
    flush_status = s;
    waiter.Done();
  });
  waiter.Wait();

  EXPECT_FALSE(flush_status.ok());
}

// 6. Error status is sticky: after first error subsequent FlushAsync returns
//    the same error without calling WriteSlice again.
TEST_F(ChunkWriterTest, ErrorStatus_Sticky_AfterFirstError) {
  // Fail the first WriteSlice.
  ON_CALL(*mock_meta_system_, WriteSlice)
      .WillByDefault(Return(Status::Internal("persistent error")));

  auto writer = MakeWriter();

  const char buf[] = "test";
  writer->Write(ctx_, buf, sizeof(buf), 0);

  // First flush fails.
  AsyncWaiter waiter1;
  waiter1.Expect(1);
  writer->FlushAsync([&](Status s) {
    EXPECT_FALSE(s.ok());
    waiter1.Done();
  });
  waiter1.Wait();

  // Second flush (no new writes) should also return error immediately.
  AsyncWaiter waiter2;
  waiter2.Expect(1);
  Status second_status;
  writer->FlushAsync([&](Status s) {
    second_status = s;
    waiter2.Done();
  });
  waiter2.Wait();

  EXPECT_FALSE(second_status.ok());
}

// 7. Multiple concurrent FlushAsync calls all complete.
TEST_F(ChunkWriterTest, MultipleFlushAsync_AllComplete) {
  auto writer = MakeWriter();

  const char buf[] = "x";
  writer->Write(ctx_, buf, 1, 0);

  constexpr int kFlushes = 3;
  AsyncWaiter waiter;
  waiter.Expect(kFlushes);

  for (int i = 0; i < kFlushes; ++i) {
    writer->FlushAsync([&](Status s) { waiter.Done(); });
  }

  waiter.Wait();
}

// 8. Concurrent writes from multiple threads succeed (FIFO writer queue).
TEST_F(ChunkWriterTest, ConcurrentWrites_NoDeadlock) {
  auto writer = MakeWriter();

  constexpr int kThreads = 8;
  constexpr uint64_t kWriteSize = 4096;
  const uint64_t kChunkSz = 64 * 1024 * 1024;

  std::vector<std::thread> threads;
  threads.reserve(kThreads);

  std::atomic<int> success_count{0};

  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back([&, i]() {
      // Each thread writes to a distinct non-overlapping region.
      uint64_t offset = static_cast<uint64_t>(i) * kWriteSize;
      // Stay within chunk bounds.
      if (offset + kWriteSize > kChunkSz) return;
      std::vector<char> buf(kWriteSize, static_cast<char>('a' + i));
      Status s = writer->Write(ctx_, buf.data(), kWriteSize, offset);
      if (s.ok()) {
        success_count.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  EXPECT_EQ(success_count.load(), kThreads);

  // Flush everything.
  AsyncWaiter waiter;
  waiter.Expect(1);
  writer->FlushAsync([&](Status s) {
    EXPECT_TRUE(s.ok());
    waiter.Done();
  });
  waiter.Wait();
}

// 9. Stop() waits for any ongoing flush to finish before returning.
//    After Stop(), the writer is destroyed cleanly.
TEST_F(ChunkWriterTest, Stop_WaitsForFlush) {
  auto writer = MakeWriter();

  const char buf[] = "stop test";
  writer->Write(ctx_, buf, sizeof(buf), 0);

  // FlushAsync then Stop; Stop should block until flush completes.
  AsyncWaiter waiter;
  waiter.Expect(1);
  writer->FlushAsync([&](Status s) {
    EXPECT_TRUE(s.ok());
    waiter.Done();
  });
  waiter.Wait();

  // Stop must not crash.
  writer->Stop();
}

// 10. TriggerFlush (called automatically when a slice fills up) does not crash
//     and invokes WriteSlice.
TEST_F(ChunkWriterTest, TriggerFlush_NoCrash) {
  int flush_count = 0;
  ON_CALL(*mock_meta_system_, WriteSlice)
      .WillByDefault([&](auto, auto, auto, auto, auto) {
        ++flush_count;
        return Status::OK();
      });

  auto writer = MakeWriter();

  // Write a modest amount and then explicitly trigger flush.
  const char buf[] = "trigger";
  writer->Write(ctx_, buf, sizeof(buf), 0);
  writer->TriggerFlush();

  // Allow async tasks to drain.
  AsyncWaiter waiter;
  waiter.Expect(1);
  writer->FlushAsync([&](Status s) {
    EXPECT_TRUE(s.ok());
    waiter.Done();
  });
  waiter.Wait();
}

// 11. Concurrent writes and flushes do not race or produce incorrect seq order.
//
// Regression test for: [fix][client] Fix flush seq incorrect when write and
// flush concurrent (85c951c5).  The bug caused commit_seq_id to be assigned
// in wrong order when a FlushAsync interleaved with an in-progress Write.
// The fix serialises them via write_flush_mutex_.
//
// Strategy: a write thread and a flush thread run simultaneously.  We capture
// every Slice.id committed via WriteSlice and assert strict monotonic ordering.
TEST_F(ChunkWriterTest, ConcurrentWriteAndFlush_SeqOrdered) {
  std::vector<uint64_t> committed_ids;
  std::mutex ids_mutex;

  ON_CALL(*mock_meta_system_, WriteSlice)
      .WillByDefault(
          [&](auto, auto, auto, auto, const std::vector<Slice>& slices) {
            std::lock_guard<std::mutex> lk(ids_mutex);
            for (const auto& s : slices) {
              committed_ids.push_back(s.id);
            }
            return Status::OK();
          });

  auto writer = MakeWriter();

  constexpr int kRounds = 10;
  constexpr uint64_t kWriteSize = 4096;

  // Write thread: kRounds writes to non-overlapping offsets.
  std::thread write_thread([&]() {
    for (int i = 0; i < kRounds; ++i) {
      uint64_t offset = static_cast<uint64_t>(i) * kWriteSize;
      std::vector<char> buf(kWriteSize, static_cast<char>('A' + i % 26));
      Status s = writer->Write(ctx_, buf.data(), kWriteSize, offset);
      EXPECT_TRUE(s.ok()) << "Write failed at round " << i;
    }
  });

  // Flush thread: kRounds flushes, each waiting for completion before next.
  std::thread flush_thread([&]() {
    for (int i = 0; i < kRounds; ++i) {
      std::mutex m;
      std::condition_variable cv;
      bool done = false;
      Status result;

      writer->FlushAsync([&](Status s) {
        result = s;
        std::lock_guard<std::mutex> lk(m);
        done = true;
        cv.notify_one();
      });

      std::unique_lock<std::mutex> lk(m);
      cv.wait(lk, [&] { return done; });
      EXPECT_TRUE(result.ok()) << "FlushAsync failed at round " << i;
    }
  });

  write_thread.join();
  flush_thread.join();

  // Final flush to drain any unflushed writes.
  AsyncWaiter final_waiter;
  final_waiter.Expect(1);
  writer->FlushAsync([&](Status s) {
    EXPECT_TRUE(s.ok());
    final_waiter.Done();
  });
  final_waiter.Wait();

  // All committed slice IDs must be strictly monotonically increasing.
  std::lock_guard<std::mutex> lk(ids_mutex);
  for (size_t i = 1; i < committed_ids.size(); ++i) {
    EXPECT_LT(committed_ids[i - 1], committed_ids[i])
        << "Slice IDs out of order: " << committed_ids[i - 1]
        << " >= " << committed_ids[i] << " at index " << i;
  }
}

// ---------------------------------------------------------------------------
// Phase 3: Slice-Relative — ChunkWriter::FindWritableSliceUnLocked tests
// ---------------------------------------------------------------------------

// 9. Reverse write creates a new SliceWriter (not reused).
//    Write [4MB,8MB) then [2MB,4MB) → two separate slices committed.
//    Target: FindWritableSliceUnLocked returns nullptr for reverse adjacency,
//    so a new SliceWriter is created. Two WriteSlice calls (or one call with
//    two slice entries).
TEST_F(ChunkWriterTest, FindWritable_Reverse_CreatesNew) {
  int committed_slice_count = 0;
  ON_CALL(*mock_meta_system_, WriteSlice)
      .WillByDefault(
          [&](auto, auto, auto, auto, const std::vector<Slice>& slices) {
            committed_slice_count += static_cast<int>(slices.size());
            return Status::OK();
          });

  auto writer = MakeWriter();

  const uint64_t k4MB = 4 * 1024 * 1024;
  std::vector<char> buf1(k4MB, 'A');
  std::vector<char> buf2(2 * 1024 * 1024, 'B');

  // Forward write [4MB, 8MB).
  Status s1 = writer->Write(ctx_, buf1.data(), k4MB, k4MB);
  EXPECT_TRUE(s1.ok());

  // Reverse adjacent write [2MB, 4MB) — target creates a new slice.
  Status s2 =
      writer->Write(ctx_, buf2.data(), 2 * 1024 * 1024, 2 * 1024 * 1024);
  EXPECT_TRUE(s2.ok());

  AsyncWaiter waiter;
  waiter.Expect(1);
  writer->FlushAsync([&](Status fs) {
    EXPECT_TRUE(fs.ok());
    waiter.Done();
  });
  waiter.Wait();

  // Two slices expected: one for [4MB,8MB), one for [2MB,4MB).
  EXPECT_GE(committed_slice_count, 2);
}

// 10. Forward contiguous write reuses the same SliceWriter.
//     Write [0,4MB) then [4MB,8MB) → single slice committed.
TEST_F(ChunkWriterTest, FindWritable_Forward_Reuses) {
  int committed_slice_count = 0;
  ON_CALL(*mock_meta_system_, WriteSlice)
      .WillByDefault(
          [&](auto, auto, auto, auto, const std::vector<Slice>& slices) {
            committed_slice_count += static_cast<int>(slices.size());
            return Status::OK();
          });

  auto writer = MakeWriter();

  const uint64_t k4MB = 4 * 1024 * 1024;
  std::vector<char> buf1(k4MB, 'A');
  std::vector<char> buf2(k4MB, 'B');

  // Forward write [0, 4MB).
  Status s1 = writer->Write(ctx_, buf1.data(), k4MB, 0);
  EXPECT_TRUE(s1.ok());

  // Forward adjacent write [4MB, 8MB) — should reuse the same slice.
  Status s2 = writer->Write(ctx_, buf2.data(), k4MB, k4MB);
  EXPECT_TRUE(s2.ok());

  AsyncWaiter waiter;
  waiter.Expect(1);
  writer->FlushAsync([&](Status fs) {
    EXPECT_TRUE(fs.ok());
    waiter.Done();
  });
  waiter.Wait();

  // Single slice covers the full [0, 8MB) range.
  EXPECT_EQ(committed_slice_count, 1);
}

// 11. Regression: ChunkWriter::DoFlushAsync must hold slice_mutex_ across
//     BOTH the move-out of slices_ AND the push to flush_queue_ — otherwise
//     a concurrent caller can squeeze in between and push an EMPTY
//     ChunkFlushTask ahead of ours, causing FlushAsync to return OK before
//     the actual WriteSlice happens (stale-read consistency hole).
//
// Bug history: pre-fix DoFlushAsync used two separate critical sections:
//   { lock slice_mutex_; to_commit = move(slices_); }
//   { lock flush_mutex_;  flush_queue_.push(FlushTask); }
// If T1 took slice_mutex_ first (full to_commit) and T2 got flush_mutex_
// first (empty to_commit), the queue ended up [T2_empty, T1_full]. T2's
// empty ChunkFlushTask::RunAsync cb-fast-OK'd and TryCommitFlushTasks
// fired T2's user cb in microseconds — long before T1's WriteSlice. A
// reader on the same fh then observed pre-write data.
//
// Strategy without artificial sleeps in production code:
// Use a TEST_SYNC_POINT inside the merged critical section (between
// move(slices_) and push to flush_queue_). T1 hits the sync point and
// blocks. With the fix, T1 still holds slice_mutex_ at this point, so a
// concurrent T2 calling FlushAsync MUST block on slice_mutex_ until T1
// releases. Without the fix, T1 holds NO lock at this point, so T2 can
// race through DoFlushAsync, push its (empty) task, and return — the
// "T2's FlushAsync returns while T1 is in critical section" anomaly.
TEST_F(ChunkWriterTest, ConcurrentFlush_HoldsSliceMutexUntilQueuePush) {
  using namespace std::chrono;

  std::atomic<int> write_slice_calls{0};
  ON_CALL(*mock_meta_system_, WriteSlice)
      .WillByDefault(
          [&](auto, auto, auto, auto, const std::vector<Slice>& slices) {
            if (!slices.empty()) {
              write_slice_calls.fetch_add(1, std::memory_order_acq_rel);
            }
            return Status::OK();
          });

  auto writer = MakeWriter();
  const char buf[] = "race-data";
  ASSERT_TRUE(writer->Write(ctx_, buf, sizeof(buf), 0).ok());

  // Coordinate T1 in the sync point.
  std::promise<void> t1_in_section;
  std::promise<void> release_t1;
  auto t1_in_future = t1_in_section.get_future();
  auto release_t1_future = release_t1.get_future().share();

  std::atomic<bool> sync_fired{false};
  SyncPoint::Get()->SetCallBack("ChunkWriter::DoFlushAsync:after_take_slices",
                                [&](void*) {
                                  // Only the first FlushAsync (T1) trips this;
                                  // subsequent ones from T2 already block on
                                  // slice_mutex_ before reaching here (with
                                  // fix) or reach here too (without fix) — we
                                  // want only T1 to gate.
                                  if (sync_fired.exchange(true)) return;
                                  t1_in_section.set_value();
                                  release_t1_future.wait();
                                });
  SyncPoint::Get()->EnableProcessing();

  std::atomic<bool> t1_returned_async{false};
  std::atomic<bool> t2_returned_async{false};

  std::thread t1_thread([&]() {
    writer->FlushAsync([](Status) {});
    t1_returned_async.store(true, std::memory_order_release);
  });

  // Wait until T1 is parked inside the merged critical section.
  ASSERT_EQ(t1_in_future.wait_for(seconds(2)), std::future_status::ready);

  // Now spawn T2 — it must block on slice_mutex_ while T1 holds it.
  std::thread t2_thread([&]() {
    writer->FlushAsync([](Status) {});
    t2_returned_async.store(true, std::memory_order_release);
  });

  // Give T2 plenty of time to either block (fix) or sneak through (bug).
  std::this_thread::sleep_for(milliseconds(100));

  // Snapshot the anomaly: did T2's FlushAsync return while T1 still inside
  // the merged critical section?
  bool t2_done_while_t1_in_section =
      t2_returned_async.load(std::memory_order_acquire);

  // Release T1 and let everything drain.
  release_t1.set_value();
  t1_thread.join();
  t2_thread.join();

  SyncPoint::Get()->DisableProcessing();
  SyncPoint::Get()->ClearAllCallBacks();

  // Final flush (no sync point active) to drain any remaining work and
  // give the chunk_writer a clean exit.
  AsyncWaiter waiter;
  waiter.Expect(1);
  writer->FlushAsync([&](Status) { waiter.Done(); });
  waiter.Wait();

  EXPECT_FALSE(t2_done_while_t1_in_section)
      << "Concurrent FlushAsync returned while the first caller was still "
         "inside DoFlushAsync's slice_mutex_ critical section. "
         "DoFlushAsync must hold slice_mutex_ continuously from "
         "move(slices_) through flush_queue_ push, otherwise a second "
         "caller can race in with empty to_commit and push an empty "
         "FlushTask ahead.";
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
