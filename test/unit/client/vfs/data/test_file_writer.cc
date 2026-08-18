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

#include <gflags/gflags.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <future>
#include <mutex>
#include <thread>
#include <unordered_set>
#include <vector>

#include "client/vfs/data/writer/file_writer.h"
#include "common/options/client.h"
#include "common/trace/trace_manager.h"
#include "common/writemempool/write_mem_pool.h"
#include "test/unit/client/vfs/test_base.h"

namespace dingofs {
namespace client {
namespace vfs {

using dingofs::client::vfs::test::VFSTestBase;
using ::testing::_;
using ::testing::AnyNumber;
using ::testing::Invoke;
using ::testing::Return;

class FileWriterTestPeer {
 public:
  static int64_t RefCount(FileWriter* writer) {
    return writer->refs_.load(std::memory_order_acquire);
  }
};

class FileWriterTest : public VFSTestBase {
 protected:
  void SetUp() override {
    trace_manager_ = std::make_unique<TraceManager>();
    ON_CALL(*mock_hub_, GetTraceManager())
        .WillByDefault(Return(trace_manager_.get()));
    EXPECT_CALL(*mock_hub_, GetTraceManager()).Times(AnyNumber());
  }

  std::unique_ptr<TraceManager> trace_manager_;

  // Creates, acquires a ref on, and opens a FileWriter.
  // The caller owns the writer; ReleaseRef() destroys it.
  // (`fh` arg kept for legacy test call sites; ignored under the per-inode
  // shared writer model.)
  FileWriter* MakeOpenWriter(uint64_t ino = 200, uint64_t /*fh*/ = 2) {
    auto* w = new FileWriter(mock_hub_, ino);
    w->AcquireRef();
    CHECK(w->Open().ok());
    return w;
  }

  void FlushCloseAndRelease(FileWriter* w) {
    ASSERT_TRUE(w->Flush().ok());
    w->Close();
    w->ReleaseRef();
  }
};

TEST_F(FileWriterTest, StopReleasesPendingPeriodicTaskRef) {
  gflags::FlagSaver flag_saver;
  FLAGS_vfs_periodic_flush_interval_ms = 60 * 60 * 1000;

  auto* writer = MakeOpenWriter();
  ASSERT_EQ(FileWriterTestPeer::RefCount(writer), 2);

  writer->Close();
  ASSERT_TRUE(write_background_executor_->Stop());
  EXPECT_EQ(FileWriterTestPeer::RefCount(writer), 1);

  writer->ReleaseRef();
}

// 1. Write() for a simple in-chunk write succeeds and returns the correct
//    written size.
TEST_F(FileWriterTest, Write_SingleChunk_CorrectSize) {
  auto* w = MakeOpenWriter();

  const char buf[] = "hello world";
  uint64_t wsize = 0;
  Status s = w->Write(ctx_, buf, sizeof(buf), /*offset=*/0, &wsize);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(wsize, sizeof(buf));

  FlushCloseAndRelease(w);
}

// 2. Write() crossing a chunk boundary creates two chunk writers and writes
//    data across both.
TEST_F(FileWriterTest, Write_CrossingChunkBoundary) {
  auto* w = MakeOpenWriter();

  const uint64_t chunk_size = mock_hub_->GetFsInfo().chunk_size;  // 64 MiB

  // Write 8 bytes that straddle the boundary between chunk 0 and chunk 1.
  constexpr uint64_t kWriteSize = 8;
  uint64_t offset = chunk_size - 4;  // 4 bytes in chunk 0, 4 bytes in chunk 1

  std::vector<char> buf(kWriteSize, 'X');
  uint64_t wsize = 0;
  Status s = w->Write(ctx_, buf.data(), kWriteSize, offset, &wsize);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(wsize, kWriteSize);

  FlushCloseAndRelease(w);
}

// 2c. First chunk itself exhausts the pool: zero bytes land, so the write is a
//     hard failure -- NoSpace + out_wsize == 0 (not a short write).
TEST_F(FileWriterTest, Write_FirstChunkNoSpace_ReturnsNoSpaceZeroWsize) {
  // 1-page pool; a 2-page write to chunk 0 can't be reserved at all.
  auto tiny = std::make_unique<WriteMemPool>(4096, 4096);
  ON_CALL(*mock_hub_, GetWriteMemPool()).WillByDefault(Return(tiny.get()));

  auto* w = MakeOpenWriter();

  std::vector<char> buf(2 * 4096, 'X');  // 2 pages, pool holds 1
  uint64_t wsize = 12345;                // poison: must end up 0
  Status s = w->Write(ctx_, buf.data(), buf.size(), /*offset=*/0, &wsize);

  EXPECT_TRUE(s.IsNoSpace()) << s.ToString();
  EXPECT_EQ(wsize, 0u) << "zero bytes written -> hard failure, not short write";

  w->Close();
  w->ReleaseRef();
}

// 3. Flush() returns OK when no data was written.
TEST_F(FileWriterTest, Flush_NoWrites_ReturnsOK) {
  auto* w = MakeOpenWriter();
  Status s = w->Flush();
  EXPECT_TRUE(s.ok());

  w->Close();
  w->ReleaseRef();
}

// 4. Flush() after a write calls WriteSlice on MetaSystem.
TEST_F(FileWriterTest, Flush_AfterWrite_CallsWriteSlice) {
  int write_slice_calls = 0;
  ON_CALL(*mock_meta_system_, WriteSlice)
      .WillByDefault([&](auto, auto, auto, auto, auto) {
        ++write_slice_calls;
        return Status::OK();
      });

  auto* w = MakeOpenWriter();

  const char buf[] = "flush me";
  uint64_t wsize = 0;
  w->Write(ctx_, buf, sizeof(buf), 0, &wsize);

  Status s = w->Flush();
  EXPECT_TRUE(s.ok());
  EXPECT_GE(write_slice_calls, 1);

  w->Close();
  w->ReleaseRef();
}

// 5. Flush() propagates WriteSlice errors.
TEST_F(FileWriterTest, Flush_WriteSliceError_Propagated) {
  ON_CALL(*mock_meta_system_, WriteSlice)
      .WillByDefault(Return(Status::Internal("flush error")));

  auto* w = MakeOpenWriter();

  const char buf[] = "data";
  uint64_t wsize = 0;
  w->Write(ctx_, buf, sizeof(buf), 0, &wsize);

  Status s = w->Flush();
  EXPECT_FALSE(s.ok());

  w->Close();
  w->ReleaseRef();
}

// 6. Concurrent writes followed by an explicit Flush and Close are safe.
// Close itself is tested against an actually in-flight flush below.
TEST_F(FileWriterTest, ConcurrentWrites_ThenFlushClose) {
  auto* w = MakeOpenWriter();

  constexpr int kThreads = 4;
  std::vector<std::thread> threads;
  threads.reserve(kThreads);

  const uint64_t chunk_size = mock_hub_->GetFsInfo().chunk_size;

  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back([&, i]() {
      uint64_t offset = static_cast<uint64_t>(i) * 4096;
      if (offset + 4096 > chunk_size) return;
      std::vector<char> buf(4096, static_cast<char>('0' + i));
      uint64_t wsize = 0;
      w->Write(ctx_, buf.data(), 4096, offset, &wsize);
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  // Close only validates/cleans up; the lifecycle owner must flush first.
  FlushCloseAndRelease(w);
}

TEST_F(FileWriterTest, Close_WaitsForInflightFlush) {
  std::mutex mutex;
  std::condition_variable cv;
  bool write_slice_entered = false;
  bool allow_write_slice = false;

  ON_CALL(*mock_meta_system_, WriteSlice)
      .WillByDefault([&](auto, auto, auto, auto, auto) {
        std::unique_lock<std::mutex> lock(mutex);
        write_slice_entered = true;
        cv.notify_all();
        cv.wait(lock, [&] { return allow_write_slice; });
        return Status::OK();
      });

  auto* w = MakeOpenWriter();
  const char buf[] = "blocked flush";
  uint64_t wsize = 0;
  ASSERT_TRUE(w->Write(ctx_, buf, sizeof(buf), 0, &wsize).ok());

  auto flush = std::async(std::launch::async, [w] { return w->Flush(); });
  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(5),
                            [&] { return write_slice_entered; }));
  }

  auto close = std::async(std::launch::async, [w] { w->Close(); });
  EXPECT_EQ(close.wait_for(std::chrono::milliseconds(50)),
            std::future_status::timeout);

  {
    std::lock_guard<std::mutex> lock(mutex);
    allow_write_slice = true;
  }
  cv.notify_all();

  ASSERT_TRUE(flush.get().ok());
  EXPECT_EQ(close.wait_for(std::chrono::seconds(5)), std::future_status::ready);
  close.get();
  w->ReleaseRef();
}

// 7. Multiple Flush() calls in sequence all succeed.
TEST_F(FileWriterTest, MultipleFlush_AllSucceed) {
  auto* w = MakeOpenWriter();

  const char buf[] = "repeat";
  uint64_t wsize = 0;
  w->Write(ctx_, buf, sizeof(buf), 0, &wsize);

  for (int i = 0; i < 3; ++i) {
    Status s = w->Flush();
    EXPECT_TRUE(s.ok()) << "Flush #" << i << " failed";
  }

  w->Close();
  w->ReleaseRef();
}

// 8. Write() on a closed FileWriter returns an error.
TEST_F(FileWriterTest, Write_AfterClose_ReturnsError) {
  auto* w = MakeOpenWriter();
  w->Close();

  const char buf[] = "after close";
  uint64_t wsize = 12345;  // poison: the fast-fail path must reset it to 0
  Status s = w->Write(ctx_, buf, sizeof(buf), 0, &wsize);
  EXPECT_FALSE(s.ok());
  EXPECT_EQ(wsize, 0u) << "fast-fail must define out_wsize = 0";

  // Release the ref (Close was already called above by test).
  w->ReleaseRef();
}

// 9. Ino() returns the value passed to the constructor — used by
// WriterTable as the inode key.
TEST_F(FileWriterTest, Ino_ReturnsConstructedValue) {
  constexpr uint64_t kIno = 4242;
  auto* w = MakeOpenWriter(kIno);
  EXPECT_EQ(w->Ino(), kIno);

  w->Close();
  w->ReleaseRef();
}

// 10. A freshly-opened writer has a clean (OK) sticky status.
TEST_F(FileWriterTest, GetStatus_Default_IsOK) {
  auto* w = MakeOpenWriter();
  EXPECT_TRUE(w->GetStatus().ok());

  w->Close();
  w->ReleaseRef();
}

// 11. SetStatusIfBroken honors first-error-wins: an OK input is a no-op,
// and a second error must NOT overwrite the first.
TEST_F(FileWriterTest, SetStatusIfBroken_FirstErrorWins) {
  auto* w = MakeOpenWriter();

  // OK input must not mutate the OK starting state.
  w->SetStatusIfBroken(Status::OK());
  EXPECT_TRUE(w->GetStatus().ok());

  // First broken status sticks.
  w->SetStatusIfBroken(Status::Internal("first"));
  EXPECT_FALSE(w->GetStatus().ok());
  std::string first_msg = w->GetStatus().ToString();

  // Second broken status must NOT replace the first.
  w->SetStatusIfBroken(Status::IoError("second"));
  EXPECT_EQ(w->GetStatus().ToString(), first_msg)
      << "first error must win; subsequent errors are dropped";

  w->Close();
  w->ReleaseRef();
}

// 12. Once status is broken, all subsequent Write calls fast-fail with
// the broken status — this is the cross-fh consistency mechanism: one fh
// observing an error makes every other fh on the same inode fail too.
TEST_F(FileWriterTest, StickyStatus_BlocksSubsequentWrites) {
  auto* w = MakeOpenWriter();

  w->SetStatusIfBroken(Status::Internal("simulated upload failure"));
  ASSERT_FALSE(w->GetStatus().ok());

  const char buf[] = "blocked";
  uint64_t wsize = 12345;  // poison: the fast-fail path must reset it to 0
  Status s = w->Write(ctx_, buf, sizeof(buf), 0, &wsize);
  EXPECT_FALSE(s.ok()) << "Write must fast-fail when sticky status is broken";
  EXPECT_EQ(wsize, 0u) << "fast-fail must define out_wsize = 0";

  w->Close();
  w->ReleaseRef();
}

// 13. A Flush failure must promote into the sticky status so subsequent
// Write calls observe the failure.
TEST_F(FileWriterTest, FlushError_BecomesStickyAndBlocksWrites) {
  ON_CALL(*mock_meta_system_, WriteSlice)
      .WillByDefault(Return(Status::Internal("flush error")));

  auto* w = MakeOpenWriter();

  // Write some data so Flush has something to commit.
  const char buf[] = "data";
  uint64_t wsize = 0;
  ASSERT_TRUE(w->Write(ctx_, buf, sizeof(buf), 0, &wsize).ok());

  // Flush fails — failure must promote to sticky status.
  Status fs = w->Flush();
  ASSERT_FALSE(fs.ok());
  EXPECT_FALSE(w->GetStatus().ok())
      << "Flush failure must be promoted to sticky status";

  // Subsequent Write fast-fails on the sticky status.
  Status ws = w->Write(ctx_, buf, sizeof(buf), 4096, &wsize);
  EXPECT_FALSE(ws.ok());

  w->Close();
  w->ReleaseRef();
}

// Close is not a persistence operation. If no Flush error explains the state,
// closing a writer whose latest write generation was never flushed is a
// lifecycle bug and must fail fast.
TEST_F(FileWriterTest, Close_UnflushedWrite_CheckFails) {
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  auto* w = MakeOpenWriter();

  const char buf[] = "unflushed";
  uint64_t wsize = 0;
  ASSERT_TRUE(w->Write(ctx_, buf, sizeof(buf), 0, &wsize).ok());

  EXPECT_DEATH(w->Close(), "Close found unflushed data");

  // EXPECT_DEATH runs in a child; clean up the unchanged parent-side writer.
  FlushCloseAndRelease(w);
}

TEST_F(FileWriterTest, Flush_AfterClose_ReturnsBadFd) {
  auto* w = MakeOpenWriter();
  w->Close();

  Status s = w->Flush();
  EXPECT_FALSE(s.ok());
  EXPECT_EQ(s.ToSysErrNo(), EBADF);
  w->ReleaseRef();
}

TEST_F(FileWriterTest, PeriodicFlushErrorBecomesStickyAndBlocksWrites) {
  gflags::FlagSaver flag_saver;
  FLAGS_vfs_periodic_flush_interval_ms = 1;

  std::mutex mutex;
  std::condition_variable cv;
  bool write_slice_called = false;
  ON_CALL(*mock_meta_system_, WriteSlice)
      .WillByDefault([&](auto, auto, auto, auto, auto) {
        {
          std::lock_guard<std::mutex> lock(mutex);
          write_slice_called = true;
        }
        cv.notify_all();
        return Status::Internal("periodic flush failed");
      });

  auto* w = MakeOpenWriter();
  const char buf[] = "periodic";
  uint64_t wsize = 0;
  ASSERT_TRUE(w->Write(ctx_, buf, sizeof(buf), 0, &wsize).ok());

  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(5),
                            [&] { return write_slice_called; }));
  }

  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (w->GetStatus().ok() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  ASSERT_FALSE(w->GetStatus().ok());

  wsize = 12345;
  Status s = w->Write(ctx_, buf, sizeof(buf), 4096, &wsize);
  EXPECT_FALSE(s.ok());
  EXPECT_EQ(wsize, 0u);

  w->Close();
  w->ReleaseRef();
}

// A successful explicit Flush owns all writeback. Close must not submit a
// second WriteSlice after metadata lifecycle code is free to close the session.
TEST_F(FileWriterTest, Close_AfterFlush_DoesNotFlushAgain) {
  int write_slice_calls = 0;
  ON_CALL(*mock_meta_system_, WriteSlice)
      .WillByDefault([&](auto, auto, auto, auto, auto) {
        ++write_slice_calls;
        return Status::OK();
      });

  auto* w = MakeOpenWriter();
  const char buf[] = "data";
  uint64_t wsize = 0;
  ASSERT_TRUE(w->Write(ctx_, buf, sizeof(buf), 0, &wsize).ok());
  ASSERT_TRUE(w->Flush().ok());
  const int calls_after_flush = write_slice_calls;

  w->Close();
  EXPECT_EQ(write_slice_calls, calls_after_flush);
  w->ReleaseRef();
}

TEST_F(FileWriterTest, ConcurrentFlushesAdvanceToLatestGeneration) {
  std::mutex mutex;
  std::condition_variable cv;
  bool first_write_slice_entered = false;
  bool allow_first_write_slice = false;
  int write_slice_calls = 0;

  ON_CALL(*mock_meta_system_, WriteSlice)
      .WillByDefault([&](auto, auto, auto, auto, auto) {
        std::unique_lock<std::mutex> lock(mutex);
        if (++write_slice_calls == 1) {
          first_write_slice_entered = true;
          cv.notify_all();
          cv.wait(lock, [&] { return allow_first_write_slice; });
        }
        return Status::OK();
      });

  auto* w = MakeOpenWriter();
  const char first[] = "first";
  uint64_t wsize = 0;
  ASSERT_TRUE(w->Write(ctx_, first, sizeof(first), 0, &wsize).ok());

  auto first_flush = std::async(std::launch::async, [w] { return w->Flush(); });
  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(5),
                            [&] { return first_write_slice_entered; }));
  }

  const char second[] = "second";
  const uint64_t second_offset = mock_hub_->GetFsInfo().chunk_size;
  ASSERT_TRUE(
      w->Write(ctx_, second, sizeof(second), second_offset, &wsize).ok());
  auto second_flush =
      std::async(std::launch::async, [w] { return w->Flush(); });

  {
    std::lock_guard<std::mutex> lock(mutex);
    allow_first_write_slice = true;
  }
  cv.notify_all();

  ASSERT_TRUE(first_flush.get().ok());
  ASSERT_TRUE(second_flush.get().ok());
  w->Close();
  w->ReleaseRef();
}

TEST_F(FileWriterTest, MultiChunkFlushFailureStillVisitsEveryChunk) {
  std::mutex mutex;
  std::unordered_set<uint64_t> visited_chunks;
  ON_CALL(*mock_meta_system_, WriteSlice)
      .WillByDefault([&](auto, auto, uint64_t chunk_index, auto, auto) {
        std::lock_guard<std::mutex> lock(mutex);
        visited_chunks.insert(chunk_index);
        return chunk_index == 0 ? Status::Internal("chunk 0 flush failed")
                                : Status::OK();
      });

  auto* w = MakeOpenWriter();
  const uint64_t chunk_size = mock_hub_->GetFsInfo().chunk_size;
  const char buf[] = "data";
  uint64_t wsize = 0;
  ASSERT_TRUE(w->Write(ctx_, buf, sizeof(buf), 0, &wsize).ok());
  ASSERT_TRUE(w->Write(ctx_, buf, sizeof(buf), chunk_size, &wsize).ok());

  Status s = w->Flush();
  EXPECT_FALSE(s.ok());
  EXPECT_FALSE(w->GetStatus().ok());
  {
    std::lock_guard<std::mutex> lock(mutex);
    EXPECT_EQ(visited_chunks, (std::unordered_set<uint64_t>{0, 1}));
  }

  // The known flush error explains the generation mismatch; Close must clean
  // up without converting the already-reported I/O failure into a CHECK.
  w->Close();
  w->ReleaseRef();
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
