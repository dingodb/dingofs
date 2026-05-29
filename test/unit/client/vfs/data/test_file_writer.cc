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

#include <cstdint>
#include <thread>
#include <vector>

#include "client/vfs/data/writer/file_writer.h"
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
};

// 1. Write() for a simple in-chunk write succeeds and returns the correct
//    written size.
TEST_F(FileWriterTest, Write_SingleChunk_CorrectSize) {
  auto* w = MakeOpenWriter();

  const char buf[] = "hello world";
  uint64_t wsize = 0;
  Status s = w->Write(ctx_, buf, sizeof(buf), /*offset=*/0, &wsize);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(wsize, sizeof(buf));

  w->Close();
  w->ReleaseRef();
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

  w->Close();
  w->ReleaseRef();
}

// 2b. Cross-chunk pool exhaustion is a POSIX short write: chunk0 succeeds,
//     chunk1 hits the exhausted pool. FileWriter must return OK with
//     out_wsize == the chunk0 prefix (not an error that discards it), so the
//     caller advances and re-issues the rest.
TEST_F(FileWriterTest, Write_CrossChunk_PoolExhaustion_ShortWrite) {
  // 1-page pool: chunk0's page fits, chunk1's page then fails.
  auto tiny = std::make_unique<WriteMemPool>(4096, 4096);
  ON_CALL(*mock_hub_, GetWriteMemPool()).WillByDefault(Return(tiny.get()));

  auto* w = MakeOpenWriter();

  const uint64_t chunk_size = mock_hub_->GetFsInfo().chunk_size;
  // 8 bytes straddling the chunk 0/1 boundary: 4 bytes in each chunk.
  constexpr uint64_t kWriteSize = 8;
  uint64_t offset = chunk_size - 4;

  std::vector<char> buf(kWriteSize, 'X');
  uint64_t wsize = 0;
  Status s = w->Write(ctx_, buf.data(), kWriteSize, offset, &wsize);

  // Short write: OK, only the 4 bytes that landed in chunk 0.
  EXPECT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(wsize, 4u);

  w->Close();
  w->ReleaseRef();
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

// 6. Close() waits for all in-flight writes to complete.
//    This test spawns a write thread and then immediately calls Close()
//    from the main thread to verify Close doesn't race or deadlock.
TEST_F(FileWriterTest, Close_WaitsForInflightWrites) {
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

  // Close must complete without deadlock.
  w->Close();
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

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
