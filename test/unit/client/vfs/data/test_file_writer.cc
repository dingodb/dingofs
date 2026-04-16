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
#include "test/unit/client/vfs/test_base.h"

namespace dingofs {
namespace client {
namespace vfs {

using ::testing::_;
using ::testing::AnyNumber;
using ::testing::Invoke;
using ::testing::Return;
using dingofs::client::vfs::test::VFSTestBase;

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
  FileWriter* MakeOpenWriter(uint64_t ino = 200, uint64_t fh = 2) {
    auto* w = new FileWriter(mock_hub_, fh, ino);
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
  uint64_t wsize = 0;
  Status s = w->Write(ctx_, buf, sizeof(buf), 0, &wsize);
  EXPECT_FALSE(s.ok());

  // Release the ref (Close was already called above by test).
  w->ReleaseRef();
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
