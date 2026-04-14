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

#include <cstring>
#include <memory>
#include <string>

#include "client/vfs/data/slice/common.h"
#include "client/vfs/data/slice/slice_writer.h"
#include "common/status.h"
#include "common/trace/trace_manager.h"
#include "test/unit/client/vfs/test_base.h"
#include "test/unit/client/vfs/test_common.h"

namespace dingofs {
namespace client {
namespace vfs {

using ::testing::_;
using ::testing::AnyNumber;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;

// RAII guard: calls DecRef on scope exit
struct SliceWriterGuard {
  SliceWriter* sw;
  explicit SliceWriterGuard(SliceWriter* s) : sw(s) {}
  ~SliceWriterGuard() {
    if (sw) sw->DecRef();
  }
  SliceWriterGuard(const SliceWriterGuard&) = delete;
  SliceWriterGuard& operator=(const SliceWriterGuard&) = delete;
};

// Block size: 4 MiB, Chunk size: 64 MiB, page size: 4 KiB
static constexpr uint64_t kBlockSize = 4 * 1024 * 1024;
static constexpr uint64_t kChunkSize = 64 * 1024 * 1024;
static constexpr uint64_t kPageSize = 4096;
static constexpr uint64_t kFsId = 1;
static constexpr uint64_t kIno = 100;
static constexpr uint64_t kChunkIndex = 0;

static SliceDataContext MakeContext() {
  return SliceDataContext(kFsId, kIno, kChunkIndex, kChunkSize, kBlockSize,
                          kPageSize);
}

class SliceWriterTest : public test::VFSTestBase {
 protected:
  void SetUp() override {
    // Set up a real TraceManager (no-op when tracing disabled)
    trace_manager_ = std::make_unique<TraceManager>();
    ON_CALL(*mock_hub_, GetTraceManager())
        .WillByDefault(Return(trace_manager_.get()));
    EXPECT_CALL(*mock_hub_, GetTraceManager()).Times(AnyNumber());

    context_ = std::make_unique<SliceDataContext>(MakeContext());
    sw_ = new SliceWriter(*context_, mock_hub_, /*chunk_offset=*/0);
    sw_->IncRef();
  }

  // Helper: allocate and fill a buffer of given size with a repeated byte val.
  std::vector<char> MakeBuf(uint64_t size, char val = 'A') {
    return std::vector<char>(size, val);
  }

  // Helper: allocate a zeroed buffer.
  std::vector<char> MakeZeroBuf(uint64_t size) {
    return std::vector<char>(size, '\0');
  }

  std::unique_ptr<TraceManager> trace_manager_;
  std::unique_ptr<SliceDataContext> context_;
  SliceWriter* sw_{nullptr};

  void TearDown() override {
    if (sw_) {
      sw_->DecRef();
      sw_ = nullptr;
    }
  }
};

// 1. Write 4096 bytes at offset 0; verify Len() == 4096.
TEST_F(SliceWriterTest, Write_Basic_LengthUpdated) {
  auto buf = MakeBuf(4096);
  Status s = sw_->Write(ctx_, buf.data(), 4096, 0);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(sw_->Len(), 4096u);
}

// 2. Write data crossing block boundaries; verify multiple blocks are created.
TEST_F(SliceWriterTest, Write_MultiBlock_BoundaryAlignment) {
  // Write kBlockSize + 1024 bytes starting at offset 0; this crosses one
  // block boundary and should touch two blocks.
  uint64_t write_size = kBlockSize + 1024;
  auto buf = MakeBuf(write_size);
  Status s = sw_->Write(ctx_, buf.data(), write_size, 0);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(sw_->Len(), write_size);
  // End should be chunk_offset + write_size
  EXPECT_EQ(sw_->End(), write_size);
}

// 3. Write data, mock NewSliceId returns id=42, mock PutAsync succeeds.
//    Verify callback called with OK, IsFlushed()==true,
//    GetCommitSlice().id==42.
TEST_F(SliceWriterTest, FlushAsync_BasicSuccess) {
  EXPECT_CALL(*mock_meta_system_, NewSliceId(_, _, _))
      .WillOnce(DoAll(SetArgPointee<2>(42u), Return(Status::OK())));
  EXPECT_CALL(*mock_block_store_, PutAsync(_, _, _))
      .WillRepeatedly(Invoke(
          [](ContextSPtr, PutReq, StatusCallback cb) { cb(Status::OK()); }));

  auto buf = MakeBuf(4096);
  ASSERT_TRUE(sw_->Write(ctx_, buf.data(), 4096, 0).ok());

  test::AsyncWaiter waiter;
  waiter.Expect(1);
  sw_->FlushAsync([&](Status s) {
    EXPECT_TRUE(s.ok());
    waiter.Done();
  });
  waiter.Wait();

  EXPECT_TRUE(sw_->IsFlushed());
  EXPECT_EQ(sw_->GetCommitSlice().id, 42u);
}

// 4. Verify slice has correct offset, length, id after flush.
TEST_F(SliceWriterTest, FlushAsync_CommitSlice_CorrectFields) {
  EXPECT_CALL(*mock_meta_system_, NewSliceId(_, _, _))
      .WillOnce(DoAll(SetArgPointee<2>(99u), Return(Status::OK())));
  EXPECT_CALL(*mock_block_store_, PutAsync(_, _, _))
      .WillRepeatedly(Invoke(
          [](ContextSPtr, PutReq, StatusCallback cb) { cb(Status::OK()); }));

  uint64_t write_size = 8192;
  auto buf = MakeBuf(write_size);
  ASSERT_TRUE(sw_->Write(ctx_, buf.data(), write_size, 0).ok());

  test::AsyncWaiter waiter;
  waiter.Expect(1);
  sw_->FlushAsync([&](Status s) {
    EXPECT_TRUE(s.ok());
    waiter.Done();
  });
  waiter.Wait();

  Slice commit = sw_->GetCommitSlice();
  EXPECT_EQ(commit.id, 99u);
  // chunk_offset_=0, len_=write_size
  EXPECT_EQ(commit.pos, 0);
  EXPECT_EQ(commit.size, write_size);
  EXPECT_EQ(commit.off, 0);
  EXPECT_EQ(commit.len, write_size);
}

// 5. Mock NewSliceId returns error; FlushAsync callback gets error.
TEST_F(SliceWriterTest, FlushAsync_NewSliceId_Fails) {
  EXPECT_CALL(*mock_meta_system_, NewSliceId(_, _, _))
      .WillOnce(Return(Status::IoError("id allocation failed")));

  auto buf = MakeBuf(4096);
  ASSERT_TRUE(sw_->Write(ctx_, buf.data(), 4096, 0).ok());

  test::AsyncWaiter waiter;
  waiter.Expect(1);
  sw_->FlushAsync([&](Status s) {
    EXPECT_FALSE(s.ok());
    waiter.Done();
  });
  waiter.Wait();
}

// 6. Mock PutAsync returns error; FlushAsync callback gets error.
TEST_F(SliceWriterTest, FlushAsync_BlockStore_PutAsync_Fails) {
  EXPECT_CALL(*mock_meta_system_, NewSliceId(_, _, _))
      .WillOnce(DoAll(SetArgPointee<2>(1u), Return(Status::OK())));
  EXPECT_CALL(*mock_block_store_, PutAsync(_, _, _))
      .WillRepeatedly(Invoke([](ContextSPtr, PutReq, StatusCallback cb) {
        cb(Status::IoError("block store write failed"));
      }));

  auto buf = MakeBuf(4096);
  ASSERT_TRUE(sw_->Write(ctx_, buf.data(), 4096, 0).ok());

  test::AsyncWaiter waiter;
  waiter.Expect(1);
  sw_->FlushAsync([&](Status s) {
    EXPECT_FALSE(s.ok());
    waiter.Done();
  });
  waiter.Wait();
}

// 7. Call FlushAsync twice; second call should CHECK-fail due to flushing_
// gate.
//    We only test that the first FlushAsync completes successfully.
TEST_F(SliceWriterTest, FlushAsync_OnlyCalledOnce) {
  EXPECT_CALL(*mock_meta_system_, NewSliceId(_, _, _))
      .WillOnce(DoAll(SetArgPointee<2>(7u), Return(Status::OK())));
  EXPECT_CALL(*mock_block_store_, PutAsync(_, _, _))
      .WillRepeatedly(Invoke(
          [](ContextSPtr, PutReq, StatusCallback cb) { cb(Status::OK()); }));

  auto buf = MakeBuf(4096);
  ASSERT_TRUE(sw_->Write(ctx_, buf.data(), 4096, 0).ok());

  test::AsyncWaiter waiter;
  waiter.Expect(1);
  sw_->FlushAsync([&](Status s) {
    EXPECT_TRUE(s.ok());
    waiter.Done();
  });
  waiter.Wait();

  // After the first flush completes, IsFlushed() must be true.
  EXPECT_TRUE(sw_->IsFlushed());
  // The SliceWriter enforces single-flush via CHECK; we do not call
  // FlushAsync a second time here to avoid a crash in the test process.
}

// 8. Write zero buffer (all zeros) — blocks are still created and PutAsync
//    is invoked (ZeroBlock detection, if any, is internal to BlockData/page).
//    The slice must still flush successfully.
TEST_F(SliceWriterTest, Write_ZeroData_FlushSucceeds) {
  EXPECT_CALL(*mock_meta_system_, NewSliceId(_, _, _))
      .WillOnce(DoAll(SetArgPointee<2>(5u), Return(Status::OK())));
  EXPECT_CALL(*mock_block_store_, PutAsync(_, _, _))
      .WillRepeatedly(Invoke(
          [](ContextSPtr, PutReq, StatusCallback cb) { cb(Status::OK()); }));

  auto buf = MakeZeroBuf(4096);
  ASSERT_TRUE(sw_->Write(ctx_, buf.data(), 4096, 0).ok());

  test::AsyncWaiter waiter;
  waiter.Expect(1);
  sw_->FlushAsync([&](Status s) {
    EXPECT_TRUE(s.ok());
    waiter.Done();
  });
  waiter.Wait();

  EXPECT_TRUE(sw_->IsFlushed());
}

// 9. After multiple writes, verify total length via Len().
TEST_F(SliceWriterTest, GetWrittenLength_CorrectValue) {
  uint64_t first = 1024;
  uint64_t second = 2048;

  auto buf1 = MakeBuf(first, 'X');
  auto buf2 = MakeBuf(second, 'Y');

  ASSERT_TRUE(sw_->Write(ctx_, buf1.data(), first, 0).ok());
  ASSERT_TRUE(sw_->Write(ctx_, buf2.data(), second, first).ok());

  EXPECT_EQ(sw_->Len(), first + second);
  EXPECT_EQ(sw_->End(), first + second);
}

// 10. No writes before flush — DoFlush CHECKs block_datas_.size() > 0, so
//     we create a separate SliceWriter and verify that Len()==0 before flush.
//     (The actual empty-flush path hits CHECK; we only verify pre-flush state.)
TEST_F(SliceWriterTest, NoWrites_LenIsZero) {
  // A fresh SliceWriter with no writes must have Len() == 0.
  EXPECT_EQ(sw_->Len(), 0u);
  EXPECT_FALSE(sw_->IsFlushed());
  EXPECT_EQ(sw_->ChunkOffset(), 0u);
}

// ---------------------------------------------------------------------------
// Phase 3: Slice-Relative block_index tests (target behavior — red-light)
// ---------------------------------------------------------------------------

// Helper fixture with configurable chunk_offset for SliceWriter construction.
class SliceWriterSliceRelativeTest : public test::VFSTestBase {
 protected:
  void SetUp() override {
    trace_manager_ = std::make_unique<TraceManager>();
    ON_CALL(*mock_hub_, GetTraceManager())
        .WillByDefault(Return(trace_manager_.get()));
    EXPECT_CALL(*mock_hub_, GetTraceManager()).Times(AnyNumber());

    context_ = std::make_unique<SliceDataContext>(MakeContext());
  }

  // Create a SliceWriter starting at the given chunk_offset.
  SliceWriter* MakeSliceWriter(int32_t chunk_offset) {
    auto* sw = new SliceWriter(*context_, mock_hub_, chunk_offset);
    sw->IncRef();
    return sw;
  }

  std::vector<char> MakeBuf(uint64_t size, char val = 'A') {
    return std::vector<char>(size, val);
  }

  std::unique_ptr<TraceManager> trace_manager_;
  std::unique_ptr<SliceDataContext> context_;
};

// 1. SliceWriter(chunk_offset=5MB), Write(9MB, off=5MB)
//    Target: block indices should be {0,1,2} (slice-relative).
//    Current (chunk-relative) produces {1,2,3} — test will fail (red).
TEST_F(SliceWriterSliceRelativeTest, Write_SliceRelative_StartsFromZero) {
  const int32_t kStartOffset = 5 * 1024 * 1024;  // 5MB
  const int32_t kWriteSize = 9 * 1024 * 1024;    // 9MB
  auto sw = MakeSliceWriter(kStartOffset);
  SliceWriterGuard guard(sw);

  // Capture all block indices from PutAsync calls.
  std::vector<uint32_t> captured_indices;
  std::mutex mu;
  EXPECT_CALL(*mock_block_store_, PutAsync(_, _, _))
      .WillRepeatedly(Invoke([&](ContextSPtr, PutReq req, StatusCallback cb) {
        {
          std::lock_guard<std::mutex> lk(mu);
          captured_indices.push_back(req.block_ctx.key.index);
        }
        cb(Status::OK());
      }));
  EXPECT_CALL(*mock_meta_system_, NewSliceId(_, _, _))
      .WillOnce(DoAll(SetArgPointee<2>(100u), Return(Status::OK())));

  auto buf = MakeBuf(kWriteSize);
  ASSERT_TRUE(sw->Write(ctx_, buf.data(), kWriteSize, kStartOffset).ok());

  test::AsyncWaiter waiter;
  waiter.Expect(1);
  sw->FlushAsync([&](Status s) {
    EXPECT_TRUE(s.ok());
    waiter.Done();
  });
  waiter.Wait();

  // Sort captured indices for deterministic comparison.
  std::sort(captured_indices.begin(), captured_indices.end());
  ASSERT_EQ(captured_indices.size(), 3u);
  EXPECT_EQ(captured_indices[0], 0u);
  EXPECT_EQ(captured_indices[1], 1u);
  EXPECT_EQ(captured_indices[2], 2u);
}

// 2. Verify 3 blocks have correct sizes: 4MB, 4MB, 1MB (slice-relative).
TEST_F(SliceWriterSliceRelativeTest, Write_MultiBlock_Sequential) {
  const int32_t kStartOffset = 5 * 1024 * 1024;
  const int32_t kWriteSize = 9 * 1024 * 1024;
  auto sw = MakeSliceWriter(kStartOffset);
  SliceWriterGuard guard(sw);

  // Capture block index -> data size.
  std::map<uint32_t, size_t> block_sizes;
  std::mutex mu;
  EXPECT_CALL(*mock_block_store_, PutAsync(_, _, _))
      .WillRepeatedly(Invoke([&](ContextSPtr, PutReq req, StatusCallback cb) {
        {
          std::lock_guard<std::mutex> lk(mu);
          block_sizes[req.block_ctx.key.index] = req.data.Size();
        }
        cb(Status::OK());
      }));
  EXPECT_CALL(*mock_meta_system_, NewSliceId(_, _, _))
      .WillOnce(DoAll(SetArgPointee<2>(101u), Return(Status::OK())));

  auto buf = MakeBuf(kWriteSize);
  ASSERT_TRUE(sw->Write(ctx_, buf.data(), kWriteSize, kStartOffset).ok());

  test::AsyncWaiter waiter;
  waiter.Expect(1);
  sw->FlushAsync([&](Status s) {
    EXPECT_TRUE(s.ok());
    waiter.Done();
  });
  waiter.Wait();

  ASSERT_EQ(block_sizes.size(), 3u);
  EXPECT_EQ(block_sizes[0], kBlockSize);        // 4MB
  EXPECT_EQ(block_sizes[1], kBlockSize);        // 4MB
  EXPECT_EQ(block_sizes[2], 1u * 1024 * 1024);  // 1MB
}

// 3. Four 1MB writes → single block (index=0), total Len()=4MB.
TEST_F(SliceWriterSliceRelativeTest, Write_MultipleSmallWrites_SameBlock) {
  auto sw = MakeSliceWriter(0);
  SliceWriterGuard guard(sw);
  const int32_t kSmallSize = 1 * 1024 * 1024;

  for (int i = 0; i < 4; ++i) {
    auto buf = MakeBuf(kSmallSize, 'A' + i);
    ASSERT_TRUE(sw->Write(ctx_, buf.data(), kSmallSize, i * kSmallSize).ok());
  }
  EXPECT_EQ(sw->Len(), static_cast<int32_t>(4 * kSmallSize));

  // Flush and verify only one block with index=0.
  std::vector<uint32_t> captured_indices;
  std::mutex mu;
  EXPECT_CALL(*mock_block_store_, PutAsync(_, _, _))
      .WillRepeatedly(Invoke([&](ContextSPtr, PutReq req, StatusCallback cb) {
        {
          std::lock_guard<std::mutex> lk(mu);
          captured_indices.push_back(req.block_ctx.key.index);
        }
        cb(Status::OK());
      }));
  EXPECT_CALL(*mock_meta_system_, NewSliceId(_, _, _))
      .WillOnce(DoAll(SetArgPointee<2>(102u), Return(Status::OK())));

  test::AsyncWaiter waiter;
  waiter.Expect(1);
  sw->FlushAsync([&](Status s) {
    EXPECT_TRUE(s.ok());
    waiter.Done();
  });
  waiter.Wait();

  ASSERT_EQ(captured_indices.size(), 1u);
  EXPECT_EQ(captured_indices[0], 0u);
}

// 4. Write(3MB) + Write(2MB) crosses block boundary → two blocks.
TEST_F(SliceWriterSliceRelativeTest, Write_SmallWrites_CrossBoundary) {
  auto sw = MakeSliceWriter(0);
  SliceWriterGuard guard(sw);
  const int32_t k3MB = 3 * 1024 * 1024;
  const int32_t k2MB = 2 * 1024 * 1024;

  auto buf1 = MakeBuf(k3MB, 'X');
  auto buf2 = MakeBuf(k2MB, 'Y');
  ASSERT_TRUE(sw->Write(ctx_, buf1.data(), k3MB, 0).ok());
  ASSERT_TRUE(sw->Write(ctx_, buf2.data(), k2MB, k3MB).ok());
  EXPECT_EQ(sw->Len(), k3MB + k2MB);

  std::vector<uint32_t> captured_indices;
  std::mutex mu;
  EXPECT_CALL(*mock_block_store_, PutAsync(_, _, _))
      .WillRepeatedly(Invoke([&](ContextSPtr, PutReq req, StatusCallback cb) {
        {
          std::lock_guard<std::mutex> lk(mu);
          captured_indices.push_back(req.block_ctx.key.index);
        }
        cb(Status::OK());
      }));
  EXPECT_CALL(*mock_meta_system_, NewSliceId(_, _, _))
      .WillOnce(DoAll(SetArgPointee<2>(103u), Return(Status::OK())));

  test::AsyncWaiter waiter;
  waiter.Expect(1);
  sw->FlushAsync([&](Status s) {
    EXPECT_TRUE(s.ok());
    waiter.Done();
  });
  waiter.Wait();

  std::sort(captured_indices.begin(), captured_indices.end());
  ASSERT_EQ(captured_indices.size(), 2u);
  EXPECT_EQ(captured_indices[0], 0u);
  EXPECT_EQ(captured_indices[1], 1u);
}

// 5. Flush last block — verify BlockKey.size equals actual data size (1MB).
TEST_F(SliceWriterSliceRelativeTest, Flush_LastBlock_ActualSize) {
  const int32_t kStartOffset = 5 * 1024 * 1024;
  const int32_t kWriteSize = 9 * 1024 * 1024;
  auto sw = MakeSliceWriter(kStartOffset);
  SliceWriterGuard guard(sw);

  // Capture BlockKey.size per block_index.
  std::map<uint32_t, uint32_t> key_sizes;
  std::mutex mu;
  EXPECT_CALL(*mock_block_store_, PutAsync(_, _, _))
      .WillRepeatedly(Invoke([&](ContextSPtr, PutReq req, StatusCallback cb) {
        {
          std::lock_guard<std::mutex> lk(mu);
          key_sizes[req.block_ctx.key.index] = req.block_ctx.key.size;
        }
        cb(Status::OK());
      }));
  EXPECT_CALL(*mock_meta_system_, NewSliceId(_, _, _))
      .WillOnce(DoAll(SetArgPointee<2>(104u), Return(Status::OK())));

  auto buf = MakeBuf(kWriteSize);
  ASSERT_TRUE(sw->Write(ctx_, buf.data(), kWriteSize, kStartOffset).ok());

  test::AsyncWaiter waiter;
  waiter.Expect(1);
  sw->FlushAsync([&](Status s) {
    EXPECT_TRUE(s.ok());
    waiter.Done();
  });
  waiter.Wait();

  // Last block (index=2) should have actual size = 1MB, not block_size.
  ASSERT_EQ(key_sizes.size(), 3u);
  EXPECT_EQ(key_sizes[0], kBlockSize);
  EXPECT_EQ(key_sizes[1], kBlockSize);
  EXPECT_EQ(key_sizes[2], 1u * 1024 * 1024);
}

// 6. Write(100 bytes) → one block, BlockKey.size=100.
TEST_F(SliceWriterSliceRelativeTest, Write_SingleBlockLessThanBlockSize) {
  auto sw = MakeSliceWriter(0);
  SliceWriterGuard guard(sw);

  uint32_t captured_key_size = 0;
  EXPECT_CALL(*mock_block_store_, PutAsync(_, _, _))
      .WillRepeatedly(Invoke([&](ContextSPtr, PutReq req, StatusCallback cb) {
        captured_key_size = req.block_ctx.key.size;
        cb(Status::OK());
      }));
  EXPECT_CALL(*mock_meta_system_, NewSliceId(_, _, _))
      .WillOnce(DoAll(SetArgPointee<2>(105u), Return(Status::OK())));

  auto buf = MakeBuf(100, 'Z');
  ASSERT_TRUE(sw->Write(ctx_, buf.data(), 100, 0).ok());
  EXPECT_EQ(sw->Len(), 100);

  test::AsyncWaiter waiter;
  waiter.Expect(1);
  sw->FlushAsync([&](Status s) {
    EXPECT_TRUE(s.ok());
    waiter.Done();
  });
  waiter.Wait();

  EXPECT_EQ(captured_key_size, 100u);
}

// 7. Reverse write should be rejected (target: only forward append allowed).
//    Current code allows reverse via `end_in_chunk == chunk_offset_`.
//    After migration, SliceWriter::Write CHECKs chunk_offset == End() only.
TEST_F(SliceWriterSliceRelativeTest, Write_ReverseWrite_CheckFails) {
  const int32_t k4MB = 4 * 1024 * 1024;
  auto sw = MakeSliceWriter(k4MB);
  SliceWriterGuard guard(sw);

  // First write at offset 4MB (the starting chunk_offset).
  auto buf1 = MakeBuf(k4MB);
  ASSERT_TRUE(sw->Write(ctx_, buf1.data(), k4MB, k4MB).ok());

  // Reverse write: [2MB, 4MB) prepends to the slice — target rejects this.
  auto buf2 = MakeBuf(2 * 1024 * 1024);
  EXPECT_DEATH(sw->Write(ctx_, buf2.data(), 2 * 1024 * 1024, 2 * 1024 * 1024),
               "");
}

// 8. GetCommitSlice returns correct fields for a non-zero chunk_offset.
TEST_F(SliceWriterSliceRelativeTest, GetCommitSlice_CorrectFields) {
  const int32_t kStartOffset = 5 * 1024 * 1024;
  const int32_t kWriteSize = 9 * 1024 * 1024;
  auto sw = MakeSliceWriter(kStartOffset);
  SliceWriterGuard guard(sw);

  EXPECT_CALL(*mock_meta_system_, NewSliceId(_, _, _))
      .WillOnce(DoAll(SetArgPointee<2>(200u), Return(Status::OK())));
  EXPECT_CALL(*mock_block_store_, PutAsync(_, _, _))
      .WillRepeatedly(Invoke(
          [](ContextSPtr, PutReq, StatusCallback cb) { cb(Status::OK()); }));

  auto buf = MakeBuf(kWriteSize);
  ASSERT_TRUE(sw->Write(ctx_, buf.data(), kWriteSize, kStartOffset).ok());

  test::AsyncWaiter waiter;
  waiter.Expect(1);
  sw->FlushAsync([&](Status s) {
    EXPECT_TRUE(s.ok());
    waiter.Done();
  });
  waiter.Wait();

  Slice commit = sw->GetCommitSlice();
  EXPECT_EQ(commit.id, 200u);
  EXPECT_EQ(commit.pos, kStartOffset);  // 5MB
  EXPECT_EQ(commit.size, kWriteSize);   // 9MB
  EXPECT_EQ(commit.off, 0);
  EXPECT_EQ(commit.len, kWriteSize);  // 9MB
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

DECLARE_int32(vfs_flush_timeout_seconds);

namespace dingofs {
namespace client {
namespace vfs {

// ---------------------------------------------------------------------------
// Phase: Streaming upload tests (TDD)
// ---------------------------------------------------------------------------

class SliceWriterStreamingTest : public test::VFSTestBase {
 protected:
  void SetUp() override {
    trace_manager_ = std::make_unique<TraceManager>();
    ON_CALL(*mock_hub_, GetTraceManager())
        .WillByDefault(Return(trace_manager_.get()));
    EXPECT_CALL(*mock_hub_, GetTraceManager()).Times(AnyNumber());

    context_ = std::make_unique<SliceDataContext>(MakeContext());
  }

  SliceWriter* MakeStreamingWriter(int32_t chunk_offset = 0) {
    auto* sw = new SliceWriter(*context_, mock_hub_, chunk_offset);
    sw->IncRef();
    sw->StartPrepareSliceId();
    return sw;
  }

  // Wait for bg_executor_ to drain (PrepareSliceId runs there).
  void WaitBGExecutorIdle() {
    test::AsyncWaiter w;
    w.Expect(1);
    bg_executor_->Execute([&w]() { w.Done(); });
    w.Wait();
  }

  // Wait for cb_executor_ to drain (OnBlockUploaded runs there).
  void WaitCBExecutorIdle() {
    test::AsyncWaiter w;
    w.Expect(1);
    cb_executor_->Execute([&w]() { w.Done(); });
    w.Wait();
  }

  std::vector<char> MakeBuf(uint64_t size, char val = 'A') {
    return std::vector<char>(size, val);
  }

  std::unique_ptr<TraceManager> trace_manager_;
  std::unique_ptr<SliceDataContext> context_;
};

// UT-1: Write 10MB (2.5 blocks). Write path uploads block[0,1] via streaming.
//       FlushAsync uploads the remaining block[2] (partial).
TEST_F(SliceWriterStreamingTest, UT1_FlushUpTo_OnlyFullBlocks) {
  EXPECT_CALL(*mock_meta_system_, NewSliceId(_, _, _))
      .WillOnce(DoAll(SetArgPointee<2>(42u), Return(Status::OK())));

  std::vector<uint32_t> uploaded_indices;
  std::mutex mu;
  EXPECT_CALL(*mock_block_store_, PutAsync(_, _, _))
      .WillRepeatedly(Invoke([&](ContextSPtr, PutReq req, StatusCallback cb) {
        {
          std::lock_guard<std::mutex> lk(mu);
          uploaded_indices.push_back(req.block_ctx.key.index);
        }
        cb(Status::OK());
      }));

  auto sw = MakeStreamingWriter(0);
  SliceWriterGuard guard(sw);
  WaitBGExecutorIdle();  // slice_id_ = 42

  // Write 10MB = 2 full blocks (4MB each) + 2MB partial
  auto buf = MakeBuf(10 * 1024 * 1024);
  ASSERT_TRUE(sw->Write(ctx_, buf.data(), 10 * 1024 * 1024, 0).ok());
  WaitCBExecutorIdle();

  // After Write: streaming uploaded block[0] and block[1]
  {
    std::lock_guard<std::mutex> lk(mu);
    ASSERT_EQ(uploaded_indices.size(), 2u);
    std::sort(uploaded_indices.begin(), uploaded_indices.end());
    EXPECT_EQ(uploaded_indices[0], 0u);
    EXPECT_EQ(uploaded_indices[1], 1u);
  }

  // FlushAsync should upload block[2] (the partial block)
  test::AsyncWaiter waiter;
  waiter.Expect(1);
  sw->FlushAsync([&](Status s) {
    EXPECT_TRUE(s.ok());
    waiter.Done();
  });
  waiter.Wait();

  {
    std::lock_guard<std::mutex> lk(mu);
    ASSERT_EQ(uploaded_indices.size(), 3u);
    std::sort(uploaded_indices.begin(), uploaded_indices.end());
    EXPECT_EQ(uploaded_indices[2], 2u);
  }
  EXPECT_TRUE(sw->IsFlushed());
  EXPECT_EQ(sw->GetCommitSlice().id, 42u);
}

// UT-2: PrepareSliceId blocked via promise. First Write skips streaming.
//       After slice_id arrives, second Write compensates and uploads all.
TEST_F(SliceWriterStreamingTest, UT2_SliceId_DelayedArrival_CatchUp) {
  std::promise<void> proceed;
  auto proceed_future = proceed.get_future();

  EXPECT_CALL(*mock_meta_system_, NewSliceId(_, _, _))
      .WillOnce(Invoke([&](ContextSPtr, Ino, uint64_t* id) {
        proceed_future.wait();  // Block until test releases
        *id = 77;
        return Status::OK();
      }));

  std::vector<uint32_t> uploaded_indices;
  std::mutex mu;
  EXPECT_CALL(*mock_block_store_, PutAsync(_, _, _))
      .WillRepeatedly(Invoke([&](ContextSPtr, PutReq req, StatusCallback cb) {
        {
          std::lock_guard<std::mutex> lk(mu);
          uploaded_indices.push_back(req.block_ctx.key.index);
        }
        cb(Status::OK());
      }));

  auto sw = MakeStreamingWriter(0);
  SliceWriterGuard guard(sw);
  // Do NOT WaitBGExecutorIdle — PrepareSliceId is blocked

  // Write 8MB = 2 full blocks. slice_id_==0, FlushUpTo skips.
  auto buf1 = MakeBuf(8 * 1024 * 1024);
  ASSERT_TRUE(sw->Write(ctx_, buf1.data(), 8 * 1024 * 1024, 0).ok());

  {
    std::lock_guard<std::mutex> lk(mu);
    EXPECT_EQ(uploaded_indices.size(), 0u);  // No streaming happened
  }

  // Release PrepareSliceId
  proceed.set_value();
  WaitBGExecutorIdle();  // slice_id_ = 77

  // Write 4MB more. FlushUpTo(12MB) catches up: block[0](end=4<=12),
  // block[1](end=8<=12), block[2](end=12<=12) — all uploaded.
  auto buf2 = MakeBuf(4 * 1024 * 1024);
  ASSERT_TRUE(
      sw->Write(ctx_, buf2.data(), 4 * 1024 * 1024, 8 * 1024 * 1024).ok());
  WaitCBExecutorIdle();

  {
    std::lock_guard<std::mutex> lk(mu);
    ASSERT_EQ(uploaded_indices.size(), 3u);
    std::sort(uploaded_indices.begin(), uploaded_indices.end());
    EXPECT_EQ(uploaded_indices[0], 0u);
    EXPECT_EQ(uploaded_indices[1], 1u);
    EXPECT_EQ(uploaded_indices[2], 2u);
  }

  // FlushAsync — no remaining blocks (all 12MB = 3 full blocks uploaded)
  test::AsyncWaiter waiter;
  waiter.Expect(1);
  sw->FlushAsync([&](Status s) {
    EXPECT_TRUE(s.ok());
    waiter.Done();
  });
  waiter.Wait();
  EXPECT_TRUE(sw->IsFlushed());
}

// UT-3: Second PutAsync returns error. FlushAsync callback receives error.
TEST_F(SliceWriterStreamingTest, UT3_UploadError_Propagation) {
  EXPECT_CALL(*mock_meta_system_, NewSliceId(_, _, _))
      .WillOnce(DoAll(SetArgPointee<2>(42u), Return(Status::OK())));

  std::atomic<int> put_count{0};
  EXPECT_CALL(*mock_block_store_, PutAsync(_, _, _))
      .WillRepeatedly(Invoke([&](ContextSPtr, PutReq, StatusCallback cb) {
        int n = put_count.fetch_add(1);
        if (n == 1) {
          cb(Status::IoError("disk full"));
        } else {
          cb(Status::OK());
        }
      }));

  auto sw = MakeStreamingWriter(0);
  SliceWriterGuard guard(sw);
  WaitBGExecutorIdle();

  // Write 8MB = 2 blocks. block[0] OK, block[1] fails.
  auto buf = MakeBuf(8 * 1024 * 1024);
  ASSERT_TRUE(sw->Write(ctx_, buf.data(), 8 * 1024 * 1024, 0).ok());
  WaitCBExecutorIdle();

  test::AsyncWaiter waiter;
  waiter.Expect(1);
  sw->FlushAsync([&](Status s) {
    EXPECT_FALSE(s.ok());
    waiter.Done();
  });
  waiter.Wait();
}

// UT-4: PutAsync callback held (not called). WaitInflightWithTimeout times out.
TEST_F(SliceWriterStreamingTest, UT4_WaitInflight_Timeout) {
  FLAGS_vfs_flush_timeout_seconds = 2;  // Short timeout for testing

  EXPECT_CALL(*mock_meta_system_, NewSliceId(_, _, _))
      .WillOnce(DoAll(SetArgPointee<2>(42u), Return(Status::OK())));

  std::vector<StatusCallback> held_cbs;
  std::mutex mu;
  EXPECT_CALL(*mock_block_store_, PutAsync(_, _, _))
      .WillRepeatedly(Invoke([&](ContextSPtr, PutReq, StatusCallback cb) {
        std::lock_guard<std::mutex> lk(mu);
        held_cbs.push_back(std::move(cb));  // Hold, don't call
      }));

  auto sw = MakeStreamingWriter(0);
  SliceWriterGuard guard(sw);
  WaitBGExecutorIdle();

  // Write 4MB = 1 full block. PutAsync held → inflight_=1
  auto buf = MakeBuf(4 * 1024 * 1024);
  ASSERT_TRUE(sw->Write(ctx_, buf.data(), 4 * 1024 * 1024, 0).ok());

  test::AsyncWaiter waiter;
  waiter.Expect(1);
  auto start = std::chrono::steady_clock::now();
  sw->FlushAsync([&](Status s) {
    EXPECT_FALSE(s.ok());
    waiter.Done();
  });
  waiter.Wait(std::chrono::seconds(10));

  auto elapsed = std::chrono::steady_clock::now() - start;
  EXPECT_GE(elapsed, std::chrono::seconds(1));
  EXPECT_LE(elapsed, std::chrono::seconds(5));

  // Cleanup: release held callbacks to avoid dangling references
  {
    std::lock_guard<std::mutex> lk(mu);
    for (auto& cb : held_cbs) {
      cb(Status::Internal("test cleanup"));
    }
  }
  WaitCBExecutorIdle();

  FLAGS_vfs_flush_timeout_seconds = 300;  // Restore default
}

// UT-5: Write 100KB (< block_size). No streaming upload during Write.
//       DoFlush handles everything.
TEST_F(SliceWriterStreamingTest, UT5_SmallFile_NoStreamingUpload) {
  EXPECT_CALL(*mock_meta_system_, NewSliceId(_, _, _))
      .WillOnce(DoAll(SetArgPointee<2>(42u), Return(Status::OK())));

  std::atomic<int> put_during_write{0};
  std::atomic<bool> flush_phase{false};
  EXPECT_CALL(*mock_block_store_, PutAsync(_, _, _))
      .WillRepeatedly(Invoke([&](ContextSPtr, PutReq, StatusCallback cb) {
        if (!flush_phase.load()) put_during_write.fetch_add(1);
        cb(Status::OK());
      }));

  auto sw = MakeStreamingWriter(0);
  SliceWriterGuard guard(sw);
  WaitBGExecutorIdle();

  auto buf = MakeBuf(100 * 1024);  // 100KB
  ASSERT_TRUE(sw->Write(ctx_, buf.data(), 100 * 1024, 0).ok());
  WaitCBExecutorIdle();

  // No PutAsync during Write (len_ < block_size)
  EXPECT_EQ(put_during_write.load(), 0);

  flush_phase.store(true);
  test::AsyncWaiter waiter;
  waiter.Expect(1);
  sw->FlushAsync([&](Status s) {
    EXPECT_TRUE(s.ok());
    waiter.Done();
  });
  waiter.Wait();

  EXPECT_TRUE(sw->IsFlushed());
  EXPECT_EQ(sw->GetCommitSlice().id, 42u);
}

// UT-6: Four consecutive 4MB writes. Each Write triggers exactly one
//       streaming upload (one block at a time).
TEST_F(SliceWriterStreamingTest, UT6_MultipleWrites_StreamingOneByOne) {
  EXPECT_CALL(*mock_meta_system_, NewSliceId(_, _, _))
      .WillOnce(DoAll(SetArgPointee<2>(42u), Return(Status::OK())));

  std::atomic<int> put_count{0};
  std::vector<uint32_t> uploaded_indices;
  std::mutex mu;
  EXPECT_CALL(*mock_block_store_, PutAsync(_, _, _))
      .WillRepeatedly(Invoke([&](ContextSPtr, PutReq req, StatusCallback cb) {
        {
          std::lock_guard<std::mutex> lk(mu);
          uploaded_indices.push_back(req.block_ctx.key.index);
        }
        put_count.fetch_add(1);
        cb(Status::OK());
      }));

  auto sw = MakeStreamingWriter(0);
  SliceWriterGuard guard(sw);
  WaitBGExecutorIdle();

  const int32_t kBSize = 4 * 1024 * 1024;
  for (int i = 0; i < 4; i++) {
    auto buf = MakeBuf(kBSize, 'A' + i);
    ASSERT_TRUE(sw->Write(ctx_, buf.data(), kBSize, i * kBSize).ok());
    WaitCBExecutorIdle();
    // Each Write should trigger exactly one streaming upload
    EXPECT_EQ(put_count.load(), i + 1) << "After Write #" << i;
  }

  {
    std::lock_guard<std::mutex> lk(mu);
    ASSERT_EQ(uploaded_indices.size(), 4u);
    EXPECT_EQ(uploaded_indices[0], 0u);
    EXPECT_EQ(uploaded_indices[1], 1u);
    EXPECT_EQ(uploaded_indices[2], 2u);
    EXPECT_EQ(uploaded_indices[3], 3u);
  }

  // FlushAsync — no remaining blocks (4*4MB = 4 full blocks, no partial)
  test::AsyncWaiter waiter;
  waiter.Expect(1);
  sw->FlushAsync([&](Status s) {
    EXPECT_TRUE(s.ok());
    waiter.Done();
  });
  waiter.Wait();
  EXPECT_TRUE(sw->IsFlushed());
}

// UT-7: PrepareSliceId fails. Write skips streaming. DoFlush retries
//       NewSliceId synchronously and succeeds.
TEST_F(SliceWriterStreamingTest, UT7_SliceId_PreallocFail_SyncFallback) {
  EXPECT_CALL(*mock_meta_system_, NewSliceId(_, _, _))
      .WillOnce(Return(Status::IoError("MDS unavailable")))  // PrepareSliceId
      .WillOnce(DoAll(SetArgPointee<2>(99u), Return(Status::OK())));  // DoFlush

  std::vector<uint32_t> uploaded_indices;
  std::mutex mu;
  EXPECT_CALL(*mock_block_store_, PutAsync(_, _, _))
      .WillRepeatedly(Invoke([&](ContextSPtr, PutReq req, StatusCallback cb) {
        {
          std::lock_guard<std::mutex> lk(mu);
          uploaded_indices.push_back(req.block_ctx.key.index);
        }
        cb(Status::OK());
      }));

  auto sw = MakeStreamingWriter(0);
  SliceWriterGuard guard(sw);
  WaitBGExecutorIdle();  // PrepareSliceId fails, alloc_failed_=true

  // Write 8MB = 2 blocks. FlushUpTo skips (alloc_failed).
  auto buf = MakeBuf(8 * 1024 * 1024);
  ASSERT_TRUE(sw->Write(ctx_, buf.data(), 8 * 1024 * 1024, 0).ok());
  WaitCBExecutorIdle();

  {
    std::lock_guard<std::mutex> lk(mu);
    EXPECT_EQ(uploaded_indices.size(), 0u);  // No streaming upload
  }

  // FlushAsync → DoFlush retries NewSliceId synchronously → id=99
  test::AsyncWaiter waiter;
  waiter.Expect(1);
  sw->FlushAsync([&](Status s) {
    EXPECT_TRUE(s.ok());
    waiter.Done();
  });
  waiter.Wait();

  {
    std::lock_guard<std::mutex> lk(mu);
    ASSERT_EQ(uploaded_indices.size(), 2u);
    std::sort(uploaded_indices.begin(), uploaded_indices.end());
    EXPECT_EQ(uploaded_indices[0], 0u);
    EXPECT_EQ(uploaded_indices[1], 1u);
  }
  EXPECT_TRUE(sw->IsFlushed());
  EXPECT_EQ(sw->GetCommitSlice().id, 99u);
}

// UT-8: Single Write of 10MB triggers FlushUpTo uploading multiple blocks at
// once.
TEST_F(SliceWriterStreamingTest, UT8_SingleWrite_MultiBlockFlushUpTo) {
  EXPECT_CALL(*mock_meta_system_, NewSliceId(_, _, _))
      .WillOnce(DoAll(SetArgPointee<2>(42u), Return(Status::OK())));

  std::vector<uint32_t> uploaded_indices;
  std::mutex mu;
  EXPECT_CALL(*mock_block_store_, PutAsync(_, _, _))
      .WillRepeatedly(Invoke([&](ContextSPtr, PutReq req, StatusCallback cb) {
        {
          std::lock_guard<std::mutex> lk(mu);
          uploaded_indices.push_back(req.block_ctx.key.index);
        }
        cb(Status::OK());
      }));

  auto sw = MakeStreamingWriter(0);
  SliceWriterGuard guard(sw);
  WaitBGExecutorIdle();

  // 10MB = 2 full blocks (4MB each) + 2MB partial
  auto buf = MakeBuf(10 * 1024 * 1024);
  ASSERT_TRUE(sw->Write(ctx_, buf.data(), 10 * 1024 * 1024, 0).ok());
  WaitCBExecutorIdle();

  {
    std::lock_guard<std::mutex> lk(mu);
    ASSERT_EQ(uploaded_indices.size(), 2u);
    std::sort(uploaded_indices.begin(), uploaded_indices.end());
    EXPECT_EQ(uploaded_indices[0], 0u);
    EXPECT_EQ(uploaded_indices[1], 1u);
  }

  test::AsyncWaiter waiter;
  waiter.Expect(1);
  sw->FlushAsync([&](Status s) {
    EXPECT_TRUE(s.ok());
    waiter.Done();
  });
  waiter.Wait();

  {
    std::lock_guard<std::mutex> lk(mu);
    ASSERT_EQ(uploaded_indices.size(), 3u);  // block[2] residual
  }
}

// UT-9: Multiple OnBlockUploaded callbacks arrive concurrently.
//       Verifies inflight counting and error collection thread safety.
TEST_F(SliceWriterStreamingTest, UT9_ConcurrentCallbacks_ThreadSafe) {
  EXPECT_CALL(*mock_meta_system_, NewSliceId(_, _, _))
      .WillOnce(DoAll(SetArgPointee<2>(42u), Return(Status::OK())));

  std::mutex cbs_mu;
  std::vector<StatusCallback> held_cbs;
  EXPECT_CALL(*mock_block_store_, PutAsync(_, _, _))
      .WillRepeatedly(Invoke([&](ContextSPtr, PutReq, StatusCallback cb) {
        std::lock_guard<std::mutex> lk(cbs_mu);
        held_cbs.push_back(std::move(cb));
      }));

  auto sw = MakeStreamingWriter(0);
  SliceWriterGuard guard(sw);
  WaitBGExecutorIdle();

  // 20MB = 4 full blocks + 4MB partial. FlushUpTo uploads block[0..3].
  auto buf = MakeBuf(20 * 1024 * 1024);
  ASSERT_TRUE(sw->Write(ctx_, buf.data(), 20 * 1024 * 1024, 0).ok());

  // Wait for 4 PutAsync callbacks to be held
  while (true) {
    std::lock_guard<std::mutex> lk(cbs_mu);
    if (held_cbs.size() >= 4) break;
  }

  // Fire all 4 from concurrent threads
  std::vector<StatusCallback> to_fire;
  {
    std::lock_guard<std::mutex> lk(cbs_mu);
    to_fire.swap(held_cbs);
  }
  std::vector<std::thread> threads;
  for (auto& cb : to_fire) {
    threads.emplace_back([cb = std::move(cb)]() mutable { cb(Status::OK()); });
  }
  for (auto& t : threads) t.join();
  WaitCBExecutorIdle();

  test::AsyncWaiter waiter;
  waiter.Expect(1);
  sw->FlushAsync([&](Status s) {
    EXPECT_TRUE(s.ok());
    waiter.Done();
  });
  waiter.Wait();
  EXPECT_TRUE(sw->IsFlushed());
}

// UT-10: Write streams some blocks, DoFlush uploads the rest.
//        Verifies mixed streaming + DoFlush residual path.
TEST_F(SliceWriterStreamingTest, UT10_MixedStreamAndFlushResidual) {
  EXPECT_CALL(*mock_meta_system_, NewSliceId(_, _, _))
      .WillOnce(DoAll(SetArgPointee<2>(42u), Return(Status::OK())));

  std::atomic<int> put_during_write{0};
  std::atomic<int> put_during_flush{0};
  std::atomic<bool> flush_phase{false};
  std::vector<uint32_t> all_indices;
  std::mutex mu;

  EXPECT_CALL(*mock_block_store_, PutAsync(_, _, _))
      .WillRepeatedly(Invoke([&](ContextSPtr, PutReq req, StatusCallback cb) {
        if (flush_phase.load()) {
          put_during_flush.fetch_add(1);
        } else {
          put_during_write.fetch_add(1);
        }
        {
          std::lock_guard<std::mutex> lk(mu);
          all_indices.push_back(req.block_ctx.key.index);
        }
        cb(Status::OK());
      }));

  auto sw = MakeStreamingWriter(0);
  SliceWriterGuard guard(sw);
  WaitBGExecutorIdle();

  // 6MB = 1 full block (4MB) + 2MB partial
  auto buf = MakeBuf(6 * 1024 * 1024);
  ASSERT_TRUE(sw->Write(ctx_, buf.data(), 6 * 1024 * 1024, 0).ok());
  WaitCBExecutorIdle();

  // block[0] streamed during Write
  EXPECT_EQ(put_during_write.load(), 1);

  // block[1] (partial) uploaded by DoFlush
  flush_phase.store(true);
  test::AsyncWaiter waiter;
  waiter.Expect(1);
  sw->FlushAsync([&](Status s) {
    EXPECT_TRUE(s.ok());
    waiter.Done();
  });
  waiter.Wait();

  EXPECT_EQ(put_during_flush.load(), 1);
  {
    std::lock_guard<std::mutex> lk(mu);
    ASSERT_EQ(all_indices.size(), 2u);
    std::sort(all_indices.begin(), all_indices.end());
    EXPECT_EQ(all_indices[0], 0u);
    EXPECT_EQ(all_indices[1], 1u);
  }
  EXPECT_TRUE(sw->IsFlushed());
}

// UT-11: Owner releases (DecRef) after flush timeout, then late callback
//        arrives and DecRef triggers delete. Verifies no use-after-free.
TEST_F(SliceWriterStreamingTest, UT11_TimeoutThenLateCallback_NoUAF) {
  FLAGS_vfs_flush_timeout_seconds = 1;

  EXPECT_CALL(*mock_meta_system_, NewSliceId(_, _, _))
      .WillOnce(DoAll(SetArgPointee<2>(42u), Return(Status::OK())));

  // Hold PutAsync callbacks — don't call them
  std::vector<StatusCallback> held_cbs;
  std::mutex mu;
  EXPECT_CALL(*mock_block_store_, PutAsync(_, _, _))
      .WillRepeatedly(Invoke([&](ContextSPtr, PutReq, StatusCallback cb) {
        std::lock_guard<std::mutex> lk(mu);
        held_cbs.push_back(std::move(cb));
      }));

  auto* sw = MakeStreamingWriter(0);
  // No SliceWriterGuard — manually control DecRef to test exact sequence
  WaitBGExecutorIdle();

  // Write 4MB = 1 full block → IncRef in UploadBlockAsync → refs=2
  auto buf = MakeBuf(4 * 1024 * 1024);
  ASSERT_TRUE(sw->Write(ctx_, buf.data(), 4 * 1024 * 1024, 0).ok());

  // FlushAsync → DoFlush → WaitInflightWithTimeout(1s) → timeout
  test::AsyncWaiter waiter;
  waiter.Expect(1);
  sw->FlushAsync([&](Status s) {
    EXPECT_FALSE(s.ok());
    waiter.Done();
  });
  waiter.Wait(std::chrono::seconds(10));

  // Owner releases its ref: refs 2→1. SliceWriter still alive (callback holds
  // ref).
  sw->DecRef();
  sw = nullptr;  // don't touch sw anymore

  // Fire the late callback → OnBlockUploaded + DecRef → refs 1→0 → delete this.
  // Must not crash (no use-after-free).
  {
    std::lock_guard<std::mutex> lk(mu);
    for (auto& cb : held_cbs) {
      cb(Status::Internal("late callback"));
    }
  }
  WaitCBExecutorIdle();

  // If we reach here without crash/ASAN, ref counting is correct.
  FLAGS_vfs_flush_timeout_seconds = 300;
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
