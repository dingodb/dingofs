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
    sw_ = std::make_unique<SliceWriter>(*context_, mock_hub_,
                                       /*chunk_offset=*/0);
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
  std::unique_ptr<SliceWriter> sw_;
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
//    Verify callback called with OK, IsFlushed()==true, GetCommitSlice().id==42.
TEST_F(SliceWriterTest, FlushAsync_BasicSuccess) {
  EXPECT_CALL(*mock_meta_system_, NewSliceId(_, _, _))
      .WillOnce(DoAll(SetArgPointee<2>(42u), Return(Status::OK())));
  EXPECT_CALL(*mock_block_store_, PutAsync(_, _, _))
      .WillRepeatedly(Invoke([](ContextSPtr, PutReq, StatusCallback cb) {
        cb(Status::OK());
      }));

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
      .WillRepeatedly(Invoke([](ContextSPtr, PutReq, StatusCallback cb) {
        cb(Status::OK());
      }));

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

// 7. Call FlushAsync twice; second call should CHECK-fail due to flushing_ gate.
//    We only test that the first FlushAsync completes successfully.
TEST_F(SliceWriterTest, FlushAsync_OnlyCalledOnce) {
  EXPECT_CALL(*mock_meta_system_, NewSliceId(_, _, _))
      .WillOnce(DoAll(SetArgPointee<2>(7u), Return(Status::OK())));
  EXPECT_CALL(*mock_block_store_, PutAsync(_, _, _))
      .WillRepeatedly(Invoke([](ContextSPtr, PutReq, StatusCallback cb) {
        cb(Status::OK());
      }));

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
      .WillRepeatedly(Invoke([](ContextSPtr, PutReq, StatusCallback cb) {
        cb(Status::OK());
      }));

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
  std::unique_ptr<SliceWriter> MakeSliceWriter(int32_t chunk_offset) {
    return std::make_unique<SliceWriter>(*context_, mock_hub_, chunk_offset);
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
  const int32_t kWriteSize = 9 * 1024 * 1024;     // 9MB
  auto sw = MakeSliceWriter(kStartOffset);

  // Capture all block indices from PutAsync calls.
  std::vector<uint32_t> captured_indices;
  std::mutex mu;
  EXPECT_CALL(*mock_block_store_, PutAsync(_, _, _))
      .WillRepeatedly(
          Invoke([&](ContextSPtr, PutReq req, StatusCallback cb) {
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

  // Capture block index -> data size.
  std::map<uint32_t, size_t> block_sizes;
  std::mutex mu;
  EXPECT_CALL(*mock_block_store_, PutAsync(_, _, _))
      .WillRepeatedly(
          Invoke([&](ContextSPtr, PutReq req, StatusCallback cb) {
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
  EXPECT_EQ(block_sizes[0], kBlockSize);              // 4MB
  EXPECT_EQ(block_sizes[1], kBlockSize);              // 4MB
  EXPECT_EQ(block_sizes[2], 1u * 1024 * 1024);        // 1MB
}

// 3. Four 1MB writes → single block (index=0), total Len()=4MB.
TEST_F(SliceWriterSliceRelativeTest, Write_MultipleSmallWrites_SameBlock) {
  auto sw = MakeSliceWriter(0);
  const int32_t kSmallSize = 1 * 1024 * 1024;

  for (int i = 0; i < 4; ++i) {
    auto buf = MakeBuf(kSmallSize, 'A' + i);
    ASSERT_TRUE(
        sw->Write(ctx_, buf.data(), kSmallSize, i * kSmallSize).ok());
  }
  EXPECT_EQ(sw->Len(), static_cast<int32_t>(4 * kSmallSize));

  // Flush and verify only one block with index=0.
  std::vector<uint32_t> captured_indices;
  std::mutex mu;
  EXPECT_CALL(*mock_block_store_, PutAsync(_, _, _))
      .WillRepeatedly(
          Invoke([&](ContextSPtr, PutReq req, StatusCallback cb) {
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
      .WillRepeatedly(
          Invoke([&](ContextSPtr, PutReq req, StatusCallback cb) {
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

  // Capture BlockKey.size per block_index.
  std::map<uint32_t, uint32_t> key_sizes;
  std::mutex mu;
  EXPECT_CALL(*mock_block_store_, PutAsync(_, _, _))
      .WillRepeatedly(
          Invoke([&](ContextSPtr, PutReq req, StatusCallback cb) {
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

  uint32_t captured_key_size = 0;
  EXPECT_CALL(*mock_block_store_, PutAsync(_, _, _))
      .WillRepeatedly(
          Invoke([&](ContextSPtr, PutReq req, StatusCallback cb) {
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

  // First write at offset 4MB (the starting chunk_offset).
  auto buf1 = MakeBuf(k4MB);
  ASSERT_TRUE(sw->Write(ctx_, buf1.data(), k4MB, k4MB).ok());

  // Reverse write: [2MB, 4MB) prepends to the slice — target rejects this.
  auto buf2 = MakeBuf(2 * 1024 * 1024);
  EXPECT_DEATH(
      sw->Write(ctx_, buf2.data(), 2 * 1024 * 1024, 2 * 1024 * 1024), "");
}

// 8. GetCommitSlice returns correct fields for a non-zero chunk_offset.
TEST_F(SliceWriterSliceRelativeTest, GetCommitSlice_CorrectFields) {
  const int32_t kStartOffset = 5 * 1024 * 1024;
  const int32_t kWriteSize = 9 * 1024 * 1024;
  auto sw = MakeSliceWriter(kStartOffset);

  EXPECT_CALL(*mock_meta_system_, NewSliceId(_, _, _))
      .WillOnce(DoAll(SetArgPointee<2>(200u), Return(Status::OK())));
  EXPECT_CALL(*mock_block_store_, PutAsync(_, _, _))
      .WillRepeatedly(Invoke([](ContextSPtr, PutReq, StatusCallback cb) {
        cb(Status::OK());
      }));

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
  EXPECT_EQ(commit.pos, kStartOffset);   // 5MB
  EXPECT_EQ(commit.size, kWriteSize);     // 9MB
  EXPECT_EQ(commit.off, 0);
  EXPECT_EQ(commit.len, kWriteSize);      // 9MB
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
