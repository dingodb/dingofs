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
  // chunk_index=0 so chunk_start_in_file = 0; chunk_offset_=0
  EXPECT_EQ(commit.offset, 0u);
  EXPECT_EQ(commit.length, write_size);
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

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
