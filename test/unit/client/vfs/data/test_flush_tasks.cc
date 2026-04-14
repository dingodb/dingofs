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
#include <map>
#include <memory>
#include <vector>

#include "client/vfs/data/slice/common.h"
#include "client/vfs/data/slice/slice_writer.h"
#include "client/vfs/data/writer/chunk_writer.h"
#include "client/vfs/data/writer/task/chunk_flush_task.h"
#include "client/vfs/data/writer/task/file_flush_task.h"
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

// ============================================================================
// Shared constants and helpers
// ============================================================================

static constexpr uint64_t kBlockSize = 4 * 1024 * 1024;
static constexpr uint64_t kChunkSize = 64 * 1024 * 1024;
static constexpr uint64_t kPageSize = 4096;
static constexpr uint64_t kFsId = 1;
static constexpr uint64_t kIno = 200;
static constexpr uint64_t kChunkIndex = 0;

static SliceDataContext MakeSliceContext(uint64_t ino = kIno,
                                         uint64_t chunk_index = kChunkIndex) {
  return SliceDataContext(kFsId, ino, chunk_index, kChunkSize, kBlockSize,
                          kPageSize);
}

// ============================================================================
// FlushTasksTest: base fixture shared by all flush task tests
// ============================================================================

class FlushTasksTest : public test::VFSTestBase {
 protected:
  void SetUp() override {
    trace_manager_ = std::make_unique<TraceManager>();
    ON_CALL(*mock_hub_, GetTraceManager())
        .WillByDefault(Return(trace_manager_.get()));
    EXPECT_CALL(*mock_hub_, GetTraceManager()).Times(AnyNumber());
  }

  // Create a SliceWriter that has written data and is ready to flush.
  SliceWriter* MakeFlushedSliceWriter(uint64_t slice_id) {
    SliceDataContext ctx = MakeSliceContext();
    auto* sw = new SliceWriter(ctx, mock_hub_, 0);
    sw->IncRef();
    std::vector<char> buf(4096, 'X');
    CHECK(sw->Write(ctx_, buf.data(), 4096, 0).ok());

    // Set expectations for the flush.
    EXPECT_CALL(*mock_meta_system_, NewSliceId(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(slice_id), Return(Status::OK())));
    EXPECT_CALL(*mock_block_store_, PutAsync(_, _, _))
        .WillRepeatedly(Invoke(
            [](ContextSPtr, PutReq, StatusCallback cb) { cb(Status::OK()); }));

    test::AsyncWaiter waiter;
    waiter.Expect(1);
    sw->FlushAsync([&waiter](Status) { waiter.Done(); });
    waiter.Wait();

    return sw;
  }

  std::unique_ptr<TraceManager> trace_manager_;
};

// ============================================================================
// SliceFlushTask tests
// ============================================================================

// Helper: build a minimal BlockData map with one block containing real data.
// We create a SliceWriter with data, then steal its block_datas_ indirectly
// by going through a SliceFlushTask constructed from scratch.
//
// Because BlockData requires the full VFSHub stack (WriteBufferManager etc.),
// we build BlockData objects by writing through a SliceWriter and capturing
// what SliceFlushTask receives via the mocked PutAsync.

// ============================================================================
// ChunkFlushTask tests
// ============================================================================
//
// ChunkFlushTask calls SliceWriter::FlushAsync for each slice it holds.
// We construct SliceWriters with real data and let them flush through the
// mocked hub (same pattern as SliceWriterTest).

// 5. ChunkFlushTask with N slices, all succeed: callback called once with OK.
TEST_F(FlushTasksTest, ChunkFlushTask_AllSlices_Success) {
  static constexpr int kSliceCount = 3;
  static const uint64_t kChunkFlushId = 1001;

  EXPECT_CALL(*mock_meta_system_, NewSliceId(_, _, _))
      .WillRepeatedly(DoAll(SetArgPointee<2>(50u), Return(Status::OK())));
  EXPECT_CALL(*mock_block_store_, PutAsync(_, _, _))
      .WillRepeatedly(Invoke(
          [](ContextSPtr, PutReq, StatusCallback cb) { cb(Status::OK()); }));

  // Build kSliceCount SliceWriters, each starting at consecutive block offsets.
  // We reuse seq numbers; SliceDataContext auto-increments seq.
  std::map<uint64_t, SliceWriterPtr> slices;
  for (int i = 0; i < kSliceCount; ++i) {
    SliceDataContext ctx = MakeSliceContext();
    uint64_t seq = ctx.seq;
    // Each slice starts at its own chunk offset (offset by kPageSize each).
    uint64_t chunk_off = static_cast<uint64_t>(i) * kPageSize;
    auto* sw = new SliceWriter(ctx, mock_hub_, chunk_off);
    sw->IncRef();
    std::vector<char> buf(kPageSize, static_cast<char>('A' + i));
    ASSERT_TRUE(sw->Write(ctx_, buf.data(), kPageSize, chunk_off).ok());
    slices.emplace(seq, sw);
  }

  ChunkFlushTask task(kIno, kChunkIndex, kChunkFlushId, std::move(slices));

  std::atomic<int> callback_count{0};
  test::AsyncWaiter waiter;
  waiter.Expect(1);
  task.RunAsync([&](Status s) {
    ++callback_count;
    EXPECT_TRUE(s.ok());
    waiter.Done();
  });
  waiter.Wait();

  EXPECT_EQ(callback_count.load(), 1);
}

// 6. ChunkFlushTask: one slice fails (NewSliceId error), error propagated.
TEST_F(FlushTasksTest, ChunkFlushTask_SliceFail_Propagated) {
  static const uint64_t kChunkFlushId = 1002;

  // First slice: NewSliceId fails; BlockStore is not reached.
  EXPECT_CALL(*mock_meta_system_, NewSliceId(_, _, _))
      .WillOnce(Return(Status::IoError("mds unavailable")));

  SliceDataContext ctx = MakeSliceContext();
  uint64_t seq = ctx.seq;
  auto* sw = new SliceWriter(ctx, mock_hub_, 0);
  sw->IncRef();
  std::vector<char> buf(kPageSize, 'F');
  ASSERT_TRUE(sw->Write(ctx_, buf.data(), kPageSize, 0).ok());

  std::map<uint64_t, SliceWriterPtr> slices;
  slices.emplace(seq, sw);

  ChunkFlushTask task(kIno, kChunkIndex, kChunkFlushId, std::move(slices));

  test::AsyncWaiter waiter;
  waiter.Expect(1);
  task.RunAsync([&](Status s) {
    EXPECT_FALSE(s.ok());
    waiter.Done();
  });
  waiter.Wait();
}

// 7. ChunkFlushTask concurrent: N slices flushed in parallel,
//    callback called exactly once.
//    Note: streaming upload uses CBExecutor for callbacks, so we use
//    synchronous PutAsync mock (not held callbacks) to avoid deadlock.
TEST_F(FlushTasksTest, ChunkFlushTask_Concurrent_ExactlyOnce) {
  static constexpr int kSliceCount = 8;
  static const uint64_t kChunkFlushId = 1003;

  EXPECT_CALL(*mock_meta_system_, NewSliceId(_, _, _))
      .WillRepeatedly(DoAll(SetArgPointee<2>(60u), Return(Status::OK())));

  std::atomic<int> put_count{0};
  EXPECT_CALL(*mock_block_store_, PutAsync(_, _, _))
      .WillRepeatedly(
          Invoke([&put_count](ContextSPtr, PutReq, StatusCallback cb) {
            ++put_count;
            cb(Status::OK());
          }));

  std::map<uint64_t, SliceWriterPtr> slices;
  for (int i = 0; i < kSliceCount; ++i) {
    SliceDataContext ctx = MakeSliceContext();
    uint64_t seq = ctx.seq;
    uint64_t chunk_off = static_cast<uint64_t>(i) * kPageSize;
    auto* sw = new SliceWriter(ctx, mock_hub_, chunk_off);
    sw->IncRef();
    std::vector<char> buf(kPageSize, static_cast<char>('a' + i));
    ASSERT_TRUE(sw->Write(ctx_, buf.data(), kPageSize, chunk_off).ok());
    slices.emplace(seq, sw);
  }

  ChunkFlushTask task(kIno, kChunkIndex, kChunkFlushId, std::move(slices));

  std::atomic<int> callback_count{0};
  test::AsyncWaiter waiter;
  waiter.Expect(1);
  task.RunAsync([&](Status s) {
    ++callback_count;
    EXPECT_TRUE(s.ok());
    waiter.Done();
  });

  waiter.Wait();
  EXPECT_EQ(callback_count.load(), 1);
  EXPECT_GE(put_count.load(), kSliceCount);
}

// ============================================================================
// FileFlushTask tests
// ============================================================================
//
// FileFlushTask::RunAsync calls ChunkWriter::FlushAsync for each chunk writer.
// We create real ChunkWriters (backed by the mocked hub) and use them.

// 1. FileFlushTask with N chunks, all succeed: callback called once with OK.
TEST_F(FlushTasksTest, FileFlushTask_AllSuccess_CallbackCalledOnce) {
  static constexpr uint64_t kFileFlushId = 2001;
  static constexpr int kChunkCount = 3;

  EXPECT_CALL(*mock_meta_system_, NewSliceId(_, _, _))
      .WillRepeatedly(DoAll(SetArgPointee<2>(80u), Return(Status::OK())));
  EXPECT_CALL(*mock_block_store_, PutAsync(_, _, _))
      .WillRepeatedly(Invoke(
          [](ContextSPtr, PutReq, StatusCallback cb) { cb(Status::OK()); }));
  ON_CALL(*mock_meta_system_, WriteSlice(_, _, _, _, _))
      .WillByDefault(Return(Status::OK()));

  // Build kChunkCount ChunkWriters, write data to each, then register them.
  std::vector<std::unique_ptr<ChunkWriter>> owned_writers;
  std::unordered_map<int64_t, ChunkWriter*> writers_map;

  for (int i = 0; i < kChunkCount; ++i) {
    auto cw = std::make_unique<ChunkWriter>(mock_hub_, /*fh=*/1, kIno,
                                            static_cast<uint64_t>(i));
    std::vector<char> buf(kPageSize, static_cast<char>('A' + i));
    uint64_t chunk_off = 0;
    ASSERT_TRUE(cw->Write(ctx_, buf.data(), kPageSize, chunk_off).ok());
    writers_map.emplace(static_cast<uint64_t>(i), cw.get());
    owned_writers.push_back(std::move(cw));
  }

  FileFlushTask task(kIno, kFileFlushId, std::move(writers_map));

  std::atomic<int> callback_count{0};
  test::AsyncWaiter waiter;
  waiter.Expect(1);
  task.RunAsync([&](Status s) {
    ++callback_count;
    EXPECT_TRUE(s.ok());
    waiter.Done();
  });
  waiter.Wait();

  EXPECT_EQ(callback_count.load(), 1);
}

// 2. FileFlushTask: one chunk fails (NewSliceId error), error propagated.
TEST_F(FlushTasksTest, FileFlushTask_OneChunkFail_ErrorPropagated) {
  static constexpr uint64_t kFileFlushId = 2002;

  // All NewSliceId calls fail (PrepareSliceId + DoFlush fallback).
  EXPECT_CALL(*mock_meta_system_, NewSliceId(_, _, _))
      .WillRepeatedly(Return(Status::IoError("mds timeout")));

  auto cw = std::make_unique<ChunkWriter>(mock_hub_, /*fh=*/1, kIno, 0);
  std::vector<char> buf(kPageSize, 'E');
  ASSERT_TRUE(cw->Write(ctx_, buf.data(), kPageSize, 0).ok());

  std::unordered_map<int64_t, ChunkWriter*> writers_map;
  writers_map.emplace(0u, cw.get());

  FileFlushTask task(kIno, kFileFlushId, std::move(writers_map));

  test::AsyncWaiter waiter;
  waiter.Expect(1);
  task.RunAsync([&](Status s) {
    EXPECT_FALSE(s.ok());
    waiter.Done();
  });
  waiter.Wait();
}

// 3. FileFlushTask with empty chunk map: callback called immediately with OK.
TEST_F(FlushTasksTest, FileFlushTask_EmptyChunks_ImmediateOK) {
  static constexpr uint64_t kFileFlushId = 2003;

  std::unordered_map<int64_t, ChunkWriter*> empty_map;
  FileFlushTask task(kIno, kFileFlushId, std::move(empty_map));

  bool called = false;
  task.RunAsync([&](Status s) {
    EXPECT_TRUE(s.ok());
    called = true;
  });

  EXPECT_TRUE(called);
}

// 4. FileFlushTask concurrent: N chunks all flush simultaneously (via real
//    executor threads), callback called exactly once.
TEST_F(FlushTasksTest, FileFlushTask_Concurrent_ExactlyOnce) {
  static constexpr uint64_t kFileFlushId = 2004;
  static constexpr int kChunkCount = 8;

  EXPECT_CALL(*mock_meta_system_, NewSliceId(_, _, _))
      .WillRepeatedly(DoAll(SetArgPointee<2>(90u), Return(Status::OK())));
  EXPECT_CALL(*mock_block_store_, PutAsync(_, _, _))
      .WillRepeatedly(Invoke(
          [](ContextSPtr, PutReq, StatusCallback cb) { cb(Status::OK()); }));
  ON_CALL(*mock_meta_system_, WriteSlice(_, _, _, _, _))
      .WillByDefault(Return(Status::OK()));

  std::vector<std::unique_ptr<ChunkWriter>> owned_writers;
  std::unordered_map<int64_t, ChunkWriter*> writers_map;

  for (int i = 0; i < kChunkCount; ++i) {
    auto cw = std::make_unique<ChunkWriter>(mock_hub_, /*fh=*/1, kIno,
                                            static_cast<uint64_t>(i));
    std::vector<char> buf(kPageSize, static_cast<char>('a' + i));
    ASSERT_TRUE(cw->Write(ctx_, buf.data(), kPageSize, 0).ok());
    writers_map.emplace(static_cast<uint64_t>(i), cw.get());
    owned_writers.push_back(std::move(cw));
  }

  FileFlushTask task(kIno, kFileFlushId, std::move(writers_map));

  std::atomic<int> callback_count{0};
  test::AsyncWaiter waiter;
  waiter.Expect(1);
  task.RunAsync([&](Status s) {
    ++callback_count;
    EXPECT_TRUE(s.ok());
    waiter.Done();
  });

  waiter.Wait();
  EXPECT_EQ(callback_count.load(), 1);
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
