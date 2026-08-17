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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <future>
#include <mutex>
#include <utility>
#include <vector>

#include "client/vfs/data/write_pressure_controller.h"
#include "client/vfs/data/writer/file_writer.h"
#include "client/vfs/data/writer_table.h"
#include "test/unit/client/vfs/test_base.h"
#include "utils/executor/thread/executor_impl.h"

namespace dingofs {
namespace client {
namespace vfs {

using dingofs::client::vfs::test::VFSTestBase;
using ::testing::AnyNumber;
using ::testing::Return;

class WriterTableTest : public VFSTestBase {
 protected:
  void SetUp() override {
    VFSTestBase::SetUp();
    table_ = std::make_unique<WriterTable>(mock_hub_);
    ASSERT_TRUE(table_->Start().ok());
  }

  void TearDown() override {
    if (table_) {
      table_->Stop();
      table_.reset();
    }
    VFSTestBase::TearDown();
  }

  std::unique_ptr<WriterTable> table_;
};

// AcquireWriter on the same ino must return the same FileWriter (sharing).
TEST_F(WriterTableTest, AcquireDedupSameIno) {
  const uint64_t ino = 100;
  auto* w1 = table_->AcquireWriter(ino);
  ASSERT_NE(w1, nullptr);
  EXPECT_EQ(table_->Size(), 1u);

  auto* w2 = table_->AcquireWriter(ino);
  ASSERT_NE(w2, nullptr);
  EXPECT_EQ(w1, w2) << "shared writer must be the same instance";
  EXPECT_EQ(table_->Size(), 1u) << "no new entry on dedup";

  table_->ReleaseWriter(w1);
  EXPECT_EQ(table_->Size(), 1u) << "still has another holder";

  table_->ReleaseWriter(w2);
  EXPECT_EQ(table_->Size(), 0u) << "evicted after last holder release";
}

// Different inos get different FileWriters.
TEST_F(WriterTableTest, AcquireDistinctIno) {
  auto* w_a = table_->AcquireWriter(100);
  auto* w_b = table_->AcquireWriter(200);
  ASSERT_NE(w_a, nullptr);
  ASSERT_NE(w_b, nullptr);
  EXPECT_NE(w_a, w_b);
  EXPECT_EQ(table_->Size(), 2u);

  table_->ReleaseWriter(w_a);
  table_->ReleaseWriter(w_b);
  EXPECT_EQ(table_->Size(), 0u);
}

// PeekWriter returns nullptr for missing inos and a held ref for present ones.
TEST_F(WriterTableTest, PeekWriter) {
  EXPECT_EQ(table_->PeekWriter(42), nullptr);

  auto* w = table_->AcquireWriter(42);
  ASSERT_NE(w, nullptr);

  auto* peeked = table_->PeekWriter(42);
  ASSERT_NE(peeked, nullptr);
  EXPECT_EQ(peeked, w);

  table_->ReleaseWriter(peeked);
  table_->ReleaseWriter(w);
  EXPECT_EQ(table_->Size(), 0u);
}

// After Stop(), AcquireWriter must return nullptr.
TEST_F(WriterTableTest, StopRefusesAcquire) {
  table_->Stop();
  EXPECT_EQ(table_->AcquireWriter(100), nullptr);
  EXPECT_EQ(table_->PeekWriter(100), nullptr);
}

// FlushAll on an empty table is a safe no-op that returns OK.
TEST_F(WriterTableTest, FlushAll_Empty_OK) {
  EXPECT_TRUE(table_->FlushAll().ok());
  EXPECT_EQ(table_->Size(), 0u);
}

// FlushAll over multiple live writers must succeed without evicting any —
// it is read-only with respect to externally-held entries (transient holder
// pins are released after each writer's Flush completes).
TEST_F(WriterTableTest, FlushAll_WithWriters_PreservesEntries) {
  auto* w1 = table_->AcquireWriter(100);
  auto* w2 = table_->AcquireWriter(200);
  ASSERT_NE(w1, nullptr);
  ASSERT_NE(w2, nullptr);
  EXPECT_EQ(table_->Size(), 2u);

  EXPECT_TRUE(table_->FlushAll().ok());
  EXPECT_EQ(table_->Size(), 2u) << "FlushAll must not evict any entry";

  table_->ReleaseWriter(w1);
  table_->ReleaseWriter(w2);
}

// Shutdown writeback must attempt every writer even after one fails, while
// returning the first observed error to the lifecycle owner.
TEST_F(WriterTableTest, FlushAll_ErrorDoesNotSkipRemainingWriters) {
  auto* w1 = table_->AcquireWriter(201);
  auto* w2 = table_->AcquireWriter(202);
  ASSERT_NE(w1, nullptr);
  ASSERT_NE(w2, nullptr);

  const char buf[] = "dirty";
  uint64_t wsize = 0;
  ASSERT_TRUE(w1->Write(ctx_, buf, sizeof(buf), 0, &wsize).ok());
  ASSERT_TRUE(w2->Write(ctx_, buf, sizeof(buf), 0, &wsize).ok());

  int write_slice_calls = 0;
  ON_CALL(*mock_meta_system_, WriteSlice)
      .WillByDefault([&](auto, auto, auto, auto, auto) {
        ++write_slice_calls;
        return Status::Internal("flush all error");
      });

  Status s = table_->FlushAll();
  EXPECT_FALSE(s.ok());
  EXPECT_EQ(write_slice_calls, 2)
      << "FlushAll must not stop after the first writer failure";

  table_->ReleaseWriter(w1);
  table_->ReleaseWriter(w2);
}

TEST_F(WriterTableTest, PressureFlushReturnsPagesAndUnblocksFifoWriter) {
  constexpr int64_t kPage = 4096;
  WriteMemPool tiny_pool(kPage, kPage);
  ON_CALL(*mock_hub_, GetWriteMemPool()).WillByDefault(Return(&tiny_pool));

  ExecutorImpl pressure_executor("test_write_pressure", 1);
  ASSERT_TRUE(pressure_executor.Start());
  WritePressureController controller(table_.get(), &pressure_executor);
  tiny_pool.SetPressureObserver(&controller);

  FileWriter* first = table_->AcquireWriter(210);
  FileWriter* second = table_->AcquireWriter(211);
  ASSERT_NE(first, nullptr);
  ASSERT_NE(second, nullptr);

  std::vector<char> first_buf(kPage, 'a');
  uint64_t first_written = 0;
  ASSERT_TRUE(
      first->Write(ctx_, first_buf.data(), first_buf.size(), 0, &first_written)
          .ok());
  ASSERT_EQ(first_written, first_buf.size());
  ASSERT_EQ(tiny_pool.GetUsedBytes(), kPage);

  std::vector<char> second_buf(kPage, 'b');
  auto blocked_write = std::async(std::launch::async, [&] {
    uint64_t written = 0;
    Status status =
        second->Write(ctx_, second_buf.data(), second_buf.size(), 0, &written);
    return std::make_pair(status, written);
  });

  ASSERT_EQ(blocked_write.wait_for(std::chrono::seconds(5)),
            std::future_status::ready);
  auto [status, written] = blocked_write.get();
  EXPECT_TRUE(status.ok()) << status.ToString();
  EXPECT_EQ(written, second_buf.size());

  ASSERT_TRUE(table_->FlushAll().ok());
  tiny_pool.Close();
  tiny_pool.SetPressureObserver(nullptr);
  controller.StopAndDrain();
  ASSERT_TRUE(pressure_executor.Stop());

  table_->ReleaseWriter(first);
  table_->ReleaseWriter(second);
  EXPECT_EQ(tiny_pool.GetUsedBytes(), 0);
}

TEST_F(WriterTableTest, PressureFlushSeesPartialChunkBeforeNextAdmission) {
  constexpr int64_t kPage = 4096;
  constexpr uint64_t kChunk = 2 * kPage;
  ON_CALL(*mock_hub_, GetFsInfo())
      .WillByDefault(Return(test::MakeTestFsInfo(kChunk, kChunk)));

  WriteMemPool tiny_pool(kChunk, kPage);
  ON_CALL(*mock_hub_, GetWriteMemPool()).WillByDefault(Return(&tiny_pool));

  ExecutorImpl pressure_executor("test_write_pressure_cross_chunk", 1);
  ASSERT_TRUE(pressure_executor.Start());
  WritePressureController controller(table_.get(), &pressure_executor);
  tiny_pool.SetPressureObserver(&controller);

  FileWriter* writer = table_->AcquireWriter(212);
  ASSERT_NE(writer, nullptr);

  // The first page ends chunk 0 but leaves a partial slice, so ChunkWriter
  // cannot trigger its full-chunk flush. Chunk 1 then needs both pool pages
  // while only one is free and must rely on pressure flush for progress.
  std::vector<char> buf(3 * kPage, 'x');
  auto write = std::async(std::launch::async, [&] {
    uint64_t written = 0;
    Status status =
        writer->Write(ctx_, buf.data(), buf.size(), kPage, &written);
    return std::make_pair(status, written);
  });

  const auto completion = write.wait_for(std::chrono::seconds(1));
  if (completion != std::future_status::ready) {
    tiny_pool.Close();
  }
  EXPECT_EQ(completion, std::future_status::ready)
      << "a completed partial chunk must become visible to pressure flush "
         "before the next chunk waits for admission";

  auto [status, written] = write.get();
  EXPECT_TRUE(status.ok()) << status.ToString();
  EXPECT_EQ(written, buf.size());

  EXPECT_TRUE(writer->Flush().ok());
  EXPECT_EQ(tiny_pool.GetUsedBytes(), 0);

  tiny_pool.Close();
  tiny_pool.SetPressureObserver(nullptr);
  controller.StopAndDrain();
  EXPECT_TRUE(pressure_executor.Stop());
  table_->ReleaseWriter(writer);
}

TEST_F(WriterTableTest, FlushAllPinsWriterAgainstLastConcurrentRelease) {
  auto* w = table_->AcquireWriter(203);
  ASSERT_NE(w, nullptr);

  const char buf[] = "dirty";
  uint64_t wsize = 0;
  ASSERT_TRUE(w->Write(ctx_, buf, sizeof(buf), 0, &wsize).ok());

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

  auto flush_all =
      std::async(std::launch::async, [&] { return table_->FlushAll(); });
  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(5),
                            [&] { return write_slice_entered; }));
  }

  // Drop the only external holder while FlushAll is blocked. Its transient
  // holder must keep the entry open until the flush finishes.
  table_->ReleaseWriter(w);
  EXPECT_EQ(table_->Size(), 1u);

  {
    std::lock_guard<std::mutex> lock(mutex);
    allow_write_slice = true;
  }
  cv.notify_all();

  ASSERT_TRUE(flush_all.get().ok());
  EXPECT_EQ(table_->Size(), 0u);
}

// Peek after Acquire must take an additional holder so the first Release
// does not evict.
TEST_F(WriterTableTest, PeekTakesAdditionalHolder) {
  auto* w = table_->AcquireWriter(300);
  ASSERT_NE(w, nullptr);
  EXPECT_EQ(table_->Size(), 1u);

  auto* p = table_->PeekWriter(300);
  ASSERT_EQ(p, w) << "Peek hit must return the same instance";

  // First release: entry must survive (Acquire holder still outstanding).
  table_->ReleaseWriter(p);
  EXPECT_EQ(table_->Size(), 1u);

  // Second release: now last holder is gone → evict.
  table_->ReleaseWriter(w);
  EXPECT_EQ(table_->Size(), 0u);
}

// ReleaseWriter(nullptr) must be a safe no-op.
TEST_F(WriterTableTest, ReleaseNullptr_NoOp) {
  table_->ReleaseWriter(nullptr);
  EXPECT_EQ(table_->Size(), 0u);
}

// Stop must be idempotent — calling it more than once must not crash and
// must not leave the table in an inconsistent state.
TEST_F(WriterTableTest, Stop_Idempotent) {
  table_->Stop();
  table_->Stop();  // second call must be safe
  EXPECT_EQ(table_->AcquireWriter(400), nullptr);
}

// Stop only refuses NEW acquires; outstanding writers must still be
// returnable through ReleaseWriter (eviction path stays valid).
TEST_F(WriterTableTest, ReleaseAfterStop_StillEvicts) {
  auto* w = table_->AcquireWriter(500);
  ASSERT_NE(w, nullptr);
  EXPECT_EQ(table_->Size(), 1u);

  table_->Stop();
  EXPECT_EQ(table_->Size(), 1u) << "Stop must not evict already-acquired";

  table_->ReleaseWriter(w);
  EXPECT_EQ(table_->Size(), 0u)
      << "ReleaseWriter after Stop must still return the writer";
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
