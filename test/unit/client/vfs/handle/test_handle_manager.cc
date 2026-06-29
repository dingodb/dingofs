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

#include <fcntl.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>

#include "client/vfs/data/writer/file_writer.h"
#include "client/vfs/data/writer_table.h"
#include "client/vfs/handle/handle_manager.h"
#include "test/unit/client/vfs/test_base.h"

namespace dingofs {
namespace client {
namespace vfs {

using dingofs::client::vfs::test::VFSTestBase;
using ::testing::AnyNumber;
using ::testing::Return;

class HandleManagerTest : public VFSTestBase {
 protected:
  void SetUp() override {
    VFSTestBase::SetUp();

    // VFSTestBase already creates handle_manager_ but does NOT create a
    // WriterTable. We need one because NewHandle for writable opens calls
    // GetWriterTable()->AcquireWriter.
    writer_table_ = std::make_unique<WriterTable>(mock_hub_);
    ASSERT_TRUE(writer_table_->Start().ok());

    ON_CALL(*mock_hub_, GetWriterTable())
        .WillByDefault(Return(writer_table_.get()));
    EXPECT_CALL(*mock_hub_, GetWriterTable()).Times(AnyNumber());
  }

  void TearDown() override {
    if (writer_table_) {
      writer_table_->Stop();
      writer_table_.reset();
    }
    VFSTestBase::TearDown();
  }

  std::unique_ptr<WriterTable> writer_table_;
};

// O_RDONLY opens must NOT acquire a writer.
TEST_F(HandleManagerTest, NewHandle_RDONLY_NoWriter) {
  auto* h = handle_manager_->NewHandle(/*fh*/ 1, /*ino*/ 100, O_RDONLY);
  ASSERT_NE(h, nullptr);
  EXPECT_NE(h->resources.reader, nullptr);
  EXPECT_EQ(h->resources.writer, nullptr);
  EXPECT_EQ(writer_table_->Size(), 0u);

  handle_manager_->ReleaseHandler(1);
}

// O_WRONLY / O_RDWR opens must acquire a writer.
TEST_F(HandleManagerTest, NewHandle_WriteMode_HasWriter) {
  auto* h = handle_manager_->NewHandle(/*fh*/ 2, /*ino*/ 200, O_WRONLY);
  ASSERT_NE(h, nullptr);
  EXPECT_NE(h->resources.reader, nullptr);
  EXPECT_NE(h->resources.writer, nullptr);
  EXPECT_EQ(writer_table_->Size(), 1u);

  handle_manager_->ReleaseHandler(2);
  EXPECT_EQ(writer_table_->Size(), 0u);
}

// Two writable fhs on the same inode must share a single FileWriter.
TEST_F(HandleManagerTest, MultipleWritableFhsShareOneWriter) {
  auto* h1 = handle_manager_->NewHandle(/*fh*/ 3, /*ino*/ 300, O_WRONLY);
  auto* h2 = handle_manager_->NewHandle(/*fh*/ 4, /*ino*/ 300, O_RDWR);
  ASSERT_NE(h1, nullptr);
  ASSERT_NE(h2, nullptr);

  EXPECT_EQ(h1->resources.writer, h2->resources.writer)
      << "shared per-inode writer expected";
  EXPECT_EQ(writer_table_->Size(), 1u);

  handle_manager_->ReleaseHandler(3);
  EXPECT_EQ(writer_table_->Size(), 1u);  // h2 still holds

  handle_manager_->ReleaseHandler(4);
  EXPECT_EQ(writer_table_->Size(), 0u);
}

// FlushByIno on an inode with no writer must succeed without error.
TEST_F(HandleManagerTest, FlushByIno_NoWriter_ReturnsOK) {
  Status s = handle_manager_->FlushByIno(/*ino*/ 999);
  EXPECT_TRUE(s.ok());
}

// Two fhs on the same inode must each get their own FileReader instance.
// (per-fh reader independence — the second half of the architectural
// invariant; the first half is shared writer.)
TEST_F(HandleManagerTest, TwoFhsOnSameInoHaveDistinctReaders) {
  auto* h1 = handle_manager_->NewHandle(/*fh*/ 10, /*ino*/ 500, O_RDONLY);
  auto* h2 = handle_manager_->NewHandle(/*fh*/ 11, /*ino*/ 500, O_RDONLY);
  ASSERT_NE(h1, nullptr);
  ASSERT_NE(h2, nullptr);

  ASSERT_NE(h1->resources.reader, nullptr);
  ASSERT_NE(h2->resources.reader, nullptr);
  EXPECT_NE(h1->resources.reader, h2->resources.reader)
      << "per-fh reader: each fh must own a distinct FileReader instance";

  handle_manager_->ReleaseHandler(10);
  handle_manager_->ReleaseHandler(11);
}

// Mixed-mode opens: O_RDONLY + O_WRONLY on the same inode produce exactly
// one entry in the WriterTable; releasing the read-only fh must NOT evict
// the shared writer.
TEST_F(HandleManagerTest, MixedRdonlyWritable_OnlyOneWriterEntry) {
  auto* hr = handle_manager_->NewHandle(/*fh*/ 20, /*ino*/ 600, O_RDONLY);
  auto* hw = handle_manager_->NewHandle(/*fh*/ 21, /*ino*/ 600, O_WRONLY);
  ASSERT_NE(hr, nullptr);
  ASSERT_NE(hw, nullptr);

  EXPECT_EQ(hr->resources.writer, nullptr)
      << "O_RDONLY must not allocate a writer";
  EXPECT_NE(hw->resources.writer, nullptr);
  EXPECT_EQ(writer_table_->Size(), 1u);

  // RDONLY release must not touch the writer.
  handle_manager_->ReleaseHandler(20);
  EXPECT_EQ(writer_table_->Size(), 1u);

  // Writable release evicts.
  handle_manager_->ReleaseHandler(21);
  EXPECT_EQ(writer_table_->Size(), 0u);
}

// Writer lifecycle is fully closed: after the last writable fh is released
// the writer is evicted, and re-opening the same inode yields a new entry.
TEST_F(HandleManagerTest, WriterEvictedThenRecreatedOnReopen) {
  auto* h1 = handle_manager_->NewHandle(/*fh*/ 30, /*ino*/ 700, O_WRONLY);
  ASSERT_NE(h1, nullptr);
  ASSERT_NE(h1->resources.writer, nullptr);
  EXPECT_EQ(writer_table_->Size(), 1u);

  handle_manager_->ReleaseHandler(30);
  EXPECT_EQ(writer_table_->Size(), 0u);
  EXPECT_EQ(writer_table_->PeekWriter(700), nullptr) << "entry must be erased";

  // Reopen — a fresh writer is allocated.
  auto* h2 = handle_manager_->NewHandle(/*fh*/ 31, /*ino*/ 700, O_WRONLY);
  ASSERT_NE(h2, nullptr);
  ASSERT_NE(h2->resources.writer, nullptr);
  EXPECT_EQ(writer_table_->Size(), 1u);

  handle_manager_->ReleaseHandler(31);
  EXPECT_EQ(writer_table_->Size(), 0u);
}

// FlushByIno's PeekWriter / ReleaseWriter must be ref-balanced — flushing
// must not evict an in-use writer.
TEST_F(HandleManagerTest, FlushByIno_WithWriter_KeepsTableSize) {
  auto* h = handle_manager_->NewHandle(/*fh*/ 40, /*ino*/ 800, O_WRONLY);
  ASSERT_NE(h, nullptr);
  EXPECT_EQ(writer_table_->Size(), 1u);

  Status s = handle_manager_->FlushByIno(/*ino*/ 800);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(writer_table_->Size(), 1u)
      << "PeekWriter+ReleaseWriter must leave holders unchanged";

  handle_manager_->ReleaseHandler(40);
  EXPECT_EQ(writer_table_->Size(), 0u);
}

// FindHandlerGuard returns a live guard for a known fh, an empty guard for an
// unknown fh, and an empty guard after release.
TEST_F(HandleManagerTest, FindHandlerGuard_KnownAndUnknown) {
  auto* h = handle_manager_->NewHandle(/*fh*/ 70, /*ino*/ 1100, O_RDONLY);
  ASSERT_NE(h, nullptr);

  {
    auto g = handle_manager_->FindHandlerGuard(70);
    EXPECT_TRUE(g);
    EXPECT_EQ(g.get(), h);
  }
  EXPECT_FALSE(handle_manager_->FindHandlerGuard(71));

  handle_manager_->ReleaseHandler(70);
  EXPECT_FALSE(handle_manager_->FindHandlerGuard(70))
      << "after release, FindHandlerGuard must be empty";
}

// ReleaseHandler on an unknown fh must be a safe no-op — it must not crash
// and must not disturb other live handles or the WriterTable.
TEST_F(HandleManagerTest, ReleaseHandler_UnknownFh_NoOp) {
  auto* h = handle_manager_->NewHandle(/*fh*/ 80, /*ino*/ 1200, O_WRONLY);
  ASSERT_NE(h, nullptr);
  EXPECT_EQ(writer_table_->Size(), 1u);

  handle_manager_->ReleaseHandler(/*fh*/ 999);  // never created
  EXPECT_EQ(writer_table_->Size(), 1u);
  EXPECT_EQ(handle_manager_->FindHandlerGuard(80).get(), h);

  handle_manager_->ReleaseHandler(80);
  EXPECT_EQ(writer_table_->Size(), 0u);
}

// --- teardown lifecycle (N1 fix: writer Close happens in Stop, not in the
// destructor after executors are gone) ---

// Stop() must release the per-fh reader/writer resources WHILE the data-path
// executors are still alive, so the writer is returned to the table during
// Stop (not deferred to ~Handle).  This is the inverse of the pre-fix
// behavior that kept the writer until ReleaseHandler.
TEST_F(HandleManagerTest, Stop_ReleasesWriterResources) {
  auto* h = handle_manager_->NewHandle(/*fh*/ 50, /*ino*/ 900, O_WRONLY);
  ASSERT_NE(h, nullptr);
  EXPECT_EQ(writer_table_->Size(), 1u);

  handle_manager_->Stop();
  EXPECT_EQ(writer_table_->Size(), 0u)
      << "Stop must release the writer while executors are still alive";
}

// Stop() detaches resources but keeps the handle identity (fh/ino/flags) so
// hot-upgrade Dump can still persist open handles.
TEST_F(HandleManagerTest, Stop_PreservesHandleIdentity) {
  auto* h = handle_manager_->NewHandle(/*fh*/ 51, /*ino*/ 901, O_WRONLY);
  ASSERT_NE(h, nullptr);
  const uint64_t fh = h->fh;
  const Ino ino = h->ino;
  const int32_t flags = h->flags;

  handle_manager_->Stop();
  EXPECT_EQ(writer_table_->Size(), 0u) << "Stop released the writer";

  // Identity survives (release path can still see it), resources are detached.
  auto g = handle_manager_->FindHandlerForRelease(51);
  ASSERT_TRUE(g);
  EXPECT_EQ(g->fh, fh);
  EXPECT_EQ(g->ino, ino);
  EXPECT_EQ(g->flags, flags);
  EXPECT_EQ(g->resources.reader, nullptr) << "reader detached by Stop";
  EXPECT_EQ(g->resources.writer, nullptr) << "writer detached by Stop";
}

// After Stop(), the data path must not be able to obtain a handle, but the
// release path must, so a late FUSE_RELEASE can still drop the fh identity.
TEST_F(HandleManagerTest, FindHandlerGuard_EmptyAfterStop_ReleasePathWorks) {
  auto* h = handle_manager_->NewHandle(/*fh*/ 52, /*ino*/ 902, O_RDONLY);
  ASSERT_NE(h, nullptr);

  handle_manager_->Stop();

  EXPECT_FALSE(handle_manager_->FindHandlerGuard(52))
      << "data path must be closed after Stop";
  EXPECT_TRUE(handle_manager_->FindHandlerForRelease(52))
      << "release path must stay open after Stop";
}

// NewHandle after Stop() must fail (AddHandle rejected) and must not leak the
// writer it briefly acquired.
TEST_F(HandleManagerTest, NewHandle_AfterStop_RejectedNoLeak) {
  handle_manager_->Stop();

  auto* h = handle_manager_->NewHandle(/*fh*/ 53, /*ino*/ 903, O_WRONLY);
  EXPECT_EQ(h, nullptr) << "NewHandle must fail once HandleManager is stopped";
  EXPECT_EQ(writer_table_->Size(), 0u)
      << "rejected NewHandle must not leak the acquired writer";
}

// A late FUSE_RELEASE after Stop() already detached resources must be
// idempotent: no double ReleaseWriter (which would underflow holders) and no
// crash.
TEST_F(HandleManagerTest, ReleaseHandler_AfterStop_Idempotent) {
  auto* h = handle_manager_->NewHandle(/*fh*/ 54, /*ino*/ 904, O_WRONLY);
  ASSERT_NE(h, nullptr);

  handle_manager_->Stop();
  EXPECT_EQ(writer_table_->Size(), 0u) << "Stop released the writer";

  // Must not double-release the (already detached) writer.
  handle_manager_->ReleaseHandler(54);
  EXPECT_EQ(writer_table_->Size(), 0u);
  EXPECT_FALSE(handle_manager_->FindHandlerForRelease(54))
      << "identity removed after late release";
}

// Core of the N1 fix: Stop() must wait for an outstanding HandleGuard (an
// in-flight request) to be released before tearing down resources.
TEST_F(HandleManagerTest, Stop_WaitsForOutstandingGuard) {
  auto* h = handle_manager_->NewHandle(/*fh*/ 55, /*ino*/ 905, O_WRONLY);
  ASSERT_NE(h, nullptr);
  EXPECT_EQ(writer_table_->Size(), 1u);

  // Simulate an in-flight request holding the handle.
  auto guard = handle_manager_->FindHandlerGuard(55);
  ASSERT_TRUE(guard);

  std::atomic<bool> stop_returned{false};
  std::thread stopper([&]() {
    handle_manager_->Stop();
    stop_returned.store(true);
  });

  // Stop must block while the guard is held (writer not yet released).
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  EXPECT_FALSE(stop_returned.load())
      << "Stop must wait for the outstanding guard to be released";
  EXPECT_EQ(writer_table_->Size(), 1u)
      << "writer must not be released while a request still holds the handle";

  // Releasing the guard unblocks Stop, which then releases the writer.
  guard = HandleGuard{};
  stopper.join();
  EXPECT_TRUE(stop_returned.load());
  EXPECT_EQ(writer_table_->Size(), 0u)
      << "writer released once the in-flight request drained";
}

// HandleGuard is move-only and transfers ownership; the moved-from guard must
// not release, the moved-to guard must, with no double release.
TEST_F(HandleManagerTest, HandleGuard_MoveTransfersOwnership) {
  auto* h = handle_manager_->NewHandle(/*fh*/ 56, /*ino*/ 906, O_RDONLY);
  ASSERT_NE(h, nullptr);

  {
    auto g1 = handle_manager_->FindHandlerGuard(56);
    ASSERT_TRUE(g1);

    auto g2 = std::move(g1);
    EXPECT_FALSE(g1) << "moved-from guard must be empty";
    ASSERT_TRUE(g2);
    EXPECT_EQ(g2.get(), h);
  }  // only g2 releases here; a double release would underflow Handle::refs

  // Handle is still alive (only the transient guard ref was dropped).
  EXPECT_EQ(handle_manager_->FindHandlerGuard(56).get(), h);
  handle_manager_->ReleaseHandler(56);
}

// A late FUSE_RELEASE racing an in-progress Stop() must be safe: no deadlock,
// no double ReleaseWriter (WriterTable holders underflow), no use-after-free.
// Stop is forced to block in its cv_.wait by pinning handle A with a guard,
// then handle B is released concurrently while Stop is parked.
// Best run under TSAN/ASAN; the race window here is probabilistic.
TEST_F(HandleManagerTest, Stop_ConcurrentReleaseHandler_Safe) {
  // A: pinned by a guard so Stop blocks.
  ASSERT_NE(handle_manager_->NewHandle(/*fh*/ 57, /*ino*/ 907, O_WRONLY),
            nullptr);
  // B: dropped by a concurrent FUSE_RELEASE while Stop is parked.
  ASSERT_NE(handle_manager_->NewHandle(/*fh*/ 58, /*ino*/ 908, O_WRONLY),
            nullptr);
  EXPECT_EQ(writer_table_->Size(), 2u);

  auto guard = handle_manager_->FindHandlerGuard(57);  // pin A
  ASSERT_TRUE(guard);

  std::atomic<bool> stop_returned{false};
  std::thread stopper([&]() {
    handle_manager_->Stop();  // blocks on A's outstanding guard
    stop_returned.store(true);
  });

  // Let Stop enter its wait, then race a release of B against it.
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  handle_manager_->ReleaseHandler(58);  // concurrent with Stop's cv_.wait

  // A is still pinned -> Stop must still be blocked.
  EXPECT_FALSE(stop_returned.load());

  guard = HandleGuard{};  // release A -> Stop proceeds
  stopper.join();
  EXPECT_TRUE(stop_returned.load());

  // Each writer returned exactly once (B by ReleaseHandler, A by Stop); a
  // double release would have tripped WriterTable's holders CHECK.
  EXPECT_EQ(writer_table_->Size(), 0u);
}

// ~HandleManager must release all writers held by surviving handles —
// otherwise we leak writer slots when the client shuts down without
// explicit per-fh release.
TEST_F(HandleManagerTest, Destructor_ReleasesAllWriters) {
  auto local_hm = std::make_unique<HandleManager>(mock_hub_);
  ASSERT_TRUE(local_hm->Start().ok());

  ASSERT_NE(local_hm->NewHandle(/*fh*/ 60, /*ino*/ 1000, O_WRONLY), nullptr);
  ASSERT_NE(local_hm->NewHandle(/*fh*/ 61, /*ino*/ 1001, O_RDWR), nullptr);
  EXPECT_EQ(writer_table_->Size(), 2u);

  // Destroy without explicit ReleaseHandler — writers must still be returned.
  local_hm.reset();
  EXPECT_EQ(writer_table_->Size(), 0u)
      << "HandleManager destructor must release writers held by surviving "
         "handles";
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
