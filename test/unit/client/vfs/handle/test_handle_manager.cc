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

#include "client/vfs/handle/handle_manager.h"

#include <fcntl.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "client/vfs/data/writer/file_writer.h"
#include "client/vfs/data/writer_table.h"
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
  EXPECT_NE(h->reader, nullptr);
  EXPECT_EQ(h->writer, nullptr);
  EXPECT_EQ(writer_table_->Size(), 0u);

  handle_manager_->ReleaseHandler(1);
}

// O_WRONLY / O_RDWR opens must acquire a writer.
TEST_F(HandleManagerTest, NewHandle_WriteMode_HasWriter) {
  auto* h = handle_manager_->NewHandle(/*fh*/ 2, /*ino*/ 200, O_WRONLY);
  ASSERT_NE(h, nullptr);
  EXPECT_NE(h->reader, nullptr);
  EXPECT_NE(h->writer, nullptr);
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

  EXPECT_EQ(h1->writer, h2->writer) << "shared per-inode writer expected";
  EXPECT_EQ(writer_table_->Size(), 1u);

  handle_manager_->ReleaseHandler(3);
  EXPECT_EQ(writer_table_->Size(), 1u);   // h2 still holds

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

  ASSERT_NE(h1->reader, nullptr);
  ASSERT_NE(h2->reader, nullptr);
  EXPECT_NE(h1->reader, h2->reader)
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

  EXPECT_EQ(hr->writer, nullptr) << "O_RDONLY must not allocate a writer";
  EXPECT_NE(hw->writer, nullptr);
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
  ASSERT_NE(h1->writer, nullptr);
  EXPECT_EQ(writer_table_->Size(), 1u);

  handle_manager_->ReleaseHandler(30);
  EXPECT_EQ(writer_table_->Size(), 0u);
  EXPECT_EQ(writer_table_->PeekWriter(700), nullptr) << "entry must be erased";

  // Reopen — a fresh writer is allocated.
  auto* h2 = handle_manager_->NewHandle(/*fh*/ 31, /*ino*/ 700, O_WRONLY);
  ASSERT_NE(h2, nullptr);
  ASSERT_NE(h2->writer, nullptr);
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

// FindHandler returns the live handle for a known fh, nullptr for unknown,
// and nullptr after release.
TEST_F(HandleManagerTest, FindHandler_KnownAndUnknown) {
  auto* h = handle_manager_->NewHandle(/*fh*/ 70, /*ino*/ 1100, O_RDONLY);
  ASSERT_NE(h, nullptr);

  EXPECT_EQ(handle_manager_->FindHandler(70), h);
  EXPECT_EQ(handle_manager_->FindHandler(71), nullptr);

  handle_manager_->ReleaseHandler(70);
  EXPECT_EQ(handle_manager_->FindHandler(70), nullptr)
      << "after release, FindHandler must not return the handle";
}

// ReleaseHandler on an unknown fh must be a safe no-op — it must not crash
// and must not disturb other live handles or the WriterTable.
TEST_F(HandleManagerTest, ReleaseHandler_UnknownFh_NoOp) {
  auto* h = handle_manager_->NewHandle(/*fh*/ 80, /*ino*/ 1200, O_WRONLY);
  ASSERT_NE(h, nullptr);
  EXPECT_EQ(writer_table_->Size(), 1u);

  handle_manager_->ReleaseHandler(/*fh*/ 999);  // never created
  EXPECT_EQ(writer_table_->Size(), 1u);
  EXPECT_EQ(handle_manager_->FindHandler(80), h);

  handle_manager_->ReleaseHandler(80);
  EXPECT_EQ(writer_table_->Size(), 0u);
}

// Stop() must close the per-fh reader (so in-flight reads drain) but must
// NOT prematurely return the writer to WriterTable — the writer is still
// owned by the handle until ReleaseHandler is called.
TEST_F(HandleManagerTest, Stop_KeepsWriterUntilHandleReleased) {
  auto* h = handle_manager_->NewHandle(/*fh*/ 50, /*ino*/ 900, O_WRONLY);
  ASSERT_NE(h, nullptr);
  EXPECT_EQ(writer_table_->Size(), 1u);

  handle_manager_->Stop();
  EXPECT_EQ(writer_table_->Size(), 1u)
      << "Stop must not return the writer to the table";

  handle_manager_->ReleaseHandler(50);
  EXPECT_EQ(writer_table_->Size(), 0u)
      << "ReleaseHandler after Stop must still return the writer";
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
