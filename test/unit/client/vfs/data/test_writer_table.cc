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

#include "client/vfs/data/writer_table.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "client/vfs/data/writer/file_writer.h"
#include "test/unit/client/vfs/test_base.h"

namespace dingofs {
namespace client {
namespace vfs {

using dingofs::client::vfs::test::VFSTestBase;
using ::testing::AnyNumber;

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
// it is read-only with respect to entries (transient AcquireRef snapshot).
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
  table_->Stop();   // second call must be safe
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
