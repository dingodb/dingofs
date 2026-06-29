// Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <cstdint>
#include <map>

#include "gtest/gtest.h"
#include "mds/common/tracing.h"
#include "mds/common/type.h"
#include "mds/filesystem/store_operation.h"
#include "mds/storage/dummy_storage.h"

namespace dingofs {
namespace mds {
namespace unit_test {

class DirStatOperationTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    auto kv_storage = DummyStorage::New();
    ASSERT_TRUE(kv_storage->Init("")) << "init kv storage fail.";

    processor = OperationProcessor::New(kv_storage);
    ASSERT_TRUE(processor->Init()) << "init operation processor fail.";
  }

  static void TearDownTestSuite() {
    if (processor != nullptr) {
      processor->Destroy();
      processor = nullptr;
    }
  }

  void SetUp() override {}
  void TearDown() override {}

 public:
  static OperationProcessorSPtr processor;
};

OperationProcessorSPtr DirStatOperationTest::processor = nullptr;

TEST_F(DirStatOperationTest, SetGetFlush) {
  uint32_t fs_id = 1000;
  dingofs::mds::Ino ino = 10;

  // set
  {
    dingofs::mds::DirStatEntry st;
    st.set_length(100);
    st.set_inodes(1);
    Trace trace;
    std::map<uint64_t, dingofs::mds::DirStatEntry> one{{ino, st}};
    dingofs::mds::BatchSetDirStatOperation op(trace, fs_id, std::move(one));
    ASSERT_TRUE(processor->RunAlone(&op).ok());
  }

  // get
  {
    Trace trace;
    dingofs::mds::GetDirStatOperation op(trace, fs_id, ino);
    ASSERT_TRUE(processor->RunAlone(&op).ok());
    ASSERT_TRUE(op.GetResult().found);
    EXPECT_EQ(op.GetResult().dir_stat.inodes(), 1);
  }

  // flush delta on existing key
  {
    std::map<uint64_t, dingofs::mds::DirStatDelta> deltas;
    deltas[ino] = {50, 1};
    Trace trace;
    dingofs::mds::FlushDirStatsOperation op(trace, fs_id, deltas);
    ASSERT_TRUE(processor->RunAlone(&op).ok());
    EXPECT_TRUE(op.GetResult().missing_inos.empty());
  }

  // verify accumulated
  {
    Trace trace;
    dingofs::mds::GetDirStatOperation op(trace, fs_id, ino);
    ASSERT_TRUE(processor->RunAlone(&op).ok());
    EXPECT_EQ(op.GetResult().dir_stat.length(), 150);
    EXPECT_EQ(op.GetResult().dir_stat.inodes(), 2);
  }

  // flush delta on missing key -> reported
  {
    std::map<uint64_t, dingofs::mds::DirStatDelta> deltas;
    deltas[999] = {1, 1};
    Trace trace;
    dingofs::mds::FlushDirStatsOperation op(trace, fs_id, deltas);
    ASSERT_TRUE(processor->RunAlone(&op).ok());
    ASSERT_EQ(op.GetResult().missing_inos.size(), 1u);
    EXPECT_EQ(op.GetResult().missing_inos[0], 999u);
  }
}

TEST_F(DirStatOperationTest, Delete) {
  uint32_t fs_id = 1000;
  dingofs::mds::Ino ino = 30;
  // seed
  {
    dingofs::mds::DirStatEntry st;
    st.set_length(100);
    st.set_inodes(1);
    Trace trace;
    std::map<uint64_t, dingofs::mds::DirStatEntry> one{{ino, st}};
    dingofs::mds::BatchSetDirStatOperation op(trace, fs_id, std::move(one));
    ASSERT_TRUE(processor->RunAlone(&op).ok());
  }
  // delete
  {
    Trace trace;
    dingofs::mds::DeleteDirStatOperation op(trace, fs_id, ino);
    ASSERT_TRUE(processor->RunAlone(&op).ok());
  }
  // get -> not found
  {
    Trace trace;
    dingofs::mds::GetDirStatOperation op(trace, fs_id, ino);
    ASSERT_TRUE(processor->RunAlone(&op).ok());
    EXPECT_FALSE(op.GetResult().found);
  }
}

TEST_F(DirStatOperationTest, FlushNegativeReportedNotWritten) {
  uint32_t fs_id = 1000;
  dingofs::mds::Ino ino = 20;
  // seed a small stat
  {
    dingofs::mds::DirStatEntry st;
    st.set_length(10);
    st.set_inodes(1);
    Trace trace;
    std::map<uint64_t, dingofs::mds::DirStatEntry> one{{ino, st}};
    dingofs::mds::BatchSetDirStatOperation op(trace, fs_id, std::move(one));
    ASSERT_TRUE(processor->RunAlone(&op).ok());
  }
  // flush a delta that would drive length negative
  {
    std::map<uint64_t, dingofs::mds::DirStatDelta> deltas;
    deltas[ino] = {-100, 0};
    Trace trace;
    dingofs::mds::FlushDirStatsOperation op(trace, fs_id, deltas);
    ASSERT_TRUE(processor->RunAlone(&op).ok());
    ASSERT_EQ(op.GetResult().missing_inos.size(), 1u);
    EXPECT_EQ(op.GetResult().missing_inos[0], ino);
  }
  // stored value must be UNCHANGED (not negative)
  {
    Trace trace;
    dingofs::mds::GetDirStatOperation op(trace, fs_id, ino);
    ASSERT_TRUE(processor->RunAlone(&op).ok());
    ASSERT_TRUE(op.GetResult().found);
    EXPECT_EQ(op.GetResult().dir_stat.length(), 10);
  }
}

// Seed creates an absent record and never clobbers an existing one.
TEST_F(DirStatOperationTest, SeedNoClobber) {
  uint32_t fs_id = 1000;
  dingofs::mds::Ino ino = 50;

  // seed on absent -> created.
  {
    dingofs::mds::DirStatEntry calc;
    calc.set_length(700);
    calc.set_inodes(3);
    Trace trace;
    dingofs::mds::SeedDirStatOperation op(trace, fs_id, ino, calc);
    ASSERT_TRUE(processor->RunAlone(&op).ok());
    EXPECT_TRUE(op.GetResult().seeded);
  }
  {
    Trace trace;
    dingofs::mds::GetDirStatOperation op(trace, fs_id, ino);
    ASSERT_TRUE(processor->RunAlone(&op).ok());
    ASSERT_TRUE(op.GetResult().found);
    EXPECT_EQ(op.GetResult().dir_stat.length(), 700);
  }

  // seed again with a different value -> no-op, existing record untouched.
  {
    dingofs::mds::DirStatEntry calc;
    calc.set_length(999);
    calc.set_inodes(9);
    Trace trace;
    dingofs::mds::SeedDirStatOperation op(trace, fs_id, ino, calc);
    ASSERT_TRUE(processor->RunAlone(&op).ok());
    EXPECT_FALSE(op.GetResult().seeded);
  }
  {
    Trace trace;
    dingofs::mds::GetDirStatOperation op(trace, fs_id, ino);
    ASSERT_TRUE(processor->RunAlone(&op).ok());
    EXPECT_EQ(op.GetResult().dir_stat.length(), 700);
  }
}

// Repair: check-only never writes; repair overwrites a mismatch and leaves a
// match untouched.
TEST_F(DirStatOperationTest, RepairCheckAndWrite) {
  uint32_t fs_id = 1000;
  dingofs::mds::Ino ino = 60;
  {
    dingofs::mds::DirStatEntry st;
    st.set_length(10);
    st.set_inodes(1);
    Trace trace;
    std::map<uint64_t, dingofs::mds::DirStatEntry> one{{ino, st}};
    dingofs::mds::BatchSetDirStatOperation op(trace, fs_id, std::move(one));
    ASSERT_TRUE(processor->RunAlone(&op).ok());
  }

  dingofs::mds::DirStatEntry calc;
  calc.set_length(42);
  calc.set_inodes(5);

  // check-only (repair=false): reports mismatch, writes nothing.
  {
    Trace trace;
    dingofs::mds::RepairDirStatOperation op(trace, fs_id, ino, calc, /*repair=*/false);
    ASSERT_TRUE(processor->RunAlone(&op).ok());
    EXPECT_TRUE(op.GetResult().found);
    EXPECT_TRUE(op.GetResult().mismatch);
    EXPECT_FALSE(op.GetResult().wrote);
  }
  {
    Trace trace;
    dingofs::mds::GetDirStatOperation op(trace, fs_id, ino);
    ASSERT_TRUE(processor->RunAlone(&op).ok());
    EXPECT_EQ(op.GetResult().dir_stat.length(), 10);  // unchanged
  }

  // repair: overwrites with calc.
  {
    Trace trace;
    dingofs::mds::RepairDirStatOperation op(trace, fs_id, ino, calc, /*repair=*/true);
    ASSERT_TRUE(processor->RunAlone(&op).ok());
    EXPECT_TRUE(op.GetResult().mismatch);
    EXPECT_TRUE(op.GetResult().wrote);
  }
  {
    Trace trace;
    dingofs::mds::GetDirStatOperation op(trace, fs_id, ino);
    ASSERT_TRUE(processor->RunAlone(&op).ok());
    EXPECT_EQ(op.GetResult().dir_stat.length(), 42);
    EXPECT_EQ(op.GetResult().dir_stat.inodes(), 5);
  }

  // repair when already matching: no write.
  {
    Trace trace;
    dingofs::mds::RepairDirStatOperation op(trace, fs_id, ino, calc, /*repair=*/true);
    ASSERT_TRUE(processor->RunAlone(&op).ok());
    EXPECT_FALSE(op.GetResult().mismatch);
    EXPECT_FALSE(op.GetResult().wrote);
  }
}

// Repair on an absent record creates it (found=false).
TEST_F(DirStatOperationTest, RepairAbsentCreates) {
  uint32_t fs_id = 1000;
  dingofs::mds::Ino ino = 70;
  dingofs::mds::DirStatEntry calc;
  calc.set_length(3);
  calc.set_inodes(2);
  {
    Trace trace;
    dingofs::mds::RepairDirStatOperation op(trace, fs_id, ino, calc, /*repair=*/true);
    ASSERT_TRUE(processor->RunAlone(&op).ok());
    EXPECT_FALSE(op.GetResult().found);
    EXPECT_TRUE(op.GetResult().mismatch);
    EXPECT_TRUE(op.GetResult().wrote);
  }
  {
    Trace trace;
    dingofs::mds::GetDirStatOperation op(trace, fs_id, ino);
    ASSERT_TRUE(processor->RunAlone(&op).ok());
    ASSERT_TRUE(op.GetResult().found);
    EXPECT_EQ(op.GetResult().dir_stat.inodes(), 2);
  }
}

}  // namespace unit_test
}  // namespace mds
}  // namespace dingofs
