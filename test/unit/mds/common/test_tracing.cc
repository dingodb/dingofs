// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
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

#include "mds/common/tracing.h"

#include <thread>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

namespace dingofs {
namespace mds {
namespace unit_test {

class TracingTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(TracingTest, TraceCreation) {
  Trace trace;

  // Trace should be created with default values
  auto& cache = trace.GetCache();
  EXPECT_FALSE(cache.is_hit_partition);
  EXPECT_FALSE(cache.is_hit_dentry);
  EXPECT_FALSE(cache.is_hit_inode);
  EXPECT_FALSE(cache.is_hit_chunk);

  auto& time = trace.GetTime();
  EXPECT_TRUE(time.elapsed_times.empty());

  EXPECT_TRUE(trace.GetTxns().empty());
}

TEST_F(TracingTest, SetHitPartition) {
  Trace trace;

  trace.SetHitPartition();

  auto& cache = trace.GetCache();
  EXPECT_TRUE(cache.is_hit_partition);
  EXPECT_FALSE(cache.is_hit_dentry);
  EXPECT_FALSE(cache.is_hit_inode);
  EXPECT_FALSE(cache.is_hit_chunk);
}

TEST_F(TracingTest, SetHitDentry) {
  Trace trace;

  trace.SetHitDentry();

  auto& cache = trace.GetCache();
  EXPECT_FALSE(cache.is_hit_partition);
  EXPECT_TRUE(cache.is_hit_dentry);
  EXPECT_FALSE(cache.is_hit_inode);
  EXPECT_FALSE(cache.is_hit_chunk);
}

TEST_F(TracingTest, SetHitInode) {
  Trace trace;

  trace.SetHitInode();

  auto& cache = trace.GetCache();
  EXPECT_FALSE(cache.is_hit_partition);
  EXPECT_FALSE(cache.is_hit_dentry);
  EXPECT_TRUE(cache.is_hit_inode);
  EXPECT_FALSE(cache.is_hit_chunk);
}

TEST_F(TracingTest, SetHitChunk) {
  Trace trace;

  trace.SetHitChunk();

  auto& cache = trace.GetCache();
  EXPECT_FALSE(cache.is_hit_partition);
  EXPECT_FALSE(cache.is_hit_dentry);
  EXPECT_FALSE(cache.is_hit_inode);
  EXPECT_TRUE(cache.is_hit_chunk);
}

TEST_F(TracingTest, SetMultipleCacheHits) {
  Trace trace;

  trace.SetHitPartition();
  trace.SetHitDentry();
  trace.SetHitInode();
  trace.SetHitChunk();

  auto& cache = trace.GetCache();
  EXPECT_TRUE(cache.is_hit_partition);
  EXPECT_TRUE(cache.is_hit_dentry);
  EXPECT_TRUE(cache.is_hit_inode);
  EXPECT_TRUE(cache.is_hit_chunk);
}

TEST_F(TracingTest, RecordElapsedTime) {
  Trace trace;

  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  trace.RecordElapsedTime("point1");

  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  trace.RecordElapsedTime("point2");

  auto& time = trace.GetTime();
  EXPECT_EQ(time.elapsed_times.size(), 2);
  EXPECT_EQ(time.elapsed_times[0].first, "point1");
  EXPECT_EQ(time.elapsed_times[1].first, "point2");

  // Elapsed times should be positive
  EXPECT_GT(time.elapsed_times[0].second, 0);
  EXPECT_GT(time.elapsed_times[1].second, 0);
}

TEST_F(TracingTest, ElapsedTimeProgression) {
  Trace trace;

  // First record might be 0 or very small as it's from construction time
  trace.RecordElapsedTime("point1");
  std::this_thread::sleep_for(std::chrono::milliseconds(5));
  trace.RecordElapsedTime("point2");
  std::this_thread::sleep_for(std::chrono::milliseconds(5));
  trace.RecordElapsedTime("point3");

  auto& time = trace.GetTime();
  EXPECT_EQ(time.elapsed_times.size(), 3);

  // Later elapsed times should be positive
  EXPECT_GT(time.elapsed_times[1].second, 0);
  EXPECT_GT(time.elapsed_times[2].second, 0);
}

TEST_F(TracingTest, AddTxn) {
  Trace trace;

  Trace::Txn txn;
  txn.txn_id = 12345;
  txn.commit_type = (char*)"write";
  txn.is_conflict = false;
  txn.read_time_us = 100;
  txn.write_time_us = 200;

  trace.AddTxn(txn);

  auto& txns = trace.GetTxns();
  EXPECT_EQ(txns.size(), 1);
  EXPECT_EQ(txns[0].txn_id, 12345);
  EXPECT_STREQ(txns[0].commit_type, "write");
  EXPECT_FALSE(txns[0].is_conflict);
  EXPECT_EQ(txns[0].read_time_us, 100);
  EXPECT_EQ(txns[0].write_time_us, 200);
}

TEST_F(TracingTest, AddMultipleTxns) {
  Trace trace;

  Trace::Txn txn1;
  txn1.txn_id = 1;
  txn1.commit_type = (char*)"read";
  txn1.is_conflict = false;

  Trace::Txn txn2;
  txn2.txn_id = 2;
  txn2.commit_type = (char*)"write";
  txn2.is_conflict = true;

  Trace::Txn txn3;
  txn3.txn_id = 3;
  txn3.commit_type = (char*)"delete";
  txn3.is_conflict = false;

  trace.AddTxn(txn1);
  trace.AddTxn(txn2);
  trace.AddTxn(txn3);

  auto& txns = trace.GetTxns();
  EXPECT_EQ(txns.size(), 3);
  EXPECT_EQ(txns[0].txn_id, 1);
  EXPECT_EQ(txns[1].txn_id, 2);
  EXPECT_EQ(txns[2].txn_id, 3);
}

TEST_F(TracingTest, TxnDefaultCommitType) {
  Trace::Txn txn;

  EXPECT_EQ(txn.txn_id, 0);
  EXPECT_STREQ(txn.commit_type, "none");
  EXPECT_FALSE(txn.is_conflict);
  EXPECT_EQ(txn.read_time_us, 0);
  EXPECT_EQ(txn.write_time_us, 0);
}

TEST_F(TracingTest, ConstGetters) {
  Trace trace;
  trace.SetHitPartition();
  trace.RecordElapsedTime("test_point");

  Trace::Txn txn;
  txn.txn_id = 1;
  trace.AddTxn(txn);

  // Test const getters
  const Trace& const_trace = trace;

  const auto& cache = const_trace.GetCache();
  EXPECT_TRUE(cache.is_hit_partition);

  const auto& time = const_trace.GetTime();
  EXPECT_EQ(time.elapsed_times.size(), 1);

  const auto& txns = const_trace.GetTxns();
  EXPECT_EQ(txns.size(), 1);
}

TEST_F(TracingTest, TraceTimeStructure) {
  Trace::Time time;

  EXPECT_TRUE(time.elapsed_times.empty());

  time.elapsed_times.emplace_back("test", 100);
  EXPECT_EQ(time.elapsed_times.size(), 1);
  EXPECT_EQ(time.elapsed_times[0].first, "test");
  EXPECT_EQ(time.elapsed_times[0].second, 100);
}

TEST_F(TracingTest, TraceCacheStructure) {
  Trace::Cache cache;

  EXPECT_FALSE(cache.is_hit_partition);
  EXPECT_FALSE(cache.is_hit_dentry);
  EXPECT_FALSE(cache.is_hit_inode);
  EXPECT_FALSE(cache.is_hit_chunk);

  cache.is_hit_partition = true;
  cache.is_hit_dentry = true;
  cache.is_hit_inode = true;
  cache.is_hit_chunk = true;

  EXPECT_TRUE(cache.is_hit_partition);
  EXPECT_TRUE(cache.is_hit_dentry);
  EXPECT_TRUE(cache.is_hit_inode);
  EXPECT_TRUE(cache.is_hit_chunk);
}

}  // namespace unit_test
}  // namespace mds
}  // namespace dingofs
