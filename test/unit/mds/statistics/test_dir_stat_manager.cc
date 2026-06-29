// Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include "mds/statistics/dir_stat_manager.h"

namespace dingofs {
namespace mds {
namespace dir_stat {

// These tests exercise only the in-memory delta buffer, so operation_processor
// and worker_set stay null; the manager just needs an FsInfo to read fs_id from.
static FsInfoSPtr MakeFsInfo(uint32_t fs_id) {
  FsInfoEntry entry;
  entry.set_fs_id(fs_id);
  return FsInfo::New(entry);
}

// UpdateDirStat appends to a per-directory log; GetPending folds the log;
// GetFlushSnapshot reports the summed delta plus the newest timepoint without
// removing anything.
TEST(DirStatManagerTest, AccumulateAndSnapshot) {
  DirStatManager mgr(MakeFsInfo(1000), nullptr, nullptr);
  mgr.UpdateDirStat(5, 100, 1, 0, "test");
  mgr.UpdateDirStat(5, -30, 0, 0, "test");
  mgr.UpdateDirStat(6, 10, 1, 0, "test");
  EXPECT_EQ(mgr.Size(), 2u);

  EXPECT_EQ(mgr.GetPending(5).length, 70);
  EXPECT_EQ(mgr.GetPending(5).inodes, 1);
  EXPECT_EQ(mgr.GetPending(6).length, 10);

  auto snap = mgr.GetFlushSnapshot();
  ASSERT_EQ(snap.size(), 2u);
  EXPECT_EQ(snap[5].delta.length, 70);
  EXPECT_EQ(snap[5].delta.inodes, 1);
  EXPECT_EQ(snap[6].delta.length, 10);
  EXPECT_GT(snap[5].timepoint, 0u);

  // GetFlushSnapshot does not remove: the buffer is unchanged.
  EXPECT_EQ(mgr.Size(), 2u);
  EXPECT_EQ(mgr.GetPending(5).length, 70);
}

// Compact pops only the prefix with time_ns <= timepoint; deltas that arrive
// after the captured timepoint survive (the "delta arrives during flush" case).
TEST(DirStatManagerTest, CompactKeepsPostTimepointDeltas) {
  DirStatManager mgr(MakeFsInfo(1000), nullptr, nullptr);
  mgr.UpdateDirStat(5, 100, 1, 0, "first");

  auto snap = mgr.GetFlushSnapshot();
  ASSERT_TRUE(snap.count(5));
  uint64_t timepoint = snap[5].timepoint;

  // a delta appended after the snapshot timepoint must survive the compaction.
  mgr.UpdateDirStat(5, 7, 0, 0, "during-flush");

  mgr.Compact(5, timepoint);

  // only the post-timepoint delta remains.
  EXPECT_EQ(mgr.GetPending(5).length, 7);
  EXPECT_EQ(mgr.GetPending(5).inodes, 0);
  EXPECT_EQ(mgr.Size(), 1u);

  // compacting past the newest timepoint drains the ino and drops the map entry.
  mgr.Compact(5, mgr.PeekMaxTimeNs(5));
  EXPECT_EQ(mgr.Size(), 0u);
  EXPECT_EQ(mgr.PeekMaxTimeNs(5), 0u);
}

// PeekMaxTimeNs returns the newest buffered timestamp (0 when empty), and each
// directory's logical clock is strictly monotonic across its own appends.
TEST(DirStatManagerTest, MonotonicTimestamps) {
  DirStatManager mgr(MakeFsInfo(1000), nullptr, nullptr);
  EXPECT_EQ(mgr.PeekMaxTimeNs(9), 0u);

  mgr.UpdateDirStat(9, 1, 0, 0, "a");
  uint64_t t1 = mgr.PeekMaxTimeNs(9);
  mgr.UpdateDirStat(9, 1, 0, 0, "b");
  uint64_t t2 = mgr.PeekMaxTimeNs(9);
  EXPECT_GT(t2, t1);

  // Each directory keeps its own clock (mirrors quota's per-Quota last_time_ns_):
  // a fresh directory just starts a new strictly-increasing sequence. Cross-
  // directory ordering is neither needed nor guaranteed -- every timestamp
  // comparison (Compact/flush/reconcile) is scoped to a single directory.
  mgr.UpdateDirStat(10, 1, 0, 0, "c");
  uint64_t u1 = mgr.PeekMaxTimeNs(10);
  EXPECT_GT(u1, 0u);
  mgr.UpdateDirStat(10, 1, 0, 0, "d");
  EXPECT_GT(mgr.PeekMaxTimeNs(10), u1);
}

// Compact on an unknown ino is a no-op (no crash, nothing removed elsewhere).
TEST(DirStatManagerTest, CompactUnknownInoIsNoop) {
  DirStatManager mgr(MakeFsInfo(1000), nullptr, nullptr);
  mgr.UpdateDirStat(5, 100, 1, 0, "test");
  mgr.Compact(999, 1u << 30);
  EXPECT_EQ(mgr.Size(), 1u);
  EXPECT_EQ(mgr.GetPending(5).length, 100);
}

}  // namespace dir_stat
}  // namespace mds
}  // namespace dingofs
