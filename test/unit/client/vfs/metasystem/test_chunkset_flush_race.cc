// Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

// Deterministic reproduction of a silent data-loss incident (2026-07-04,
// fs_id=10004 ino=20007683286): a file overwrite committed slices in two
// batches; the second batch was swallowed by the 1000ms commit throttle
// (kChunkCommitIntervalMs, chunk.cc) while MDSMetaSystem::WriteSlice had
// already returned OK (enqueue-only). FlushSlice's drain loop
// (metasystem.cc, HasStage/HasCommitting/HasCommitTask emptiness check)
// raced with the late Append of the tail batch, passed while empty, and
// FlushFile committed length=2390200 although persisted slices only
// covered [0, 2129920). Result: torn file, no error anywhere.
//
// These tests reproduce the mechanism at the ChunkSet layer with the real
// incident slice geometry. No RPC/mock needed: TryCommitSlice() creating a
// CommitTask is the exact precondition for a WriteSlice RPC (consumed by
// batch_processor); "no CommitTask created" == "no RPC will ever be sent".

#include <cstdint>
#include <vector>

#include "client/vfs/metasystem/mds/chunk.h"
#include "client/vfs/vfs_meta.h"
#include "gtest/gtest.h"
#include "utils/time.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {

namespace {

constexpr Ino kIno = 20007683286;
constexpr uint32_t kChunkIndex = 0;
constexpr uint64_t kFileLength = 2390200;
constexpr uint64_t kBatch1End = 2129920;  // = 0x208000, gen3 first batch end

Slice MakeSlice(uint64_t id, uint64_t offset, uint64_t length) {
  Slice slice;
  slice.id = id;
  slice.offset = offset;
  slice.length = length;
  slice.compaction = 0;
  slice.is_zero = false;
  slice.size = length;
  return slice;
}

// gen3 first batch: slices 890..898 tiling [0, 2129920), real incident ranges
std::vector<Slice> Batch1() {
  return {
      MakeSlice(10032863890, 0, 163840),
      MakeSlice(10032863891, 163840, 131072),
      MakeSlice(10032863892, 294912, 131072),
      MakeSlice(10032863893, 425984, 98304),
      MakeSlice(10032863894, 524288, 163840),
      MakeSlice(10032863895, 688128, 131072),
      MakeSlice(10032863896, 819200, 393216),
      MakeSlice(10032863897, 1212416, 393216),
      MakeSlice(10032863898, 1605632, 524288),
  };
}

// gen3 second batch: slices 899/900 tiling [2129920, 2390200)
std::vector<Slice> Batch2() {
  return {
      MakeSlice(10032863899, 2129920, 196608),
      MakeSlice(10032863900, 2326528, 63672),
  };
}

// Simulate a successful MDS WriteSlice response for the first batch
// (equivalent to "writeslice finish, chunk(0,4)" on the MDS side).
void AckFirstBatch(ChunkSet& chunk_set) {
  auto tasks = chunk_set.ListCommitTask();
  ASSERT_EQ(1, tasks.size());

  ChunkEntry entry;
  entry.set_index(kChunkIndex);
  entry.set_version(4);
  entry.set_just_descriptor(true);
  chunk_set.FinishCommitTask(tasks[0]->TaskID(), {entry});

  ASSERT_FALSE(chunk_set.HasStage());
  ASSERT_FALSE(chunk_set.HasCommitting());
  ASSERT_FALSE(chunk_set.HasCommitTask());
}

}  // namespace

// Incident step 1: the second batch arriving inside the 1000ms commit
// window is throttled — it stays in stage and NO CommitTask (hence no
// WriteSlice RPC) is created, although WriteSlice() already returned OK to
// the data layer at Append time.
TEST(ChunkSetFlushRaceTest, SecondBatchThrottledInsideCommitWindow) {
  ChunkSet chunk_set(kIno);

  chunk_set.SetLastWriteLength(0, kBatch1End);
  chunk_set.Append(kChunkIndex, Batch1());
  ASSERT_EQ(9, chunk_set.TryCommitSlice(false));  // first commit: no throttle
  uint64_t first_commit_ms = utils::TimestampMs();
  AckFirstBatch(chunk_set);

  // incident: +702ms, inside the 1000ms window; here: immediately after
  chunk_set.SetLastWriteLength(kBatch1End, kFileLength - kBatch1End);
  chunk_set.Append(kChunkIndex, Batch2());
  uint32_t committed = chunk_set.TryCommitSlice(false);
  ASSERT_LT(utils::TimestampMs() - first_commit_ms, 1000)
      << "test ran too slow to stay inside the throttle window";

  EXPECT_EQ(0, committed);                      // throttled: nothing commits
  EXPECT_TRUE(chunk_set.HasStage());            // batch2 stranded in stage
  EXPECT_FALSE(chunk_set.HasCommitTask());      // no task -> no RPC, ever
  EXPECT_TRUE(chunk_set.HasUncommitedSlice());  // yet upper layer saw OK
}

// Incident step 2: FlushSlice's drain emptiness check runs BEFORE the tail
// batch is appended (2ms race in the incident). The drain passes, FlushFile
// commits full length, then the tail batch arrives and is throttled. Final
// state violates the durability invariant: committed slice coverage ends at
// 2129920 while the flushed length says 2390200 — the exact torn-file
// condition, with no error surfaced anywhere.
TEST(ChunkSetFlushRaceTest, FlushDrainRaceLeavesLengthSliceInconsistency) {
  ChunkSet chunk_set(kIno);

  // all 73 fuse writes already accounted: length memo is full size
  chunk_set.SetLastWriteLength(0, kFileLength);

  chunk_set.Append(kChunkIndex, Batch1());
  ASSERT_EQ(9, chunk_set.TryCommitSlice(false));
  uint64_t first_commit_ms = utils::TimestampMs();
  AckFirstBatch(chunk_set);

  // [T1] FlushSlice drain emptiness check (metasystem.cc FlushSlice loop):
  // tail batch not yet appended -> loop breaks, FlushFile proceeds
  bool drain_passes = !chunk_set.HasStage() && !chunk_set.HasCommitting() &&
                      !chunk_set.HasCommitTask();
  ASSERT_TRUE(drain_passes);
  uint64_t flushed_length = chunk_set.GetLastWriteLength();  // -> FlushFile
  EXPECT_EQ(kFileLength, flushed_length);

  // [T2] tail batch appended just after the check (upload callback was late)
  chunk_set.Append(kChunkIndex, Batch2());
  uint32_t committed = chunk_set.TryCommitSlice(false);
  ASSERT_LT(utils::TimestampMs() - first_commit_ms, 1000)
      << "test ran too slow to stay inside the throttle window";
  EXPECT_EQ(0, committed);  // throttled again: still no RPC

  // [T3] torn invariant at the raw ChunkSet layer: flushed length exceeds
  // committed slice coverage while the covering slices sit in stage with no
  // commit task.  Production close/expiry paths must add the missing barrier
  // and must not erase this uncommitted stage silently.
  EXPECT_GT(flushed_length, kBatch1End);
  EXPECT_TRUE(chunk_set.HasStage());
  EXPECT_FALSE(chunk_set.HasCommitTask());
}

// Control: one forced commit at close would rescue the stranded batch —
// demonstrates both that the loss needs the exact race (drain check before
// append) and the fix direction (flush path must force-commit after the
// last append, or WriteSlice OK must be tied to task completion).
TEST(ChunkSetFlushRaceTest, ForceCommitRescuesStagedSlices) {
  ChunkSet chunk_set(kIno);

  chunk_set.SetLastWriteLength(0, kFileLength);
  chunk_set.Append(kChunkIndex, Batch1());
  ASSERT_EQ(9, chunk_set.TryCommitSlice(false));
  AckFirstBatch(chunk_set);

  chunk_set.Append(kChunkIndex, Batch2());
  ASSERT_EQ(0, chunk_set.TryCommitSlice(false));  // throttled

  EXPECT_EQ(2, chunk_set.TryCommitSlice(true));  // force bypasses throttle
  EXPECT_FALSE(chunk_set.HasStage());
  EXPECT_TRUE(chunk_set.HasCommitTask());  // RPC would now be sent
}

// Problem 2 (out-of-order overwrite family), client-side invariant: an ACK
// carrying a version <= commited_version_ makes MarkCommited() clear the
// committing slices WITHOUT moving them to commited (chunk.cc MarkCommited).
// Combined with FinishCommitTask deleting the task, the batch evaporates:
// no stage, no committing, no task, not in GetAllSlice — yet every call
// returned success. This is how a duplicated/stale/epoch-switched MDS reply
// silently drops a committed-in-flight batch.
TEST(ChunkSetFlushRaceTest, StaleVersionAckSilentlyDropsCommittingSlices) {
  ChunkSet chunk_set(kIno);

  // establish commited_version_ = 4 via a normal batch
  chunk_set.Append(kChunkIndex, Batch1());
  ASSERT_EQ(9, chunk_set.TryCommitSlice(true));
  AckFirstBatch(chunk_set);  // version 4

  // next batch goes committing (task created, RPC in flight)
  chunk_set.Append(kChunkIndex, Batch2());
  ASSERT_EQ(2, chunk_set.TryCommitSlice(true));
  auto tasks = chunk_set.ListCommitTask();
  ASSERT_EQ(1, tasks.size());

  // MDS reply arrives with a NON-advancing version (duplicate/stale reply,
  // or another instance already at version 4): MarkCommited(4 <= 4)
  ChunkEntry stale;
  stale.set_index(kChunkIndex);
  stale.set_version(4);  // == commited_version_, does not advance
  stale.set_just_descriptor(true);
  chunk_set.FinishCommitTask(tasks[0]->TaskID(), {stale});

  // the batch is gone from every queue, and nothing will ever resend it
  EXPECT_FALSE(chunk_set.HasStage());
  EXPECT_FALSE(chunk_set.HasCommitting());
  EXPECT_FALSE(chunk_set.HasCommitTask());
  EXPECT_FALSE(chunk_set.HasUncommitedSlice());

  // and it is not visible as commited data either: Batch2 slices vanished
  auto chunk = chunk_set.Get(kChunkIndex);
  ASSERT_NE(nullptr, chunk);
  uint64_t version = 0;
  auto slices = chunk->GetAllSlice(version);
  bool has_tail = false;
  for (const auto& slice : slices) {
    if (slice.id == 10032863899 || slice.id == 10032863900) has_tail = true;
  }
  EXPECT_FALSE(has_tail);  // silently dropped, all calls returned success
}

}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs
