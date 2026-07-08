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

// Regression coverage for the O_TRUNC ChunkSet split behind the torn-write
// incidents (xn01 2026-07-04, local repro w4r181 2026-07-08).
//
// The broken sequence was:
//   1. Open(): file_session_map_.Put() bound session->chunk_set to A.
//   2. DoOpen(O_TRUNC): chunk_cache_.Delete(ino) erased A from cache.
//   3. WriteSlice(): chunk_cache_.GetOrCreate(ino) created B and appended
//      slices there.
//   4. Flush(): inspected session->chunk_set A and missed B.
//
// The fix is to rebind the file session to the fresh cache ChunkSet after a
// successful O_TRUNC, so WriteSlice and Flush use the same object again.

#include <fcntl.h>

#include <cstdint>
#include <limits>
#include <vector>

#include "client/vfs/metasystem/mds/chunk.h"
#include "client/vfs/metasystem/mds/file_session.h"
#include "client/vfs/metasystem/mds/inode_cache.h"
#include "client/vfs/vfs_meta.h"
#include "gtest/gtest.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {

namespace {

constexpr uint32_t kFsId = 10008;
constexpr Ino kIno = 20000000024;  // w4r181 incident inode
constexpr uint32_t kChunkIndex = 0;
constexpr uint64_t kOldHeadEnd = 2063968;

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

std::vector<Slice> OldTail() {
  return {MakeSlice(10140578651, kOldHeadEnd, 260280)};
}

std::vector<Slice> OldHead() {
  return {MakeSlice(10140578646, 0, kOldHeadEnd)};
}

void AckTask(ChunkSet& chunk_set, uint64_t version) {
  auto tasks = chunk_set.ListCommitTask();
  ASSERT_EQ(1, tasks.size());
  ChunkEntry entry;
  entry.set_index(kChunkIndex);
  entry.set_version(version);
  entry.set_just_descriptor(true);
  chunk_set.FinishCommitTask(tasks[0]->TaskID(), {entry});
}

ChunkSetSPtr SimulateSuccessfulTruncOpen(FileSessionSPtr session,
                                         ChunkCache& chunk_cache, Ino ino) {
  chunk_cache.Delete(ino);
  auto fresh = chunk_cache.GetOrCreate(ino);
  session->SetChunkSet(fresh);
  return fresh;
}

}  // namespace

TEST(TruncChunkSetSplitTest, TruncOpenRebindsSessionToFreshCacheChunkSet) {
  InodeCache inode_cache(kFsId);
  ChunkCache chunk_cache;
  FileSessionMap session_map(inode_cache, chunk_cache);

  auto session = session_map.Put(kIno, 4296, "sess-4296", O_WRONLY | O_TRUNC);
  auto old_chunk_set = session->GetChunkSet();

  auto fresh_chunk_set =
      SimulateSuccessfulTruncOpen(session, chunk_cache, kIno);
  ASSERT_NE(old_chunk_set.get(), fresh_chunk_set.get());
  ASSERT_EQ(fresh_chunk_set.get(), session->GetChunkSet().get());
  ASSERT_EQ(fresh_chunk_set.get(), chunk_cache.GetOrCreate(kIno).get());

  fresh_chunk_set->Append(kChunkIndex, OldTail());

  auto flush_target = session->GetChunkSet();
  EXPECT_EQ(fresh_chunk_set.get(), flush_target.get());
  EXPECT_TRUE(flush_target->HasStage());
  EXPECT_EQ(1, flush_target->TryCommitSlice(true));
  EXPECT_TRUE(flush_target->HasCommitTask());
}

TEST(TruncChunkSetSplitTest, ExpiredCacheKeepsUncommittedTail) {
  ChunkCache chunk_cache;
  auto chunk_set = chunk_cache.GetOrCreate(kIno);
  chunk_set->Append(kChunkIndex, OldTail());

  ASSERT_TRUE(chunk_cache.HasUncommitedSlice());
  chunk_cache.CleanExpired(std::numeric_limits<uint64_t>::max());

  EXPECT_EQ(chunk_set.get(), chunk_cache.Get(kIno).get());
  EXPECT_TRUE(chunk_cache.HasUncommitedSlice());
}

TEST(TruncChunkSetSplitTest, NextTruncOpenDoesNotInheritPreviousTail) {
  InodeCache inode_cache(kFsId);
  ChunkCache chunk_cache;
  FileSessionMap session_map(inode_cache, chunk_cache);

  auto session1 = session_map.Put(kIno, 4296, "sess-4296", O_WRONLY | O_TRUNC);
  auto chunk_set_b = SimulateSuccessfulTruncOpen(session1, chunk_cache, kIno);

  chunk_set_b->Append(kChunkIndex, OldHead());
  ASSERT_EQ(1, chunk_set_b->TryCommitSlice(true));
  AckTask(*chunk_set_b, 359);

  chunk_set_b->Append(kChunkIndex, OldTail());
  auto flush_target = session1->GetChunkSet();
  ASSERT_EQ(chunk_set_b.get(), flush_target.get());
  ASSERT_TRUE(flush_target->HasStage());
  ASSERT_EQ(1, flush_target->TryCommitSlice(true));

  auto tasks = flush_target->ListCommitTask();
  ASSERT_EQ(1, tasks.size());
  const auto& delta_slices = tasks[0]->DeltaSlices();
  ASSERT_EQ(1, delta_slices.size());
  EXPECT_EQ(359, delta_slices[0].version);
  ASSERT_EQ(1, delta_slices[0].slices.size());
  EXPECT_EQ(10140578651, delta_slices[0].slices[0].id);

  AckTask(*chunk_set_b, 360);
  session_map.Delete(kIno, 4296);

  auto session2 = session_map.Put(kIno, 4320, "sess-4320", O_WRONLY | O_TRUNC);
  auto chunk_set_c = SimulateSuccessfulTruncOpen(session2, chunk_cache, kIno);

  EXPECT_NE(chunk_set_b.get(), chunk_set_c.get());
  EXPECT_EQ(chunk_set_c.get(), session2->GetChunkSet().get());
  EXPECT_FALSE(session2->GetChunkSet()->HasStage());
  EXPECT_FALSE(session2->GetChunkSet()->HasCommitTask());
}

TEST(TruncChunkSetSplitTest, NoTruncNoSplitFlushSeesTail) {
  InodeCache inode_cache(kFsId);
  ChunkCache chunk_cache;
  FileSessionMap session_map(inode_cache, chunk_cache);

  auto session = session_map.Put(kIno, 4296, "sess-4296", O_WRONLY);
  auto chunk_set = chunk_cache.GetOrCreate(kIno);
  EXPECT_EQ(session->GetChunkSet().get(), chunk_set.get());

  chunk_set->Append(kChunkIndex, OldTail());

  auto flush_target = session->GetChunkSet();
  EXPECT_TRUE(flush_target->HasStage());
  EXPECT_EQ(1, flush_target->TryCommitSlice(true));
  EXPECT_TRUE(flush_target->HasCommitTask());
}

TEST(TruncChunkSetSplitTest, UnlinkKeepsCacheWhenFileSessionIsActive) {
  InodeCache inode_cache(kFsId);
  ChunkCache chunk_cache;
  FileSessionMap session_map(inode_cache, chunk_cache);

  auto session = session_map.Put(kIno, 4296, "sess-4296", O_WRONLY);
  auto live_chunk_set = chunk_cache.GetOrCreate(kIno);
  ASSERT_EQ(live_chunk_set.get(), session->GetChunkSet().get());

  // Fixed unlink(nlink==0) behavior with an active open fd: do not delete the
  // cache entry.  An open-unlinked file must keep using the same ChunkSet;
  // deleting here would make later WriteSlice create B while flush still sees
  // A.
  ASSERT_NE(nullptr, session_map.GetSession(kIno));

  auto write_target = chunk_cache.GetOrCreate(kIno);
  EXPECT_EQ(live_chunk_set.get(), write_target.get());
  EXPECT_EQ(session->GetChunkSet().get(), write_target.get());

  write_target->Append(kChunkIndex, OldTail());

  auto flush_target = session->GetChunkSet();
  EXPECT_TRUE(flush_target->HasStage());
  EXPECT_EQ(1, flush_target->TryCommitSlice(true));
  EXPECT_TRUE(flush_target->HasCommitTask());
}

// Fix-path regression: replay the FIXED DoOpen sequence
// (Delete -> GetOrCreate -> SetChunkSet).  Session and cache converge on the
// same object again, the WriteSlice target IS the flush target, and the
// drain sees the pending tail -- the split is gone.  This is the assertion
// that must stay green after the fix (and would have been red before it).
TEST(TruncChunkSetSplitTest, FixedTruncOpenRebindUnifiesSessionAndCache) {
  InodeCache inode_cache(kFsId);
  ChunkCache chunk_cache;
  FileSessionMap session_map(inode_cache, chunk_cache);

  auto session = session_map.Put(kIno, 4296, "sess-4296", O_WRONLY | O_TRUNC);

  // fixed DoOpen(O_TRUNC) sequence
  chunk_cache.Delete(kIno);
  auto rebound = chunk_cache.GetOrCreate(kIno);
  session->SetChunkSet(rebound);

  // WriteSlice path and flush path now resolve to the same object
  auto write_target = chunk_cache.GetOrCreate(kIno);
  EXPECT_EQ(session->GetChunkSet().get(), write_target.get());

  write_target->Append(kChunkIndex, OldTail());

  auto flush_target = session->GetChunkSet();
  EXPECT_TRUE(flush_target->HasStage());             // drain sees the tail
  EXPECT_EQ(1, flush_target->TryCommitSlice(true));  // and commits it
  EXPECT_TRUE(flush_target->HasCommitTask());
}

}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs
