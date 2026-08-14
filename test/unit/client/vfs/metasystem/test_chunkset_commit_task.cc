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

// Regression tests for the O_TRUNC / in-flight commit task race that crashed
// dingo-client on the manila mount (chunk.cc CHECK on commit_task_map_).
//
// The race: ChunkSet is per-ino and shared by every fd of a file. fd A's
// WriteSlice RPC is in flight when fd B opens the same file with O_TRUNC;
// MDSMetaSystem::DoOpen calls ChunkSet::Reset(), which wipes commit_task_map_.
// When the RPC returns, FinishCommitTask() can no longer find its own task.
//
// These tests drive ChunkSet directly, so "the RPC is in flight" is simply the
// gap between TryCommitSlice() and FinishCommitTask().

#include <gtest/gtest.h>

#include <atomic>
#include <set>
#include <thread>
#include <vector>

#include "client/vfs/metasystem/mds/chunk.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {
namespace unit_test {

namespace {

constexpr Ino kIno = 20001437836;  // the inode that crashed in production

// The production write shape: 106496 @0 + 5531 @106496 (user.config.json).
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

// Stage one slice and force it into a commit task, mimicking
// MDSMetaSystem::WriteSlice -> Append -> AsyncFlushSlice -> TryCommitSlice.
// Returns the task that was just created. ListCommitTask() walks an unordered
// map, so the new task has to be identified by diffing the task ids rather
// than by position.
CommitTaskSPtr StageAndCommit(const ChunkSetSPtr& chunk_set, uint32_t index,
                              uint64_t offset, uint64_t length) {
  std::set<uint64_t> before;
  for (const auto& task : chunk_set->ListCommitTask()) {
    before.insert(task->TaskID());
  }

  chunk_set->Append(index, {MakeSlice((index * 1000) + offset, offset, length)});
  EXPECT_GT(chunk_set->TryCommitSlice(/*is_force=*/true), 0u);

  for (const auto& task : chunk_set->ListCommitTask()) {
    if (before.count(task->TaskID()) == 0) return task;
  }

  ADD_FAILURE() << "TryCommitSlice did not create a new task";
  return nullptr;
}

}  // namespace

class ChunkSetCommitTaskTest : public ::testing::Test {
 protected:
  void SetUp() override { chunk_set_ = ChunkSet::New(kIno); }

  ChunkSetSPtr chunk_set_;
};

// ---------------------------------------------------------------------------
// 1. Happy path: nothing changed for the normal commit flow.
// ---------------------------------------------------------------------------

TEST_F(ChunkSetCommitTaskTest, FinishAppliesResultWhenEpochMatches) {
  auto task = StageAndCommit(chunk_set_, 0, 0, 106496);
  ASSERT_NE(task, nullptr);
  ASSERT_EQ(task->Epoch(), chunk_set_->Epoch());
  ASSERT_TRUE(chunk_set_->HasCommitTask());
  ASSERT_EQ(chunk_set_->GetLastComitedLength(), 0u);

  EXPECT_EQ(chunk_set_->FinishCommitTask(task->TaskID(), task->Epoch(), {}),
            ChunkSet::CommitResult::kApplied);

  EXPECT_FALSE(chunk_set_->HasCommitTask());
  EXPECT_EQ(chunk_set_->GetCommitTaskSize(), 0u);
  // The commited length must be published for DoFlushFile to pick up.
  EXPECT_EQ(chunk_set_->GetLastComitedLength(), 106496u);
}

TEST_F(ChunkSetCommitTaskTest, MultipleChunksCommitIndependently) {
  auto task0 = StageAndCommit(chunk_set_, 0, 0, 106496);
  auto task1 = StageAndCommit(chunk_set_, 1, 0, 5531);
  ASSERT_NE(task0, nullptr);
  ASSERT_NE(task1, nullptr);
  ASSERT_NE(task0->TaskID(), task1->TaskID());
  EXPECT_EQ(chunk_set_->GetCommitTaskSize(), 2u);

  EXPECT_EQ(chunk_set_->FinishCommitTask(task0->TaskID(), task0->Epoch(), {}),
            ChunkSet::CommitResult::kApplied);
  EXPECT_EQ(chunk_set_->GetCommitTaskSize(), 1u);
  EXPECT_TRUE(chunk_set_->HasCommitTask());

  EXPECT_EQ(chunk_set_->FinishCommitTask(task1->TaskID(), task1->Epoch(), {}),
            ChunkSet::CommitResult::kApplied);
  EXPECT_FALSE(chunk_set_->HasCommitTask());
}

// ---------------------------------------------------------------------------
// 2. The production crash, reproduced exactly. Before the fix this aborted the
//    process; it must now be rejected and survive.
// ---------------------------------------------------------------------------

TEST_F(ChunkSetCommitTaskTest, FinishAfterResetIsRejectedAndDoesNotCrash) {
  // fd A: write + flush -> commit task created, WriteSlice RPC dispatched.
  auto task = StageAndCommit(chunk_set_, 0, 0, 106496);
  ASSERT_NE(task, nullptr);
  const uint64_t epoch_before = chunk_set_->Epoch();
  ASSERT_EQ(task->Epoch(), epoch_before);

  // fd B: open(O_WRONLY|O_TRUNC) on the same ino -> DoOpen -> Reset().
  chunk_set_->Reset();
  EXPECT_EQ(chunk_set_->Epoch(), epoch_before + 1);
  EXPECT_FALSE(chunk_set_->HasCommitTask());

  // The RPC returns. This used to be a CHECK failure -> LOG(FATAL) -> abort.
  // Must be reported as a truncate-invalidated task, not as an unexplained
  // missing task: that distinction is the whole point of the epoch.
  EXPECT_EQ(chunk_set_->FinishCommitTask(task->TaskID(), task->Epoch(), {}),
            ChunkSet::CommitResult::kStaleEpoch);
}

// The whole point of the epoch: a stale task must not publish state that
// outlives the truncate. This is what would corrupt ChunkMemo in production.
TEST_F(ChunkSetCommitTaskTest, StaleTaskDoesNotPublishCommitedLength) {
  auto task = StageAndCommit(chunk_set_, 0, 0, 106496);
  ASSERT_NE(task, nullptr);
  ASSERT_GT(task->GetLength(), 0u);

  chunk_set_->Reset();
  ASSERT_EQ(chunk_set_->GetLastComitedLength(), 0u);

  ASSERT_EQ(chunk_set_->FinishCommitTask(task->TaskID(), task->Epoch(), {}),
            ChunkSet::CommitResult::kStaleEpoch);

  // Must still be 0. If the stale task leaked through, the truncated file
  // would report its pre-truncate length.
  EXPECT_EQ(chunk_set_->GetLastComitedLength(), 0u);
}

// A fresh task created after the truncate must still work normally.
TEST_F(ChunkSetCommitTaskTest, NewTaskAfterResetCommitsNormally) {
  auto stale = StageAndCommit(chunk_set_, 0, 0, 106496);
  ASSERT_NE(stale, nullptr);

  chunk_set_->Reset();

  auto fresh = StageAndCommit(chunk_set_, 0, 0, 5531);
  ASSERT_NE(fresh, nullptr);
  EXPECT_EQ(fresh->Epoch(), chunk_set_->Epoch());
  EXPECT_NE(fresh->Epoch(), stale->Epoch());

  // Stale one rejected, fresh one applied — order must not matter.
  EXPECT_EQ(chunk_set_->FinishCommitTask(stale->TaskID(), stale->Epoch(), {}),
            ChunkSet::CommitResult::kStaleEpoch);
  EXPECT_EQ(chunk_set_->FinishCommitTask(fresh->TaskID(), fresh->Epoch(), {}),
            ChunkSet::CommitResult::kApplied);
  EXPECT_EQ(chunk_set_->GetLastComitedLength(), 5531u);
}

// Production saw up to 4 fds truncating within 10ms.
TEST_F(ChunkSetCommitTaskTest, RepeatedResetsKeepRejectingOlderTasks) {
  std::vector<CommitTaskSPtr> tasks;
  for (int i = 0; i < 4; ++i) {
    auto task = StageAndCommit(chunk_set_, 0, 0, 106496);
    ASSERT_NE(task, nullptr);
    tasks.push_back(task);
    chunk_set_->Reset();
  }

  EXPECT_EQ(chunk_set_->Epoch(), 4u);
  for (auto& task : tasks) {
    EXPECT_EQ(chunk_set_->FinishCommitTask(task->TaskID(), task->Epoch(), {}),
              ChunkSet::CommitResult::kStaleEpoch)
        << "task " << task->TaskID() << " epoch " << task->Epoch();
  }
}

// ---------------------------------------------------------------------------
// 3. Epoch mismatch must stay distinguishable from a genuine invariant break.
// ---------------------------------------------------------------------------

TEST_F(ChunkSetCommitTaskTest, DoubleFinishIsRejectedWithinSameEpoch) {
  auto task = StageAndCommit(chunk_set_, 0, 0, 106496);
  ASSERT_NE(task, nullptr);

  EXPECT_EQ(chunk_set_->FinishCommitTask(task->TaskID(), task->Epoch(), {}),
            ChunkSet::CommitResult::kApplied);
  // Same epoch, task already erased: a real bug, reported but not fatal.
  EXPECT_EQ(chunk_set_->FinishCommitTask(task->TaskID(), task->Epoch(), {}),
            ChunkSet::CommitResult::kNotFound);
}

TEST_F(ChunkSetCommitTaskTest, UnknownTaskIdIsRejected) {
  EXPECT_EQ(chunk_set_->FinishCommitTask(/*task_id=*/999999,
                                         chunk_set_->Epoch(), {}),
            ChunkSet::CommitResult::kNotFound);
}

// ---------------------------------------------------------------------------
// 4. Reset() must also release the per-chunk committing guard, otherwise the
//    chunk stays permanently blocked from committing after a truncate.
// ---------------------------------------------------------------------------

TEST_F(ChunkSetCommitTaskTest, SameChunkIsBlockedWhileCommittingAndFreedByReset) {
  ASSERT_NE(StageAndCommit(chunk_set_, 0, 0, 106496), nullptr);

  // chunk 0 is in committing_chunk_index_set_, so a second commit is skipped.
  chunk_set_->Append(0, {MakeSlice(2, 106496, 5531)});
  EXPECT_EQ(chunk_set_->TryCommitSlice(/*is_force=*/true), 0u);
  EXPECT_EQ(chunk_set_->GetCommitTaskSize(), 1u);

  chunk_set_->Reset();

  // After the truncate the chunk must be committable again.
  chunk_set_->Append(0, {MakeSlice(3, 0, 4096)});
  EXPECT_GT(chunk_set_->TryCommitSlice(/*is_force=*/true), 0u);
  EXPECT_EQ(chunk_set_->GetCommitTaskSize(), 1u);
}

// ---------------------------------------------------------------------------
// 5. Concurrency: this is the shape that actually crashed production — a
//    writer thread committing while an opener thread truncates. Run it hot and
//    assert we neither abort, deadlock, nor apply a stale result.
// ---------------------------------------------------------------------------

TEST_F(ChunkSetCommitTaskTest, ConcurrentCommitAndResetIsSafe) {
  constexpr int kWriters = 4;  // production saw 4 concurrent fds
  constexpr int kIterations = 3000;

  std::atomic<bool> stop{false};
  std::atomic<uint64_t> applied{0};
  std::atomic<uint64_t> discarded{0};
  std::atomic<uint64_t> unexplained{0};

  std::vector<std::thread> writers;
  writers.reserve(kWriters);
  for (int w = 0; w < kWriters; ++w) {
    writers.emplace_back([&, w] {
      for (int i = 0; i < kIterations && !stop.load(); ++i) {
        const uint32_t index = static_cast<uint32_t>(w);
        chunk_set_->Append(index, {MakeSlice(i, 0, 106496)});
        if (chunk_set_->TryCommitSlice(/*is_force=*/true) == 0) continue;

        // Mirror AsyncFlushSlice: claim the task with MaybeRun() so exactly
        // one thread ever finishes it, then snapshot what LaunchWriteSlice
        // captures before dispatching the RPC.
        for (auto& task : chunk_set_->ListCommitTask()) {
          if (!task->MaybeRun()) continue;

          const uint64_t id = task->TaskID();
          const uint64_t epoch = task->Epoch();
          std::this_thread::yield();  // widen the in-flight window
          switch (chunk_set_->FinishCommitTask(id, epoch, {})) {
            case ChunkSet::CommitResult::kApplied:
              applied.fetch_add(1, std::memory_order_relaxed);
              break;
            case ChunkSet::CommitResult::kStaleEpoch:
              discarded.fetch_add(1, std::memory_order_relaxed);
              break;
            case ChunkSet::CommitResult::kNotFound:
              // Must never happen: a task can only vanish via Reset(), which
              // always bumps the epoch, or via its own successful finish.
              unexplained.fetch_add(1, std::memory_order_relaxed);
              break;
          }
        }
      }
    });
  }

  // The O_TRUNC opener.
  std::thread truncater([&] {
    for (int i = 0; i < kIterations && !stop.load(); ++i) {
      chunk_set_->Reset();
      std::this_thread::yield();
    }
  });

  for (auto& t : writers) t.join();
  stop.store(true);
  truncater.join();

  // Survived without abort or deadlock, and the race really was exercised.
  EXPECT_EQ(unexplained.load(), 0u)
      << "FinishCommitTask reported kNotFound; a task disappeared without a "
         "Reset(), which means the epoch no longer covers every removal path";
  EXPECT_GT(applied.load() + discarded.load(), 0u);
  EXPECT_GT(discarded.load(), 0u)
      << "no task was ever invalidated by Reset(); the race window was never "
         "hit, so this run proves nothing";
  EXPECT_GE(chunk_set_->Epoch(), 1u);
}

// Concurrent Reset() alone must keep the epoch strictly monotonic — the whole
// scheme collapses if two resets can share an epoch.
TEST_F(ChunkSetCommitTaskTest, EpochIsMonotonicUnderConcurrentReset) {
  constexpr int kThreads = 8;
  constexpr int kResets = 2000;

  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&] {
      for (int i = 0; i < kResets; ++i) chunk_set_->Reset();
    });
  }
  for (auto& t : threads) t.join();

  EXPECT_EQ(chunk_set_->Epoch(),
            static_cast<uint64_t>(kThreads) * kResets);
}

}  // namespace unit_test
}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs
