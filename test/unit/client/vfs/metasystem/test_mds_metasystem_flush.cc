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
#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <utility>

#include "client/vfs/metasystem/mds/metasystem.h"
#include "common/trace/context.h"
#include "common/trace/trace_manager.h"
#include "test/unit/client/vfs/mock/mock_compactor.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {

class MDSMetaSystemTestPeer {
 public:
  static CommitTaskSPtr PrepareRunningCommit(MDSMetaSystem& metasystem, Ino ino,
                                             uint64_t fh) {
    auto file_session = metasystem.file_session_map_.Put(
        ino, fh, "flush-test-session", O_WRONLY);

    mds::AttrEntry attr;
    attr.set_ino(ino);
    attr.set_type(pb::mds::FileType::FILE);
    attr.set_length(0);
    file_session->SetInode(metasystem.PutInodeToCache(attr));

    auto chunk_set = file_session->GetChunkSet();
    chunk_set->Append(
        0, {Slice{.id = 1, .size = 4096, .off = 0, .len = 4096, .pos = 0}});
    EXPECT_EQ(chunk_set->TryCommitSlice(true), 1);

    auto tasks = chunk_set->ListCommitTask();
    EXPECT_EQ(tasks.size(), 1);
    EXPECT_TRUE(tasks.front()->MaybeRun());
    return tasks.front();
  }

  static void CompleteForCleanup(MDSMetaSystem& metasystem, Ino ino,
                                 const CommitTaskSPtr& task) {
    auto file_session = metasystem.file_session_map_.GetSession(ino);
    ASSERT_NE(file_session, nullptr);

    std::vector<mds::ChunkEntry> chunks;
    for (uint32_t chunk_index : task->GetChunkIndexs()) {
      auto* chunk = &chunks.emplace_back();
      chunk->set_index(chunk_index);
      chunk->set_version(1);
      chunk->set_just_descriptor(true);
    }
    file_session->GetChunkSet()->FinishCommitTask(task->TaskID(), chunks);
    task->SetDone(Status::OK());
  }

  static ChunkSetSPtr GetChunkSet(MDSMetaSystem& metasystem, Ino ino) {
    auto file_session = metasystem.file_session_map_.GetSession(ino);
    return file_session == nullptr ? nullptr : file_session->GetChunkSet();
  }
};

namespace test {

using namespace std::chrono_literals;

static mds::FsInfoEntry NewFsInfo() {
  mds::FsInfoEntry fs_info;
  fs_info.set_fs_id(1);
  fs_info.set_fs_name("flush-test");
  fs_info.set_chunk_size(1 << 20);
  fs_info.set_block_size(4096);
  return fs_info;
}

TEST(MDSMetaSystemFlushTest, FailedAttemptTerminatesCurrentFlush) {
  TraceManager trace_manager;
  ::dingofs::client::vfs::test::MockCompactor compactor;
  MDSMetaSystem metasystem(NewFsInfo(), ClientId(), RPC(butil::EndPoint()),
                           trace_manager, compactor);

  constexpr Ino kIno = 100;
  constexpr uint64_t kFh = 200;
  auto task =
      MDSMetaSystemTestPeer::PrepareRunningCommit(metasystem, kIno, kFh);
  auto ctx = std::make_shared<Context>("flush_failure_test");

  std::promise<void> flush_started;
  auto flush = std::async(std::launch::async, [&]() {
    flush_started.set_value();
    return metasystem.Flush(ctx, kIno, kFh);
  });
  flush_started.get_future().wait();
  const auto waiter_deadline = std::chrono::steady_clock::now() + 1s;
  while (task->WaiterCount() == 0 &&
         std::chrono::steady_clock::now() < waiter_deadline) {
    std::this_thread::yield();
  }
  ASSERT_GT(task->WaiterCount(), 0);

  const Status injected = Status::Internal("injected writeslice failure");
  task->SetDone(injected);

  if (flush.wait_for(1s) != std::future_status::ready) {
    // Keep a regression from hanging the test process.  The old implementation
    // immediately reset and retried the failed task; finish that retry so the
    // future can be joined and report the wrong result normally.
    MDSMetaSystemTestPeer::CompleteForCleanup(metasystem, kIno, task);
  }

  ASSERT_EQ(flush.wait_for(1s), std::future_status::ready);
  Status status = flush.get();
  ASSERT_FALSE(status.ok());
  EXPECT_EQ(status.ToString(), injected.ToString());
  EXPECT_EQ(task->Retries(), 0);
  EXPECT_FALSE(task->MaybeRun(/*retry_failed=*/false));
  EXPECT_EQ(task->GetStatus().ToString(), injected.ToString());

  auto retry = std::async(std::launch::async,
                          [&]() { return metasystem.Flush(ctx, kIno, kFh); });
  const auto retry_deadline = std::chrono::steady_clock::now() + 1s;
  while (task->Retries() == 0 &&
         std::chrono::steady_clock::now() < retry_deadline) {
    std::this_thread::yield();
  }
  ASSERT_EQ(task->Retries(), 1);

  // No MDS is reachable in this unit test, so the retried write-slice bthread
  // fails its RPC and the explicit Flush propagates that error.  Waiting on the
  // future also joins the detached bthread: its SetDone is what unblocks
  // AsyncFlushSlice's Wait, so the metasystem is never destroyed while the
  // bthread still dereferences `this`.
  ASSERT_EQ(retry.wait_for(1s), std::future_status::ready);
  EXPECT_FALSE(retry.get().ok());
}

TEST(MDSMetaSystemFlushTest, ConcurrentWriteDoesNotExtendActiveFlush) {
  TraceManager trace_manager;
  ::dingofs::client::vfs::test::MockCompactor compactor;
  MDSMetaSystem metasystem(NewFsInfo(), ClientId(), RPC(butil::EndPoint()),
                           trace_manager, compactor);

  constexpr Ino kIno = 100;
  constexpr uint64_t kFh = 200;
  auto task =
      MDSMetaSystemTestPeer::PrepareRunningCommit(metasystem, kIno, kFh);
  auto ctx = std::make_shared<Context>("flush_barrier_test");

  auto flush = std::async(std::launch::async,
                          [&]() { return metasystem.Flush(ctx, kIno, kFh); });
  const auto waiter_deadline = std::chrono::steady_clock::now() + 1s;
  while (task->WaiterCount() == 0 &&
         std::chrono::steady_clock::now() < waiter_deadline) {
    std::this_thread::yield();
  }
  ASSERT_GT(task->WaiterCount(), 0);

  std::promise<void> write_started;
  auto write = std::async(std::launch::async, [&]() {
    write_started.set_value();
    return metasystem.WriteSlice(
        ctx, kIno, 1, kFh,
        {Slice{.id = 2, .size = 4096, .off = 0, .len = 4096, .pos = 0}});
  });
  write_started.get_future().wait();
  const bool write_was_blocked =
      write.wait_for(20ms) == std::future_status::timeout;

  auto chunk_set = MDSMetaSystemTestPeer::GetChunkSet(metasystem, kIno);
  ASSERT_NE(chunk_set, nullptr);
  if (!write_was_blocked) {
    EXPECT_TRUE(write.get().ok());
    chunk_set->TryCommitSlice(true);
    for (const auto& pending : chunk_set->ListCommitTask()) {
      if (pending != task) {
        MDSMetaSystemTestPeer::CompleteForCleanup(metasystem, kIno, pending);
      }
    }
  }

  MDSMetaSystemTestPeer::CompleteForCleanup(metasystem, kIno, task);
  ASSERT_EQ(flush.wait_for(1s), std::future_status::ready);
  EXPECT_TRUE(flush.get().ok());
  EXPECT_TRUE(write_was_blocked);

  if (write_was_blocked) {
    ASSERT_EQ(write.wait_for(1s), std::future_status::ready);
    EXPECT_TRUE(write.get().ok());
  }

  EXPECT_TRUE(chunk_set->ListCommitTask().empty());
  if (write_was_blocked) {
    EXPECT_TRUE(chunk_set->HasStage());
  }
}

}  // namespace test
}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs
