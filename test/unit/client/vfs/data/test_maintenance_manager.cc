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

#include <butil/time.h>
#include <gflags/gflags.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include "absl/hash/hash.h"
#include "client/vfs/components/maintenance_manager.h"
#include "client/vfs/data/reader/file_reader.h"
#include "client/vfs/data/reader/reader_registry.h"
#include "client/vfs/data/reader/reader_registry_task.h"
#include "client/vfs/data/writer/file_writer.h"
#include "client/vfs/data/writer_table.h"
#include "client/vfs/data/writer_table_task.h"
#include "client/vfs/data_buffer.h"
#include "common/options/client.h"
#include "common/readmempool/read_mem_pool.h"
#include "common/sync_point.h"
#include "test/unit/client/vfs/test_base.h"
#include "utils/executor/executor.h"
#include "utils/scoped_cleanup.h"

namespace dingofs {
namespace client {
namespace vfs {

using dingofs::client::vfs::test::VFSTestBase;
using ::testing::_;
using ::testing::DoAll;
using ::testing::Return;
using ::testing::SetArgPointee;

// White-box peer (the friend declaration in file_reader.h names this class):
// lets tests age cached requests deterministically and observe the request
// set shrinking after a maintenance tick instead of guessing timing.
class ReaderRegistryTaskTestPeer {
 public:
  static size_t RequestCount(FileReader* reader) {
    std::lock_guard<std::mutex> lock(reader->mutex_);
    return reader->requests_.size();
  }

  static int64_t RefCount(FileReader* reader) {
    return reader->refs_.load(std::memory_order_acquire);
  }

  // Pretend every cached request has been idle for an hour so ShrinkIfOpen's
  // reclaim conditions are met without sleeping.
  static void AgeAllRequests(FileReader* reader) {
    const int64_t old = butil::monotonic_time_s() - 3600;
    std::lock_guard<std::mutex> lock(reader->mutex_);
    for (auto& [id, req] : reader->requests_) {
      std::lock_guard<std::mutex> req_lock(req->mutex);
      req->access_sec = old;
    }
  }
};

namespace {

bool WaitFor(std::function<bool()> pred,
             std::chrono::milliseconds timeout = std::chrono::seconds(10)) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (pred()) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  return pred();
}

// Deterministically collect `count` inos that route to `shard` under the
// registries' 64-way sharding, starting the search at `base`.
std::vector<uint64_t> InosInShard(size_t shard, size_t count, uint64_t base) {
  std::vector<uint64_t> inos;
  for (uint64_t ino = base; inos.size() < count; ++ino) {
    if ((absl::HashOf(ino) & 63u) == shard) inos.push_back(ino);
  }
  CHECK_EQ(inos.size(), count);
  return inos;
}

// Executor double for Start-rejection tests: Schedule either rejects or
// queues into a deque the test executes by hand, so a "late" wakeup really
// runs against the drained control state.
class ManualScheduleExecutor final : public Executor {
 public:
  bool Start() override {
    std::lock_guard<std::mutex> lock(mutex_);
    running_ = true;
    return true;
  }
  bool Stop() override {
    std::lock_guard<std::mutex> lock(mutex_);
    running_ = false;
    return true;
  }
  bool Execute(std::function<void()> func) override {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!running_) return false;
    tasks_.push_back(std::move(func));
    return true;
  }
  bool Schedule(std::function<void()> func, int /*delay_ms*/) override {
    if (reject_schedule_) return false;
    return Execute(std::move(func));
  }
  int ThreadNum() const override { return 1; }
  int TaskNum() const override {
    std::lock_guard<std::mutex> lock(mutex_);
    return static_cast<int>(tasks_.size());
  }
  std::string Name() const override { return "manual_schedule"; }

  void SetRejectSchedule(bool reject) { reject_schedule_ = reject; }

  // Actually executes one queued closure (a "late" wakeup that really runs).
  void RunOne() {
    std::function<void()> task;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      CHECK(!tasks_.empty());
      task = std::move(tasks_.front());
      tasks_.pop_front();
    }
    task();
  }

 private:
  mutable std::mutex mutex_;
  bool running_{false};
  bool reject_schedule_{false};
  std::deque<std::function<void()>> tasks_;
};

}  // namespace

static std::unique_ptr<MaintenanceManager> MakeVfsMaintenance(
    ReaderRegistry* readers, WriterTable* writers, Executor* read_executor,
    Executor* write_executor, Executor* cleanup_executor, int interval_ms = 1) {
  auto manager = std::make_unique<MaintenanceManager>();
  CHECK(manager
            ->RegisterTask("reader-shrink",
                           std::make_shared<ReaderRegistryTask>(readers),
                           read_executor, interval_ms)
            .ok());
  CHECK(manager
            ->RegisterTask(
                "writer-flush",
                std::make_shared<WriterTableTask>(writers, cleanup_executor),
                write_executor, interval_ms)
            .ok());
  return manager;
}

class MaintenanceManagerTest : public VFSTestBase {
 protected:
  void SetUp() override {
    VFSTestBase::SetUp();

    maintenance_ = MakeVfsMaintenance(
        reader_registry_.get(), writer_table_.get(),
        read_cleanup_executor_.get(), write_background_executor_.get(),
        cleanup_executor_.get());
  }

  void TearDown() override {
    if (maintenance_) {
      maintenance_->StopAndDrain();
      maintenance_.reset();
    }
    VFSTestBase::TearDown();
  }

  std::unique_ptr<MaintenanceManager> maintenance_;
};

// Registration validates each task's interval without poisoning the manager.
TEST_F(MaintenanceManagerTest, RegistrationRejectsNonPositiveInterval) {
  MaintenanceManager manager;
  auto task = std::make_shared<ReaderRegistryTask>(reader_registry_.get());
  EXPECT_TRUE(
      manager.RegisterTask("zero", task, read_cleanup_executor_.get(), 0)
          .IsInvalidParam());
  EXPECT_TRUE(
      manager.RegisterTask("negative", task, read_cleanup_executor_.get(), -1)
          .IsInvalidParam());
  EXPECT_TRUE(
      manager.RegisterTask("valid", task, read_cleanup_executor_.get(), 1)
          .ok());
}

// A rejected FIRST wakeup submission is a startup failure, not a process
// abort: Start returns non-OK and the component rolls back without hanging.
TEST_F(MaintenanceManagerTest, StartFailsWhenFirstWakeupRejected) {
  gflags::FlagSaver flag_saver;

  ManualScheduleExecutor read_executor;
  ManualScheduleExecutor write_executor;
  ASSERT_TRUE(read_executor.Start());
  ASSERT_TRUE(write_executor.Start());
  read_executor.SetRejectSchedule(true);

  maintenance_ = MakeVfsMaintenance(reader_registry_.get(), writer_table_.get(),
                                    &read_executor, &write_executor,
                                    cleanup_executor_.get());

  Status s = maintenance_->Start();
  ASSERT_FALSE(s.ok());
  EXPECT_TRUE(s.IsInternal()) << s.ToString();

  // Rollback is clean: drain returns promptly, nothing was accepted.
  maintenance_->StopAndDrain();
  EXPECT_EQ(read_executor.TaskNum(), 0);
  EXPECT_EQ(write_executor.TaskNum(), 0);
  maintenance_.reset();
}

// When the SECOND wakeup submission is rejected, the already-accepted first
// timer must be neutralized by the rollback: after StopAndDrain the late
// wakeup REALLY RUNS (executed by hand below) and only observes the stopped
// state without touching any dependency.
TEST_F(MaintenanceManagerTest, StartRollsBackAcceptedWakeupWhenSecondRejected) {
  gflags::FlagSaver flag_saver;

  ManualScheduleExecutor read_executor;   // accepts, queues for manual run
  ManualScheduleExecutor write_executor;  // rejects
  ASSERT_TRUE(read_executor.Start());
  ASSERT_TRUE(write_executor.Start());
  write_executor.SetRejectSchedule(true);

  maintenance_ = MakeVfsMaintenance(reader_registry_.get(), writer_table_.get(),
                                    &read_executor, &write_executor,
                                    cleanup_executor_.get());

  Status s = maintenance_->Start();
  ASSERT_FALSE(s.ok());
  EXPECT_TRUE(s.IsInternal()) << s.ToString();

  // The reader wakeup was accepted before the failure; the rollback closed
  // admission. Execute it for real now (the "late timer" case): it must
  // observe stopped and return without touching the cleared dependencies
  // and without enqueuing anything.
  maintenance_.reset();
  ASSERT_EQ(read_executor.TaskNum(), 1);
  read_executor.RunOne();
  EXPECT_EQ(read_executor.TaskNum(), 0)
      << "the late wakeup must not re-arm or submit anything";

  EXPECT_EQ(reader_registry_->Size(), 0u);
  EXPECT_EQ(writer_table_->Size(), 0u);
}

TEST_F(MaintenanceManagerTest, LateWakeupsDoNotResurrectDestroyedOwner) {
  gflags::FlagSaver flag_saver;
  ManualScheduleExecutor read_executor;
  ManualScheduleExecutor write_executor;
  ASSERT_TRUE(read_executor.Start());
  ASSERT_TRUE(write_executor.Start());
  maintenance_ = MakeVfsMaintenance(reader_registry_.get(), writer_table_.get(),
                                    &read_executor, &write_executor,
                                    cleanup_executor_.get());
  ASSERT_TRUE(maintenance_->Start().ok());
  EXPECT_FALSE(maintenance_->Start().ok());
  maintenance_->StopAndDrain();
  EXPECT_FALSE(maintenance_->Start().ok());
  maintenance_.reset();

  ASSERT_EQ(read_executor.TaskNum(), 1);
  ASSERT_EQ(write_executor.TaskNum(), 1);
  read_executor.RunOne();
  write_executor.RunOne();
  EXPECT_EQ(read_executor.TaskNum(), 0);
  EXPECT_EQ(write_executor.TaskNum(), 0);
}

TEST_F(MaintenanceManagerTest, StopBeforeStartClosesAdmissionPermanently) {
  maintenance_->StopAndDrain();
  maintenance_->StopAndDrain();
  EXPECT_FALSE(maintenance_->Start().ok());
}

// The unified maintenance tick must flush dirty writers without any explicit
// Flush and without per-object periodic tasks.
TEST_F(MaintenanceManagerTest, TickFlushesDirtyWriterWithoutExplicitFlush) {
  gflags::FlagSaver flag_saver;

  std::mutex mutex;
  std::condition_variable cv;
  int write_slice_calls = 0;
  ON_CALL(*mock_meta_system_, WriteSlice)
      .WillByDefault([&](auto, auto, auto, auto, auto) {
        {
          std::lock_guard<std::mutex> lock(mutex);
          ++write_slice_calls;
        }
        cv.notify_all();
        return Status::OK();
      });

  FileWriter* writer = writer_table_->AcquireWriter(700);
  ASSERT_NE(writer, nullptr);
  const char buf[] = "dirty";
  uint64_t wsize = 0;
  ASSERT_TRUE(writer->Write(ctx_, buf, sizeof(buf), 0, &wsize).ok());

  ASSERT_TRUE(maintenance_->Start().ok());
  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(5),
                            [&] { return write_slice_calls >= 1; }));
  }
  // The tick flushed the dirty data; the writer is clean and error-free.
  EXPECT_TRUE(writer->GetStatus().ok());

  maintenance_->StopAndDrain();
  maintenance_.reset();

  writer_table_->ReleaseWriter(writer);
  EXPECT_EQ(writer_table_->Size(), 0u);
}

// A round must walk through empty shards and reach objects that do not live
// in shard 0. Pre-fix, an empty shard snapshot was rebuilt forever and the
// scan never advanced past it.
TEST_F(MaintenanceManagerTest, TickFlushesWriterBeyondShardZero) {
  gflags::FlagSaver flag_saver;

  const uint64_t ino = InosInShard(/*shard=*/7, /*count=*/1, /*base=*/900)[0];

  std::mutex mutex;
  std::condition_variable cv;
  int write_slice_calls = 0;
  ON_CALL(*mock_meta_system_, WriteSlice)
      .WillByDefault([&](auto, auto ino_called, auto, auto, auto) {
        {
          std::lock_guard<std::mutex> lock(mutex);
          if (ino_called == ino) ++write_slice_calls;
        }
        cv.notify_all();
        return Status::OK();
      });

  FileWriter* writer = writer_table_->AcquireWriter(ino);
  ASSERT_NE(writer, nullptr);
  const char buf[] = "dirty";
  uint64_t wsize = 0;
  ASSERT_TRUE(writer->Write(ctx_, buf, sizeof(buf), 0, &wsize).ok());

  ASSERT_TRUE(maintenance_->Start().ok());
  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(5), [&] {
      return write_slice_calls >= 1;
    })) << "scan must skip the empty shards 0..6 and reach shard 7";
  }
  EXPECT_TRUE(writer->GetStatus().ok());

  maintenance_->StopAndDrain();
  maintenance_.reset();

  writer_table_->ReleaseWriter(writer);
  EXPECT_EQ(writer_table_->Size(), 0u);
}

// The reader stream of the maintenance tick must reclaim cached read
// requests that satisfy the existing shrink conditions (aged out under
// read-pool pressure) and must skip closed readers safely. The reader lives
// beyond shard 0, covering empty-shard advance on the reader side too.
TEST_F(MaintenanceManagerTest, TickShrinksAgedReadaheadRequests) {
  gflags::FlagSaver flag_saver;
  // Suppress speculative readahead entirely: with the tiny pool below it
  // could not allocate anyway, and the aged-request reclaim must not depend
  // on it.
  FLAGS_vfs_read_mempool_readahead_watermark = 0.0;

  // A non-zero shard: the scan must pass the empty shards before it.
  const Ino kReaderIno =
      static_cast<Ino>(InosInShard(/*shard=*/9, /*count=*/1, /*base=*/950)[0]);
  const Attr attr = test::MakeFileAttr(kReaderIno, 4 * 1024 * 1024);
  ON_CALL(*mock_meta_system_, GetAttr(_, kReaderIno, _))
      .WillByDefault(DoAll(SetArgPointee<2>(attr), Return(Status::OK())));
  // One non-hole slice covering the whole file so reads consult the
  // (zero-filling) mock block store instead of zero-filling holes.
  ON_CALL(*mock_meta_system_, ReadSlice)
      .WillByDefault([](ContextSPtr, Ino, uint64_t, uint64_t,
                        std::vector<Slice>* slices, uint64_t& version) {
        slices->clear();
        Slice slice;
        slice.id = 1;
        slice.pos = 0;
        slice.size = 4 * 1024 * 1024;
        slice.off = 0;
        slice.len = slice.size;
        slices->push_back(slice);
        version = 1;
        return Status::OK();
      });

  FileReader* reader = new FileReader(mock_hub_, 11, kReaderIno);
  reader->AcquireRef();
  reader_registry_->Register(reader);
  auto cleanup = MakeScopedCleanup([&] {
    maintenance_->StopAndDrain();
    reader_registry_->Unregister(reader);
    reader->Close();
    reader->ReleaseRef();
  });

  auto read_4k = [&](int64_t offset) {
    DataBuffer buffer;
    uint64_t rsize = 0;
    EXPECT_TRUE(reader->Read(ctx_, &buffer, 4096, offset, &rsize).ok());
    EXPECT_EQ(rsize, 4096u);
  };
  read_4k(0);
  read_4k(4096);
  ASSERT_EQ(ReaderRegistryTaskTestPeer::RequestCount(reader), 2u)
      << "both reads must be served from cached requests";

  // Meet the shrink conditions deterministically: pool usage is high and the
  // cached requests count as idle.
  ReaderRegistryTaskTestPeer::AgeAllRequests(reader);

  ASSERT_TRUE(maintenance_->Start().ok());
  EXPECT_TRUE(WaitFor(
      [&] { return ReaderRegistryTaskTestPeer::RequestCount(reader) == 0; },
      std::chrono::seconds(5)))
      << "maintenance tick must reclaim the aged-out cached requests";

  // Close WITHOUT unregistering: the registered-but-closed reader is the
  // concurrent-Close race the maintenance snapshot must handle. Subsequent
  // ticks (and StopAndDrain's own step handoff) call ShrinkIfOpen on it;
  // the closing flag makes that a safe no-op. The scoped cleanup performs
  // the real Unregister afterwards.
  reader->Close();
  EXPECT_EQ(reader_registry_->Size(), 1u);
}

// A slow writer must not block periodic maintenance for the other writers,
// and the slow writer itself must never receive a second overlapping
// periodic flush while its first one is in flight.
TEST_F(MaintenanceManagerTest,
       SlowWriterDoesNotBlockOthersAndIsNotResubmitted) {
  gflags::FlagSaver flag_saver;

  constexpr uint64_t kSlowIno = 800;
  constexpr int kWriters = 8;

  std::mutex mutex;
  std::condition_variable cv;
  std::condition_variable gate_cv;
  bool slow_upload_entered = false;
  bool allow_slow_upload = false;
  std::unordered_map<uint64_t, int> writeslice_per_ino;
#ifndef NDEBUG
  std::unordered_map<uint64_t, int> periodic_calls;
#endif
  ON_CALL(*mock_block_store_, PutAsync)
      .WillByDefault([&](ContextSPtr, PutReq, StatusCallback cb) {
        {
          std::unique_lock<std::mutex> lock(mutex);
          // Gate exactly one upload (the "slow" writer, whichever flushed
          // first); every other upload completes inline.
          if (!slow_upload_entered) {
            slow_upload_entered = true;
            gate_cv.wait(lock, [&] { return allow_slow_upload; });
          }
        }
        cb(Status::OK());
      });
  ON_CALL(*mock_meta_system_, WriteSlice)
      .WillByDefault([&](auto, auto ino, auto, auto, auto) {
        std::lock_guard<std::mutex> lock(mutex);
        ++writeslice_per_ino[ino];
        cv.notify_all();
        return Status::OK();
      });

  std::vector<FileWriter*> writers;
  for (int i = 0; i < kWriters; ++i) {
    FileWriter* w = writer_table_->AcquireWriter(kSlowIno + i);
    ASSERT_NE(w, nullptr);
    const char buf[] = "dirty";
    uint64_t wsize = 0;
    ASSERT_TRUE(w->Write(ctx_, buf, sizeof(buf), 0, &wsize).ok());
    writers.push_back(w);
  }
  auto cleanup = MakeScopedCleanup([&] {
    {
      std::lock_guard<std::mutex> lock(mutex);
      allow_slow_upload = true;
    }
    gate_cv.notify_all();
    maintenance_->StopAndDrain();
#ifndef NDEBUG
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
#endif
    for (auto* w : writers) {
      EXPECT_TRUE(w->Flush().ok());
      writer_table_->ReleaseWriter(w);
    }
  });
#ifndef NDEBUG
  SyncPoint::GetInstance()->SetCallBack(
      "WriterTableTask:before_flush", [&](void* arg) {
        auto* writer = static_cast<FileWriter*>(arg);
        std::lock_guard<std::mutex> lock(mutex);
        ++periodic_calls[writer->Ino()];
      });
  SyncPoint::GetInstance()->EnableProcessing();
#endif

  ASSERT_TRUE(maintenance_->Start().ok());

  // While the slow upload is blocked, every other writer must still get its
  // periodic flush.
  EXPECT_TRUE(WaitFor(
      [&] {
        std::lock_guard<std::mutex> lock(mutex);
        int flushed = 0;
        for (auto& [ino, n] : writeslice_per_ino) flushed += (n > 0) ? 1 : 0;
        return flushed >= kWriters - 1;
      },
      std::chrono::seconds(5)))
      << "a slow writer must not block maintenance for the other writers";

  {
    std::lock_guard<std::mutex> lock(mutex);
    for (auto& [ino, n] : writeslice_per_ino) {
      EXPECT_LE(n, 1) << "writer " << ino << " submitted " << n << " flushes";
    }
  }

#ifndef NDEBUG
  // A third visit to any fast writer proves another complete scan has
  // passed the still-blocked writer, not merely that its commit is pending.
  EXPECT_TRUE(WaitFor([&] {
    std::lock_guard<std::mutex> lock(mutex);
    for (const auto& [ino, count] : periodic_calls) {
      if (count >= 3) return true;
    }
    return false;
  }));
  {
    std::lock_guard<std::mutex> lock(mutex);
    for (auto* writer : writers) {
      if (writeslice_per_ino.count(writer->Ino()) == 0) {
        EXPECT_EQ(periodic_calls[writer->Ino()], 1);
      }
    }
  }
#endif

  // Release the slow upload; the last writer completes too.
  {
    std::lock_guard<std::mutex> lock(mutex);
    allow_slow_upload = true;
  }
  gate_cv.notify_all();

  EXPECT_TRUE(WaitFor(
      [&] {
        std::lock_guard<std::mutex> lock(mutex);
        int flushed = 0;
        for (auto& [ino, n] : writeslice_per_ino) flushed += (n > 0) ? 1 : 0;
        return flushed == kWriters;
      },
      std::chrono::seconds(5)));

  // The original dirty slices are committed once. Later clean maintenance
  // visits must not manufacture duplicate slice commits.
  {
    std::lock_guard<std::mutex> lock(mutex);
    EXPECT_EQ(writeslice_per_ino.size(), static_cast<size_t>(kWriters));
    for (auto& [ino, n] : writeslice_per_ino) {
      EXPECT_EQ(n, 1) << "writer " << ino << " must not be flushed twice";
    }
  }
}

// Stop must not wait for an armed-but-unfired wakeup: StopAndDrain returns
// immediately, and the later executor teardown destroys the pending closure
// (which holds only shared control state) without running it.
TEST_F(MaintenanceManagerTest, StopWithArmedWakeupReturnsImmediately) {
  gflags::FlagSaver flag_saver;
  maintenance_ = MakeVfsMaintenance(reader_registry_.get(), writer_table_.get(),
                                    read_cleanup_executor_.get(),
                                    write_background_executor_.get(),
                                    cleanup_executor_.get(), 60 * 60 * 1000);

  ASSERT_TRUE(maintenance_->Start().ok());

  const auto begin = std::chrono::steady_clock::now();
  maintenance_->StopAndDrain();
  const auto elapsed = std::chrono::steady_clock::now() - begin;
  EXPECT_LT(elapsed, std::chrono::seconds(1))
      << "StopAndDrain must not wait for the pending hour-long wakeup";

  maintenance_.reset();
  // Fixture teardown stops the executors (destroying the pending timer
  // closures) and destroys the registry/table afterwards; any use-after-free
  // here fails under ASAN/TSAN.
}

// Stop during an in-flight periodic flush must wait for the flush callback
// AND the actual holder release on the cleanup executor -- not just for the
// callback to return.
TEST_F(MaintenanceManagerTest, StopWaitsForInflightFlushHolderRelease) {
  gflags::FlagSaver flag_saver;

  std::mutex mutex;
  std::condition_variable cv;
  std::condition_variable gate_cv;
  bool upload_entered = false;
  bool allow_upload = false;
  ON_CALL(*mock_block_store_, PutAsync)
      .WillByDefault([&](ContextSPtr, PutReq, StatusCallback cb) {
        {
          std::unique_lock<std::mutex> lock(mutex);
          upload_entered = true;
          cv.notify_all();
          gate_cv.wait(lock, [&] { return allow_upload; });
        }
        cb(Status::OK());
      });

  FileWriter* writer = writer_table_->AcquireWriter(820);
  ASSERT_NE(writer, nullptr);
  const char buf[] = "dirty";
  uint64_t wsize = 0;
  ASSERT_TRUE(writer->Write(ctx_, buf, sizeof(buf), 0, &wsize).ok());

  ASSERT_TRUE(maintenance_->Start().ok());
  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(5),
                            [&] { return upload_entered; }));
  }

  // Drop the external holder: the maintenance's snapshot holder is the only
  // thing keeping the entry alive now.
  writer_table_->ReleaseWriter(writer);
  EXPECT_EQ(writer_table_->Size(), 1u);

  auto stopped = std::async(std::launch::async, [&] {
    maintenance_->StopAndDrain();
    return true;
  });
  EXPECT_EQ(stopped.wait_for(std::chrono::milliseconds(100)),
            std::future_status::timeout)
      << "StopAndDrain must wait for the in-flight flush and its cleanup";

  {
    std::lock_guard<std::mutex> lock(mutex);
    allow_upload = true;
  }
  gate_cv.notify_all();

  ASSERT_EQ(stopped.wait_for(std::chrono::seconds(5)),
            std::future_status::ready);
  EXPECT_TRUE(stopped.get());

  // The cleanup executor actually released the last holder: the entry is
  // gone and the writer was closed by the maintenance stream.
  EXPECT_EQ(writer_table_->Size(), 0u);
}

// When every maintenance slot is busy mid-shard, the scan pauses. The
// unconsumed tail (the 65th writer of the SAME shard snapshot) must be
// cancelled by Stop -- handed to ONE cleanup task, not flushed, not
// double-released: after the drain, every external holder still evicts its
// entry exactly once.
TEST_F(MaintenanceManagerTest, StopWhilePausedCancelsTailWithoutDoubleRelease) {
#ifdef NDEBUG
  GTEST_SKIP() << "Deterministic pause staging requires TEST_SYNC_POINT.";
#else
  gflags::FlagSaver flag_saver;

  constexpr int kMaintenanceSlots = 64;
  constexpr int kWriters = kMaintenanceSlots + 1;

  std::mutex mutex;
  std::condition_variable cv;
  std::condition_variable gate_cv;
  bool allow_uploads = false;
  int put_async_entered = 0;
  int write_slice_calls = 0;
  bool scan_paused = false;
  ON_CALL(*mock_block_store_, PutAsync)
      .WillByDefault([&](ContextSPtr, PutReq, StatusCallback cb) {
        {
          std::unique_lock<std::mutex> lock(mutex);
          ++put_async_entered;
          cv.notify_all();
          gate_cv.wait(lock, [&] { return allow_uploads; });
        }
        cb(Status::OK());
      });
  ON_CALL(*mock_meta_system_, WriteSlice)
      .WillByDefault([&](auto, auto, auto, auto, auto) {
        std::lock_guard<std::mutex> lock(mutex);
        ++write_slice_calls;
        cv.notify_all();
        return Status::OK();
      });

  SyncPoint::GetInstance()->SetCallBack("WriterTableTask:paused", [&](void*) {
    std::lock_guard<std::mutex> lock(mutex);
    scan_paused = true;
    cv.notify_all();
  });
  SyncPoint::GetInstance()->EnableProcessing();
  auto disable_syncpoint = MakeScopedCleanup([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });

  // All writers in ONE shard: batch 1 consumes 64, the 65th stays in the
  // same snapshot as the unconsumed tail.
  const auto inos = InosInShard(/*shard=*/5, kWriters, /*base=*/1000);
  std::vector<FileWriter*> writers;
  for (uint64_t ino : inos) {
    FileWriter* w = writer_table_->AcquireWriter(ino);
    ASSERT_NE(w, nullptr);
    const char buf[] = "dirty";
    uint64_t wsize = 0;
    ASSERT_TRUE(w->Write(ctx_, buf, sizeof(buf), 0, &wsize).ok());
    writers.push_back(w);
  }
  ASSERT_TRUE(maintenance_->Start().ok());
  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(10), [&] {
      return scan_paused && put_async_entered >= 1;
    })) << "scan must pause once all maintenance slots are busy";
  }

  auto stopped = std::async(std::launch::async, [&] {
    maintenance_->StopAndDrain();
    return true;
  });
  EXPECT_EQ(stopped.wait_for(std::chrono::milliseconds(100)),
            std::future_status::timeout)
      << "StopAndDrain still owes the in-flight member cleanups";

  {
    std::lock_guard<std::mutex> lock(mutex);
    allow_uploads = true;
  }
  gate_cv.notify_all();

  ASSERT_EQ(stopped.wait_for(std::chrono::seconds(10)),
            std::future_status::ready);
  EXPECT_TRUE(stopped.get());

  {
    std::lock_guard<std::mutex> lock(mutex);
    // Exactly the slot-count worth of members were submitted and flushed;
    // the paused tail was cancelled, not flushed by maintenance.
    EXPECT_EQ(write_slice_calls, kMaintenanceSlots);
  }

  // Every pin was returned exactly once: the 64 consumed members' holders
  // by their cleanup tasks, the tail holder by the cancel task. Releasing
  // the external holders now must evict each entry cleanly -- a double
  // release of any snapshot entry would have CHECK-failed or left the
  // accounting broken (and Flush of an already-closed writer would fail).
  for (size_t i = 0; i < writers.size(); ++i) {
    ASSERT_TRUE(writers[i]->Flush().ok()) << "writer " << inos[i];
  }
  EXPECT_EQ(writer_table_->Size(), static_cast<size_t>(kWriters));
  for (auto* w : writers) {
    writer_table_->ReleaseWriter(w);
  }
  EXPECT_EQ(writer_table_->Size(), 0u)
      << "every entry must evict exactly once after the drain";
#endif
}

// The pause/resume boundary must not lose wakeups: releasing the gated
// uploads AT the pause point (the sync callback runs right after the atomic
// pause retreat) must let the returned slots resume the scan, and the 65th
// object of the shard must still be processed.
TEST_F(MaintenanceManagerTest, PausedScanResumesWhenSlotsReturn) {
#ifdef NDEBUG
  GTEST_SKIP() << "Deterministic pause staging requires TEST_SYNC_POINT.";
#else
  gflags::FlagSaver flag_saver;

  constexpr int kMaintenanceSlots = 64;
  constexpr int kWriters = kMaintenanceSlots + 1;

  std::mutex mutex;
  std::condition_variable cv;
  std::condition_variable gate_cv;
  bool allow_uploads = false;
  int put_async_entered = 0;
  std::unordered_map<uint64_t, int> writeslice_per_ino;
  ON_CALL(*mock_block_store_, PutAsync)
      .WillByDefault([&](ContextSPtr, PutReq, StatusCallback cb) {
        {
          std::unique_lock<std::mutex> lock(mutex);
          ++put_async_entered;
          cv.notify_all();
          gate_cv.wait(lock, [&] { return allow_uploads; });
        }
        cb(Status::OK());
      });
  ON_CALL(*mock_meta_system_, WriteSlice)
      .WillByDefault([&](auto, auto ino, auto, auto, auto) {
        std::lock_guard<std::mutex> lock(mutex);
        ++writeslice_per_ino[ino];
        cv.notify_all();
        return Status::OK();
      });

  const auto inos = InosInShard(/*shard=*/6, kWriters, /*base=*/2000);
  std::vector<FileWriter*> writers;
  for (uint64_t ino : inos) {
    FileWriter* w = writer_table_->AcquireWriter(ino);
    ASSERT_NE(w, nullptr);
    const char buf[] = "dirty";
    uint64_t wsize = 0;
    ASSERT_TRUE(w->Write(ctx_, buf, sizeof(buf), 0, &wsize).ok());
    writers.push_back(w);
  }
  auto cleanup = MakeScopedCleanup([&] {
    {
      std::lock_guard<std::mutex> lock(mutex);
      allow_uploads = true;
    }
    gate_cv.notify_all();
    maintenance_->StopAndDrain();
    for (auto* w : writers) {
      EXPECT_TRUE(w->Flush().ok());
      writer_table_->ReleaseWriter(w);
    }
  });

  // Release the upload gate exactly when the scan has committed its atomic
  // pause: the returning slots race the pause as tightly as possible.
  SyncPoint::GetInstance()->SetCallBack("WriterTableTask:paused", [&](void*) {
    std::lock_guard<std::mutex> lock(mutex);
    allow_uploads = true;
    gate_cv.notify_all();
  });
  SyncPoint::GetInstance()->EnableProcessing();
  auto disable_syncpoint = MakeScopedCleanup([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });

  ASSERT_TRUE(maintenance_->Start().ok());
  gate_cv.notify_all();  // wake any upload that entered before the gate opened

  // The paused scan must resume from its position: ALL kWriters (including
  // the 65th tail object) get flushed, each exactly once.
  EXPECT_TRUE(WaitFor(
      [&] {
        std::lock_guard<std::mutex> lock(mutex);
        return writeslice_per_ino.size() == kWriters;
      },
      std::chrono::seconds(10)))
      << "the tail object must still be processed after the pause";
  {
    std::lock_guard<std::mutex> lock(mutex);
    for (auto& [ino, n] : writeslice_per_ino) {
      EXPECT_EQ(n, 1) << "writer " << ino << " must not be flushed twice";
    }
  }

  maintenance_->StopAndDrain();
#endif
}

// Reader-side tail accounting: stopping mid-shard (the step is gated between
// two batches) must cancel only the unconsumed tail. The 64 readers of the
// consumed prefix were already released; releasing them again would
// underflow their refcounts. Every reader must still be alive with exactly
// its owner reference after the drain.
TEST_F(MaintenanceManagerTest, StopMidShardReleasesOnlyUnconsumedReaderTail) {
#ifdef NDEBUG
  GTEST_SKIP() << "Deterministic mid-shard staging requires TEST_SYNC_POINT.";
#else
  gflags::FlagSaver flag_saver;

  constexpr int kMaintenanceSlots = 64;
  constexpr int kReaders = kMaintenanceSlots + 1;

  // Gate the FIRST reader batch: the step blocks right after processing 64
  // readers, with the 65th still unconsumed in the same shard snapshot.
  std::mutex gate_mutex;
  std::condition_variable gate_cv;
  bool batch_done = false;
  bool allow_step = false;
  SyncPoint::GetInstance()->SetCallBack(
      "ReaderRegistryTask:after_batch", [&](void*) {
        std::unique_lock<std::mutex> lock(gate_mutex);
        if (!batch_done) {
          batch_done = true;
          gate_cv.notify_all();
          gate_cv.wait(lock, [&] { return allow_step; });
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  auto disable_syncpoint = MakeScopedCleanup([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });

  const auto inos = InosInShard(/*shard=*/11, kReaders, /*base=*/3000);
  std::vector<FileReader*> readers;
  for (uint64_t ino : inos) {
    auto* reader = new FileReader(mock_hub_, /*fh=*/ino, ino);
    reader->AcquireRef();
    reader_registry_->Register(reader);
    readers.push_back(reader);
  }
  auto cleanup = MakeScopedCleanup([&] {
    for (auto* reader : readers) {
      reader_registry_->Unregister(reader);
      reader->Close();
      reader->ReleaseRef();
    }
  });

  ASSERT_TRUE(maintenance_->Start().ok());
  {
    std::unique_lock<std::mutex> lock(gate_mutex);
    ASSERT_TRUE(gate_cv.wait_for(lock, std::chrono::seconds(10),
                                 [&] { return batch_done; }));
  }

  // Stop while the step is parked mid-shard: it owes the tail cancellation.
  auto stopped = std::async(std::launch::async, [&] {
    maintenance_->StopAndDrain();
    return true;
  });
  EXPECT_EQ(stopped.wait_for(std::chrono::milliseconds(100)),
            std::future_status::timeout)
      << "StopAndDrain must wait for the parked step";

  {
    std::lock_guard<std::mutex> lock(gate_mutex);
    allow_step = true;
  }
  gate_cv.notify_all();

  ASSERT_EQ(stopped.wait_for(std::chrono::seconds(5)),
            std::future_status::ready);
  EXPECT_TRUE(stopped.get());

  // Every reader must hold EXACTLY its owner reference: the 64 consumed
  // prefix refs were released once by the scan, the tail ref once by the
  // cancel. A double release would have CHECK-failed in ReleaseRef (or
  // destroyed the object, making this refcount read crash).
  for (size_t i = 0; i < readers.size(); ++i) {
    EXPECT_EQ(ReaderRegistryTaskTestPeer::RefCount(readers[i]), 1)
        << "reader " << inos[i]
        << " must hold exactly its owner reference after the drain";
  }
#endif
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
