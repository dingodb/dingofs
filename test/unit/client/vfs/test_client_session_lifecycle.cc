/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#include <bvar/variable.h>
#include <fcntl.h>
#include <gflags/gflags.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <spdlog/sinks/base_sink.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>

#include "client/vfs/access_log.h"
#include "client/vfs/client_session.h"
#include "client/vfs/vfs_impl.h"
#include "common/metrics/client/client.h"
#include "common/options/client.h"
#include "common/trace/trace_manager.h"
#include "test/unit/client/vfs/test_base.h"
#include "utils/scoped_cleanup.h"

DECLARE_string(log_dir);
namespace dingofs {
namespace client {

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

namespace {
constexpr auto kWaitLimit = std::chrono::seconds(5);

class Event {
 public:
  void Signal() {
    std::lock_guard<std::mutex> lock(mutex_);
    ready_ = true;
    cv_.notify_all();
  }

  bool Wait() {
    std::unique_lock<std::mutex> lock(mutex_);
    return cv_.wait_for(lock, kWaitLimit, [this] { return ready_; });
  }

 private:
  std::mutex mutex_;
  std::condition_variable cv_;
  bool ready_{false};
};

// Every staged call has a cleanup guard that releases its pause and joins it,
// including on an ASSERT failure. A timeout fails the test, never forces Stop.
class Pause {
 public:
  void Block() {
    if (visited_.exchange(true)) return;
    entered.Signal();
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this] { return released_; });
  }

  void Release() {
    std::lock_guard<std::mutex> lock(mutex_);
    released_ = true;
    cv_.notify_all();
  }

  Event entered;

 private:
  std::atomic<bool> visited_{false};
  std::mutex mutex_;
  std::condition_variable cv_;
  bool released_{false};
};

int64_t Metric(const char* name) {
  const std::string value = bvar::Variable::describe_exposed(name);
  EXPECT_FALSE(value.empty()) << name;
  return value.empty() ? 0 : std::stoll(value);
}

int64_t ActiveOperations() { return Metric("vfs_active_public_operations"); }

int64_t RejectedOperations() {
  return Metric("vfs_rejected_public_operations_total");
}

class PausingLogSink final : public spdlog::sinks::base_sink<std::mutex> {
 public:
  Pause pause;
  std::atomic<bool> completed{false};

 private:
  void sink_it_(const spdlog::details::log_msg&) override {
    pause.Block();
    completed = true;
  }
  void flush_() override {}
};

// Reuse the component fixture's real handles, readers, and executors. The
// mocked hub and metadata only expose the existing dependency boundaries.
class RealVFSResources : public vfs::test::VFSTestBase {
 public:
  void TestBody() override {}

  std::unique_ptr<vfs::VFSHub> TakeHub() { return std::move(hub_uptr_); }
  vfs::test::MockVFSHub& Hub() { return *mock_hub_; }
  vfs::test::MockMetaSystem& Meta() { return *mock_meta_system_; }
  vfs::HandleManager& Handles() { return *handle_manager_; }
  vfs::ReaderRegistry* Readers() { return reader_registry_.get(); }

  void StopResources() {
    EXPECT_TRUE(handle_manager_->Stop().ok());
    writer_table_->Stop();
    write_background_executor_->Stop();
    flush_executor_->Stop();
    read_executor_->Stop();
    read_cleanup_executor_->Stop();
    cb_executor_->Stop();
  }
};

class MockLifecycleVFS : public vfs::VFS {
 public:
  MOCK_METHOD(Status, Start, (bool), (override));
  MOCK_METHOD(Status, Stop, (bool), (override));
  MOCK_METHOD(bool, Dump, (ContextSPtr, Json::Value&), (override));
  MOCK_METHOD(bool, Load, (ContextSPtr, const Json::Value&), (override));
  MOCK_METHOD(Status, Lookup, (ContextSPtr, Ino, const std::string&, Attr*),
              (override));
  MOCK_METHOD(Status, GetAttr, (ContextSPtr, Ino, Attr*), (override));
  MOCK_METHOD(Status, SetAttr, (ContextSPtr, Ino, int, const Attr&, Attr*),
              (override));
  MOCK_METHOD(Status, Fallocate, (ContextSPtr, Ino, int, uint64_t, uint64_t),
              (override));
  MOCK_METHOD(Status, CopyFileRange,
              (ContextSPtr, Ino, uint64_t, uint64_t, Ino, uint64_t, uint64_t,
               uint64_t, uint32_t, uint64_t*),
              (override));
  MOCK_METHOD(Status, ReadLink, (ContextSPtr, Ino, std::string*), (override));
  MOCK_METHOD(Status, MkNod,
              (ContextSPtr, Ino, const std::string&, uint32_t, uint32_t,
               uint32_t, uint64_t, Attr*),
              (override));
  MOCK_METHOD(Status, Unlink, (ContextSPtr, Ino, const std::string&),
              (override));
  MOCK_METHOD(Status, Symlink,
              (ContextSPtr, Ino, const std::string&, uint32_t, uint32_t,
               const std::string&, Attr*),
              (override));
  MOCK_METHOD(Status, Rename,
              (ContextSPtr, Ino, const std::string&, Ino, const std::string&),
              (override));
  MOCK_METHOD(Status, Link, (ContextSPtr, Ino, Ino, const std::string&, Attr*),
              (override));
  MOCK_METHOD(Status, Open, (ContextSPtr, Ino, int, uint64_t*, bool*),
              (override));
  MOCK_METHOD(Status, Create,
              (ContextSPtr, Ino, const std::string&, uint32_t, uint32_t,
               uint32_t, int, uint64_t*, Attr*),
              (override));
  MOCK_METHOD(Status, Read,
              (ContextSPtr, Ino, DataBuffer*, uint64_t, uint64_t, uint64_t,
               uint64_t*),
              (override));
  MOCK_METHOD(Status, Write,
              (ContextSPtr, Ino, const char*, uint64_t, uint64_t, uint64_t,
               uint64_t*),
              (override));
  MOCK_METHOD(Status, Flush, (ContextSPtr, Ino, uint64_t), (override));
  MOCK_METHOD(Status, Release, (ContextSPtr, Ino, uint64_t), (override));
  MOCK_METHOD(Status, Fsync, (ContextSPtr, Ino, int, uint64_t), (override));
  MOCK_METHOD(Status, SetXattr,
              (ContextSPtr, Ino, const std::string&, const std::string&, int),
              (override));
  MOCK_METHOD(Status, GetXattr,
              (ContextSPtr, Ino, const std::string&, std::string*), (override));
  MOCK_METHOD(Status, RemoveXattr, (ContextSPtr, Ino, const std::string&),
              (override));
  MOCK_METHOD(Status, ListXattr, (ContextSPtr, Ino, std::vector<std::string>*),
              (override));
  MOCK_METHOD(Status, MkDir,
              (ContextSPtr, Ino, const std::string&, uint32_t, uint32_t,
               uint32_t, Attr*),
              (override));
  MOCK_METHOD(Status, OpenDir, (ContextSPtr, Ino, uint64_t*, bool&),
              (override));
  MOCK_METHOD(Status, ReadDir,
              (ContextSPtr, Ino, uint64_t, uint64_t, bool, ReadDirHandler,
               uint32_t&),
              (override));
  MOCK_METHOD(Status, ReleaseDir, (ContextSPtr, Ino, uint64_t), (override));
  MOCK_METHOD(Status, RmDir, (ContextSPtr, Ino, const std::string&),
              (override));
  MOCK_METHOD(Status, StatFs, (ContextSPtr, Ino, FsStat*), (override));
  MOCK_METHOD(Status, Ioctl,
              (ContextSPtr, Ino, uint32_t, unsigned int, unsigned, const void*,
               size_t, char*, size_t),
              (override));
  MOCK_METHOD(Status, GetInfo, (std::string*), (override));
};

}  // namespace

class ClientSessionLifecycleTest : public ::testing::Test {
 protected:
  void SetUp() override {
    FLAGS_vfs_access_logging = false;
    FLAGS_fuse_attr_cache_timeout_s = 1;
    FLAGS_fuse_entry_cache_timeout_s = 2;
    FLAGS_vfs_meta_max_name_length = 255;
    session_ = std::make_unique<ClientSession>();
    session_->trace_manager_ = std::make_unique<TraceManager>();
    auto core = std::make_unique<MockLifecycleVFS>();
    core_ = core.get();
    session_->vfs_ = std::move(core);
    session_->lifecycle_state_ = ClientSession::LifecycleState::kRunning;
    session_->operations_.OpenOnce();
  }

  void TearDown() override {
    if (session_ != nullptr) {
      // Test-body captures are already gone. Verify unmet expectations before
      // teardown, then discard actions so a failed staging assertion cannot
      // invoke callbacks borrowing those expired locals.
      if (core_ != nullptr) {
        ::testing::Mock::VerifyAndClearExpectations(core_);
      }
      if (resources_ != nullptr) {
        ::testing::Mock::VerifyAndClearExpectations(&resources_->Hub());
        ::testing::Mock::VerifyAndClearExpectations(&resources_->Meta());
      }
      session_->Stop();
      if (resources_ != nullptr) resources_->StopResources();
      session_.reset();
    }
    resources_.reset();
  }

  void SetTraceStarted(bool started) { session_->trace_started_ = started; }

  bool TraceStarted() const { return session_->trace_started_; }

  void InitializeMetrics() {
    session_->client_metrics_ =
        std::make_unique<metrics::client::ClientOpMetric>();
  }

  void RunLocalStart() {
    char temp[] = "/tmp/dingofs-session-start-XXXXXX";
    ASSERT_NE(mkdtemp(temp), nullptr);
    const char* old_base = std::getenv("DINGOFS_BASE_DIR");
    const std::optional<std::string> previous_base =
        old_base == nullptr ? std::nullopt
                            : std::make_optional<std::string>(old_base);
    auto restore = MakeScopedCleanup([&] {
      session_.reset();
      if (previous_base) {
        setenv("DINGOFS_BASE_DIR", previous_base->c_str(), 1);
      } else {
        unsetenv("DINGOFS_BASE_DIR");
      }
      std::filesystem::remove_all(temp);
    });
    ASSERT_EQ(setenv("DINGOFS_BASE_DIR", temp, 1), 0);
    FLAGS_log_dir = temp;
    FLAGS_enable_trace = false;
    FLAGS_vfs_bthread_worker_num = 0;
    FLAGS_vfs_dummy_server_port = 0;
    FLAGS_vfs_write_buffer_total_mb = 64;
    FLAGS_vfs_write_buffer_page_size = 4096;
    FLAGS_vfs_read_buffer_total_mb = 64;
    FLAGS_vfs_compact_buffer_total_mb = 64;
    // Existing IO-isolation mode avoids starting an unrelated cache stack.
    // Metadata, VFSHub, VFSImpl, and ClientSession Start/Stop remain real.
    FLAGS_vfs_use_fake_block_store = true;
    FLAGS_vfs_meta_access_logging = false;
    DingofsConfig config;
    config.fs_name = "session-start";
    config.mount_point = std::string(temp) + "/mount";
    config.metasystem_type = "local";
    config.storage_info = std::string("storage=file&path=") + temp + "/blocks";

    const auto status = session_->Start(config);
    ASSERT_TRUE(status.ok()) << status.ToString();
    Attr attr;
    EXPECT_TRUE(session_->GetAttr(Context{0, 0, 0, 0}, kRootIno, &attr).ok());
    EXPECT_EQ(attr.ino, kRootIno);
    EXPECT_TRUE(session_->Stop().ok());
    EXPECT_TRUE(
        session_->GetAttr(Context{0, 0, 0, 0}, kRootIno, &attr).IsStop());
  }

  auto AcquireOperation() { return session_->TryAcquireOperation(); }

  bool WaitForAdmissionClosed() {
    // A rejected public call observes closure; it does not identify the
    // stopping thread's position inside its drain wait.
    const auto deadline = std::chrono::steady_clock::now() + kWaitLimit;
    std::string info;
    do {
      if (session_->GetInfo(&info).IsStop()) return true;
      std::this_thread::yield();
    } while (std::chrono::steady_clock::now() < deadline);
    return false;
  }

  void ResetCreatedSession(bool install_core = false) {
    EXPECT_CALL(*core_, Stop(false)).WillOnce(Return(Status::OK()));
    session_.reset();
    session_ = std::make_unique<ClientSession>();
    core_ = nullptr;
    if (install_core) {
      session_->trace_manager_ = std::make_unique<TraceManager>();
      auto core = std::make_unique<MockLifecycleVFS>();
      core_ = core.get();
      session_->vfs_ = std::move(core);
    }
  }

  RealVFSResources& UseRealVFS() {
    InitializeMetrics();
    resources_ = std::make_unique<RealVFSResources>();
    ON_CALL(resources_->Hub(), GetTraceManager())
        .WillByDefault(Return(session_->trace_manager_.get()));
    EXPECT_CALL(resources_->Hub(), GetTraceManager())
        .Times(::testing::AnyNumber());
    EXPECT_CALL(resources_->Hub(), GetBlockAccesserOptions())
        .Times(::testing::AnyNumber())
        .WillRepeatedly(Return(blockaccess::BlockAccessOptions{}));
    session_->vfs_.reset(
        new vfs::VFSImpl(resources_->TakeHub(), *session_->trace_manager_));
    core_ = nullptr;
    return *resources_;
  }

  gflags::FlagSaver flag_saver_;
  std::unique_ptr<RealVFSResources> resources_;
  std::unique_ptr<ClientSession> session_;
  MockLifecycleVFS* core_{nullptr};
};

TEST_F(ClientSessionLifecycleTest, StopWaitsForAdmittedOperation) {
  Pause admitted;
  std::atomic<bool> business_completed{false};
  std::atomic<bool> core_stopped{false};
  std::string operation_info;
  const auto business_status = Status::Internal("operation failed");
  EXPECT_CALL(*core_, GetInfo(_)).WillRepeatedly(Return(Status::OK()));
  EXPECT_CALL(*core_, GetInfo(&operation_info))
      .WillOnce(Invoke([&](std::string*) {
        admitted.Block();
        EXPECT_FALSE(core_stopped.load());
        business_completed = true;
        return business_status;
      }));
  EXPECT_CALL(*core_, Stop(false)).WillOnce(Invoke([&](bool) {
    EXPECT_TRUE(business_completed.load());
    core_stopped = true;
    return Status::OK();
  }));

  auto operation = std::async(
      std::launch::async, [&] { return session_->GetInfo(&operation_info); });
  auto join = MakeScopedCleanup([&] {
    admitted.Release();
    if (operation.valid()) operation.wait();
  });
  ASSERT_TRUE(admitted.entered.Wait());
  auto stop_future =
      std::async(std::launch::async, [&] { return session_->Stop(false); });
  auto release_stop = MakeScopedCleanup([&] { admitted.Release(); });
  ASSERT_TRUE(WaitForAdmissionClosed());
  EXPECT_FALSE(core_stopped.load());
  admitted.Release();
  EXPECT_EQ(operation.get().ToString(), business_status.ToString());
  EXPECT_TRUE(stop_future.get().ok());
  EXPECT_TRUE(core_stopped.load());
}

TEST_F(ClientSessionLifecycleTest, ConcurrentStopRunsCoreStopOnce) {
  Pause teardown;
  std::atomic<bool> teardown_completed{false};
  EXPECT_CALL(*core_, Stop(false)).WillOnce(Invoke([&](bool) {
    teardown.Block();
    teardown_completed = true;
    return Status::OK();
  }));
  auto first =
      std::async(std::launch::async, [&] { return session_->Stop(false); });
  auto join = MakeScopedCleanup([&] {
    teardown.Release();
    if (first.valid()) first.wait();
  });
  ASSERT_TRUE(teardown.entered.Wait());
  // Rendezvous only starts the competing task; the public result and once-only
  // expectation are checked without claiming that it entered the wait branch.
  Event second_called;
  auto second = std::async(std::launch::async, [&] {
    second_called.Signal();
    auto status = session_->Stop(false);
    EXPECT_TRUE(teardown_completed.load());
    return status;
  });
  auto release_second = MakeScopedCleanup([&] { teardown.Release(); });
  ASSERT_TRUE(second_called.Wait());
  teardown.Release();
  EXPECT_TRUE(first.get().ok());
  EXPECT_TRUE(second.get().ok());
}
TEST_F(ClientSessionLifecycleTest, TraceOutlivesCoreStop) {
  SetTraceStarted(true);
  EXPECT_CALL(*core_, Stop(false)).WillOnce(Invoke([&](bool) {
    EXPECT_TRUE(TraceStarted());
    return Status::OK();
  }));

  EXPECT_TRUE(session_->Stop(false).ok());
  EXPECT_FALSE(TraceStarted());
}

TEST_F(ClientSessionLifecycleTest, DestructorStopsRunningCore) {
  EXPECT_CALL(*core_, Stop(false)).WillOnce(Return(Status::OK()));
  session_.reset();
}

TEST(ClientSessionOptionTest, GettersReadFlagsWithoutSuccessfulStart) {
  const uint32_t previous_attr_timeout = FLAGS_fuse_attr_cache_timeout_s;
  const uint32_t previous_entry_timeout = FLAGS_fuse_entry_cache_timeout_s;
  const uint32_t previous_max_name_length = FLAGS_vfs_meta_max_name_length;
  auto restore_flags = MakeScopedCleanup([&]() {
    FLAGS_fuse_attr_cache_timeout_s = previous_attr_timeout;
    FLAGS_fuse_entry_cache_timeout_s = previous_entry_timeout;
    FLAGS_vfs_meta_max_name_length = previous_max_name_length;
  });

  FLAGS_fuse_attr_cache_timeout_s = 3;
  FLAGS_fuse_entry_cache_timeout_s = 4;
  FLAGS_vfs_meta_max_name_length = 5;

  ClientSession session;
  EXPECT_DOUBLE_EQ(session.GetAttrTimeout(kFile), 3.0);
  EXPECT_DOUBLE_EQ(session.GetEntryTimeout(kDirectory), 4.0);
  EXPECT_EQ(session.GetMaxNameLength(), 5u);
}

TEST_F(ClientSessionLifecycleTest,
       RuntimeOptionsAffectGettersAndPathValidation) {
  InitializeMetrics();
  FLAGS_fuse_attr_cache_timeout_s = 3;
  FLAGS_fuse_entry_cache_timeout_s = 4;
  FLAGS_vfs_meta_max_name_length = 4;

  EXPECT_DOUBLE_EQ(session_->GetAttrTimeout(kFile), 3.0);
  EXPECT_DOUBLE_EQ(session_->GetEntryTimeout(kDirectory), 4.0);
  EXPECT_EQ(session_->GetMaxNameLength(), 4u);

  EXPECT_CALL(*core_, Stop(false)).WillOnce(Return(Status::OK()));
  EXPECT_CALL(*core_, Lookup).Times(0);
  Attr attr;
  Status status =
      session_->Lookup(Context{0, 0, 0, 0}, kRootIno, "12345", &attr);

  EXPECT_TRUE(status.IsNameTooLong());
  EXPECT_TRUE(session_->Stop(false).ok());
}

TEST_F(ClientSessionLifecycleTest, StoppedSessionRejectsBeforeCoreAccess) {
  EXPECT_CALL(*core_, Stop(false)).WillOnce(Return(Status::OK()));
  ASSERT_TRUE(session_->Stop(false).ok());

  EXPECT_CALL(*core_, GetInfo(_)).Times(0);
  std::string info;
  Status status = session_->GetInfo(&info);
  EXPECT_TRUE(status.IsStop());
  EXPECT_EQ(status.ToSysErrNo(), EIO);

  EXPECT_EQ(session_->GetMaxNameLength(), 255u);
  EXPECT_DOUBLE_EQ(session_->GetAttrTimeout(kFile), 1.0);
  EXPECT_DOUBLE_EQ(session_->GetEntryTimeout(kDirectory), 2.0);
}

TEST_F(ClientSessionLifecycleTest, QuiescingRejectsBeforeCoreAccess) {
  Pause teardown;
  const auto rejected = RejectedOperations();
  EXPECT_CALL(*core_, Stop(false)).WillOnce(Invoke([&](bool) {
    teardown.Block();
    return Status::OK();
  }));
  EXPECT_CALL(*core_, GetInfo(_)).Times(0);
  auto stop = std::async(std::launch::async, [&] { return session_->Stop(); });
  auto join = MakeScopedCleanup([&] {
    teardown.Release();
    if (stop.valid()) stop.wait();
  });
  ASSERT_TRUE(teardown.entered.Wait());
  std::string info;
  const auto status = session_->GetInfo(&info);
  EXPECT_TRUE(status.IsStop());
  EXPECT_EQ(status.ToSysErrNo(), EIO);
  EXPECT_EQ(RejectedOperations(), rejected + 1);
  teardown.Release();
  EXPECT_TRUE(stop.get().ok());
}

TEST_F(ClientSessionLifecycleTest, StartFailureCleansUpAndNeverOpensAdmission) {
  ResetCreatedSession(true);
  SetTraceStarted(true);
  EXPECT_CALL(*core_, Start(_)).Times(0);
  EXPECT_CALL(*core_, GetInfo(_)).Times(0);
  EXPECT_CALL(*core_, Stop(false))
      .WillOnce(Return(Status::Internal("cleanup failed")));
  const auto rejected = RejectedOperations();
  const auto start = session_->Start(DingofsConfig{});
  EXPECT_TRUE(start.IsInvalidParam());
  EXPECT_FALSE(TraceStarted());
  std::string info;
  EXPECT_TRUE(session_->GetInfo(&info).IsStop());
  EXPECT_EQ(RejectedOperations(), rejected + 1);
  EXPECT_EQ(session_->Stop().ToString(), start.ToString());
  EXPECT_EQ(session_->Stop(true).ToString(), start.ToString());
  EXPECT_TRUE(session_->Start(DingofsConfig{}).IsInvalidParam());
}

TEST_F(ClientSessionLifecycleTest, CreatedStopPreventsAnyLaterStart) {
  ResetCreatedSession();
  const auto rejected = RejectedOperations();
  std::string info;
  EXPECT_TRUE(session_->GetInfo(&info).IsStop());
  EXPECT_TRUE(session_->Stop().ok());
  DingofsConfig config;
  config.fs_name = "must-not-start";
  config.mount_point = "/unused";
  config.metasystem_type = "memory";
  EXPECT_TRUE(session_->Start(config).IsInvalidParam());
  EXPECT_TRUE(session_->GetInfo(&info).IsStop());
  EXPECT_TRUE(session_->Stop(true).ok());
  EXPECT_EQ(RejectedOperations(), rejected + 2);
}

TEST_F(ClientSessionLifecycleTest, ConcurrentStopSharesStartFailure) {
  ResetCreatedSession(true);
  Pause cleanup;
  std::atomic<bool> cleanup_completed{false};
  EXPECT_CALL(*core_, GetInfo(_)).Times(0);
  EXPECT_CALL(*core_, Stop(false)).WillOnce(Invoke([&](bool) {
    cleanup.Block();
    cleanup_completed = true;
    return Status::Internal("cleanup failed");
  }));
  auto start = std::async(std::launch::async,
                          [&] { return session_->Start(DingofsConfig{}); });
  auto join = MakeScopedCleanup([&] {
    cleanup.Release();
    if (start.valid()) start.wait();
  });
  ASSERT_TRUE(cleanup.entered.Wait());
  Event stop_called;
  auto stop = std::async(std::launch::async, [&] {
    stop_called.Signal();
    auto status = session_->Stop();
    EXPECT_TRUE(cleanup_completed.load());
    return status;
  });
  auto release_stop = MakeScopedCleanup([&] { cleanup.Release(); });
  ASSERT_TRUE(stop_called.Wait());
  std::string info;
  EXPECT_TRUE(session_->GetInfo(&info).IsStop());
  cleanup.Release();
  const auto result = start.get();
  EXPECT_TRUE(result.IsInvalidParam());
  EXPECT_EQ(stop.get().ToString(), result.ToString());
  EXPECT_EQ(session_->Stop().ToString(), result.ToString());
}

TEST_F(ClientSessionLifecycleTest, RepeatedStopPreservesOriginalFailure) {
  const auto failure = Status::Internal("core shutdown failed");
  EXPECT_CALL(*core_, Stop(false)).WillOnce(Return(failure));
  EXPECT_CALL(*core_, Dump(_, _)).Times(0);
  EXPECT_EQ(session_->Stop().ToString(), failure.ToString());
  EXPECT_EQ(session_->Stop().ToString(), failure.ToString());
  // Once Stopped, the existing contract returns the recorded result even if
  // the caller changes mode; only an overlapping conflicting Stop is invalid.
  EXPECT_EQ(session_->Stop(true).ToString(), failure.ToString());
  std::string info;
  EXPECT_TRUE(session_->GetInfo(&info).IsStop());
}

TEST_F(ClientSessionLifecycleTest, ConcurrentSameModeStopsShareCoreFailure) {
  Pause teardown;
  std::atomic<bool> teardown_completed{false};
  const auto failure = Status::Internal("core shutdown failed");
  EXPECT_CALL(*core_, Stop(false)).WillOnce(Invoke([&](bool) {
    teardown.Block();
    teardown_completed = true;
    return failure;
  }));
  auto first = std::async(std::launch::async, [&] { return session_->Stop(); });
  auto join = MakeScopedCleanup([&] {
    teardown.Release();
    if (first.valid()) first.wait();
  });
  ASSERT_TRUE(teardown.entered.Wait());
  Event second_called;
  auto second = std::async(std::launch::async, [&] {
    second_called.Signal();
    auto status = session_->Stop();
    EXPECT_TRUE(teardown_completed.load());
    return status;
  });
  auto release_second = MakeScopedCleanup([&] { teardown.Release(); });
  ASSERT_TRUE(second_called.Wait());
  teardown.Release();
  EXPECT_EQ(first.get().ToString(), failure.ToString());
  EXPECT_EQ(second.get().ToString(), failure.ToString());
}

TEST_F(ClientSessionLifecycleTest, HandoverFailureSkipsDumpAndStopsTrace) {
  SetTraceStarted(true);
  const auto failure = Status::Internal("handover stop failed");
  EXPECT_CALL(*core_, Stop(true)).WillOnce(Invoke([&](bool) {
    EXPECT_TRUE(TraceStarted());
    return failure;
  }));
  EXPECT_CALL(*core_, Dump(_, _)).Times(0);
  EXPECT_EQ(session_->Stop(true).ToString(), failure.ToString());
  EXPECT_FALSE(TraceStarted());
  EXPECT_EQ(session_->Stop(true).ToString(), failure.ToString());
}

TEST_F(ClientSessionLifecycleTest, ConcurrentHandoverStopsShareDumpFailure) {
  Pause dump;
  std::atomic<bool> dump_completed{false};
  SetTraceStarted(true);
  {
    ::testing::InSequence order;
    EXPECT_CALL(*core_, Stop(true)).WillOnce(Return(Status::OK()));
    EXPECT_CALL(*core_, Dump(_, _))
        .WillOnce(Invoke([&](ContextSPtr, Json::Value&) {
          EXPECT_TRUE(TraceStarted());
          dump.Block();
          dump_completed = true;
          return false;
        }));
  }
  auto first =
      std::async(std::launch::async, [&] { return session_->Stop(true); });
  auto join = MakeScopedCleanup([&] {
    dump.Release();
    if (first.valid()) first.wait();
  });
  ASSERT_TRUE(dump.entered.Wait());
  Event second_called;
  auto second = std::async(std::launch::async, [&] {
    second_called.Signal();
    auto status = session_->Stop(true);
    EXPECT_TRUE(dump_completed.load());
    return status;
  });
  auto release_second = MakeScopedCleanup([&] { dump.Release(); });
  ASSERT_TRUE(second_called.Wait());
  std::string info;
  EXPECT_TRUE(session_->GetInfo(&info).IsStop());
  dump.Release();
  const auto result = first.get();
  EXPECT_TRUE(result.IsInvalidParam());
  EXPECT_EQ(second.get().ToString(), result.ToString());
  EXPECT_EQ(session_->Stop(true).ToString(), result.ToString());
  EXPECT_FALSE(TraceStarted());
}

TEST_F(ClientSessionLifecycleTest, MovedFromLeaseDoesNotChangeActiveOwnership) {
  const auto active = ActiveOperations();
  const auto rejected = RejectedOperations();
  auto source = AcquireOperation();
  ASSERT_TRUE(source.has_value());
  EXPECT_EQ(ActiveOperations(), active + 1);
  auto destination = std::move(source);
  source.reset();
  EXPECT_EQ(ActiveOperations(), active + 1);
  EXPECT_EQ(RejectedOperations(), rejected);

  EXPECT_CALL(*core_, GetInfo(_)).WillRepeatedly(Return(Status::OK()));
  EXPECT_CALL(*core_, Stop(false)).WillOnce(Invoke([&](bool) {
    EXPECT_EQ(ActiveOperations(), active);
    return Status::OK();
  }));
  auto stop = std::async(std::launch::async, [&] { return session_->Stop(); });
  auto release_lease = MakeScopedCleanup([&] { destination.reset(); });
  ASSERT_TRUE(WaitForAdmissionClosed());
  EXPECT_EQ(RejectedOperations(), rejected + 1);
  EXPECT_EQ(ActiveOperations(), active + 1);
  destination.reset();
  EXPECT_TRUE(stop.get().ok());
  EXPECT_EQ(ActiveOperations(), active);
}

TEST_F(ClientSessionLifecycleTest, StopWaitsForUnpublishedRealHandle) {
  auto& resources = UseRealVFS();
  Pause before_publish;
  uint64_t internal_fh = 0;
  EXPECT_CALL(resources.Meta(), Open(_, 42, O_RDONLY, _, _))
      .WillOnce(Invoke([&](ContextSPtr, Ino, int, uint64_t fh, bool*) {
        internal_fh = fh;
        return Status::OK();
      }));
  EXPECT_CALL(resources.Hub(), GetReaderRegistry())
      .WillOnce(Invoke([&] {
        before_publish.Block();
        return resources.Readers();
      }))
      .RetiresOnSaturation();
  std::atomic<bool> stopped{false};
  EXPECT_CALL(resources.Hub(), Stop(false)).WillOnce(Invoke([&](bool) {
    EXPECT_TRUE(resources.Handles().FindHandlerForRelease(internal_fh));
    EXPECT_EQ(resources.Readers()->Size(), 1u);
    stopped = true;
    resources.StopResources();
    EXPECT_EQ(resources.Readers()->Size(), 0u);
    return Status::OK();
  }));

  uint64_t fh = 0;
  auto operation = std::async(std::launch::async, [&] {
    bool keep_cache = false;
    return session_->Open(Context{0, 0, 0, 0}, 42, O_RDONLY, &fh, &keep_cache);
  });
  auto join = MakeScopedCleanup([&] {
    before_publish.Release();
    if (operation.valid()) operation.wait();
  });
  ASSERT_TRUE(before_publish.entered.Wait());
  EXPECT_FALSE(resources.Handles().FindHandlerForRelease(internal_fh));
  auto stop = std::async(std::launch::async, [&] { return session_->Stop(); });
  auto release_stop = MakeScopedCleanup([&] { before_publish.Release(); });
  ASSERT_TRUE(WaitForAdmissionClosed());
  EXPECT_FALSE(stopped.load());
  before_publish.Release();
  EXPECT_TRUE(operation.get().ok());
  EXPECT_TRUE(stop.get().ok());
  EXPECT_EQ(fh, internal_fh);
  // Stop detaches resources, but preserves the published fh identity.
  EXPECT_TRUE(resources.Handles().FindHandlerForRelease(fh));
}

TEST_F(ClientSessionLifecycleTest, StopWaitsAfterRealHandleErase) {
  auto& resources = UseRealVFS();
  uint64_t fh = 0;
  bool keep_cache = false;
  ASSERT_TRUE(
      session_->Open(Context{0, 0, 0, 0}, 42, O_RDONLY, &fh, &keep_cache).ok());
  ASSERT_TRUE(resources.Handles().FindHandlerForRelease(fh));
  Pause after_erase;
  EXPECT_CALL(resources.Hub(), GetReaderRegistry()).WillOnce(Invoke([&] {
    after_erase.Block();
    return resources.Readers();
  }));
  std::atomic<bool> stopped{false};
  EXPECT_CALL(resources.Hub(), Stop(false)).WillOnce(Invoke([&](bool) {
    EXPECT_EQ(resources.Readers()->Size(), 0u);
    stopped = true;
    resources.StopResources();
    return Status::OK();
  }));

  auto operation = std::async(std::launch::async, [&] {
    return session_->Release(Context{0, 0, 0, 0}, 42, fh);
  });
  auto join = MakeScopedCleanup([&] {
    after_erase.Release();
    if (operation.valid()) operation.wait();
  });
  ASSERT_TRUE(after_erase.entered.Wait());
  EXPECT_FALSE(resources.Handles().FindHandlerForRelease(fh));
  auto stop = std::async(std::launch::async, [&] { return session_->Stop(); });
  auto release_stop = MakeScopedCleanup([&] { after_erase.Release(); });
  ASSERT_TRUE(WaitForAdmissionClosed());
  EXPECT_FALSE(stopped.load());
  after_erase.Release();
  EXPECT_TRUE(operation.get().ok());
  EXPECT_TRUE(stop.get().ok());
}

TEST_F(ClientSessionLifecycleTest, StopWaitsForRealNoHandleMetadataPath) {
  auto& resources = UseRealVFS();
  Pause metadata;
  std::atomic<bool> stopped{false};
  std::atomic<bool> metadata_completed{false};
  const auto expected_attr = vfs::test::MakeFileAttr(42);
  EXPECT_CALL(resources.Meta(), GetAttr(_, 42, _))
      .WillOnce(Invoke([&](ContextSPtr, Ino, Attr* attr) {
        metadata.Block();
        EXPECT_FALSE(stopped.load());
        *attr = expected_attr;
        metadata_completed = true;
        return Status::OK();
      }));
  EXPECT_CALL(resources.Hub(), Stop(false)).WillOnce(Invoke([&](bool) {
    EXPECT_TRUE(metadata_completed.load());
    stopped = true;
    resources.StopResources();
    return Status::OK();
  }));
  Attr attr;
  auto operation = std::async(std::launch::async, [&] {
    return session_->GetAttr(Context{0, 0, 0, 0}, 42, &attr);
  });
  auto join = MakeScopedCleanup([&] {
    metadata.Release();
    if (operation.valid()) operation.wait();
  });
  ASSERT_TRUE(metadata.entered.Wait());
  auto stop = std::async(std::launch::async, [&] { return session_->Stop(); });
  auto release_stop = MakeScopedCleanup([&] { metadata.Release(); });
  ASSERT_TRUE(WaitForAdmissionClosed());
  EXPECT_FALSE(stopped.load());
  metadata.Release();
  EXPECT_TRUE(operation.get().ok());
  EXPECT_EQ(attr.ino, expected_attr.ino);
  EXPECT_TRUE(stop.get().ok());
}

TEST_F(ClientSessionLifecycleTest, AccessLogDestructorRemainsInsideLease) {
  InitializeMetrics();
  auto sink = std::make_shared<PausingLogSink>();
  auto previous_logger = logger;
  logger = std::make_shared<spdlog::logger>("session-tail", sink);
  auto restore_logger =
      MakeScopedCleanup([&] { logger = std::move(previous_logger); });
  FLAGS_vfs_access_logging = true;
  FLAGS_vfs_access_log_threshold_us = 0;
  EXPECT_CALL(*core_, GetInfo(_)).WillRepeatedly(Return(Status::OK()));
  const auto active = ActiveOperations();
  std::atomic<bool> business_returned{false};
  std::atomic<bool> stopped{false};
  const auto business_status = Status::Internal("metadata failed");
  EXPECT_CALL(*core_, GetAttr(_, 42, _))
      .WillOnce(Invoke([&](ContextSPtr, Ino, Attr* attr) {
        *attr = vfs::test::MakeFileAttr(42);
        business_returned = true;
        return business_status;
      }));
  EXPECT_CALL(*core_, Stop(false)).WillOnce(Invoke([&](bool) {
    EXPECT_TRUE(sink->completed.load());
    stopped = true;
    return Status::OK();
  }));
  auto operation = std::async(std::launch::async, [&] {
    Attr attr;
    return session_->GetAttr(Context{0, 0, 0, 0}, 42, &attr);
  });
  auto join = MakeScopedCleanup([&] {
    sink->pause.Release();
    if (operation.valid()) operation.wait();
  });
  ASSERT_TRUE(sink->pause.entered.Wait());
  EXPECT_TRUE(business_returned.load());
  EXPECT_EQ(ActiveOperations(), active + 1);
  auto stop = std::async(std::launch::async, [&] { return session_->Stop(); });
  auto release_stop = MakeScopedCleanup([&] { sink->pause.Release(); });
  ASSERT_TRUE(WaitForAdmissionClosed());
  EXPECT_FALSE(stopped.load());
  sink->pause.Release();
  EXPECT_EQ(operation.get().ToString(), business_status.ToString());
  EXPECT_TRUE(stop.get().ok());
  EXPECT_EQ(ActiveOperations(), active);
}

TEST_F(ClientSessionLifecycleTest, SuccessfulLocalStartOpensAdmission) {
  ResetCreatedSession();
  const auto previous_style = ::testing::FLAGS_gtest_death_test_style;
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  auto restore = MakeScopedCleanup(
      [&] { ::testing::FLAGS_gtest_death_test_style = previous_style; });
  // Start registers process-global loggers; reexec keeps this independent of
  // test ordering, repeats, and other fixtures' logger ownership.
  ASSERT_EXIT(
      {
        RunLocalStart();
        std::_Exit(::testing::Test::HasFailure() ? 1 : 0);
      },
      ::testing::ExitedWithCode(0), "");
}

}  // namespace client
}  // namespace dingofs
