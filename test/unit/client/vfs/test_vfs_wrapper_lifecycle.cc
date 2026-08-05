/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <thread>

#include "client/vfs/vfs_wrapper.h"
#include "common/options/client.h"

namespace dingofs {
namespace client {

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

namespace {

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
  MOCK_METHOD(uint64_t, GetFsId, (), (override));
  MOCK_METHOD(double, GetAttrTimeout, (const FileType&), (override));
  MOCK_METHOD(double, GetEntryTimeout, (const FileType&), (override));
  MOCK_METHOD(uint64_t, GetMaxNameLength, (), (override));
  MOCK_METHOD(TraceManager*, GetTraceManager, (), (override));
  MOCK_METHOD(Status, GetInfo, (std::string*), (override));
};

}  // namespace

class VFSWrapperLifecycleTest : public ::testing::Test {
 protected:
  void SetUp() override {
    previous_access_logging_ = FLAGS_vfs_access_logging;
    FLAGS_vfs_access_logging = false;
    auto core = std::make_unique<MockLifecycleVFS>();
    core_ = core.get();
    wrapper_.vfs_ = std::move(core);
    wrapper_.lifecycle_state_ = VFSWrapper::LifecycleState::kRunning;
    wrapper_.fs_id_ = 10;
    wrapper_.max_name_length_ = 255;
    wrapper_.attr_timeout_ = 1.0;
    wrapper_.entry_timeout_ = 2.0;
    wrapper_.immutable_values_published_.store(true, std::memory_order_release);
  }

  void TearDown() override {
    FLAGS_vfs_access_logging = previous_access_logging_;
  }

  VFSWrapper wrapper_;
  MockLifecycleVFS* core_{nullptr};
  bool previous_access_logging_{true};
};

TEST_F(VFSWrapperLifecycleTest, StopWaitsForAdmittedOperation) {
  std::promise<void> entered;
  std::promise<void> release;
  auto release_future = release.get_future();
  std::atomic<bool> core_stopped{false};

  EXPECT_CALL(*core_, GetInfo(_)).WillOnce(Invoke([&](std::string*) {
    entered.set_value();
    release_future.wait();
    return Status::OK();
  }));
  EXPECT_CALL(*core_, Stop(false)).WillOnce(Invoke([&](bool) {
    core_stopped.store(true);
    return Status::OK();
  }));

  std::thread operation([&] {
    std::string info;
    EXPECT_TRUE(wrapper_.GetInfo(&info).ok());
  });
  ASSERT_EQ(entered.get_future().wait_for(std::chrono::seconds(5)),
            std::future_status::ready);

  auto stop_future =
      std::async(std::launch::async, [&] { return wrapper_.Stop(false); });
  EXPECT_EQ(stop_future.wait_for(std::chrono::milliseconds(100)),
            std::future_status::timeout);
  EXPECT_FALSE(core_stopped.load());

  release.set_value();
  operation.join();
  EXPECT_TRUE(stop_future.get().ok());
  EXPECT_TRUE(core_stopped.load());
}

TEST_F(VFSWrapperLifecycleTest, ConcurrentStopRunsCoreStopOnce) {
  std::promise<void> stop_entered;
  std::promise<void> release_stop;
  auto release_future = release_stop.get_future();

  EXPECT_CALL(*core_, Stop(false)).WillOnce(Invoke([&](bool) {
    stop_entered.set_value();
    release_future.wait();
    return Status::OK();
  }));

  auto first =
      std::async(std::launch::async, [&] { return wrapper_.Stop(false); });
  ASSERT_EQ(stop_entered.get_future().wait_for(std::chrono::seconds(5)),
            std::future_status::ready);
  auto second =
      std::async(std::launch::async, [&] { return wrapper_.Stop(false); });

  EXPECT_EQ(second.wait_for(std::chrono::milliseconds(100)),
            std::future_status::timeout);
  release_stop.set_value();
  EXPECT_TRUE(first.get().ok());
  EXPECT_TRUE(second.get().ok());
}

TEST_F(VFSWrapperLifecycleTest, StoppedSessionRejectsBeforeCoreAccess) {
  EXPECT_CALL(*core_, Stop(false)).WillOnce(Return(Status::OK()));
  ASSERT_TRUE(wrapper_.Stop(false).ok());

  EXPECT_CALL(*core_, GetInfo(_)).Times(0);
  std::string info;
  Status status = wrapper_.GetInfo(&info);
  EXPECT_TRUE(status.IsStop());
  EXPECT_EQ(status.ToSysErrNo(), EIO);

  EXPECT_EQ(wrapper_.GetFsId(), 10u);
  EXPECT_EQ(wrapper_.GetMaxNameLength(), 255u);
  EXPECT_DOUBLE_EQ(wrapper_.GetAttrTimeout(kFile), 1.0);
  EXPECT_DOUBLE_EQ(wrapper_.GetEntryTimeout(kDirectory), 2.0);
}

}  // namespace client
}  // namespace dingofs
