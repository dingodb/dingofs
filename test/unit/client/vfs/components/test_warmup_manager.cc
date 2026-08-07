/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include <thread>

#include "client/vfs/components/context.h"
#include "client/vfs/components/warmup_manager.h"
#include "common/status.h"
#include "common/trace/trace_manager.h"
#include "test/unit/client/vfs/test_base.h"
#include "test/unit/client/vfs/test_common.h"

namespace dingofs {
namespace client {
namespace vfs {

using ::testing::AnyNumber;
using ::testing::Return;

class WarmupManagerTest : public test::VFSTestBase {
 protected:
  void SetUp() override {
    trace_manager_ = std::make_unique<TraceManager>();
    ON_CALL(*mock_hub_, GetTraceManager())
        .WillByDefault(Return(trace_manager_.get()));
    EXPECT_CALL(*mock_hub_, GetTraceManager()).Times(AnyNumber());

    mgr_ = std::make_unique<WarmupManager>(mock_hub_);
  }

  void TearDown() override {
    if (mgr_) {
      mgr_->Stop();
    }
  }

  std::unique_ptr<WarmupManager> mgr_;
  std::unique_ptr<TraceManager> trace_manager_;
};

TEST_F(WarmupManagerTest, Start_Stop_NoCrash) {
  ASSERT_TRUE(mgr_->Start(2).ok());
  ASSERT_TRUE(mgr_->Stop().ok());
}

TEST_F(WarmupManagerTest, SubmitTask_TaskCreated) {
  ASSERT_TRUE(mgr_->Start(2).ok());

  constexpr Ino kTaskKey = 1001;
  WarmupTaskContext ctx(kTaskKey);
  mgr_->SubmitTask(ctx);

  // Give the execution queue time to pick up the task.
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Either the task is still running (returns "total/finish/errors")
  // or it has already completed and been removed (returns "0/0/0").
  // Either way, GetWarmupTaskStatus must not crash.
  std::string status = mgr_->GetWarmupTaskStatus(kTaskKey);
  EXPECT_FALSE(status.empty());
}

TEST_F(WarmupManagerTest, DuplicateTask_Rejected) {
  ASSERT_TRUE(mgr_->Start(2).ok());

  constexpr Ino kTaskKey = 2001;
  WarmupTaskContext ctx(kTaskKey);

  // First submit - should be accepted.
  mgr_->SubmitTask(ctx);

  // Give queue a moment to register the first task.
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  // Second submit with same key - should be silently rejected.
  mgr_->SubmitTask(ctx);

  // Let tasks process.
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // No crash and the manager is still operational.
  std::string status = mgr_->GetWarmupTaskStatus(kTaskKey);
  EXPECT_FALSE(status.empty());
}

TEST_F(WarmupManagerTest, Stop_ClearsAllTasks) {
  ASSERT_TRUE(mgr_->Start(2).ok());

  // Submit a few tasks.
  for (Ino ino = 3000; ino < 3005; ++ino) {
    mgr_->SubmitTask(WarmupTaskContext(ino));
  }

  // Stop should drain and clear all tasks without hanging.
  ASSERT_TRUE(mgr_->Stop().ok());
}

TEST_F(WarmupManagerTest, StopWaitsForInflightPrefetchCallback) {
  constexpr Ino kTaskKey = 4001;
  std::promise<void> prefetch_started;
  StatusCallback pending_callback;
  std::mutex callback_mutex;

  ON_CALL(*mock_meta_system_, GetAttr)
      .WillByDefault([](ContextSPtr, Ino ino, Attr* attr) {
        *attr = test::MakeFileAttr(ino, 4096);
        return Status::OK();
      });
  ON_CALL(*mock_meta_system_, ReadSlice)
      .WillByDefault([](ContextSPtr, Ino, uint64_t, uint64_t,
                        std::vector<Slice>* slices, uint64_t& version) {
        *slices = {test::MakeSlice(1, 0, 4096)};
        version = 1;
        return Status::OK();
      });
  ON_CALL(*mock_block_store_, PrefetchAsync)
      .WillByDefault([&](ContextSPtr, PrefetchReq, StatusCallback cb) {
        {
          std::lock_guard<std::mutex> lock(callback_mutex);
          pending_callback = std::move(cb);
        }
        prefetch_started.set_value();
      });

  ASSERT_TRUE(mgr_->Start(2).ok());
  ASSERT_TRUE(mgr_->SubmitTask(WarmupTaskContext(kTaskKey)).ok());
  ASSERT_EQ(prefetch_started.get_future().wait_for(std::chrono::seconds(5)),
            std::future_status::ready);

  auto stop = std::async(std::launch::async, [this] { return mgr_->Stop(); });
  EXPECT_EQ(stop.wait_for(std::chrono::milliseconds(50)),
            std::future_status::timeout);

  StatusCallback callback;
  {
    std::lock_guard<std::mutex> lock(callback_mutex);
    callback = std::move(pending_callback);
  }
  ASSERT_TRUE(static_cast<bool>(callback));
  callback(Status::OK());

  ASSERT_EQ(stop.wait_for(std::chrono::seconds(5)), std::future_status::ready);
  EXPECT_TRUE(stop.get().ok());
}

TEST_F(WarmupManagerTest, SubmitRejectedAfterStop) {
  ASSERT_TRUE(mgr_->Start(2).ok());
  ASSERT_TRUE(mgr_->Stop().ok());
  EXPECT_TRUE(mgr_->SubmitTask(WarmupTaskContext(5001)).IsStop());
}

TEST_F(WarmupManagerTest, FinishedTaskKeyIsDeduplicated) {
  constexpr Ino kTaskKey = 6001;
  EXPECT_CALL(*mock_meta_system_, GetAttr(testing::_, kTaskKey, testing::_))
      .Times(1)
      .WillOnce([](ContextSPtr, Ino ino, Attr* attr) {
        *attr = test::MakeFileAttr(ino, 4096);
        return Status::OK();
      });
  ON_CALL(*mock_meta_system_, ReadSlice)
      .WillByDefault([](ContextSPtr, Ino, uint64_t, uint64_t,
                        std::vector<Slice>* slices, uint64_t& version) {
        *slices = {test::MakeSlice(1, 0, 4096)};
        version = 1;
        return Status::OK();
      });

  ASSERT_TRUE(mgr_->Start(2).ok());
  ASSERT_TRUE(mgr_->SubmitTask(WarmupTaskContext(kTaskKey)).ok());

  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (mgr_->GetWarmupTaskStatus(kTaskKey) != "1/1/0" &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  ASSERT_EQ(mgr_->GetWarmupTaskStatus(kTaskKey), "1/1/0");

  // The retained finished key must reject a duplicate without resolving the
  // inode a second time. SubmitTask reports enqueue success; dedup is decided
  // by the single-writer event handler.
  ASSERT_TRUE(mgr_->SubmitTask(WarmupTaskContext(kTaskKey)).ok());
  ASSERT_TRUE(mgr_->Stop().ok());
}

TEST_F(WarmupManagerTest, BlockCreditLimitsAndResumesDispatch) {
  constexpr Ino kTaskKey = 7001;
  const uint64_t previous_max_inflight =
      FLAGS_vfs_warmup_max_inflight_blocks;
  struct FlagRestore {
    uint64_t value;
    ~FlagRestore() { FLAGS_vfs_warmup_max_inflight_blocks = value; }
  } restore{previous_max_inflight};
  FLAGS_vfs_warmup_max_inflight_blocks = 2;

  ON_CALL(*mock_meta_system_, GetAttr)
      .WillByDefault([](ContextSPtr, Ino ino, Attr* attr) {
        *attr = test::MakeFileAttr(ino, 12 * 1024 * 1024);
        return Status::OK();
      });
  ON_CALL(*mock_meta_system_, ReadSlice)
      .WillByDefault([](ContextSPtr, Ino, uint64_t, uint64_t,
                        std::vector<Slice>* slices, uint64_t& version) {
        *slices = {test::MakeSlice(1, 0, 12 * 1024 * 1024)};
        version = 1;
        return Status::OK();
      });

  std::mutex callbacks_mutex;
  std::condition_variable callbacks_cv;
  std::vector<StatusCallback> callbacks;
  ON_CALL(*mock_block_store_, PrefetchAsync)
      .WillByDefault([&](ContextSPtr, PrefetchReq, StatusCallback cb) {
        {
          std::lock_guard<std::mutex> lock(callbacks_mutex);
          callbacks.push_back(std::move(cb));
        }
        callbacks_cv.notify_all();
      });

  auto wait_for_callbacks = [&](size_t count,
                                std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(callbacks_mutex);
    return callbacks_cv.wait_for(
        lock, timeout, [&] { return callbacks.size() >= count; });
  };
  auto get_callback = [&](size_t index) {
    std::lock_guard<std::mutex> lock(callbacks_mutex);
    return callbacks.at(index);
  };

  ASSERT_TRUE(mgr_->Start(2).ok());
  ASSERT_TRUE(mgr_->SubmitTask(WarmupTaskContext(kTaskKey)).ok());
  ASSERT_TRUE(wait_for_callbacks(2, std::chrono::seconds(5)));
  EXPECT_FALSE(wait_for_callbacks(3, std::chrono::milliseconds(50)));

  get_callback(0)(Status::OK());
  ASSERT_TRUE(wait_for_callbacks(3, std::chrono::seconds(5)));
  get_callback(1)(Status::OK());
  get_callback(2)(Status::OK());

  ASSERT_TRUE(mgr_->Stop().ok());
  EXPECT_EQ(mgr_->GetWarmupTaskStatus(kTaskKey), "3/3/0");
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
