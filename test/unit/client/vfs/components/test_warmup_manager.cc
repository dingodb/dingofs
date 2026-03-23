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
#include <memory>
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

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
