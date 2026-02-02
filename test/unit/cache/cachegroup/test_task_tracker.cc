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

/*
 * Project: DingoFS
 * Created Date: 2026-02-02
 * Author: AI
 */

#include <gtest/gtest.h>

#include <memory>
#include <sstream>

#include "cache/cachegroup/task_tracker.h"

namespace dingofs {
namespace cache {

class TaskTrackerTest : public ::testing::Test {
 protected:
  void SetUp() override { tracker_ = std::make_unique<TaskTracker>(); }

  std::unique_ptr<TaskTracker> tracker_;
};

TEST_F(TaskTrackerTest, DownloadTaskAttr) {
  auto ctx = NewContext();
  BlockKey key(1, 2, 3, 4, 5);
  size_t length = 1024;

  auto task = std::make_shared<DownloadTask>(ctx, key, length);
  EXPECT_EQ(task->Attr().key.fs_id, 1);
  EXPECT_EQ(task->Attr().key.ino, 2);
  EXPECT_EQ(task->Attr().key.id, 3);
  EXPECT_EQ(task->Attr().key.index, 4);
  EXPECT_EQ(task->Attr().key.version, 5);
  EXPECT_EQ(task->Attr().length, 1024);
}

TEST_F(TaskTrackerTest, DownloadTaskResult) {
  auto ctx = NewContext();
  BlockKey key(1, 2, 3, 4, 5);

  auto task = std::make_shared<DownloadTask>(ctx, key, 1024);
  task->Result().status = Status::OK();
  EXPECT_TRUE(task->Result().status.ok());

  task->Result().status = Status::NotFound("not found");
  EXPECT_TRUE(task->Result().status.IsNotFound());
}

TEST_F(TaskTrackerTest, DownloadTaskRunAndWait) {
  auto ctx = NewContext();
  BlockKey key(1, 2, 3, 4, 5);

  auto task = std::make_shared<DownloadTask>(ctx, key, 1024);

  // Wait with timeout should return false if not run
  EXPECT_FALSE(task->Wait(1));

  // Run the task
  task->Run();

  // Wait should return true after run
  EXPECT_TRUE(task->Wait(1));
}

TEST_F(TaskTrackerTest, GetOrCreateTaskNew) {
  auto ctx = NewContext();
  BlockKey key(1, 2, 3, 4, 5);
  DownloadTaskSPtr task;

  bool created = tracker_->GetOrCreateTask(ctx, key, 1024, task);
  EXPECT_TRUE(created);
  EXPECT_NE(task, nullptr);
  EXPECT_EQ(task->Attr().key.fs_id, 1);
}

TEST_F(TaskTrackerTest, GetOrCreateTaskExisting) {
  auto ctx = NewContext();
  BlockKey key(1, 2, 3, 4, 5);
  DownloadTaskSPtr task1, task2;

  bool created1 = tracker_->GetOrCreateTask(ctx, key, 1024, task1);
  EXPECT_TRUE(created1);

  bool created2 = tracker_->GetOrCreateTask(ctx, key, 1024, task2);
  EXPECT_FALSE(created2);

  // Should return the same task
  EXPECT_EQ(task1, task2);
}

TEST_F(TaskTrackerTest, RemoveTask) {
  auto ctx = NewContext();
  BlockKey key(1, 2, 3, 4, 5);
  DownloadTaskSPtr task1, task2;

  tracker_->GetOrCreateTask(ctx, key, 1024, task1);
  tracker_->RemoveTask(key);

  // After removal, should create a new task
  bool created = tracker_->GetOrCreateTask(ctx, key, 1024, task2);
  EXPECT_TRUE(created);
  EXPECT_NE(task1, task2);
}

TEST_F(TaskTrackerTest, DownloadTaskStreamOperator) {
  auto ctx = NewContext();
  BlockKey key(1, 2, 3, 4, 5);
  auto task = std::make_shared<DownloadTask>(ctx, key, 1024);

  std::ostringstream oss;
  oss << task;

  EXPECT_NE(oss.str().find("DownloadTask"), std::string::npos);
  EXPECT_NE(oss.str().find("1_2_3_4_5"), std::string::npos);
  EXPECT_NE(oss.str().find("1024"), std::string::npos);
}

}  // namespace cache
}  // namespace dingofs
